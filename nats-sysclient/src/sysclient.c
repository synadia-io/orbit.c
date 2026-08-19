// Copyright 2026 Synadia Communications Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "sysclientp.h"

#include "unmarshal.h"

#include "os_shims.h"
#include "requestmany.h"

#include <stdio.h>
#include <string.h>

static const sysField _serverInfoFields[] = {
    SYS_F(SYS_FLD_STR, natsSysServerInfo, Name, "name"),
    SYS_F(SYS_FLD_STR, natsSysServerInfo, Host, "host"),
    SYS_F(SYS_FLD_STR, natsSysServerInfo, ID, "id"),
    SYS_F(SYS_FLD_STR, natsSysServerInfo, Cluster, "cluster"),
    SYS_F(SYS_FLD_STR, natsSysServerInfo, Domain, "domain"),
    SYS_F(SYS_FLD_STR, natsSysServerInfo, Version, "ver"),
    SYS_FA(natsSysServerInfo, Tags, TagsCount, "tags"),
    SYS_F(SYS_FLD_U64, natsSysServerInfo, Seq, "seq"),
    SYS_F(SYS_FLD_BOOL, natsSysServerInfo, JetStream, "jetstream"),
    SYS_F(SYS_FLD_STR, natsSysServerInfo, Time, "time"),
};

static const sysField _apiErrorFields[] = {
    SYS_F(SYS_FLD_INT, natsSysAPIError, Code, "code"),
    SYS_F(SYS_FLD_U16, natsSysAPIError, ErrCode, "err_code"),
    SYS_F(SYS_FLD_STR, natsSysAPIError, Description, "description"),
};

natsStatus
natsSysClientOpts_Init(natsSysClientOpts *opts)
{
    if (opts == NULL)
        return NATS_INVALID_ARG;

    memset(opts, 0, sizeof(*opts));
    opts->StallInterval = NATS_SYS_DEFAULT_STALL;
    // -1 means "no limit".
    opts->ServerCount = -1;

    return NATS_OK;
}

natsStatus
natsSysClient_Create(natsSysClient **newClient, natsConnection *nc,
                     const natsSysClientOpts *opts)
{
    natsSysClient    *client;
    natsSysClientOpts defaults;

    if (newClient == NULL)
        return NATS_INVALID_ARG;

    // Cleared before anything else is judged: the headers promise the out-param
    // is NULL on *every* error, and a caller that destroys it unconditionally
    // would otherwise free an indeterminate pointer.
    *newClient = NULL;

    if (nc == NULL)
        return NATS_INVALID_ARG;

    if (opts == NULL)
    {
        natsSysClientOpts_Init(&defaults);
        opts = &defaults;
    }

    // -1 is the "unset" sentinel, so it is allowed alongside any positive
    // count.
    if ((opts->ServerCount < -1) || (opts->ServerCount == 0))
        return NATS_INVALID_ARG;
    // Rejected rather than treated as "no stall": orbit.go's StallTimer refuses
    // interval <= 0, and silently disabling the stall would make every gather
    // wait out the full request timeout.
    if (opts->StallInterval <= 0)
        return NATS_INVALID_ARG;

    client = (natsSysClient *) NATS_CALLOC(1, sizeof(natsSysClient));
    if (client == NULL)
        return NATS_NO_MEMORY;

    client->nc            = nc;
    client->serverCount   = opts->ServerCount;
    client->stallInterval = opts->StallInterval;

    *newClient = client;
    return NATS_OK;
}

void
natsSysClient_Destroy(natsSysClient *client)
{
    // The connection is borrowed; leave it alone.
    NATS_FREE(client);
}

// Formats one of the SYS_SUBJ_* templates with a server ID or "PING".
static natsStatus
_buildSubject(char **out, const char *fmt, const char *target)
{
    int   n;
    char *subj;

    n = snprintf(NULL, 0, fmt, target);
    if (n < 0)
        return NATS_ERR;

    subj = (char *) NATS_MALLOC((size_t) n + 1);
    if (subj == NULL)
        return NATS_NO_MEMORY;

    snprintf(subj, (size_t) n + 1, fmt, target);
    *out = subj;
    return NATS_OK;
}

natsStatus
sysclient_requestByID(natsMsg **replyMsg, natsSysClient *client, const char *serverID,
                      const char *subjFmt, const char *payload, int payloadLen,
                      int64_t timeout)
{
    natsStatus s;
    char      *subj = NULL;

    if ((replyMsg == NULL) || (client == NULL) || (subjFmt == NULL))
        return NATS_INVALID_ARG;
    if (nats_IsStringEmpty(serverID))
        return NATS_INVALID_ARG;
    if (timeout < 0)
        return NATS_INVALID_ARG;

    *replyMsg = NULL;

    if (timeout == 0)
        timeout = NATS_SYS_DEFAULT_REQUEST_TIMEOUT;
    timeout = sysclient_capTimeout(timeout);

    s = _buildSubject(&subj, subjFmt, serverID);
    if (s != NATS_OK)
        return s;

    s = natsConnection_Request(replyMsg, client->nc, subj, payload, payloadLen, timeout);

    // The server ID is part of the subject, so "nobody answered" means "no
    // such server". Note this is also what you get when the connection is not
    // on the system account at all; the two are indistinguishable.
    if (s == NATS_NO_RESPONDERS)
        s = NATS_NOT_FOUND;

    NATS_FREE(subj);
    return s;
}

natsStatus
sysclient_pingServers(natsMsgList *list, natsSysClient *client, const char *subjFmt,
                      const char *payload, int payloadLen, int64_t timeout)
{
    natsStatus          s;
    char               *subj = NULL;
    natsRequestManyOpts rmOpts;

    if ((list == NULL) || (client == NULL) || (subjFmt == NULL))
        return NATS_INVALID_ARG;
    if (timeout < 0)
        return NATS_INVALID_ARG;

    if (timeout == 0)
        timeout = NATS_SYS_DEFAULT_REQUEST_TIMEOUT;
    timeout = sysclient_capTimeout(timeout);

    s = _buildSubject(&subj, subjFmt, SYS_PING_TARGET);
    if (s != NATS_OK)
        return s;

    natsRequestManyOpts_Init(&rmOpts);
    rmOpts.Timeout = (uint64_t) timeout;
    if (client->stallInterval > 0)
        rmOpts.Stall = (uint64_t) client->stallInterval;
    if (client->serverCount > 0)
        rmOpts.Count = (uint64_t) client->serverCount;

    s = natsRequestMany_Request(list, client->nc, subj, payload, payloadLen, &rmOpts);

    // Reaching the deadline is how a scatter-gather normally ends: there is no
    // way to know how many servers should answer, so the gather returns
    // whatever arrived, which may be nothing at all. A gather that times out
    // empty is therefore a success with Count == 0.
    //
    // NATS_NO_RESPONDERS is deliberately NOT folded in here. It means nobody is
    // subscribed to the subject at all — typically the connection is not on the
    // system account — which is a real failure rather than an empty result.
    // Unlike the by-ID path there is no server ID to blame, so the status is
    // passed through as-is instead of being flattened to NATS_NOT_FOUND.
    if (s == NATS_TIMEOUT)
        s = NATS_OK;

    NATS_FREE(subj);
    return s;
}

natsStatus
sysclient_decodeResp(natsMsg *msg, natsSysServerInfo *server, natsSysAPIError *apiErr,
                     void *payloadDst, const char *payloadKey, sysParseFn parsePayload)
{
    natsStatus s;
    natsJSON  *root    = NULL;
    natsJSON  *payload = NULL;

    if ((msg == NULL) || (parsePayload == NULL))
        return NATS_INVALID_ARG;

    s = natsJSON_Parse(&root, natsMsg_GetData(msg), natsMsg_GetDataLength(msg));
    IFOK(s, sysclient_parseEnvelope(server, apiErr, &payload, root, payloadKey));

    // A server reporting an error sends no payload, so an absent key is not a
    // failure; the caller checks apiErr.
    if ((s == NATS_OK) && (payload != NULL))
        s = parsePayload(payloadDst, payload);

    natsJSON_Destroy(root);
    return sysclient_responseStatus(s);
}

natsStatus
sysclient_pingList(void ***resps, int *count, natsSysClient *client, const char *subjFmt,
                   const char *payload, int payloadLen, int64_t timeout,
                   sysRespFromMsgFn fromMsg, sysRespDestroyFn destroyResp)
{
    natsStatus  s;
    natsMsgList msgs = {NULL, 0};
    void      **arr;
    int         n = 0;
    int         i;

    if ((resps == NULL) || (count == NULL) || (fromMsg == NULL) || (destroyResp == NULL))
        return NATS_INVALID_ARG;

    *resps = NULL;
    *count = 0;

    s = sysclient_pingServers(&msgs, client, subjFmt, payload, payloadLen, timeout);
    if (s != NATS_OK)
    {
        natsMsgList_Destroy(&msgs);
        return s;
    }
    if (msgs.Count == 0)
    {
        natsMsgList_Destroy(&msgs);
        return NATS_OK;
    }

    arr = (void **) NATS_CALLOC((size_t) msgs.Count, sizeof(void *));
    if (arr == NULL)
    {
        natsMsgList_Destroy(&msgs);
        return NATS_NO_MEMORY;
    }

    for (i = 0; (i < msgs.Count) && (s == NATS_OK); i++)
    {
        s = fromMsg(&arr[n], msgs.Msgs[i]);
        if (s == NATS_OK)
            n++;
    }

    natsMsgList_Destroy(&msgs);

    // One bad response discards the whole batch rather than yielding a
    // partial one.
    if (s != NATS_OK)
    {
        for (i = 0; i < n; i++)
            destroyResp(arr[i]);
        NATS_FREE(arr);
        return s;
    }

    *resps = arr;
    *count = n;
    return NATS_OK;
}

natsStatus
sysclient_request(void **newResp, natsSysClient *client, const char *serverID,
                  const char *subjFmt, sysMarshalFn marshal, const void *opts,
                  int bufHint, int64_t timeout, sysRespFromMsgFn fromMsg)
{
    natsStatus s;
    natsBuffer buf   = NATS_EMPTY_BUFFER;
    natsMsg   *reply = NULL;

    if ((newResp == NULL) || (client == NULL) || (marshal == NULL) || (fromMsg == NULL))
        return NATS_INVALID_ARG;

    *newResp = NULL;

    s = natsBuf_Init(&buf, bufHint);
    IFOK(s, marshal(&buf, opts));
    if (s == NATS_OK)
        s = sysclient_requestByID(&reply, client, serverID, subjFmt, natsBuf_Data(&buf),
                                  natsBuf_Len(&buf), timeout);
    natsBuf_Destroy(&buf);
    if (s != NATS_OK)
        return s;

    s = fromMsg(newResp, reply);
    natsMsg_Destroy(reply);
    return s;
}

natsStatus
sysclient_ping(void ***resps, int *count, natsSysClient *client, const char *subjFmt,
               sysMarshalFn marshal, const void *opts, int bufHint, int64_t timeout,
               sysRespFromMsgFn fromMsg, sysRespDestroyFn destroyResp)
{
    natsStatus s;
    natsBuffer buf = NATS_EMPTY_BUFFER;

    if ((resps == NULL) || (count == NULL) || (client == NULL) || (marshal == NULL))
        return NATS_INVALID_ARG;

    *resps = NULL;
    *count = 0;

    s = natsBuf_Init(&buf, bufHint);
    IFOK(s, marshal(&buf, opts));
    if (s == NATS_OK)
        s = sysclient_pingList(resps, count, client, subjFmt, natsBuf_Data(&buf),
                               natsBuf_Len(&buf), timeout, fromMsg, destroyResp);
    natsBuf_Destroy(&buf);
    return s;
}

void
sysclient_freeRespList(void ***resps, int *count, sysRespDestroyFn destroyResp)
{
    void **arr;
    int    i;

    if ((resps == NULL) || (count == NULL))
        return;

    arr = *resps;
    for (i = 0; (arr != NULL) && (destroyResp != NULL) && (i < *count); i++)
        destroyResp(arr[i]);

    NATS_FREE(arr);
    *resps = NULL;
    *count = 0;
}

static natsStatus
_parseServerInfo(void *dst, natsJSON *node)
{
    return sysclient_scanFields(dst, node, _serverInfoFields, SYS_NFIELDS(_serverInfoFields));
}

static natsStatus
_parseAPIError(void *dst, natsJSON *node)
{
    return sysclient_scanFields(dst, node, _apiErrorFields, SYS_NFIELDS(_apiErrorFields));
}

natsStatus
sysclient_parseEnvelope(natsSysServerInfo *server, natsSysAPIError *apiErr,
                        natsJSON **payload, natsJSON *root, const char *payloadKey)
{
    natsStatus s;
    natsJSON  *node = NULL;

    if ((server == NULL) || (apiErr == NULL) || (payload == NULL) || (root == NULL)
        || (payloadKey == NULL))
        return NATS_INVALID_ARG;

    *payload = NULL;

    if (natsJSON_Type(root) != NATS_JSON_OBJECT)
        return NATS_INVALID_ARG;

    // Absent or null leaves the zero value; a present non-object is malformed.
    // Letting a bad "error" through would hand the caller a zeroed Error
    // alongside NATS_OK, and the documented "check Error.Code != 0" would then
    // clear a response the server had in fact rejected.
    s = sysclient_objectInline(server, root, "server", _parseServerInfo);
    IFOK(s, sysclient_objectInline(apiErr, root, "error", _parseAPIError));
    if (s != NATS_OK)
        return s;

    // The payload is absent when the server reports an error, so a missing key
    // is not a failure — the caller is expected to check apiErr.
    node = NULL;
    s    = natsJSON_Field(root, payloadKey, &node);
    if (s == NATS_NOT_FOUND)
        return NATS_OK;
    if (s != NATS_OK)
        return s;
    if (natsJSON_Type(node) == NATS_JSON_NULL)
        return NATS_OK;

    *payload = node;
    return NATS_OK;
}

void
sysclient_freeServerInfo(natsSysServerInfo *server)
{
    sysclient_freeFields(server, _serverInfoFields, SYS_NFIELDS(_serverInfoFields));
}

void
sysclient_freeAPIError(natsSysAPIError *apiErr)
{
    sysclient_freeFields(apiErr, _apiErrorFields, SYS_NFIELDS(_apiErrorFields));
}

//
// Shared types declared in sysclient.h.
//

static const sysField _slowConsumersStatsFields[] = {
    SYS_F(SYS_FLD_U64, natsSysSlowConsumersStats, Clients, "clients"),
    SYS_F(SYS_FLD_U64, natsSysSlowConsumersStats, Routes, "routes"),
    SYS_F(SYS_FLD_U64, natsSysSlowConsumersStats, Gateways, "gateways"),
    SYS_F(SYS_FLD_U64, natsSysSlowConsumersStats, Leafs, "leafs"),
};

static const sysField _subDetailFields[] = {
    SYS_F(SYS_FLD_STR, natsSysSubDetail, Account, "account"),
    SYS_F(SYS_FLD_STR, natsSysSubDetail, Subject, "subject"),
    SYS_F(SYS_FLD_STR, natsSysSubDetail, Queue, "qgroup"),
    SYS_F(SYS_FLD_STR, natsSysSubDetail, Sid, "sid"),
    SYS_F(SYS_FLD_I64, natsSysSubDetail, Msgs, "msgs"),
    SYS_F(SYS_FLD_I64, natsSysSubDetail, Max, "max"),
    SYS_F(SYS_FLD_U64, natsSysSubDetail, Cid, "cid"),
};

static const sysField _jetStreamAPIStatsFields[] = {
    SYS_F(SYS_FLD_U64, natsSysJetStreamAPIStats, Total, "total"),
    SYS_F(SYS_FLD_U64, natsSysJetStreamAPIStats, Errors, "errors"),
    SYS_F(SYS_FLD_U64, natsSysJetStreamAPIStats, Inflight, "inflight"),
};

static const sysField _jetStreamStatsFields[] = {
    SYS_F(SYS_FLD_U64, natsSysJetStreamStats, Memory, "memory"),
    SYS_F(SYS_FLD_U64, natsSysJetStreamStats, Store, "storage"),
    SYS_F(SYS_FLD_U64, natsSysJetStreamStats, ReservedMemory, "reserved_memory"),
    SYS_F(SYS_FLD_U64, natsSysJetStreamStats, ReservedStore, "reserved_storage"),
    SYS_F(SYS_FLD_INT, natsSysJetStreamStats, Accounts, "accounts"),
    SYS_F(SYS_FLD_INT, natsSysJetStreamStats, HAAssets, "ha_assets"),
};

static const sysField _jetStreamConfigFields[] = {
    SYS_F(SYS_FLD_I64, natsSysJetStreamConfig, MaxMemory, "max_memory"),
    SYS_F(SYS_FLD_I64, natsSysJetStreamConfig, MaxStore, "max_storage"),
    SYS_F(SYS_FLD_STR, natsSysJetStreamConfig, StoreDir, "store_dir"),
    SYS_F(SYS_FLD_I64, natsSysJetStreamConfig, SyncInterval, "sync_interval"),
    SYS_F(SYS_FLD_BOOL, natsSysJetStreamConfig, SyncAlways, "sync_always"),
    SYS_F(SYS_FLD_STR, natsSysJetStreamConfig, Domain, "domain"),
    SYS_F(SYS_FLD_BOOL, natsSysJetStreamConfig, CompressOK, "compress_ok"),
    SYS_F(SYS_FLD_STR, natsSysJetStreamConfig, UniqueTag, "unique_tag"),
};

static const sysField _peerInfoFields[] = {
    SYS_F(SYS_FLD_STR, natsSysPeerInfo, Name, "name"),
    SYS_F(SYS_FLD_BOOL, natsSysPeerInfo, Current, "current"),
    SYS_F(SYS_FLD_BOOL, natsSysPeerInfo, Offline, "offline"),
    SYS_F(SYS_FLD_I64, natsSysPeerInfo, Active, "active"),
    SYS_F(SYS_FLD_U64, natsSysPeerInfo, Lag, "lag"),
    SYS_F(SYS_FLD_STR, natsSysPeerInfo, Peer, "peer"),
};

static const sysField _metaClusterInfoFields[] = {
    SYS_F(SYS_FLD_STR, natsSysMetaClusterInfo, Name, "name"),
    SYS_F(SYS_FLD_STR, natsSysMetaClusterInfo, Leader, "leader"),
    SYS_F(SYS_FLD_STR, natsSysMetaClusterInfo, Peer, "peer"),
    SYS_F(SYS_FLD_INT, natsSysMetaClusterInfo, Size, "cluster_size"),
    SYS_F(SYS_FLD_INT, natsSysMetaClusterInfo, Pending, "pending"),
};

static natsStatus
_parseJetStreamAPIStats(void *dst, natsJSON *node)
{
    return sysclient_scanFields(dst, node, _jetStreamAPIStatsFields,
                                SYS_NFIELDS(_jetStreamAPIStatsFields));
}

static natsStatus
_parsePeerInfo(void *dst, natsJSON *node)
{
    return sysclient_scanFields(dst, node, _peerInfoFields, SYS_NFIELDS(_peerInfoFields));
}

static void
_freePeerInfo(void *dst)
{
    sysclient_freeFields(dst, _peerInfoFields, SYS_NFIELDS(_peerInfoFields));
}

natsStatus
sysclient_parseSlowConsumersStats(void *dst, natsJSON *node)
{
    return sysclient_scanFields(dst, node, _slowConsumersStatsFields,
                                SYS_NFIELDS(_slowConsumersStatsFields));
}

natsStatus
sysclient_parseSubDetail(void *dst, natsJSON *node)
{
    return sysclient_scanFields(dst, node, _subDetailFields, SYS_NFIELDS(_subDetailFields));
}

void
sysclient_freeSubDetail(void *dst)
{
    sysclient_freeFields(dst, _subDetailFields, SYS_NFIELDS(_subDetailFields));
}

natsStatus
sysclient_parseJetStreamStats(void *dst, natsJSON *node)
{
    natsSysJetStreamStats *stats = (natsSysJetStreamStats *) dst;
    natsStatus             s;

    s = sysclient_scanFields(stats, node, _jetStreamStatsFields,
                             SYS_NFIELDS(_jetStreamStatsFields));
    IFOK(s, sysclient_objectInline(&stats->API, node, "api", _parseJetStreamAPIStats));
    return s;
}

natsStatus
sysclient_parseJetStreamConfig(void *dst, natsJSON *node)
{
    return sysclient_scanFields(dst, node, _jetStreamConfigFields,
                                SYS_NFIELDS(_jetStreamConfigFields));
}

void
sysclient_freeJetStreamConfig(void *dst)
{
    sysclient_freeFields(dst, _jetStreamConfigFields, SYS_NFIELDS(_jetStreamConfigFields));
}

natsStatus
sysclient_parseMetaClusterInfo(void *dst, natsJSON *node)
{
    natsSysMetaClusterInfo *meta = (natsSysMetaClusterInfo *) dst;
    natsStatus              s;

    s = sysclient_scanFields(meta, node, _metaClusterInfoFields,
                             SYS_NFIELDS(_metaClusterInfoFields));
    IFOK(s, sysclient_ptrArray((void ***) &meta->Replicas, &meta->ReplicasCount, node,
                               "replicas", sizeof(natsSysPeerInfo), _parsePeerInfo,
                               _freePeerInfo));
    return s;
}

void
sysclient_freeMetaClusterInfo(void *dst)
{
    natsSysMetaClusterInfo *meta = (natsSysMetaClusterInfo *) dst;

    sysclient_freeFields(meta, _metaClusterInfoFields, SYS_NFIELDS(_metaClusterInfoFields));
    sysclient_freePtrArray((void ***) &meta->Replicas, &meta->ReplicasCount, _freePeerInfo);
}

natsStatus
sysclient_parseJetStreamVarz(void *dst, natsJSON *node)
{
    natsSysJetStreamVarz *jsv = (natsSysJetStreamVarz *) dst;
    natsStatus            s   = NATS_OK;

    IFOK(s, sysclient_objectPtr((void **) &jsv->Config, node, "config",
                                sizeof(natsSysJetStreamConfig),
                                sysclient_parseJetStreamConfig,
                                sysclient_freeJetStreamConfig));
    IFOK(s, sysclient_objectPtr((void **) &jsv->Stats, node, "stats",
                                sizeof(natsSysJetStreamStats),
                                sysclient_parseJetStreamStats, NULL));
    IFOK(s, sysclient_objectPtr((void **) &jsv->Meta, node, "meta",
                                sizeof(natsSysMetaClusterInfo),
                                sysclient_parseMetaClusterInfo,
                                sysclient_freeMetaClusterInfo));
    return s;
}

void
sysclient_freeJetStreamVarz(void *dst)
{
    natsSysJetStreamVarz *jsv = (natsSysJetStreamVarz *) dst;

    sysclient_freeObjectPtr((void **) &jsv->Config, sysclient_freeJetStreamConfig);
    sysclient_freeObjectPtr((void **) &jsv->Stats, NULL);
    sysclient_freeObjectPtr((void **) &jsv->Meta, sysclient_freeMetaClusterInfo);
}

//
// Option copying.
//

int64_t
sysclient_nowMs(void)
{
    return nats_NowMonotonicInNanoSeconds() / 1000000;
}

int64_t
sysclient_capTimeout(int64_t timeout)
{
    return (timeout > SYSCLIENT_MAX_TIMEOUT_MS) ? SYSCLIENT_MAX_TIMEOUT_MS : timeout;
}

int64_t
sysclient_deadline(int64_t timeout)
{
    return sysclient_nowMs() + sysclient_capTimeout(timeout);
}

natsStatus
sysclient_walkServerID(char **dst, const char *id)
{
    // Every page after the first is fetched by server ID, so a reply that did
    // not name its sender cannot be walked at all. NATS_ERR (a malformed
    // response) rather than NATS_INVALID_ARG, which is reserved for the
    // caller's own arguments.
    if (nats_IsStringEmpty(id))
        return NATS_ERR;
    return sysclient_dupStr(dst, id);
}

natsStatus
sysclient_dupStr(char **dst, const char *src)
{
    if (dst == NULL)
        return NATS_INVALID_ARG;

    if (src == NULL)
    {
        *dst = NULL;
        return NATS_OK;
    }

    *dst = NATS_STRDUP(src);
    return (*dst == NULL) ? NATS_NO_MEMORY : NATS_OK;
}

// Local to the event filter, the only option member that is a string array.
static natsStatus
_dupOptStrArray(const char ***dst, int *dstCount, const char *const *src, int count)
{
    const char **arr;
    int          i;

    if ((dst == NULL) || (dstCount == NULL))
        return NATS_INVALID_ARG;

    *dst      = NULL;
    *dstCount = 0;

    if ((src == NULL) || (count <= 0))
        return NATS_OK;

    arr = (const char **) NATS_CALLOC((size_t) count, sizeof(char *));
    if (arr == NULL)
        return NATS_NO_MEMORY;

    for (i = 0; i < count; i++)
    {
        if (src[i] == NULL)
            continue;
        arr[i] = NATS_STRDUP(src[i]);
        if (arr[i] == NULL)
        {
            int j;

            for (j = 0; j < i; j++)
                NATS_FREE((char *) arr[j]);
            NATS_FREE(arr);
            return NATS_NO_MEMORY;
        }
    }

    *dst      = arr;
    *dstCount = count;
    return NATS_OK;
}

natsStatus
sysclient_dupOptStr(const char **dst, const char *src)
{
    return sysclient_dupStr((char **) dst, src);
}

void
sysclient_freeOptStr(const char **str)
{
    if (str == NULL)
        return;

    NATS_FREE((char *) *str);
    *str = NULL;
}

natsStatus
sysclient_copyEventFilter(natsSysEventFilterOptions *dst,
                          const natsSysEventFilterOptions *src)
{
    natsStatus s = NATS_OK;

    if (dst == NULL)
        return NATS_INVALID_ARG;

    memset(dst, 0, sizeof(*dst));
    if (src == NULL)
        return NATS_OK;

    IFOK(s, sysclient_dupOptStr(&dst->Name, src->Name));
    IFOK(s, sysclient_dupOptStr(&dst->Cluster, src->Cluster));
    IFOK(s, sysclient_dupOptStr(&dst->Host, src->Host));
    IFOK(s, sysclient_dupOptStr(&dst->Domain, src->Domain));
    IFOK(s, _dupOptStrArray(&dst->Tags, &dst->TagsCount, src->Tags, src->TagsCount));

    if (s != NATS_OK)
        sysclient_freeEventFilter(dst);
    return s;
}

void
sysclient_freeEventFilter(natsSysEventFilterOptions *filter)
{
    if (filter == NULL)
        return;

    sysclient_freeOptStr(&filter->Name);
    sysclient_freeOptStr(&filter->Cluster);
    sysclient_freeOptStr(&filter->Host);
    sysclient_freeOptStr(&filter->Domain);
    sysclient_freeStrArray((char ***) &filter->Tags, &filter->TagsCount);
    memset(filter, 0, sizeof(*filter));
}

//
// RFC 3339 parsing.
//

static bool
_readDigits(const char **p, int n, int *out)
{
    int v = 0;
    int i;

    for (i = 0; i < n; i++)
    {
        if ((**p < '0') || (**p > '9'))
            return false;
        v = v * 10 + (**p - '0');
        (*p)++;
    }
    *out = v;
    return true;
}

static int
_daysInMonth(int y, int m)
{
    static const int dm[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    if ((m == 2) && ((((y % 4) == 0) && ((y % 100) != 0)) || ((y % 400) == 0)))
        return 29;
    return dm[m - 1];
}

// Days from 1970-01-01 to y-m-d, proleptic Gregorian. Pure arithmetic, so no
// OS date facility (and therefore no os_shims entry) is needed.
static int64_t
_daysFromCivil(int64_t y, int m, int d)
{
    int64_t era;
    int64_t yoe;
    int64_t doy;
    int64_t doe;

    y -= (m <= 2);
    era = ((y >= 0) ? y : y - 399) / 400;
    yoe = y - era * 400;                                   // [0, 399]
    doy = (153 * (m + ((m > 2) ? -3 : 9)) + 2) / 5 + d - 1; // [0, 365]
    doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;           // [0, 146096]

    return era * 146097 + doe - 719468;
}

natsStatus
natsSysTime_Parse(int64_t *unixNanos, const char *rfc3339)
{
    const char *p = rfc3339;
    int         year, mon, day, hour, min, sec;
    int64_t     nanos     = 0;
    int64_t     offsetSec = 0;
    int64_t     days;
    int64_t     secs;

    if ((unixNanos == NULL) || (rfc3339 == NULL))
        return NATS_INVALID_ARG;

    if (!_readDigits(&p, 4, &year) || (*p++ != '-'))
        return NATS_INVALID_ARG;
    if (!_readDigits(&p, 2, &mon) || (*p++ != '-'))
        return NATS_INVALID_ARG;
    if (!_readDigits(&p, 2, &day))
        return NATS_INVALID_ARG;

    if ((*p != 'T') && (*p != 't') && (*p != ' '))
        return NATS_INVALID_ARG;
    p++;

    if (!_readDigits(&p, 2, &hour) || (*p++ != ':'))
        return NATS_INVALID_ARG;
    if (!_readDigits(&p, 2, &min) || (*p++ != ':'))
        return NATS_INVALID_ARG;
    if (!_readDigits(&p, 2, &sec))
        return NATS_INVALID_ARG;

    if (*p == '.')
    {
        int64_t scale = 100000000; // digits beyond nanosecond precision are dropped

        p++;
        if ((*p < '0') || (*p > '9'))
            return NATS_INVALID_ARG;
        while ((*p >= '0') && (*p <= '9'))
        {
            if (scale > 0)
            {
                nanos += (int64_t) (*p - '0') * scale;
                scale /= 10;
            }
            p++;
        }
    }

    if ((*p == 'Z') || (*p == 'z'))
    {
        p++;
    }
    else if ((*p == '+') || (*p == '-'))
    {
        int sign = (*p == '-') ? -1 : 1;
        int oh, om;

        p++;
        if (!_readDigits(&p, 2, &oh) || (*p++ != ':'))
            return NATS_INVALID_ARG;
        if (!_readDigits(&p, 2, &om))
            return NATS_INVALID_ARG;
        if ((oh > 23) || (om > 59))
            return NATS_INVALID_ARG;
        offsetSec = sign * (oh * 3600 + om * 60);
    }
    else
    {
        return NATS_INVALID_ARG;
    }

    if (*p != '\0')
        return NATS_INVALID_ARG;

    // A leap second (60) is legal in RFC 3339 and is folded into the next
    // minute, as most implementations do.
    if ((mon < 1) || (mon > 12) || (hour > 23) || (min > 59) || (sec > 60))
        return NATS_INVALID_ARG;
    if ((day < 1) || (day > _daysInMonth(year, mon)))
        return NATS_INVALID_ARG;

    days = _daysFromCivil(year, mon, day);
    secs = days * 86400 + hour * 3600 + min * 60 + sec - offsetSec;

    // Reject what int64 nanoseconds cannot hold, rather than overflowing into a
    // plausible-looking wrong answer. This is the same window Go documents for
    // time.Time.UnixNano(), and the boundary is real: a JetStream stream that
    // has never been written reports "0001-01-01T00:00:00Z" for first_ts and
    // last_ts, which is 24 orders of magnitude outside it.
    // nanos is always >= 0, so it can only push the result up.
    if ((secs > (INT64_MAX - nanos) / 1000000000LL)
        || (secs < INT64_MIN / 1000000000LL))
        return NATS_INVALID_ARG;

    *unixNanos = secs * 1000000000LL + nanos;
    return NATS_OK;
}
