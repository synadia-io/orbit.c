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
    natsStatus s = sysclient_initOpts(opts, sizeof(*opts));

    if (s != NATS_OK)
        return s;

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

    *newClient = NULL;

    if (nc == NULL)
        return NATS_INVALID_ARG;

    if (opts == NULL)
    {
        natsSysClientOpts_Init(&defaults);
        opts = &defaults;
    }

    // -1 means "no limit".
    if ((opts->ServerCount < -1) || (opts->ServerCount == 0))
        return NATS_INVALID_ARG;
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
    NATS_FREE(client);
}

// Timeouts are capped so that now + timeout cannot overflow an int64.
#define MAX_TIMEOUT_MS ((int64_t) 7 * 24 * 60 * 60 * 1000)

// Negative is an error, 0 selects the default, and the result is capped.
static natsStatus
_checkTimeout(int64_t *timeout)
{
    if (*timeout < 0)
        return NATS_INVALID_ARG;
    if (*timeout == 0)
        *timeout = NATS_SYS_DEFAULT_REQUEST_TIMEOUT;
    if (*timeout > MAX_TIMEOUT_MS)
        *timeout = MAX_TIMEOUT_MS;
    return NATS_OK;
}

static natsStatus
_dupStr(char **dst, const char *src)
{
    if (src == NULL)
    {
        *dst = NULL;
        return NATS_OK;
    }

    *dst = NATS_STRDUP(src);
    return (*dst == NULL) ? NATS_NO_MEMORY : NATS_OK;
}

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

static natsStatus
_requestByID(natsMsg **replyMsg, natsSysClient *client, const char *serverID,
             const char *subjFmt, const char *payload, int payloadLen, int64_t timeout)
{
    natsStatus s;
    char      *subj = NULL;

    if ((replyMsg == NULL) || (client == NULL) || (subjFmt == NULL))
        return NATS_INVALID_ARG;
    if (nats_IsStringEmpty(serverID))
        return NATS_INVALID_ARG;

    *replyMsg = NULL;

    s = _checkTimeout(&timeout);
    IFOK(s, _buildSubject(&subj, subjFmt, serverID));
    if (s != NATS_OK)
        return s;

    s = natsConnection_Request(replyMsg, client->nc, subj, payload, payloadLen, timeout);

    // The server ID is in the subject, so no responder means no such server
    // (or a connection that is not on the system account).
    if (s == NATS_NO_RESPONDERS)
        s = NATS_NOT_FOUND;

    NATS_FREE(subj);
    return s;
}

// Scatters a request to every server and gathers the raw replies.
static natsStatus
_pingServers(natsMsgList *list, natsSysClient *client, const char *subjFmt,
             const char *payload, int payloadLen, int64_t timeout)
{
    natsStatus          s;
    char               *subj = NULL;
    natsRequestManyOpts rmOpts;

    s = _checkTimeout(&timeout);
    IFOK(s, _buildSubject(&subj, subjFmt, SYS_PING_TARGET));
    if (s != NATS_OK)
        return s;

    natsRequestManyOpts_Init(&rmOpts);
    rmOpts.Timeout = (uint64_t) timeout;
    rmOpts.Stall   = (uint64_t) client->stallInterval;
    // -1 is the "no limit" sentinel.
    if (client->serverCount > 0)
        rmOpts.Count = (uint64_t) client->serverCount;

    s = natsRequestMany_Request(list, client->nc, subj, payload, payloadLen, &rmOpts);

    // Reaching the deadline is how a gather normally ends, so it is a success
    // with whatever arrived. NATS_NO_RESPONDERS (nobody subscribed at all,
    // typically a connection off the system account) stays an error.
    if (s == NATS_TIMEOUT)
        s = NATS_OK;

    NATS_FREE(subj);
    return s;
}

#define RESP_SERVER(r, ep)  ((natsSysServerInfo *) ((char *) (r) + (ep)->ServerOff))
#define RESP_ERROR(r, ep)   ((natsSysAPIError *) ((char *) (r) + (ep)->ErrorOff))
#define RESP_PAYLOAD(r, ep) ((void *) ((char *) (r) + (ep)->PayloadOff))

#define REQ_BUF_HINT (256)

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

// Splits a response envelope; *payload borrows from 'root' and is NULL when
// the key is absent.
static natsStatus
_parseEnvelope(natsSysServerInfo *server, natsSysAPIError *apiErr, natsJSON **payload,
               natsJSON *root, const char *payloadKey)
{
    natsStatus s;
    natsJSON  *node = NULL;

    *payload = NULL;

    if (natsJSON_Type(root) != NATS_JSON_OBJECT)
        return NATS_INVALID_ARG;

    // A present non-object "error" is malformed: letting it through would
    // hand the caller a zeroed Error for a response the server rejected.
    s = sysclient_objectInline(server, root, "server", _parseServerInfo);
    IFOK(s, sysclient_objectInline(apiErr, root, "error", _parseAPIError));
    if (s != NATS_OK)
        return s;

    // The payload is absent when the server reports an error.
    s = natsJSON_Lookup(root, payloadKey, &node);
    if (s == NATS_NOT_FOUND)
        return NATS_OK;
    if (s != NATS_OK)
        return s;

    *payload = node;
    return NATS_OK;
}

void
sysclient_destroyResp(void *resp, const void *epv)
{
    const sysEndpoint *ep = (const sysEndpoint *) epv;

    if ((resp == NULL) || (ep == NULL))
        return;

    sysclient_freeFields(RESP_SERVER(resp, ep), _serverInfoFields, SYS_NFIELDS(_serverInfoFields));
    sysclient_freeFields(RESP_ERROR(resp, ep), _apiErrorFields, SYS_NFIELDS(_apiErrorFields));
    ep->FreePayload(RESP_PAYLOAD(resp, ep));
    NATS_FREE(resp);
}

// The json.h accessors report a wrong type as NATS_INVALID_ARG; to the caller
// that is a malformed response, and NATS_INVALID_ARG is reserved for their own
// arguments.
static natsStatus
_responseStatus(natsStatus s)
{
    if ((s != NATS_OK) && (s != NATS_NO_MEMORY))
        return NATS_ERR;
    return s;
}

// Decodes one reply into a new response; *newResp is NULL on error.
static natsStatus
_respFromMsg(void **newResp, natsMsg *msg, const sysEndpoint *ep)
{
    natsStatus s;
    natsJSON  *root    = NULL;
    natsJSON  *payload = NULL;
    void      *resp;

    *newResp = NULL;

    resp = NATS_CALLOC(1, ep->RespSize);
    if (resp == NULL)
        return NATS_NO_MEMORY;

    s = natsJSON_Parse(&root, natsMsg_GetData(msg), natsMsg_GetDataLength(msg));
    IFOK(s, _parseEnvelope(RESP_SERVER(resp, ep), RESP_ERROR(resp, ep), &payload, root,
                           ep->PayloadKey));

    if ((s == NATS_OK) && (payload != NULL))
        s = ep->ParsePayload(RESP_PAYLOAD(resp, ep), payload);

    // The tree's raw spans borrow from the message, which the caller still holds.
    natsJSON_Destroy(root);

    s = _responseStatus(s);
    if (s != NATS_OK)
    {
        sysclient_destroyResp(resp, ep);
        return s;
    }

    *newResp = resp;
    return NATS_OK;
}

natsStatus
sysclient_request(void **newResp, natsSysClient *client, const char *serverID,
                  const void *opts, int64_t timeout, const sysEndpoint *ep)
{
    natsStatus s;
    natsBuffer buf   = NATS_EMPTY_BUFFER;
    natsMsg   *reply = NULL;

    if ((newResp == NULL) || (client == NULL) || (ep == NULL))
        return NATS_INVALID_ARG;

    *newResp = NULL;

    s = natsBuf_Init(&buf, REQ_BUF_HINT);
    IFOK(s, ep->Marshal(&buf, opts));
    if (s == NATS_OK)
        s = _requestByID(&reply, client, serverID, ep->Subject, natsBuf_Data(&buf),
                         natsBuf_Len(&buf), timeout);
    natsBuf_Destroy(&buf);
    if (s != NATS_OK)
        return s;

    s = _respFromMsg(newResp, reply, ep);
    natsMsg_Destroy(reply);
    return s;
}

natsStatus
sysclient_ping(void ***resps, int *count, natsSysClient *client, const void *opts,
               int64_t timeout, const sysEndpoint *ep)
{
    natsStatus  s;
    natsBuffer  buf  = NATS_EMPTY_BUFFER;
    natsMsgList msgs = {NULL, 0};
    void      **arr  = NULL;
    int         n    = 0;
    int         i;

    if ((resps == NULL) || (count == NULL) || (client == NULL) || (ep == NULL))
        return NATS_INVALID_ARG;

    *resps = NULL;
    *count = 0;

    s = natsBuf_Init(&buf, REQ_BUF_HINT);
    IFOK(s, ep->Marshal(&buf, opts));
    if (s == NATS_OK)
        s = _pingServers(&msgs, client, ep->Subject, natsBuf_Data(&buf), natsBuf_Len(&buf),
                         timeout);
    natsBuf_Destroy(&buf);

    if ((s == NATS_OK) && (msgs.Count > 0))
    {
        arr = (void **) NATS_CALLOC((size_t) msgs.Count, sizeof(void *));
        if (arr == NULL)
            s = NATS_NO_MEMORY;
    }

    for (i = 0; (s == NATS_OK) && (i < msgs.Count); i++)
    {
        s = _respFromMsg(&arr[n], msgs.Msgs[i], ep);
        if (s == NATS_OK)
            n++;
    }

    natsMsgList_Destroy(&msgs);

    if (s != NATS_OK)
    {
        sysclient_freeList(&arr, &n, sysclient_destroyResp, ep);
        return s;
    }

    *resps = arr;
    *count = n;
    return NATS_OK;
}

void
sysclient_freeList(void ***items, int *count, sysDestroyFn destroy, const void *ctx)
{
    void **arr;
    int    i;

    if ((items == NULL) || (count == NULL))
        return;

    arr = *items;
    for (i = 0; (arr != NULL) && (destroy != NULL) && (i < *count); i++)
        destroy(arr[i], ctx);

    NATS_FREE(arr);
    *items = NULL;
    *count = 0;
}

//
// Page walks.
//

// The members a walk table locates by offset.
#define WALK_INT(base, off) (*(int *) ((char *) (base) + (off)))

static bool
_isPaged(const sysWalkOps *ops, const void *opts)
{
    return (ops->Paged == NULL) || ops->Paged(opts);
}

// Hands one page to the handler, releases it, and advances *offset. With
// 'refresh', *total is re-read from the page. Stopping on an empty page keeps
// a shrinking result set from looping forever; orbit.go's per-server ping
// iterators lack that guard.
static void
_deliverPage(void *page, int *offset, int *total, bool refresh, bool paged,
             sysPageHandler handler, void *closure, const sysWalkOps *ops, bool *stop)
{
    int  n = WALK_INT(page, ops->CountOff);
    bool wantMore;

    if (refresh)
        *total = WALK_INT(page, ops->TotalOff);
    wantMore = handler(page, closure);
    sysclient_destroyResp(page, ops->Endpoint);

    *offset += n;
    *stop = (!wantMore || !paged || (n == 0) || (*offset >= *total));
}

// Fetches one page by ID and delivers it.
static natsStatus
_walkPage(natsSysClient *client, const char *serverID, const void *opts, int *offset,
          int *total, bool refresh, bool paged, int64_t deadline, sysPageHandler handler,
          void *closure, const sysWalkOps *ops, bool *stop)
{
    void      *page = NULL;
    natsStatus s;
    int64_t    left;

    left = deadline - natsSys_NowMs();
    if (left <= 0)
        return NATS_TIMEOUT;

    s = ops->Fetch(&page, client, serverID, opts, *offset, left);
    if (s != NATS_OK)
        return s;

    _deliverPage(page, offset, total, refresh, paged, handler, closure, ops, stop);
    return NATS_OK;
}

natsStatus
sysclient_walkEach(natsSysClient *client, const char *serverID, const void *opts,
                   int64_t timeout, sysPageHandler handler, void *closure,
                   const sysWalkOps *ops)
{
    natsStatus s;
    int64_t    deadline;
    int        offset;
    int        total = 0;
    bool       paged;
    bool       stop  = false;

    if ((client == NULL) || (ops == NULL))
        return NATS_INVALID_ARG;

    s = _checkTimeout(&timeout);
    if (s != NATS_OK)
        return s;

    deadline = natsSys_NowMs() + timeout;
    offset   = (opts != NULL) ? WALK_INT(opts, ops->OffsetOff) : 0;
    paged    = _isPaged(ops, opts);

    do
    {
        s = _walkPage(client, serverID, opts, &offset, &total, true, paged, deadline,
                      handler, closure, ops, &stop);
        if (s != NATS_OK)
            return s;
    } while (!stop);

    return NATS_OK;
}

const char *
sysclient_walkID(const sysWalk *walk)
{
    return (walk != NULL) ? walk->serverID : NULL;
}

void
sysclient_walkDestroy(void *walkv, const void *ctx)
{
    sysWalk *walk = (sysWalk *) walkv;

    (void) ctx;
    if (walk == NULL)
        return;

    sysclient_destroyResp(walk->first, walk->ops->Endpoint);
    if (walk->opts != NULL)
        walk->ops->FreeOptions(walk->opts);
    NATS_FREE(walk->opts);
    NATS_FREE(walk->serverID);
    NATS_FREE(walk);
}

// Builds a walk from a ping's page, which it adopts on NATS_OK.
static natsStatus
_newWalk(sysWalk **newWalk, natsSysClient *client, const void *opts, void *page,
         const sysWalkOps *ops)
{
    sysWalk    *walk;
    const char *id;
    natsStatus  s;

    *newWalk = NULL;

    walk = (sysWalk *) NATS_CALLOC(1, sizeof(sysWalk));
    if (walk == NULL)
        return NATS_NO_MEMORY;

    walk->ops    = ops;
    walk->client = client;

    // Every later page is fetched by server ID, so a reply without one cannot
    // be walked.
    id = RESP_SERVER(page, ops->Endpoint)->ID;
    s  = nats_IsStringEmpty(id) ? NATS_ERR : _dupStr(&walk->serverID, id);

    if (s == NATS_OK)
    {
        walk->opts = NATS_CALLOC(1, ops->OptsSize);
        s          = (walk->opts == NULL) ? NATS_NO_MEMORY
                                          : ops->CopyOptions(walk->opts, opts);
    }
    if (s != NATS_OK)
    {
        sysclient_walkDestroy(walk, NULL);
        return s;
    }

    walk->first  = page;
    walk->total  = WALK_INT(page, ops->TotalOff);
    walk->offset = WALK_INT(walk->opts, ops->OffsetOff);

    *newWalk = walk;
    return NATS_OK;
}

natsStatus
sysclient_pingEach(void ***walks, int *count, natsSysClient *client, const void *opts,
                   int64_t timeout, const sysWalkOps *ops)
{
    natsStatus s;
    void     **pages     = NULL;
    void     **arr       = NULL;
    int        pageCount = 0;
    int        n;
    int        i;

    if ((walks == NULL) || (count == NULL) || (client == NULL) || (ops == NULL))
        return NATS_INVALID_ARG;

    *walks = NULL;
    *count = 0;

    s = sysclient_ping(&pages, &pageCount, client, opts, timeout, ops->Endpoint);
    if ((s != NATS_OK) || (pageCount == 0))
        return s;

    n   = pageCount;
    arr = (void **) NATS_CALLOC((size_t) n, sizeof(void *));
    if (arr == NULL)
        s = NATS_NO_MEMORY;

    for (i = 0; (s == NATS_OK) && (i < n); i++)
    {
        s = _newWalk((sysWalk **) &arr[i], client, opts, pages[i], ops);
        // The walk owns the page now; clear the slot so the release below
        // does not free it a second time.
        if (s == NATS_OK)
            pages[i] = NULL;
    }

    sysclient_freeList(&pages, &pageCount, sysclient_destroyResp, ops->Endpoint);

    if (s != NATS_OK)
    {
        // Every slot is a walk or NULL (the failed one, and all after it).
        sysclient_freeList(&arr, &n, sysclient_walkDestroy, NULL);
        return s;
    }

    *walks = arr;
    *count = n;
    return NATS_OK;
}

natsStatus
sysclient_walkRun(sysWalk *walk, int64_t timeout, sysPageHandler handler, void *closure)
{
    natsStatus s;
    int64_t    deadline;
    bool       paged;
    bool       stop = false;

    if (walk == NULL)
        return NATS_INVALID_ARG;

    s = _checkTimeout(&timeout);
    if (s != NATS_OK)
        return s;
    if (walk->done)
        return NATS_OK;

    deadline = natsSys_NowMs() + timeout;
    paged    = _isPaged(walk->ops, walk->opts);

    // The ping's page first; its total is never refreshed.
    if (walk->first != NULL)
    {
        void *page = walk->first;

        walk->first = NULL;
        _deliverPage(page, &walk->offset, &walk->total, false, paged, handler, closure,
                     walk->ops, &stop);
    }

    while (!stop)
    {
        s = _walkPage(walk->client, walk->serverID, walk->opts, &walk->offset, &walk->total,
                      false, paged, deadline, handler, closure, walk->ops, &stop);
        if (s != NATS_OK)
            return s;
    }

    walk->done = true;
    return NATS_OK;
}

// Shared types declared in sysclient.h.

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

// Option copying.

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

    // A NULL entry was already rejected when the ping marshalled these.
    for (i = 0; i < count; i++)
    {
        arr[i] = NATS_STRDUP(src[i]);
        if (arr[i] == NULL)
        {
            sysclient_freeStrArray((char ***) &arr, &i);
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
    if (dst == NULL)
        return NATS_INVALID_ARG;
    return _dupStr((char **) dst, src);
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

// RFC 3339 parsing.

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

// Days from 1970-01-01 to y-m-d, proleptic Gregorian.
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

    // A leap second (60) folds into the next minute.
    if ((mon < 1) || (mon > 12) || (hour > 23) || (min > 59) || (sec > 60))
        return NATS_INVALID_ARG;
    if ((day < 1) || (day > _daysInMonth(year, mon)))
        return NATS_INVALID_ARG;

    days = _daysFromCivil(year, mon, day);
    secs = days * 86400 + hour * 3600 + min * 60 + sec - offsetSec;

    // Reject what int64 nanoseconds cannot hold rather than overflow; a
    // never-written stream reports "0001-01-01T00:00:00Z". nanos >= 0.
    if ((secs > (INT64_MAX - nanos) / 1000000000LL)
        || (secs < INT64_MIN / 1000000000LL))
        return NATS_INVALID_ARG;

    *unixNanos = secs * 1000000000LL + nanos;
    return NATS_OK;
}
