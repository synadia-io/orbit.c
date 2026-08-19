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

// JSZ paginates over accounts rather than a flat list, and only when
// opts->Accounts is set, so its walk differs from the CONNZ and SUBSZ ones in
// two places: the total comes from the flattened JetStreamStats.Accounts, and
// a request without Accounts yields exactly one page.

#include "jsz.h"

#include "marshal.h"
#include "sysclientp.h"
#include "unmarshal.h"

#include "buf.h"
#include "os_shims.h"

#include <string.h>

struct __natsSysJszWalk
{
    natsSysClient    *client;   // borrowed; must outlive the walk
    char             *serverID; // owned
    natsSysJszOptions opts;     // deep copy
    natsSysJszResp   *first;    // owned until delivered by the first _Run
    int               total;    // accounts reported by the first page
    int               offset;   // offset of the next page to request
    bool              done;
};

static const sysField _raftGroupDetailFields[] = {
    SYS_F(SYS_FLD_STR, natsSysRaftGroupDetail, Name, "name"),
    SYS_F(SYS_FLD_STR, natsSysRaftGroupDetail, RaftGroup, "raft_group"),
};

static const sysField _streamDetailFields[] = {
    SYS_F(SYS_FLD_STR, natsSysStreamDetail, Name, "name"),
    SYS_F(SYS_FLD_STR, natsSysStreamDetail, Created, "created"),
    SYS_F(SYS_FLD_STR, natsSysStreamDetail, RaftGroup, "stream_raft_group"),
    // Carried verbatim; see the note in jsz.h.
    SYS_F(SYS_FLD_RAWJSON, natsSysStreamDetail, ClusterJSON, "cluster"),
    SYS_F(SYS_FLD_RAWJSON, natsSysStreamDetail, ConfigJSON, "config"),
    SYS_F(SYS_FLD_RAWJSON, natsSysStreamDetail, StateJSON, "state"),
    SYS_F(SYS_FLD_RAWJSON, natsSysStreamDetail, MirrorJSON, "mirror"),
};

static const sysField _accountDetailFields[] = {
    SYS_F(SYS_FLD_STR, natsSysAccountDetail, Name, "name"),
    SYS_F(SYS_FLD_STR, natsSysAccountDetail, Id, "id"),
};

static const sysField _jsInfoFields[] = {
    SYS_F(SYS_FLD_STR, natsSysJSInfo, ID, "server_id"),
    SYS_F(SYS_FLD_STR, natsSysJSInfo, Now, "now"),
    SYS_F(SYS_FLD_BOOL, natsSysJSInfo, Disabled, "disabled"),
    SYS_F(SYS_FLD_INT, natsSysJSInfo, Streams, "streams"),
    SYS_F(SYS_FLD_INT, natsSysJSInfo, Consumers, "consumers"),
    SYS_F(SYS_FLD_U64, natsSysJSInfo, Messages, "messages"),
    SYS_F(SYS_FLD_U64, natsSysJSInfo, Bytes, "bytes"),
};

natsStatus
natsSysJszOptions_Init(natsSysJszOptions *opts)
{
    return sysclient_initOpts(opts, sizeof(*opts));
}

// Every field is optional, so a zeroed options struct marshals to "{}".
static natsStatus
_marshalOptions(natsBuffer *buf, const void *optsv)
{
    const natsSysJszOptions *opts = (const natsSysJszOptions *) optsv;
    natsJSONWriter w;

    natsJSONWriter_Init(&w, buf);
    natsJSONWriter_StartObject(&w);

    if (opts != NULL)
    {
        sysclient_optStr(&w, "account", opts->Account);
        sysclient_optBool(&w, "accounts", opts->Accounts);
        sysclient_optBool(&w, "streams", opts->Streams);
        sysclient_optBool(&w, "consumer", opts->Consumer);
        sysclient_optBool(&w, "config", opts->Config);
        sysclient_optBool(&w, "leader_only", opts->LeaderOnly);
        sysclient_optInt(&w, "offset", opts->Offset);
        sysclient_optInt(&w, "limit", opts->Limit);
        sysclient_optBool(&w, "raft", opts->RaftGroups);
        sysclient_optBool(&w, "stream_leader_only", opts->StreamLeaderOnly);

        sysclient_writeEventFilter(&w, &opts->Filter);
    }

    natsJSONWriter_EndObject(&w);
    return natsJSONWriter_Status(&w);
}

static natsStatus
_copyOptions(natsSysJszOptions *dst, const natsSysJszOptions *src)
{
    natsStatus s = NATS_OK;

    natsSysJszOptions_Init(dst);
    if (src == NULL)
        return NATS_OK;

    dst->Accounts         = src->Accounts;
    dst->Streams          = src->Streams;
    dst->Consumer         = src->Consumer;
    dst->Config           = src->Config;
    dst->LeaderOnly       = src->LeaderOnly;
    dst->Offset           = src->Offset;
    dst->Limit            = src->Limit;
    dst->RaftGroups       = src->RaftGroups;
    dst->StreamLeaderOnly = src->StreamLeaderOnly;

    IFOK(s, sysclient_dupOptStr(&dst->Account, src->Account));
    IFOK(s, sysclient_copyEventFilter(&dst->Filter, &src->Filter));

    return s;
}

static void
_freeOptionsCopy(natsSysJszOptions *opts)
{
    if (opts == NULL)
        return;

    sysclient_freeOptStr(&opts->Account);
    sysclient_freeEventFilter(&opts->Filter);
    memset(opts, 0, sizeof(*opts));
}

static natsStatus
_parseRaftGroupDetail(void *dst, natsJSON *node)
{
    return sysclient_scanFields(dst, node, _raftGroupDetailFields,
                                SYS_NFIELDS(_raftGroupDetailFields));
}

static void
_freeRaftGroupDetail(void *dst)
{
    sysclient_freeFields(dst, _raftGroupDetailFields, SYS_NFIELDS(_raftGroupDetailFields));
}

static natsStatus
_parseStreamDetail(void *dst, natsJSON *node)
{
    natsSysStreamDetail *sd = (natsSysStreamDetail *) dst;
    natsStatus           s;

    s = sysclient_scanFields(sd, node, _streamDetailFields,
                             SYS_NFIELDS(_streamDetailFields));
    IFOK(s, sysclient_rawJSONArray(&sd->ConsumerJSON, &sd->ConsumerJSONCount, node,
                                   "consumer_detail"));
    IFOK(s, sysclient_rawJSONArray(&sd->SourcesJSON, &sd->SourcesJSONCount, node,
                                   "sources"));
    IFOK(s, sysclient_ptrArray((void ***) &sd->ConsumerRaftGroups,
                               &sd->ConsumerRaftGroupsCount, node,
                               "consumer_raft_groups", sizeof(natsSysRaftGroupDetail),
                               _parseRaftGroupDetail, _freeRaftGroupDetail));
    return s;
}

static void
_freeStreamDetail(void *dst)
{
    natsSysStreamDetail *sd = (natsSysStreamDetail *) dst;

    sysclient_freeFields(sd, _streamDetailFields, SYS_NFIELDS(_streamDetailFields));
    sysclient_freeStrArray(&sd->ConsumerJSON, &sd->ConsumerJSONCount);
    sysclient_freeStrArray(&sd->SourcesJSON, &sd->SourcesJSONCount);
    sysclient_freePtrArray((void ***) &sd->ConsumerRaftGroups,
                           &sd->ConsumerRaftGroupsCount, _freeRaftGroupDetail);
}

static natsStatus
_parseAccountDetail(void *dst, natsJSON *node)
{
    natsSysAccountDetail *ad = (natsSysAccountDetail *) dst;
    natsStatus            s;

    s = sysclient_scanFields(ad, node, _accountDetailFields,
                             SYS_NFIELDS(_accountDetailFields));
    // Flattened on the wire, so its keys are read from this same node.
    IFOK(s, sysclient_parseJetStreamStats(&ad->JetStreamStats, node));
    IFOK(s, sysclient_valueArray((void **) &ad->Streams, &ad->StreamsCount, node,
                                 "stream_detail", sizeof(natsSysStreamDetail),
                                 _parseStreamDetail, _freeStreamDetail));
    return s;
}

static void
_freeAccountDetail(void *dst)
{
    natsSysAccountDetail *ad = (natsSysAccountDetail *) dst;

    sysclient_freeFields(ad, _accountDetailFields, SYS_NFIELDS(_accountDetailFields));
    sysclient_freeValueArray((void **) &ad->Streams, &ad->StreamsCount,
                             sizeof(natsSysStreamDetail), _freeStreamDetail);
}

static natsStatus
_parseJSInfo(void *dst, natsJSON *node)
{
    natsSysJSInfo *info = (natsSysJSInfo *) dst;
    natsStatus     s;

    s = sysclient_scanFields(info, node, _jsInfoFields, SYS_NFIELDS(_jsInfoFields));

    // Config is a nested object; JetStreamStats is flattened into this one.
    IFOK(s, sysclient_objectInline(&info->Config, node, "config",
                                   sysclient_parseJetStreamConfig));
    IFOK(s, sysclient_parseJetStreamStats(&info->JetStreamStats, node));

    IFOK(s, sysclient_objectPtr((void **) &info->Meta, node, "meta_cluster",
                                sizeof(natsSysMetaClusterInfo),
                                sysclient_parseMetaClusterInfo,
                                sysclient_freeMetaClusterInfo));
    IFOK(s, sysclient_ptrArray((void ***) &info->AccountDetails,
                               &info->AccountDetailsCount, node, "account_details",
                               sizeof(natsSysAccountDetail), _parseAccountDetail,
                               _freeAccountDetail));
    return s;
}

static void
_freeJSInfo(natsSysJSInfo *info)
{
    sysclient_freeFields(info, _jsInfoFields, SYS_NFIELDS(_jsInfoFields));
    sysclient_freeJetStreamConfig(&info->Config);
    sysclient_freeObjectPtr((void **) &info->Meta, sysclient_freeMetaClusterInfo);
    sysclient_freePtrArray((void ***) &info->AccountDetails, &info->AccountDetailsCount,
                           _freeAccountDetail);
}

static natsStatus
_respFromMsg(void **newResp, natsMsg *msg)
{
    natsSysJszResp *resp;
    natsStatus      s;

    *newResp = NULL;

    resp = (natsSysJszResp *) NATS_CALLOC(1, sizeof(natsSysJszResp));
    if (resp == NULL)
        return NATS_NO_MEMORY;

    s = sysclient_decodeResp(msg, &resp->Server, &resp->Error, &resp->JSInfo, "data",
                             _parseJSInfo);
    if (s != NATS_OK)
    {
        natsSysJszResp_Destroy(resp);
        return s;
    }

    *newResp = resp;
    return NATS_OK;
}

static void
_destroyResp(void *resp)
{
    natsSysJszResp_Destroy((natsSysJszResp *) resp);
}

natsStatus
natsSysClient_Jsz(natsSysJszResp **newResp, natsSysClient *client, const char *serverID,
                  const natsSysJszOptions *opts, int64_t timeout)
{
    return sysclient_request((void **) newResp, client, serverID, SYS_SUBJ_JSZ,
                             _marshalOptions, opts, 128, timeout,
                             _respFromMsg);
}

natsStatus
natsSysClient_JszPing(natsSysJszRespList *list, natsSysClient *client,
                      const natsSysJszOptions *opts, int64_t timeout)
{
    if (list == NULL)
        return NATS_INVALID_ARG;

    return sysclient_ping((void ***) &list->Resps, &list->Count, client, SYS_SUBJ_JSZ,
                          _marshalOptions, opts, 128, timeout, _respFromMsg,
                          _destroyResp);
}

natsStatus
natsSysClient_JszEach(natsSysClient *client, const char *serverID,
                      const natsSysJszOptions *opts, int64_t timeout,
                      natsSysJszPageHandler handler, void *closure)
{
    natsSysJszOptions page;
    natsSysJszOptions defaults;
    int64_t           deadline;
    int               offset;

    if ((client == NULL) || (handler == NULL))
        return NATS_INVALID_ARG;
    if (timeout < 0)
        return NATS_INVALID_ARG;

    if (opts == NULL)
    {
        natsSysJszOptions_Init(&defaults);
        opts = &defaults;
    }

    if (timeout == 0)
        timeout = NATS_SYS_DEFAULT_REQUEST_TIMEOUT;

    deadline = sysclient_deadline(timeout);

    // Without account details there is nothing to page over: one request,
    // one page.
    if (!opts->Accounts)
    {
        natsSysJszResp *resp = NULL;
        natsStatus      s;

        s = natsSysClient_Jsz(&resp, client, serverID, opts, timeout);
        if (s != NATS_OK)
            return s;

        // Single page: there is nothing left for a false return to stop.
        (void) handler(resp, closure);
        natsSysJszResp_Destroy(resp);
        return NATS_OK;
    }

    offset = opts->Offset;

    for (;;)
    {
        natsSysJszResp *resp = NULL;
        natsStatus      s;
        int64_t         left;
        int             received;
        int             n;
        int             total;
        bool            wantMore;

        left = deadline - sysclient_nowMs();
        if (left <= 0)
            return NATS_TIMEOUT;

        page        = *opts;
        page.Offset = offset;

        s = natsSysClient_Jsz(&resp, client, serverID, &page, left);
        if (s != NATS_OK)
            return s;

        n = resp->JSInfo.AccountDetailsCount;
        // The total is the account count from the flattened JetStreamStats,
        // not a dedicated field as in CONNZ and SUBSZ.
        total    = resp->JSInfo.JetStreamStats.Accounts;
        wantMore = handler(resp, closure);
        natsSysJszResp_Destroy(resp);

        if (!wantMore)
            return NATS_OK;

        received = offset + n;
        if ((received >= total) || (n == 0))
            return NATS_OK;

        offset = received;
    }
}

static void
_walkDestroy(void *w)
{
    natsSysJszWalk *walk = (natsSysJszWalk *) w;

    if (walk == NULL)
        return;

    natsSysJszResp_Destroy(walk->first);
    _freeOptionsCopy(&walk->opts);
    NATS_FREE(walk->serverID);
    NATS_FREE(walk);
}

static natsStatus
_initWalk(void *walkv, natsSysClient *client, const void *optsv, void *pagev)
{
    natsSysJszWalk *walk = (natsSysJszWalk *) walkv;
    natsSysJszResp *page = (natsSysJszResp *) pagev;
    natsStatus      s;

    walk->client = client;

    s = sysclient_walkServerID(&walk->serverID, page->Server.ID);
    IFOK(s, _copyOptions(&walk->opts, (const natsSysJszOptions *) optsv));
    if (s != NATS_OK)
        return s;

    walk->first  = page;
    walk->total  = page->JSInfo.JetStreamStats.Accounts;
    walk->offset = walk->opts.Offset;
    return NATS_OK;
}

static const sysWalkOps _walkOps = {
    sizeof(natsSysJszWalk),
    _initWalk,
    _walkDestroy,
    _destroyResp,
};

natsStatus
natsSysClient_JszPingEach(natsSysJszWalkList *list, natsSysClient *client,
                          const natsSysJszOptions *opts, int64_t timeout)
{
    natsSysJszRespList pages = {NULL, 0};
    natsStatus          s;

    if ((list == NULL) || (client == NULL))
        return NATS_INVALID_ARG;

    list->Walks = NULL;
    list->Count = 0;

    s = natsSysClient_JszPing(&pages, client, opts, timeout);
    if (s != NATS_OK)
    {
        natsSysJszRespList_Destroy(&pages);
        return s;
    }

    return sysclient_buildWalks((void ***) &list->Walks, &list->Count, client,
                                (void ***) &pages.Resps, &pages.Count, opts, &_walkOps);
}

const char *
natsSysJszWalk_ServerID(const natsSysJszWalk *walk)
{
    return (walk != NULL) ? walk->serverID : NULL;
}

natsStatus
natsSysJszWalk_Run(natsSysJszWalk *walk, int64_t timeout, natsSysJszPageHandler handler,
                   void *closure)
{
    int64_t deadline;

    if ((walk == NULL) || (handler == NULL))
        return NATS_INVALID_ARG;
    if (timeout < 0)
        return NATS_INVALID_ARG;
    if (walk->done)
        return NATS_OK;

    if (timeout == 0)
        timeout = NATS_SYS_DEFAULT_REQUEST_TIMEOUT;

    deadline = sysclient_deadline(timeout);

    if (walk->first != NULL)
    {
        natsSysJszResp *resp = walk->first;
        int             n    = resp->JSInfo.AccountDetailsCount;
        bool            wantMore;

        walk->first = NULL;
        wantMore    = handler(resp, closure);
        natsSysJszResp_Destroy(resp);

        walk->offset += n;

        // See the note in connz.c on the empty-page guard.
        // Nothing to page over unless account details were requested; with
        // Accounts unset the loop below would not run anyway (total is 0).
        if (!wantMore || !walk->opts.Accounts || (n == 0))
        {
            walk->done = true;
            return NATS_OK;
        }
    }

    while (walk->offset < walk->total)
    {
        natsSysJszOptions page;
        natsSysJszResp   *resp = NULL;
        natsStatus        s;
        int64_t           left;
        int               n;
        bool              wantMore;

        left = deadline - sysclient_nowMs();
        if (left <= 0)
            return NATS_TIMEOUT;

        page        = walk->opts;
        page.Offset = walk->offset;

        s = natsSysClient_Jsz(&resp, walk->client, walk->serverID, &page, left);
        if (s != NATS_OK)
            return s;

        n        = resp->JSInfo.AccountDetailsCount;
        wantMore = handler(resp, closure);
        natsSysJszResp_Destroy(resp);

        if (!wantMore || (n == 0))
        {
            walk->done = true;
            return NATS_OK;
        }

        walk->offset += n;
    }

    walk->done = true;
    return NATS_OK;
}

void
natsSysJszWalkList_Destroy(natsSysJszWalkList *list)
{
    if (list == NULL)
        return;

    sysclient_freeRespList((void ***) &list->Walks, &list->Count, _walkDestroy);
}

void
natsSysJszResp_Destroy(natsSysJszResp *resp)
{
    if (resp == NULL)
        return;

    sysclient_freeServerInfo(&resp->Server);
    sysclient_freeAPIError(&resp->Error);
    _freeJSInfo(&resp->JSInfo);
    NATS_FREE(resp);
}

void
natsSysJszRespList_Destroy(natsSysJszRespList *list)
{
    if (list == NULL)
        return;

    sysclient_freeRespList((void ***) &list->Resps, &list->Count, _destroyResp);
}
