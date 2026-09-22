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

// JSZ paginates over accounts, only when opts->Accounts is set and
// opts->Account is not; the total is the flattened JetStreamStats.Accounts.

#include "jsz.h"

#include "marshal.h"
#include "sysclientp.h"
#include "unmarshal.h"

#include "buf.h"
#include "os_shims.h"

#include <string.h>

static const sysField _raftGroupDetailFields[] = {
    SYS_F(SYS_FLD_STR, natsSysRaftGroupDetail, Name, "name"),
    SYS_F(SYS_FLD_STR, natsSysRaftGroupDetail, RaftGroup, "raft_group"),
};

static const sysField _streamDetailFields[] = {
    SYS_F(SYS_FLD_STR, natsSysStreamDetail, Name, "name"),
    SYS_F(SYS_FLD_STR, natsSysStreamDetail, Created, "created"),
    SYS_F(SYS_FLD_STR, natsSysStreamDetail, RaftGroup, "stream_raft_group"),
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

// sysWalkOps.CopyOptions.
static natsStatus
_copyOptions(void *dstv, const void *srcv)
{
    natsSysJszOptions       *dst = (natsSysJszOptions *) dstv;
    const natsSysJszOptions *src = (const natsSysJszOptions *) srcv;
    natsStatus s   = NATS_OK;

    if (src == NULL)
        return natsSysJszOptions_Init(dst);

    // Struct-copy the scalars, then clear and duplicate the strings so a
    // failure part-way leaves only owned or NULL pointers.
    *dst = *src;
    dst->Account = NULL;
    memset(&dst->Filter, 0, sizeof(dst->Filter));

    IFOK(s, sysclient_dupOptStr(&dst->Account, src->Account));
    IFOK(s, sysclient_copyEventFilter(&dst->Filter, &src->Filter));

    return s;
}

// sysWalkOps.FreeOptions.
static void
_freeOptionsCopy(void *optsv)
{
    natsSysJszOptions *opts = (natsSysJszOptions *) optsv;

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
_freeJSInfo(void *dst)
{
    natsSysJSInfo *info = (natsSysJSInfo *) dst;

    sysclient_freeFields(info, _jsInfoFields, SYS_NFIELDS(_jsInfoFields));
    sysclient_freeJetStreamConfig(&info->Config);
    sysclient_freeObjectPtr((void **) &info->Meta, sysclient_freeMetaClusterInfo);
    sysclient_freePtrArray((void ***) &info->AccountDetails, &info->AccountDetailsCount,
                           _freeAccountDetail);
}

static const sysEndpoint _endpoint = SYS_ENDPOINT(natsSysJszResp, JSInfo, SYS_SUBJ_JSZ, "data",
                                                  _marshalOptions, _parseJSInfo, _freeJSInfo);

natsStatus
natsSysClient_Jsz(natsSysJszResp **newResp, natsSysClient *client,
                  const char *serverID, const natsSysJszOptions *opts,
                  int64_t timeout)
{
    return sysclient_request((void **) newResp, client, serverID, opts, timeout, &_endpoint);
}

natsStatus
natsSysClient_JszPing(natsSysJszRespList *list, natsSysClient *client,
                      const natsSysJszOptions *opts, int64_t timeout)
{
    if (list == NULL)
        return NATS_INVALID_ARG;

    return sysclient_ping((void ***) &list->Resps, &list->Count, client, opts, timeout,
                          &_endpoint);
}

void
natsSysJszResp_Destroy(natsSysJszResp *resp)
{
    sysclient_destroyResp(resp, &_endpoint);
}

void
natsSysJszRespList_Destroy(natsSysJszRespList *list)
{
    if (list == NULL)
        return;

    sysclient_freeList((void ***) &list->Resps, &list->Count, sysclient_destroyResp,
                       &_endpoint);
}

// sysWalkOps.Fetch: the offset is advanced on a shallow copy of the options.
static natsStatus
_fetch(void **page, natsSysClient *client, const char *serverID, const void *optsv,
       int offset, int64_t timeout)
{
    natsSysJszOptions pageOpts;

    if (optsv != NULL)
        pageOpts = *(const natsSysJszOptions *) optsv;
    else
        natsSysJszOptions_Init(&pageOpts);
    pageOpts.Offset = offset;

    return natsSysClient_Jsz((natsSysJszResp **) page, client, serverID, &pageOpts, timeout);
}

// sysWalkOps.Paged: the server pages account details only when asked for
// all of them; a named Account returns just that one at every offset.
static bool
_paged(const void *optsv)
{
    const natsSysJszOptions *opts = (const natsSysJszOptions *) optsv;

    return (opts != NULL) && opts->Accounts && nats_IsStringEmpty(opts->Account);
}

static const sysWalkOps _walkOps = {
    &_endpoint,
    sizeof(natsSysJszOptions),
    SYS_INT_OFF(natsSysJszOptions, Offset),
    SYS_INT_OFF(natsSysJszResp, JSInfo.AccountDetailsCount),
    SYS_INT_OFF(natsSysJszResp, JSInfo.JetStreamStats.Accounts),
    _copyOptions,
    _freeOptionsCopy,
    _fetch,
    _paged,
};

SYS_WALK_SINK(natsSysJszPageHandler, natsSysJszResp)

natsStatus
natsSysClient_JszEach(natsSysClient *client, const char *serverID,
                      const natsSysJszOptions *opts, int64_t timeout,
                      natsSysJszPageHandler handler, void *closure)
{
    _sink sink = {handler, closure};

    if (handler == NULL)
        return NATS_INVALID_ARG;

    return sysclient_walkEach(client, serverID, opts, timeout, _deliver, &sink, &_walkOps);
}

natsStatus
natsSysClient_JszPingEach(natsSysJszWalkList *list, natsSysClient *client,
                          const natsSysJszOptions *opts, int64_t timeout)
{
    if (list == NULL)
        return NATS_INVALID_ARG;

    return sysclient_pingEach((void ***) &list->Walks, &list->Count, client, opts, timeout,
                              &_walkOps);
}

const char *
natsSysJszWalk_ServerID(const natsSysJszWalk *walk)
{
    return sysclient_walkID((const sysWalk *) walk);
}

natsStatus
natsSysJszWalk_Run(natsSysJszWalk *walk, int64_t timeout,
                   natsSysJszPageHandler handler, void *closure)
{
    _sink sink = {handler, closure};

    if (handler == NULL)
        return NATS_INVALID_ARG;

    return sysclient_walkRun((sysWalk *) walk, timeout, _deliver, &sink);
}

void
natsSysJszWalkList_Destroy(natsSysJszWalkList *list)
{
    if (list == NULL)
        return;

    sysclient_freeList((void ***) &list->Walks, &list->Count, sysclient_walkDestroy, NULL);
}
