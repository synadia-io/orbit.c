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

#include "subsz.h"

#include "marshal.h"
#include "sysclientp.h"
#include "unmarshal.h"

#include "buf.h"
#include "os_shims.h"

#include <string.h>

static const sysField _sublistStatsFields[] = {
    SYS_F(SYS_FLD_U32, natsSysSublistStats, NumSubs, "num_subscriptions"),
    SYS_F(SYS_FLD_U32, natsSysSublistStats, NumCache, "num_cache"),
    SYS_F(SYS_FLD_U64, natsSysSublistStats, NumInserts, "num_inserts"),
    SYS_F(SYS_FLD_U64, natsSysSublistStats, NumRemoves, "num_removes"),
    SYS_F(SYS_FLD_U64, natsSysSublistStats, NumMatches, "num_matches"),
    SYS_F(SYS_FLD_DOUBLE, natsSysSublistStats, CacheHitRate, "cache_hit_rate"),
    SYS_F(SYS_FLD_U32, natsSysSublistStats, MaxFanout, "max_fanout"),
    SYS_F(SYS_FLD_DOUBLE, natsSysSublistStats, AvgFanout, "avg_fanout"),
};

static const sysField _subszFields[] = {
    SYS_F(SYS_FLD_STR, natsSysSubsz, ID, "server_id"),
    SYS_F(SYS_FLD_STR, natsSysSubsz, Now, "now"),
    SYS_F(SYS_FLD_INT, natsSysSubsz, Total, "total"),
    SYS_F(SYS_FLD_INT, natsSysSubsz, Offset, "offset"),
    SYS_F(SYS_FLD_INT, natsSysSubsz, Limit, "limit"),
};

natsStatus
natsSysSubszOptions_Init(natsSysSubszOptions *opts)
{
    return sysclient_initOpts(opts, sizeof(*opts));
}

// SUBSZ is the mixed case: offset, limit and subscriptions are always emitted,
// account and test only when set.
static natsStatus
_marshalOptions(natsBuffer *buf, const void *optsv)
{
    const natsSysSubszOptions *opts = (const natsSysSubszOptions *) optsv;
    natsJSONWriter      w;
    natsSysSubszOptions defaults;

    if (opts == NULL)
    {
        natsSysSubszOptions_Init(&defaults);
        opts = &defaults;
    }

    natsJSONWriter_Init(&w, buf);
    natsJSONWriter_StartObject(&w);

    natsJSONWriter_AddInt(&w, "offset", opts->Offset);
    natsJSONWriter_AddInt(&w, "limit", opts->Limit);
    natsJSONWriter_AddBool(&w, "subscriptions", opts->Subscriptions);

    sysclient_optStr(&w, "account", opts->Account);
    sysclient_optStr(&w, "test", opts->Test);

    natsJSONWriter_EndObject(&w);
    return natsJSONWriter_Status(&w);
}

// sysWalkOps.CopyOptions.
static natsStatus
_copyOptions(void *dstv, const void *srcv)
{
    natsSysSubszOptions       *dst = (natsSysSubszOptions *) dstv;
    const natsSysSubszOptions *src = (const natsSysSubszOptions *) srcv;
    natsStatus s   = NATS_OK;

    if (src == NULL)
        return natsSysSubszOptions_Init(dst);

    // Struct-copy the scalars, then clear and duplicate the strings so a
    // failure part-way leaves only owned or NULL pointers.
    *dst = *src;
    dst->Account = NULL;
    dst->Test    = NULL;

    IFOK(s, sysclient_dupOptStr(&dst->Account, src->Account));
    IFOK(s, sysclient_dupOptStr(&dst->Test, src->Test));

    return s;
}

// sysWalkOps.FreeOptions.
static void
_freeOptionsCopy(void *optsv)
{
    natsSysSubszOptions *opts = (natsSysSubszOptions *) optsv;

    if (opts == NULL)
        return;

    sysclient_freeOptStr(&opts->Account);
    sysclient_freeOptStr(&opts->Test);
    memset(opts, 0, sizeof(*opts));
}

// The sublist keys are flattened into the SUBSZ object, so presence is
// tested key by key.
static bool
_hasSublistStats(natsJSON *node)
{
    natsJSON *field = NULL;
    int       i;

    for (i = 0; i < SYS_NFIELDS(_sublistStatsFields); i++)
    {
        if (natsJSON_Lookup(node, _sublistStatsFields[i].Key, &field) == NATS_OK)
            return true;
    }
    return false;
}

static natsStatus
_parseSubsz(void *dst, natsJSON *node)
{
    natsSysSubsz *subsz = (natsSysSubsz *) dst;
    natsStatus    s;

    s = sysclient_scanFields(subsz, node, _subszFields, SYS_NFIELDS(_subszFields));
    IFOK(s, sysclient_valueArray((void **) &subsz->Subs, &subsz->SubsCount, node,
                                 "subscriptions_list", sizeof(natsSysSubDetail),
                                 sysclient_parseSubDetail, sysclient_freeSubDetail));

    if ((s == NATS_OK) && _hasSublistStats(node))
    {
        natsSysSublistStats *stats;

        stats = (natsSysSublistStats *) NATS_CALLOC(1, sizeof(natsSysSublistStats));
        if (stats == NULL)
            return NATS_NO_MEMORY;

        // Read from the parent object: the keys are flattened, not nested.
        s = sysclient_scanFields(stats, node, _sublistStatsFields,
                                 SYS_NFIELDS(_sublistStatsFields));
        if (s != NATS_OK)
        {
            NATS_FREE(stats);
            return s;
        }
        subsz->SublistStats = stats;
    }

    return s;
}

static void
_freeSubsz(void *dst)
{
    natsSysSubsz *subsz = (natsSysSubsz *) dst;

    sysclient_freeFields(subsz, _subszFields, SYS_NFIELDS(_subszFields));
    sysclient_freeValueArray((void **) &subsz->Subs, &subsz->SubsCount,
                             sizeof(natsSysSubDetail), sysclient_freeSubDetail);
    NATS_FREE(subsz->SublistStats);
    subsz->SublistStats = NULL;
}

static const sysEndpoint _endpoint = SYS_ENDPOINT(natsSysSubszResp, Subsz, SYS_SUBJ_SUBSZ, "data",
                                                  _marshalOptions, _parseSubsz, _freeSubsz);

natsStatus
natsSysClient_Subsz(natsSysSubszResp **newResp, natsSysClient *client,
                    const char *serverID, const natsSysSubszOptions *opts,
                    int64_t timeout)
{
    return sysclient_request((void **) newResp, client, serverID, opts, timeout, &_endpoint);
}

natsStatus
natsSysClient_SubszPing(natsSysSubszRespList *list, natsSysClient *client,
                        const natsSysSubszOptions *opts, int64_t timeout)
{
    if (list == NULL)
        return NATS_INVALID_ARG;

    return sysclient_ping((void ***) &list->Resps, &list->Count, client, opts, timeout,
                          &_endpoint);
}

void
natsSysSubszResp_Destroy(natsSysSubszResp *resp)
{
    sysclient_destroyResp(resp, &_endpoint);
}

void
natsSysSubszRespList_Destroy(natsSysSubszRespList *list)
{
    if (list == NULL)
        return;

    sysclient_freeList((void ***) &list->Resps, &list->Count, sysclient_destroyResp,
                       &_endpoint);
}

static const sysWalkOps _walkOps = {
    &_endpoint,
    sizeof(natsSysSubszOptions),
    SYS_INT_OFF(natsSysSubszOptions, Offset),
    SYS_INT_OFF(natsSysSubszResp, Subsz.SubsCount),
    SYS_INT_OFF(natsSysSubszResp, Subsz.Total),
    _copyOptions,
    _freeOptionsCopy,
    NULL,
};

SYS_WALK_SINK(natsSysSubszPageHandler, natsSysSubszResp)

natsStatus
natsSysClient_SubszEach(natsSysClient *client, const char *serverID,
                        const natsSysSubszOptions *opts, int64_t timeout,
                        natsSysSubszPageHandler handler, void *closure)
{
    _sink sink = {handler, closure};

    if (handler == NULL)
        return NATS_INVALID_ARG;

    return sysclient_walkEach(client, serverID, opts, timeout, _deliver, &sink, &_walkOps);
}

natsStatus
natsSysClient_SubszPingEach(natsSysSubszWalkList *list, natsSysClient *client,
                            const natsSysSubszOptions *opts, int64_t timeout)
{
    if (list == NULL)
        return NATS_INVALID_ARG;

    return sysclient_pingEach((void ***) &list->Walks, &list->Count, client, opts, timeout,
                              &_walkOps);
}

const char *
natsSysSubszWalk_ServerID(const natsSysSubszWalk *walk)
{
    return sysclient_walkID((const sysWalk *) walk);
}

natsStatus
natsSysSubszWalk_Run(natsSysSubszWalk *walk, int64_t timeout,
                     natsSysSubszPageHandler handler, void *closure)
{
    _sink sink = {handler, closure};

    if (handler == NULL)
        return NATS_INVALID_ARG;

    return sysclient_walkRun((sysWalk *) walk, timeout, _deliver, &sink);
}

void
natsSysSubszWalkList_Destroy(natsSysSubszWalkList *list)
{
    if (list == NULL)
        return;

    sysclient_freeList((void ***) &list->Walks, &list->Count, sysclient_walkDestroy, NULL);
}
