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

// The walk machinery here mirrors connz.c almost line for line, and jsz.c
// carries a third near-copy. That is deliberate — but only for the page loop,
// and the reason is narrow enough to be worth stating exactly.
//
// What is defended: `resp->Subsz.Total` and `resp->Subsz.SubsCount` are
// checked by the compiler today. Behind a driver they become accessor
// callbacks over a void *, where reaching for the wrong one is a silent error
// in the pagination arithmetic — which is both where this port deliberately
// deviates (the empty-page guard below) and what a reviewer most needs to read
// closely. A shared driver would be shorter, and this is the price of not
// having one.
//
// What is *not* defended: the surrounding _PingEach, _copyOptions and
// _freeOptionsCopy scaffolding is duplicated too, and none of it touches a
// typed payload member — so the argument above says nothing about it. It is
// still triplicated only because collapsing it has not been worth a separate
// change; if a fourth paginated endpoint ever appears, hoist that half first.
//
// Note the page loop is the opposite call from the request/ping path in
// sysclient.c, where the type erasure is confined to an options pointer that
// each _marshalOptions re-types on its first line.

#include "subsz.h"

#include "marshal.h"
#include "sysclientp.h"
#include "unmarshal.h"

#include "buf.h"
#include "os_shims.h"

#include <string.h>

struct __natsSysSubszWalk
{
    natsSysClient      *client;   // borrowed; must outlive the walk
    char               *serverID; // owned
    natsSysSubszOptions opts;     // deep copy
    natsSysSubszResp   *first;    // owned until delivered by the first _Run
    int                 total;    // taken from the first page, never refreshed
    int                 offset;   // offset of the next page to request
    bool                done;
};

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
    if (opts == NULL)
        return NATS_INVALID_ARG;

    memset(opts, 0, sizeof(*opts));
    return NATS_OK;
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

static natsStatus
_copyOptions(natsSysSubszOptions *dst, const natsSysSubszOptions *src)
{
    natsStatus s = NATS_OK;

    natsSysSubszOptions_Init(dst);
    if (src == NULL)
        return NATS_OK;

    dst->Offset        = src->Offset;
    dst->Limit         = src->Limit;
    dst->Subscriptions = src->Subscriptions;

    IFOK(s, sysclient_dupOptStr(&dst->Account, src->Account));
    IFOK(s, sysclient_dupOptStr(&dst->Test, src->Test));

    return s;
}

static void
_freeOptionsCopy(natsSysSubszOptions *opts)
{
    if (opts == NULL)
        return;

    sysclient_freeOptStr(&opts->Account);
    sysclient_freeOptStr(&opts->Test);
    memset(opts, 0, sizeof(*opts));
}

// The sublist keys are flattened into the SUBSZ object, so there is no nested
// value whose absence would signal "no stats"; presence is tested key by key.
static bool
_hasSublistStats(natsJSON *node)
{
    natsJSON *field = NULL;
    int       i;

    for (i = 0; i < SYS_NFIELDS(_sublistStatsFields); i++)
    {
        if (natsJSON_Field(node, _sublistStatsFields[i].Key, &field) == NATS_OK)
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
_freeSubsz(natsSysSubsz *subsz)
{
    sysclient_freeFields(subsz, _subszFields, SYS_NFIELDS(_subszFields));
    sysclient_freeValueArray((void **) &subsz->Subs, &subsz->SubsCount,
                             sizeof(natsSysSubDetail), sysclient_freeSubDetail);
    NATS_FREE(subsz->SublistStats);
    subsz->SublistStats = NULL;
}

static natsStatus
_respFromMsg(void **newResp, natsMsg *msg)
{
    natsSysSubszResp *resp;
    natsStatus        s;

    *newResp = NULL;

    resp = (natsSysSubszResp *) NATS_CALLOC(1, sizeof(natsSysSubszResp));
    if (resp == NULL)
        return NATS_NO_MEMORY;

    s = sysclient_decodeResp(msg, &resp->Server, &resp->Error, &resp->Subsz, "data",
                             _parseSubsz);
    if (s != NATS_OK)
    {
        natsSysSubszResp_Destroy(resp);
        return s;
    }

    *newResp = resp;
    return NATS_OK;
}

static void
_destroyResp(void *resp)
{
    natsSysSubszResp_Destroy((natsSysSubszResp *) resp);
}

natsStatus
natsSysClient_Subsz(natsSysSubszResp **newResp, natsSysClient *client,
                    const char *serverID, const natsSysSubszOptions *opts,
                    int64_t timeout)
{
    return sysclient_request((void **) newResp, client, serverID, SYS_SUBJ_SUBSZ,
                             _marshalOptions, opts, 128, timeout,
                             _respFromMsg);
}

natsStatus
natsSysClient_SubszPing(natsSysSubszRespList *list, natsSysClient *client,
                        const natsSysSubszOptions *opts, int64_t timeout)
{
    if (list == NULL)
        return NATS_INVALID_ARG;

    return sysclient_ping((void ***) &list->Resps, &list->Count, client, SYS_SUBJ_SUBSZ,
                          _marshalOptions, opts, 128, timeout, _respFromMsg,
                          _destroyResp);
}

natsStatus
natsSysClient_SubszEach(natsSysClient *client, const char *serverID,
                        const natsSysSubszOptions *opts, int64_t timeout,
                        natsSysSubszPageHandler handler, void *closure)
{
    natsSysSubszOptions page;
    natsSysSubszOptions defaults;
    int64_t             deadline;
    int                 offset;

    if ((client == NULL) || (handler == NULL))
        return NATS_INVALID_ARG;
    if (timeout < 0)
        return NATS_INVALID_ARG;

    if (opts == NULL)
    {
        natsSysSubszOptions_Init(&defaults);
        opts = &defaults;
    }

    if (timeout == 0)
        timeout = NATS_SYS_DEFAULT_REQUEST_TIMEOUT;

    deadline = sysclient_deadline(timeout);
    offset   = opts->Offset;

    for (;;)
    {
        natsSysSubszResp *resp = NULL;
        natsStatus        s;
        int64_t           left;
        int               received;
        int               n;
        int               total;
        bool              wantMore;

        left = deadline - sysclient_nowMs();
        if (left <= 0)
            return NATS_TIMEOUT;

        page        = *opts;
        page.Offset = offset;

        s = natsSysClient_Subsz(&resp, client, serverID, &page, left);
        if (s != NATS_OK)
            return s;

        n        = resp->Subsz.SubsCount;
        total    = resp->Subsz.Total;
        wantMore = handler(resp, closure);
        natsSysSubszResp_Destroy(resp);

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
    natsSysSubszWalk *walk = (natsSysSubszWalk *) w;

    if (walk == NULL)
        return;

    natsSysSubszResp_Destroy(walk->first);
    _freeOptionsCopy(&walk->opts);
    NATS_FREE(walk->serverID);
    NATS_FREE(walk);
}

natsStatus
natsSysClient_SubszPingEach(natsSysSubszWalkList *list, natsSysClient *client,
                            const natsSysSubszOptions *opts, int64_t timeout)
{
    natsStatus           s;
    natsSysSubszRespList pages = {NULL, 0};
    natsSysSubszWalk   **walks;
    int                  count;
    int                  i;

    if ((list == NULL) || (client == NULL))
        return NATS_INVALID_ARG;

    list->Walks = NULL;
    list->Count = 0;

    s = natsSysClient_SubszPing(&pages, client, opts, timeout);
    if (s != NATS_OK)
    {
        natsSysSubszRespList_Destroy(&pages);
        return s;
    }
    if (pages.Count == 0)
    {
        natsSysSubszRespList_Destroy(&pages);
        return NATS_OK;
    }

    // Captured before the list is destroyed below, which zeroes its Count.
    count = pages.Count;

    walks = (natsSysSubszWalk **) NATS_CALLOC((size_t) count, sizeof(natsSysSubszWalk *));
    if (walks == NULL)
    {
        natsSysSubszRespList_Destroy(&pages);
        return NATS_NO_MEMORY;
    }

    for (i = 0; (i < count) && (s == NATS_OK); i++)
    {
        natsSysSubszWalk *walk;

        walk = (natsSysSubszWalk *) NATS_CALLOC(1, sizeof(natsSysSubszWalk));
        if (walk == NULL)
        {
            s = NATS_NO_MEMORY;
            break;
        }
        walks[i] = walk;

        walk->client = client;

        s = sysclient_walkServerID(&walk->serverID, pages.Resps[i]->Server.ID);
        IFOK(s, _copyOptions(&walk->opts, opts));

        if (s == NATS_OK)
        {
            walk->first    = pages.Resps[i];
            pages.Resps[i] = NULL;
            walk->total    = walk->first->Subsz.Total;
            walk->offset   = walk->opts.Offset;
        }
    }

    natsSysSubszRespList_Destroy(&pages);

    if (s != NATS_OK)
    {
        sysclient_freeRespList((void ***) &walks, &count, _walkDestroy);
        return s;
    }

    list->Walks = walks;
    list->Count = count;
    return NATS_OK;
}

const char *
natsSysSubszWalk_ServerID(const natsSysSubszWalk *walk)
{
    return (walk != NULL) ? walk->serverID : NULL;
}

natsStatus
natsSysSubszWalk_Run(natsSysSubszWalk *walk, int64_t timeout,
                     natsSysSubszPageHandler handler, void *closure)
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
        natsSysSubszResp *resp = walk->first;
        int               n    = resp->Subsz.SubsCount;
        bool              wantMore;

        walk->first = NULL;
        wantMore    = handler(resp, closure);
        natsSysSubszResp_Destroy(resp);

        walk->offset += n;

        // See the note in connz.c: this empty-page guard is a deliberate
        // deviation from orbit.go, whose per-server ping iterators lack it and
        // can spin forever.
        if (!wantMore || (n == 0))
        {
            walk->done = true;
            return NATS_OK;
        }
    }

    while (walk->offset < walk->total)
    {
        natsSysSubszOptions page;
        natsSysSubszResp   *resp = NULL;
        natsStatus          s;
        int64_t             left;
        int                 n;
        bool                wantMore;

        left = deadline - sysclient_nowMs();
        if (left <= 0)
            return NATS_TIMEOUT;

        page        = walk->opts;
        page.Offset = walk->offset;

        s = natsSysClient_Subsz(&resp, walk->client, walk->serverID, &page, left);
        if (s != NATS_OK)
            return s;

        n        = resp->Subsz.SubsCount;
        wantMore = handler(resp, closure);
        natsSysSubszResp_Destroy(resp);

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
natsSysSubszWalkList_Destroy(natsSysSubszWalkList *list)
{
    if (list == NULL)
        return;

    sysclient_freeRespList((void ***) &list->Walks, &list->Count, _walkDestroy);
}

void
natsSysSubszResp_Destroy(natsSysSubszResp *resp)
{
    if (resp == NULL)
        return;

    sysclient_freeServerInfo(&resp->Server);
    sysclient_freeAPIError(&resp->Error);
    _freeSubsz(&resp->Subsz);
    NATS_FREE(resp);
}

void
natsSysSubszRespList_Destroy(natsSysSubszRespList *list)
{
    if (list == NULL)
        return;

    sysclient_freeRespList((void ***) &list->Resps, &list->Count, _destroyResp);
}
