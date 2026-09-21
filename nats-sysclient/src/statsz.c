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

#include "statsz.h"

#include "marshal.h"
#include "sysclientp.h"
#include "unmarshal.h"

#include "buf.h"
#include "os_shims.h"

#include <string.h>

static const sysField _dataStatsFields[] = {
    SYS_F(SYS_FLD_I64, natsSysDataStats, Msgs, "msgs"),
    SYS_F(SYS_FLD_I64, natsSysDataStats, Bytes, "bytes"),
};

static const sysField _routeStatFields[] = {
    SYS_F(SYS_FLD_U64, natsSysRouteStat, ID, "rid"),
    SYS_F(SYS_FLD_STR, natsSysRouteStat, Name, "name"),
    SYS_F(SYS_FLD_I64, natsSysRouteStat, Pending, "pending"),
};

static const sysField _gatewayStatFields[] = {
    SYS_F(SYS_FLD_U64, natsSysGatewayStat, ID, "gwid"),
    SYS_F(SYS_FLD_STR, natsSysGatewayStat, Name, "name"),
    SYS_F(SYS_FLD_INT, natsSysGatewayStat, NumInbound, "inbound_connections"),
};

static const sysField _serverStatsFields[] = {
    SYS_F(SYS_FLD_STR, natsSysServerStats, Start, "start"),
    SYS_F(SYS_FLD_I64, natsSysServerStats, Mem, "mem"),
    SYS_F(SYS_FLD_INT, natsSysServerStats, Cores, "cores"),
    SYS_F(SYS_FLD_DOUBLE, natsSysServerStats, CPU, "cpu"),
    SYS_F(SYS_FLD_INT, natsSysServerStats, Connections, "connections"),
    SYS_F(SYS_FLD_U64, natsSysServerStats, TotalConnections, "total_connections"),
    SYS_F(SYS_FLD_INT, natsSysServerStats, ActiveAccounts, "active_accounts"),
    SYS_F(SYS_FLD_U32, natsSysServerStats, NumSubs, "subscriptions"),
    SYS_F(SYS_FLD_I64, natsSysServerStats, SlowConsumers, "slow_consumers"),
    SYS_F(SYS_FLD_INT, natsSysServerStats, ActiveServers, "active_servers"),
};

natsStatus
natsSysStatszOptions_Init(natsSysStatszOptions *opts)
{
    return sysclient_initOpts(opts, sizeof(*opts));
}

// STATSZ has no options of its own, so the request is the five optional server
// filter keys and nothing else.
static natsStatus
_marshalOptions(natsBuffer *buf, const void *optsv)
{
    const natsSysStatszOptions *opts = (const natsSysStatszOptions *) optsv;

    return sysclient_marshalFilterOnly(buf, (opts != NULL) ? &opts->Filter : NULL);
}

static natsStatus
_parseDataStats(void *dst, natsJSON *node)
{
    return sysclient_scanFields(dst, node, _dataStatsFields, SYS_NFIELDS(_dataStatsFields));
}

static natsStatus
_parseRouteStat(void *dst, natsJSON *node)
{
    natsSysRouteStat *route = (natsSysRouteStat *) dst;
    natsStatus        s;

    s = sysclient_scanFields(route, node, _routeStatFields, SYS_NFIELDS(_routeStatFields));
    IFOK(s, sysclient_objectInline(&route->Sent, node, "sent", _parseDataStats));
    IFOK(s, sysclient_objectInline(&route->Received, node, "received", _parseDataStats));
    return s;
}

static void
_freeRouteStat(void *dst)
{
    sysclient_freeFields(dst, _routeStatFields, SYS_NFIELDS(_routeStatFields));
}

static natsStatus
_parseGatewayStat(void *dst, natsJSON *node)
{
    natsSysGatewayStat *gw = (natsSysGatewayStat *) dst;
    natsStatus          s;

    s = sysclient_scanFields(gw, node, _gatewayStatFields, SYS_NFIELDS(_gatewayStatFields));
    IFOK(s, sysclient_objectInline(&gw->Sent, node, "sent", _parseDataStats));
    IFOK(s, sysclient_objectInline(&gw->Received, node, "received", _parseDataStats));
    return s;
}

static void
_freeGatewayStat(void *dst)
{
    sysclient_freeFields(dst, _gatewayStatFields, SYS_NFIELDS(_gatewayStatFields));
}

static natsStatus
_parseServerStats(void *dst, natsJSON *node)
{
    natsSysServerStats *stats = (natsSysServerStats *) dst;
    natsStatus          s;

    s = sysclient_scanFields(stats, node, _serverStatsFields,
                             SYS_NFIELDS(_serverStatsFields));
    IFOK(s, sysclient_objectInline(&stats->Sent, node, "sent", _parseDataStats));
    IFOK(s, sysclient_objectInline(&stats->Received, node, "received", _parseDataStats));
    IFOK(s, sysclient_ptrArray((void ***) &stats->Routes, &stats->RoutesCount, node,
                               "routes", sizeof(natsSysRouteStat), _parseRouteStat,
                               _freeRouteStat));
    IFOK(s, sysclient_ptrArray((void ***) &stats->Gateways, &stats->GatewaysCount, node,
                               "gateways", sizeof(natsSysGatewayStat), _parseGatewayStat,
                               _freeGatewayStat));
    IFOK(s, sysclient_objectPtr((void **) &stats->JetStream, node, "jetstream",
                                sizeof(natsSysJetStreamVarz), sysclient_parseJetStreamVarz,
                                sysclient_freeJetStreamVarz));
    return s;
}

static void
_freeServerStats(void *dst)
{
    natsSysServerStats *stats = (natsSysServerStats *) dst;

    sysclient_freeFields(stats, _serverStatsFields, SYS_NFIELDS(_serverStatsFields));
    sysclient_freePtrArray((void ***) &stats->Routes, &stats->RoutesCount, _freeRouteStat);
    sysclient_freePtrArray((void ***) &stats->Gateways, &stats->GatewaysCount,
                           _freeGatewayStat);
    sysclient_freeObjectPtr((void **) &stats->JetStream, sysclient_freeJetStreamVarz);
}

// STATSZ is the one endpoint whose payload key is not "data".
static const sysEndpoint _endpoint = SYS_ENDPOINT(natsSysStatszResp, Statsz, SYS_SUBJ_STATSZ, "statsz",
                                                  _marshalOptions, _parseServerStats, _freeServerStats);

natsStatus
natsSysClient_Statsz(natsSysStatszResp **newResp, natsSysClient *client,
                     const char *serverID, const natsSysStatszOptions *opts,
                     int64_t timeout)
{
    return sysclient_request((void **) newResp, client, serverID, opts, timeout, &_endpoint);
}

natsStatus
natsSysClient_StatszPing(natsSysStatszRespList *list, natsSysClient *client,
                         const natsSysStatszOptions *opts, int64_t timeout)
{
    if (list == NULL)
        return NATS_INVALID_ARG;

    return sysclient_ping((void ***) &list->Resps, &list->Count, client, opts, timeout,
                          &_endpoint);
}

void
natsSysStatszResp_Destroy(natsSysStatszResp *resp)
{
    sysclient_destroyResp(resp, &_endpoint);
}

void
natsSysStatszRespList_Destroy(natsSysStatszRespList *list)
{
    if (list == NULL)
        return;

    sysclient_freeList((void ***) &list->Resps, &list->Count, sysclient_destroyResp,
                       &_endpoint);
}
