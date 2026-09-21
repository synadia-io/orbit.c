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

#ifndef NATS_SYSCLIENT_STATSZ_H_
#define NATS_SYSCLIENT_STATSZ_H_

#include "sysclient.h"

#include <nats/nats.h>
#include <stdbool.h>
#include <stdint.h>

#if defined(__cplusplus)
extern "C" {
#endif

/** \defgroup natsSysStatszGroup STATSZ
 *
 * Server statistics, from `$SYS.REQ.SERVER.<id>.STATSZ`.
 * @{
 */

/** \brief A message and byte count. */
typedef struct __natsSysDataStats
{
    int64_t Msgs;
    int64_t Bytes;

} natsSysDataStats;

/** \brief Traffic on one route. */
typedef struct __natsSysRouteStat
{
    uint64_t         ID;      ///< Wire key `rid`.
    char            *Name;
    natsSysDataStats Sent;
    natsSysDataStats Received;
    int64_t          Pending; ///< Bytes waiting to be written; may exceed 2GB.

} natsSysRouteStat;

/** \brief Traffic on one gateway. */
typedef struct __natsSysGatewayStat
{
    uint64_t         ID;         ///< Wire key `gwid`.
    char            *Name;
    natsSysDataStats Sent;
    natsSysDataStats Received;
    int              NumInbound; ///< Wire key `inbound_connections`.

} natsSysGatewayStat;

/** \brief The statistics one server reports. */
typedef struct __natsSysServerStats
{
    char                 *Start; ///< RFC 3339; see #natsSysTime_Parse.
    int64_t               Mem;   ///< Bytes.
    int                   Cores;
    double                CPU;   ///< Percent.
    int                   Connections;
    uint64_t              TotalConnections;
    int                   ActiveAccounts;
    uint32_t              NumSubs; ///< Wire key `subscriptions`.
    natsSysDataStats      Sent;
    natsSysDataStats      Received;
    int64_t               SlowConsumers;
    natsSysRouteStat    **Routes;
    int                   RoutesCount;
    natsSysGatewayStat  **Gateways;
    int                   GatewaysCount;
    int                   ActiveServers;
    natsSysJetStreamVarz *JetStream; ///< `NULL` when not reported.

} natsSysServerStats;

/** \brief A STATSZ response. Check `Error.Code` before reading #Statsz. */
typedef struct __natsSysStatszResp
{
    natsSysServerInfo  Server;
    natsSysServerStats Statsz; ///< Wire key `statsz`, unlike every other endpoint.
    natsSysAPIError    Error;

} natsSysStatszResp;

/** \brief Responses gathered by #natsSysClient_StatszPing; see
 * #natsSysHealthzRespList. */
typedef struct __natsSysStatszRespList
{
    natsSysStatszResp **Resps;
    int                 Count;

} natsSysStatszRespList;

/** \brief STATSZ request options: only the server filter. */
typedef struct __natsSysStatszOptions
{
    natsSysEventFilterOptions Filter;

} natsSysStatszOptions;

/** \brief Initializes options to their defaults (all unset). */
NATS_EXTERN natsStatus
natsSysStatszOptions_Init(natsSysStatszOptions *opts);

/** \brief Requests statistics from one server; see #natsSysClient_Healthz. */
NATS_EXTERN natsStatus
natsSysClient_Statsz(natsSysStatszResp **newResp, natsSysClient *client,
                     const char *serverID, const natsSysStatszOptions *opts,
                     int64_t timeout);

/** \brief Requests statistics from every server; see
 * #natsSysClient_HealthzPing. */
NATS_EXTERN natsStatus
natsSysClient_StatszPing(natsSysStatszRespList *list, natsSysClient *client,
                         const natsSysStatszOptions *opts, int64_t timeout);

/** \brief Destroys a response; `NULL` is a no-op. */
NATS_EXTERN void
natsSysStatszResp_Destroy(natsSysStatszResp *resp);

/** \brief Destroys the contents of a list, not the list itself. */
NATS_EXTERN void
natsSysStatszRespList_Destroy(natsSysStatszRespList *list);

/** @} */ // end natsSysStatszGroup

#if defined(__cplusplus)
}
#endif

#endif /* NATS_SYSCLIENT_STATSZ_H_ */
