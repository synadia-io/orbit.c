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
 * Periodic server statistics, from the `$SYS.REQ.SERVER.<id>.STATSZ` endpoint.
 * @{
 */

/** \brief A message and byte count, used for both directions of a link. */
typedef struct __natsSysDataStats
{
    int64_t Msgs;  ///< Number of messages.
    int64_t Bytes; ///< Number of bytes.

} natsSysDataStats;

/** \brief Traffic statistics for one route. */
typedef struct __natsSysRouteStat
{
    uint64_t         ID;       ///< Route ID (wire key `rid`).
    char            *Name;     ///< Remote server name.
    natsSysDataStats Sent;     ///< Traffic sent on this route.
    natsSysDataStats Received; ///< Traffic received on this route.
    int              Pending;  ///< Bytes waiting to be written.

} natsSysRouteStat;

/** \brief Traffic statistics for one gateway. */
typedef struct __natsSysGatewayStat
{
    uint64_t         ID;         ///< Gateway ID (wire key `gwid`).
    char            *Name;       ///< Remote gateway name.
    natsSysDataStats Sent;       ///< Traffic sent to this gateway.
    natsSysDataStats Received;   ///< Traffic received from this gateway.
    int              NumInbound; ///< Inbound connections from this gateway.

} natsSysGatewayStat;

/** \brief The statistics one server reports.
 *
 * \note A zero-valued numeric member may mean absent, present-and-zero, or
 * `null`; the payload does not distinguish them.
 */
typedef struct __natsSysServerStats
{
    char                 *Start;            ///< Server start time, RFC 3339; see #natsSysTime_Parse.
    int64_t               Mem;              ///< Memory in use, in bytes.
    int                   Cores;            ///< CPU cores detected.
    double                CPU;              ///< CPU usage, as a percentage.
    int                   Connections;      ///< Current client connections.
    uint64_t              TotalConnections; ///< Client connections since start.
    int                   ActiveAccounts;   ///< Accounts with activity.
    uint32_t              NumSubs;          ///< Current subscriptions (wire key `subscriptions`).
    natsSysDataStats      Sent;             ///< Total traffic sent.
    natsSysDataStats      Received;         ///< Total traffic received.
    int64_t               SlowConsumers;    ///< Connections dropped for being too slow.
    natsSysRouteStat    **Routes;           ///< Per-route statistics.
    int                   RoutesCount;      ///< Number of entries in #Routes.
    natsSysGatewayStat  **Gateways;         ///< Per-gateway statistics.
    int                   GatewaysCount;    ///< Number of entries in #Gateways.
    int                   ActiveServers;    ///< Servers seen in the cluster.
    natsSysJetStreamVarz *JetStream;        ///< JetStream state; `NULL` when absent.

} natsSysServerStats;

/** \brief A STATSZ response from one server.
 *
 * \warning #Error is decoded but never acted on. Check `Error.Code != 0`
 * before trusting #Statsz.
 */
typedef struct __natsSysStatszResp
{
    natsSysServerInfo  Server; ///< Which server answered.

    /** \brief The payload.
     *
     * \note Read from the envelope key `statsz`, not `data`. STATSZ is the one
     * endpoint that differs, and the difference is preserved rather than
     * normalised.
     */
    natsSysServerStats Statsz;

    natsSysAPIError    Error;  ///< Server-reported error, if any.

} natsSysStatszResp;

/** \brief The responses gathered by #natsSysClient_StatszPing.
 *
 * Caller-provided, typically on the stack. #natsSysStatszRespList_Destroy
 * releases the contents, not the list object itself.
 */
typedef struct __natsSysStatszRespList
{
    natsSysStatszResp **Resps; ///< One response per server that answered.
    int                 Count; ///< Number of entries in #Resps.

} natsSysStatszRespList;

/** \brief Options for a STATSZ request.
 *
 * Only the shared server filters apply; STATSZ has no options of its own.
 */
typedef struct __natsSysStatszOptions
{
    natsSysEventFilterOptions Filter; ///< Which servers should answer.

} natsSysStatszOptions;

/** \brief Initialises a #natsSysStatszOptions to its defaults (all unset). */
NATS_EXTERN natsStatus
natsSysStatszOptions_Init(natsSysStatszOptions *opts);

/** \brief Requests statistics from one server.
 *
 * @param newResp out-param set to the response; destroy with
 * #natsSysStatszResp_Destroy. Set to `NULL` on error.
 * @param client the system client.
 * @param serverID the target server's ID. Cannot be `NULL` or empty.
 * @param opts the request options, or `NULL` for the defaults.
 * @param timeout milliseconds to wait, or 0 for
 * #NATS_SYS_DEFAULT_REQUEST_TIMEOUT.
 * @return #NATS_OK on success, #NATS_INVALID_ARG for a bad argument,
 * #NATS_NOT_FOUND when no server with that ID answered, #NATS_TIMEOUT if it
 * did not answer in time, #NATS_ERR for a malformed response.
 */
NATS_EXTERN natsStatus
natsSysClient_Statsz(natsSysStatszResp **newResp, natsSysClient *client,
                     const char *serverID, const natsSysStatszOptions *opts,
                     int64_t timeout);

/** \brief Requests statistics from every server in the cluster.
 *
 * \note As with every `*Ping` call, running out of time is normal termination:
 * a gather that hears nothing returns #NATS_OK with a `Count` of 0. See
 * #natsSysClient_HealthzPing for the full description.
 *
 * @param list out-param populated with the responses; unless the return is
 * #NATS_INVALID_ARG, always release it with #natsSysStatszRespList_Destroy.
 * @param client the system client.
 * @param opts the request options, or `NULL` for the defaults.
 * @param timeout milliseconds to wait, or 0 for
 * #NATS_SYS_DEFAULT_REQUEST_TIMEOUT.
 * @return #NATS_OK on success, #NATS_INVALID_ARG for a bad argument,
 * #NATS_NO_RESPONDERS when nothing is listening on the system subject,
 * #NATS_ERR for a malformed response.
 */
NATS_EXTERN natsStatus
natsSysClient_StatszPing(natsSysStatszRespList *list, natsSysClient *client,
                         const natsSysStatszOptions *opts, int64_t timeout);

/** \brief Destroys a response returned by #natsSysClient_Statsz.
 *
 * Passing `NULL` is a no-op.
 */
NATS_EXTERN void
natsSysStatszResp_Destroy(natsSysStatszResp *resp);

/** \brief Releases the contents of a #natsSysStatszRespList.
 *
 * Passing `NULL` is a no-op. The list object itself is not freed.
 */
NATS_EXTERN void
natsSysStatszRespList_Destroy(natsSysStatszRespList *list);

/** @} */ // end natsSysStatszGroup

#if defined(__cplusplus)
}
#endif

#endif /* NATS_SYSCLIENT_STATSZ_H_ */
