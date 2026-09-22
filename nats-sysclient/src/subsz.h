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

#ifndef NATS_SYSCLIENT_SUBSZ_H_
#define NATS_SYSCLIENT_SUBSZ_H_

#include "sysclient.h"

#include <nats/nats.h>
#include <stdbool.h>
#include <stdint.h>

#if defined(__cplusplus)
extern "C" {
#endif

/** \defgroup natsSysSubszGroup SUBSZ
 *
 * Server subscriptions, from `$SYS.REQ.SERVER.<id>.SUBSZ`. The result is
 * paginated; #natsSysClient_SubszEach walks every page. SUBSZ has no server
 * filter, so a ping cannot select servers.
 *
 * \warning nats-server pages by offset over a sublist with no stable order
 * (nats-server#7009), so a walk can miss subscriptions and repeat others.
 * For a complete list, request a #natsSysSubszOptions.Limit that covers
 * #natsSysSubsz.Total and take the single page.
 * @{
 */

/** \brief Sublist statistics. */
typedef struct __natsSysSublistStats
{
    uint32_t NumSubs;      ///< Wire key `num_subscriptions`.
    uint32_t NumCache;
    uint64_t NumInserts;
    uint64_t NumRemoves;
    uint64_t NumMatches;
    double   CacheHitRate;
    uint32_t MaxFanout;
    double   AvgFanout;

} natsSysSublistStats;

/** \brief One page of subscriptions. */
typedef struct __natsSysSubsz
{
    char *ID;  ///< Wire key `server_id`.
    char *Now; ///< RFC 3339.

    natsSysSublistStats *SublistStats; ///< `NULL` when the server sent none.

    int Total;  ///< Matching subscriptions across all pages.
    int Offset;
    int Limit;

    natsSysSubDetail *Subs; ///< Wire key `subscriptions_list`.
    int               SubsCount;

} natsSysSubsz;

/** \brief A SUBSZ response. Check `Error.Code` before reading #Subsz. */
typedef struct __natsSysSubszResp
{
    natsSysServerInfo Server;
    natsSysSubsz      Subsz; ///< Wire key `data`.
    natsSysAPIError   Error;

} natsSysSubszResp;

/** \brief Responses gathered by #natsSysClient_SubszPing; see
 * #natsSysHealthzRespList. */
typedef struct __natsSysSubszRespList
{
    natsSysSubszResp **Resps;
    int                Count;

} natsSysSubszRespList;

/** \brief SUBSZ request options.
 *
 * #Offset, #Limit and #Subscriptions are always sent; #Account and #Test
 * are left out when empty.
 */
typedef struct __natsSysSubszOptions
{
    int         Offset;
    int         Limit;
    bool        Subscriptions; ///< Include #natsSysSubsz.Subs.
    const char *Account;
    const char *Test;          ///< Return only subscriptions matching this literal subject.

} natsSysSubszOptions;

/** \brief Called once per page of a walk; see #natsSysConnzPageHandler. */
typedef bool (*natsSysSubszPageHandler)(const natsSysSubszResp *page, void *closure);

/** \brief A resumable pagination over one server. */
typedef struct __natsSysSubszWalk natsSysSubszWalk;

/** \brief One walk per server that answered a ping. */
typedef struct __natsSysSubszWalkList
{
    natsSysSubszWalk **Walks;
    int                Count;

} natsSysSubszWalkList;

/** \brief Initializes options to their defaults (all unset). */
NATS_EXTERN natsStatus
natsSysSubszOptions_Init(natsSysSubszOptions *opts);

/** \brief Requests one page of subscriptions from one server; see
 * #natsSysClient_Healthz. */
NATS_EXTERN natsStatus
natsSysClient_Subsz(natsSysSubszResp **newResp, natsSysClient *client,
                    const char *serverID, const natsSysSubszOptions *opts,
                    int64_t timeout);

/** \brief Requests one page of subscriptions from every server; see
 * #natsSysClient_HealthzPing. */
NATS_EXTERN natsStatus
natsSysClient_SubszPing(natsSysSubszRespList *list, natsSysClient *client,
                        const natsSysSubszOptions *opts, int64_t timeout);

/** \brief Walks every page of subscriptions on one server; see
 * #natsSysClient_ConnzEach. */
NATS_EXTERN natsStatus
natsSysClient_SubszEach(natsSysClient *client, const char *serverID,
                        const natsSysSubszOptions *opts, int64_t timeout,
                        natsSysSubszPageHandler handler, void *closure);

/** \brief Pings every server and returns one walk per reply; see
 * #natsSysClient_ConnzPingEach. */
NATS_EXTERN natsStatus
natsSysClient_SubszPingEach(natsSysSubszWalkList *list, natsSysClient *client,
                            const natsSysSubszOptions *opts, int64_t timeout);

/** \brief Returns the ID of the server a walk covers. */
NATS_EXTERN const char *
natsSysSubszWalk_ServerID(const natsSysSubszWalk *walk);

/** \brief Runs a walk to completion; see #natsSysConnzWalk_Run. */
NATS_EXTERN natsStatus
natsSysSubszWalk_Run(natsSysSubszWalk *walk, int64_t timeout,
                     natsSysSubszPageHandler handler, void *closure);

/** \brief Destroys the contents of a walk list, not the list itself. */
NATS_EXTERN void
natsSysSubszWalkList_Destroy(natsSysSubszWalkList *list);

/** \brief Destroys a response; `NULL` is a no-op. */
NATS_EXTERN void
natsSysSubszResp_Destroy(natsSysSubszResp *resp);

/** \brief Destroys the contents of a list, not the list itself. */
NATS_EXTERN void
natsSysSubszRespList_Destroy(natsSysSubszRespList *list);

/** @} */ // end natsSysSubszGroup

#if defined(__cplusplus)
}
#endif

#endif /* NATS_SYSCLIENT_SUBSZ_H_ */
