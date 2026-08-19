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
 * Server subscription state, from the `$SYS.REQ.SERVER.<id>.SUBSZ` endpoint.
 *
 * This endpoint paginates; use #natsSysClient_SubszEach to walk every page.
 *
 * \note SUBSZ is the one endpoint with no server filter: there is no way to
 * select servers by name, cluster, host, tags or domain.
 *
 * \warning Paging this endpoint can miss subscriptions and repeat others.
 * nats-server pages by offset over a sublist with no stable ordering
 * (nats-server#7009), so successive pages are not successive slices of one
 * consistent list, even though the per-page counts and #natsSysSubsz.Total
 * remain consistent. The page loop here is correct; the pages are not. When
 * you need a complete list, request a #natsSysSubszOptions.Limit large enough
 * to cover #natsSysSubsz.Total and take the single page.
 * @{
 */

/** \brief Sublist statistics for a server.
 *
 * In the payload these keys sit alongside the SUBSZ fields rather than in a
 * nested object. The whole struct is `NULL` when the server sent none of them.
 */
typedef struct __natsSysSublistStats
{
    uint32_t NumSubs;      ///< Subscriptions in the sublist.
    uint32_t NumCache;     ///< Entries in the matching cache.
    uint64_t NumInserts;   ///< Subscriptions ever inserted.
    uint64_t NumRemoves;   ///< Subscriptions ever removed.
    uint64_t NumMatches;   ///< Match operations performed.
    double   CacheHitRate; ///< Fraction of matches served from cache.
    uint32_t MaxFanout;    ///< Largest fanout seen.
    double   AvgFanout;    ///< Average fanout.

} natsSysSublistStats;

/** \brief One page of server subscription state. */
typedef struct __natsSysSubsz
{
    char *ID;  ///< Reporting server's ID (wire key `server_id`).
    char *Now; ///< Time the page was produced, RFC 3339.

    /** \brief Sublist statistics, or `NULL` when the server sent none.
     *
     * These keys are flattened into the enclosing object rather than nested,
     * so the pointer is allocated only when at least one of them is present.
     */
    natsSysSublistStats *SublistStats;

    int Total;  ///< Subscriptions matching the request across all pages.
    int Offset; ///< Offset this page starts at.
    int Limit;  ///< Page size the server applied.

    natsSysSubDetail *Subs;      ///< Subscriptions in this page.
    int               SubsCount; ///< Number of entries in #Subs.

} natsSysSubsz;

/** \brief A SUBSZ response from one server.
 *
 * \warning #Error is decoded but never acted on. Check `Error.Code != 0`
 * before trusting #Subsz.
 */
typedef struct __natsSysSubszResp
{
    natsSysServerInfo Server; ///< Which server answered.
    natsSysSubsz      Subsz;  ///< The payload (wire key `data`).
    natsSysAPIError   Error;  ///< Server-reported error, if any.

} natsSysSubszResp;

/** \brief The responses gathered by #natsSysClient_SubszPing. */
typedef struct __natsSysSubszRespList
{
    natsSysSubszResp **Resps; ///< One response per server that answered.
    int                Count; ///< Number of entries in #Resps.

} natsSysSubszRespList;

/** \brief Options for a SUBSZ request.
 *
 * \note SUBSZ is the mixed case: #Offset, #Limit and #Subscriptions are always
 * sent, so a zeroed options struct marshals to
 * `{"offset":0,"limit":0,"subscriptions":false}`, while #Account and #Test are
 * omitted when empty.
 */
typedef struct __natsSysSubszOptions
{
    int  Offset;        ///< First subscription to return.
    int  Limit;         ///< Maximum subscriptions per page.
    bool Subscriptions; ///< Include #natsSysSubsz.Subs.
    const char *Account; ///< Return only this account's subscriptions.

    /** \brief Return only subscriptions that would match this subject.
     *
     * Must be a literal publish subject, not a wildcard pattern.
     */
    const char *Test;

} natsSysSubszOptions;

/** \brief Invoked once per page of a SUBSZ walk.
 *
 * @param page borrowed, and destroyed by the library as soon as the handler
 * returns. Copy anything you need to keep.
 * @param closure the opaque pointer passed to the walk.
 * @return `true` to fetch the next page, `false` to stop the walk.
 */
typedef bool (*natsSysSubszPageHandler)(const natsSysSubszResp *page, void *closure);

/** \brief An independent, resumable pagination over one server. */
typedef struct __natsSysSubszWalk natsSysSubszWalk;

/** \brief One walk per server that answered a ping. */
typedef struct __natsSysSubszWalkList
{
    natsSysSubszWalk **Walks; ///< One walk per responding server.
    int                Count; ///< Number of entries in #Walks.

} natsSysSubszWalkList;

/** \brief Initialises a #natsSysSubszOptions to its defaults (all unset). */
NATS_EXTERN natsStatus
natsSysSubszOptions_Init(natsSysSubszOptions *opts);

/** \brief Requests one page of subscriptions from one server.
 *
 * @param newResp out-param set to the response; destroy with
 * #natsSysSubszResp_Destroy. Set to `NULL` on error.
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
natsSysClient_Subsz(natsSysSubszResp **newResp, natsSysClient *client,
                    const char *serverID, const natsSysSubszOptions *opts,
                    int64_t timeout);

/** \brief Requests one page of subscriptions from every server in the cluster.
 *
 * \note As with every `*Ping` call, running out of time is normal termination.
 * See #natsSysClient_HealthzPing.
 */
NATS_EXTERN natsStatus
natsSysClient_SubszPing(natsSysSubszRespList *list, natsSysClient *client,
                        const natsSysSubszOptions *opts, int64_t timeout);

/** \brief Walks every page of subscriptions on one server.
 *
 * `timeout` is the budget for the whole walk. See #natsSysClient_ConnzEach for
 * the full description; the semantics are identical.
 */
NATS_EXTERN natsStatus
natsSysClient_SubszEach(natsSysClient *client, const char *serverID,
                        const natsSysSubszOptions *opts, int64_t timeout,
                        natsSysSubszPageHandler handler, void *closure);

/** \brief Pings every server, then hands back one independent walk each.
 *
 * See #natsSysClient_ConnzPingEach; the semantics are identical.
 */
NATS_EXTERN natsStatus
natsSysClient_SubszPingEach(natsSysSubszWalkList *list, natsSysClient *client,
                            const natsSysSubszOptions *opts, int64_t timeout);

/** \brief Returns the ID of the server a walk covers; borrowed.
 *
 * Never `NULL` for a walk handed back by #natsSysClient_SubszPingEach — a
 * reply that did not name its sender is rejected there rather than turned
 * into a walk. Returns `NULL` only if `walk` itself is `NULL`.
 */
NATS_EXTERN const char *
natsSysSubszWalk_ServerID(const natsSysSubszWalk *walk);

/** \brief Drives one walk to completion.
 *
 * See #natsSysConnzWalk_Run; the semantics are identical.
 */
NATS_EXTERN natsStatus
natsSysSubszWalk_Run(natsSysSubszWalk *walk, int64_t timeout,
                     natsSysSubszPageHandler handler, void *closure);

/** \brief Releases the contents of a #natsSysSubszWalkList.
 *
 * Passing `NULL` is a no-op. The list object itself is not freed.
 */
NATS_EXTERN void
natsSysSubszWalkList_Destroy(natsSysSubszWalkList *list);

/** \brief Destroys a response returned by #natsSysClient_Subsz.
 *
 * Passing `NULL` is a no-op.
 */
NATS_EXTERN void
natsSysSubszResp_Destroy(natsSysSubszResp *resp);

/** \brief Releases the contents of a #natsSysSubszRespList.
 *
 * Passing `NULL` is a no-op. The list object itself is not freed.
 */
NATS_EXTERN void
natsSysSubszRespList_Destroy(natsSysSubszRespList *list);

/** @} */ // end natsSysSubszGroup

#if defined(__cplusplus)
}
#endif

#endif /* NATS_SYSCLIENT_SUBSZ_H_ */
