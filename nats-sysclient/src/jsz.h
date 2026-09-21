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

#ifndef NATS_SYSCLIENT_JSZ_H_
#define NATS_SYSCLIENT_JSZ_H_

#include "sysclient.h"

#include <nats/nats.h>
#include <stdbool.h>
#include <stdint.h>

#if defined(__cplusplus)
extern "C" {
#endif

/** \defgroup natsSysJszGroup JSZ
 *
 * JetStream state, from the `$SYS.REQ.SERVER.<id>.JSZ` endpoint.
 *
 * This endpoint paginates over **accounts**, and a walk pages only when
 * #natsSysJszOptions.Accounts is set. The server also returns account
 * details when #natsSysJszOptions.Streams, #natsSysJszOptions.Consumer or
 * #natsSysJszOptions.Account is set, but a walk treats such a request as a
 * single page.
 * @{
 */

/** \brief The Raft group backing one consumer. */
typedef struct __natsSysRaftGroupDetail
{
    char *Name;      ///< Consumer name.
    char *RaftGroup; ///< Raft group name.

} natsSysRaftGroupDetail;

/** \brief Details of one stream.
 *
 * \note Six members carry the server's JSON verbatim rather than a decoded
 * structure. They describe JetStream assets — cluster info, stream config and
 * state, consumers, mirror and sources — and while cnats declares matching
 * types (`jsStreamConfig` and friends) it exposes no public JSON unmarshaller
 * for them, and orbit.c ships no model of its own. The text is sliced out of
 * the reply exactly as the server sent it, so nothing is lost, whereas
 * decoding only the fields we happen to model would silently drop the rest.
 * Feed them to any JSON reader when you need them.
 */
typedef struct __natsSysStreamDetail
{
    char *Name;    ///< Stream name.
    char *Created; ///< Creation time, RFC 3339; see #natsSysTime_Parse.

    char *ClusterJSON; ///< Raw `cluster` object; `NULL` when absent.
    char *ConfigJSON;  ///< Raw `config` object; `NULL` when absent.
    char *StateJSON;   ///< Raw `state` object; `NULL` when absent.
    char *MirrorJSON;  ///< Raw `mirror` object; `NULL` when absent.

    char **ConsumerJSON;      ///< Raw `consumer_detail` elements.
    int    ConsumerJSONCount; ///< Number of entries in #ConsumerJSON.
    char **SourcesJSON;       ///< Raw `sources` elements.
    int    SourcesJSONCount;  ///< Number of entries in #SourcesJSON.

    char *RaftGroup; ///< Stream Raft group (wire key `stream_raft_group`).

    natsSysRaftGroupDetail **ConsumerRaftGroups;      ///< Per-consumer Raft groups.
    int                      ConsumerRaftGroupsCount; ///< Number of entries above.

} natsSysStreamDetail;

/** \brief JetStream details for one account. */
typedef struct __natsSysAccountDetail
{
    char *Name; ///< Account name.
    char *Id;   ///< Account ID.

    /** \brief Resource usage, flattened into this object on the wire.
     *
     * These keys sit alongside `name` and `id` rather than in a nested
     * object.
     */
    natsSysJetStreamStats JetStreamStats;

    natsSysStreamDetail *Streams;      ///< Stream details (wire key `stream_detail`).
    int                  StreamsCount; ///< Number of entries in #Streams.

} natsSysAccountDetail;

/** \brief One page of a server's JetStream state. */
typedef struct __natsSysJSInfo
{
    char *ID;  ///< Reporting server's ID (wire key `server_id`).
    char *Now; ///< Time the page was produced, RFC 3339.

    bool                   Disabled; ///< Whether JetStream is disabled here.
    natsSysJetStreamConfig Config;   ///< This server's JetStream configuration.

    /** \brief Resource usage, flattened into this object on the wire.
     *
     * \note The pagination total lives here, as
     * `JetStreamStats.Accounts` — there is no `Total` member, unlike CONNZ
     * and SUBSZ.
     */
    natsSysJetStreamStats JetStreamStats;

    int      Streams;   ///< Streams on this server.
    int      Consumers; ///< Consumers on this server.
    uint64_t Messages;  ///< Messages stored on this server.
    uint64_t Bytes;     ///< Bytes stored on this server.

    natsSysMetaClusterInfo *Meta; ///< Meta group (wire key `meta_cluster`); `NULL` when absent.

    natsSysAccountDetail **AccountDetails;      ///< Per-account details.
    int                    AccountDetailsCount; ///< Number of entries above.

} natsSysJSInfo;

/** \brief A JSZ response from one server.
 *
 * \warning #Error is decoded but never acted on. Check `Error.Code != 0`
 * before trusting #JSInfo.
 */
typedef struct __natsSysJszResp
{
    natsSysServerInfo Server; ///< Which server answered.
    natsSysJSInfo     JSInfo; ///< The payload (wire key `data`).
    natsSysAPIError   Error;  ///< Server-reported error, if any.

} natsSysJszResp;

/** \brief The responses gathered by #natsSysClient_JszPing. */
typedef struct __natsSysJszRespList
{
    natsSysJszResp **Resps; ///< One response per server that answered.
    int              Count; ///< Number of entries in #Resps.

} natsSysJszRespList;

/** \brief Options for a JSZ request.
 *
 * Every field is optional and a zero-valued one is left out of the request.
 */
typedef struct __natsSysJszOptions
{
    /** \brief Report only this account.
     *
     * \note Takes precedence over #Accounts on the server, which then
     * ignores #Offset and #Limit. A walk with both set therefore delivers
     * the same single account once per page until the reported account
     * count is covered.
     */
    const char *Account;

    bool Accounts;         ///< Include per-account details, and enable pagination.
    bool Streams;          ///< Include stream details; implies #Accounts server-side.
    bool Consumer;         ///< Include consumer details; implies #Streams server-side.
    bool Config;           ///< Include stream and consumer configuration.
    bool LeaderOnly;       ///< Only the meta leader answers.
    int  Offset;           ///< First account to return.
    int  Limit;            ///< Maximum accounts per page.
    bool RaftGroups;       ///< Include Raft group detail (wire key `raft`).
    bool StreamLeaderOnly; ///< Only report streams this server leads.

    natsSysEventFilterOptions Filter; ///< Which servers should answer.

} natsSysJszOptions;

/** \brief Invoked once per page of a JSZ walk.
 *
 * @param page borrowed, and destroyed by the library as soon as the handler
 * returns. Copy anything you need to keep.
 * @param closure the opaque pointer passed to the walk.
 * @return `true` to fetch the next page, `false` to stop the walk.
 */
typedef bool (*natsSysJszPageHandler)(const natsSysJszResp *page, void *closure);

/** \brief An independent, resumable pagination over one server. */
typedef struct __natsSysJszWalk natsSysJszWalk;

/** \brief One walk per server that answered a ping. */
typedef struct __natsSysJszWalkList
{
    natsSysJszWalk **Walks; ///< One walk per responding server.
    int              Count; ///< Number of entries in #Walks.

} natsSysJszWalkList;

/** \brief Initialises a #natsSysJszOptions to its defaults (all unset). */
NATS_EXTERN natsStatus
natsSysJszOptions_Init(natsSysJszOptions *opts);

/** \brief Requests JetStream state from one server.
 *
 * @param newResp out-param set to the response; destroy with
 * #natsSysJszResp_Destroy. Set to `NULL` on error.
 * @param client the system client.
 * @param serverID the target server's ID. Cannot be `NULL` or empty.
 * @param opts the request options, or `NULL` for the defaults.
 * @param timeout milliseconds to wait, or 0 for
 * #NATS_SYS_DEFAULT_REQUEST_TIMEOUT.
 * @return #NATS_OK on success, #NATS_INVALID_ARG for a bad argument,
 * #NATS_NOT_FOUND when no server with that ID answered, #NATS_TIMEOUT if it
 * did not answer in time, #NATS_ERR for a malformed response,
 * #NATS_NO_MEMORY on allocation failure.
 */
NATS_EXTERN natsStatus
natsSysClient_Jsz(natsSysJszResp **newResp, natsSysClient *client,
                  const char *serverID, const natsSysJszOptions *opts, int64_t timeout);

/** \brief Requests JetStream state from every server in the cluster.
 *
 * \note As with every `*Ping` call, running out of time is normal termination.
 * See #natsSysClient_HealthzPing.
 */
NATS_EXTERN natsStatus
natsSysClient_JszPing(natsSysJszRespList *list, natsSysClient *client,
                      const natsSysJszOptions *opts, int64_t timeout);

/** \brief Walks every page of account details on one server.
 *
 * \note Pagination applies only when #natsSysJszOptions.Accounts is set.
 * Without it this issues a single request and invokes `handler` exactly once.
 *
 * `timeout` is the budget for the whole walk. See #natsSysClient_ConnzEach for
 * the full description.
 */
NATS_EXTERN natsStatus
natsSysClient_JszEach(natsSysClient *client, const char *serverID,
                      const natsSysJszOptions *opts, int64_t timeout,
                      natsSysJszPageHandler handler, void *closure);

/** \brief Pings every server, then hands back one independent walk each.
 *
 * See #natsSysClient_ConnzPingEach; the semantics are identical, except that a
 * walk paginates only when #natsSysJszOptions.Accounts was set and the server
 * reported at least one account.
 */
NATS_EXTERN natsStatus
natsSysClient_JszPingEach(natsSysJszWalkList *list, natsSysClient *client,
                          const natsSysJszOptions *opts, int64_t timeout);

/** \brief Returns the ID of the server a walk covers; borrowed.
 *
 * Never `NULL` for a walk handed back by #natsSysClient_JszPingEach — a
 * reply that did not name its sender is rejected there rather than turned
 * into a walk. Returns `NULL` only if `walk` itself is `NULL`.
 */
NATS_EXTERN const char *
natsSysJszWalk_ServerID(const natsSysJszWalk *walk);

/** \brief Drives one walk to completion.
 *
 * See #natsSysConnzWalk_Run; the semantics are identical.
 */
NATS_EXTERN natsStatus
natsSysJszWalk_Run(natsSysJszWalk *walk, int64_t timeout,
                   natsSysJszPageHandler handler, void *closure);

/** \brief Releases the contents of a #natsSysJszWalkList.
 *
 * Passing `NULL` is a no-op. The list object itself is not freed.
 */
NATS_EXTERN void
natsSysJszWalkList_Destroy(natsSysJszWalkList *list);

/** \brief Destroys a response returned by #natsSysClient_Jsz.
 *
 * Passing `NULL` is a no-op.
 */
NATS_EXTERN void
natsSysJszResp_Destroy(natsSysJszResp *resp);

/** \brief Releases the contents of a #natsSysJszRespList.
 *
 * Passing `NULL` is a no-op. The list object itself is not freed.
 */
NATS_EXTERN void
natsSysJszRespList_Destroy(natsSysJszRespList *list);

/** @} */ // end natsSysJszGroup

#if defined(__cplusplus)
}
#endif

#endif /* NATS_SYSCLIENT_JSZ_H_ */
