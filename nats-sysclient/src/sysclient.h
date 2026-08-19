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

#ifndef NATS_SYSCLIENT_H_
#define NATS_SYSCLIENT_H_

#include <nats/nats.h>
#include <stdbool.h>
#include <stdint.h>

#if defined(__cplusplus)
extern "C" {
#endif

/** \defgroup natsSysClientGroup NATS System Client
 *
 * A client for the NATS server monitoring endpoints published over the `$SYS`
 * account: `VARZ`, `STATSZ`, `CONNZ`, `SUBSZ`, `HEALTHZ` and `JSZ`.
 *
 * Every endpoint can be queried two ways: from one server by its ID, or from
 * every server in the cluster by scattering the request and gathering the
 * replies. The connection handed to #natsSysClient_Create must be authenticated
 * to the system account, otherwise no server will answer.
 *
 * Response structures follow the NATS server v2.10.23 wire format.
 * @{
 */

/** \defgroup natsSysClientTypesGroup Types
 *
 *  System client types.
 *  @{
 */

/** \brief Default request timeout, in milliseconds.
 *
 * Used whenever a request function is passed a `timeout` of 0.
 */
#define NATS_SYS_DEFAULT_REQUEST_TIMEOUT (10000)

/** \brief Default stall interval, in milliseconds.
 *
 * Once the first reply to a scatter-gather has arrived, the gather ends if no
 * further reply turns up within this window.
 */
#define NATS_SYS_DEFAULT_STALL (300)

/** \brief A client for the NATS system monitoring endpoints.
 *
 * Created with #natsSysClient_Create and released with #natsSysClient_Destroy.
 * The client borrows its #natsConnection and never closes it.
 *
 * The client holds no mutable state after creation, so it is safe to use from
 * multiple threads concurrently.
 */
typedef struct __natsSysClient natsSysClient;

/** \brief Options controlling how a scatter-gather request terminates.
 *
 * Initialise with #natsSysClientOpts_Init before setting fields. Both settings
 * apply only to the `*Ping` calls; a request aimed at a single server ID always
 * waits for exactly one reply.
 */
typedef struct __natsSysClientOpts
{
    /** \brief Number of servers to wait for a reply from.
     *
     * The gather stops as soon as this many replies have arrived. Set it to
     * the number of servers in the cluster as seen by the connected server.
     * Leave at the #natsSysClientOpts_Init default of -1 to rely on
     * #StallInterval alone.
     */
    int ServerCount;

    /** \brief Stall interval in milliseconds; see #NATS_SYS_DEFAULT_STALL. */
    int64_t StallInterval;

} natsSysClientOpts;

/** \brief Identifies the server that produced a response.
 *
 * Present on every response, decoded from the envelope's `server` object.
 */
typedef struct __natsSysServerInfo
{
    char    *Name;      ///< Configured server name.
    char    *Host;      ///< Host the server is listening on.
    char    *ID;        ///< Server ID; the value to pass as `serverID`.
    char    *Cluster;   ///< Cluster name; `NULL` when not clustered.
    char    *Domain;    ///< JetStream domain; `NULL` when unset.
    char    *Version;   ///< Server version (wire key `ver`).
    char   **Tags;      ///< Server tags.
    int      TagsCount; ///< Number of entries in #Tags.
    uint64_t Seq;       ///< Sequence number of this event.
    bool     JetStream; ///< Whether JetStream is enabled.
    char    *Time;      ///< Event timestamp as RFC 3339 text; see #natsSysTime_Parse.

} natsSysServerInfo;

/** \brief An error reported by the server in a response envelope.
 *
 * \warning The client decodes this field but never acts on it. A response
 * carrying an error still returns #NATS_OK with a zeroed payload, so check
 * `Code != 0` before trusting the payload.
 */
typedef struct __natsSysAPIError
{
    int      Code;        ///< HTTP-like status code; 0 when no error.
    uint16_t ErrCode;     ///< Server error code.
    char    *Description; ///< Human-readable description; `NULL` when absent.

} natsSysAPIError;

/** \brief Filters that select which servers answer a scatter-gather request.
 *
 * Every field is optional; a zero-valued field is left out of the request.
 * These keys are flattened into the request object rather than nested. VARZ,
 * STATSZ, CONNZ and JSZ carry this filter; SUBSZ has none.
 */
typedef struct __natsSysEventFilterOptions
{
    const char  *Name;      ///< Match one server by name (wire key `server_name`).
    const char  *Cluster;   ///< Match servers in this cluster.
    const char  *Host;      ///< Match servers on this host.
    const char **Tags;      ///< Match servers carrying all of these tags.
    int          TagsCount; ///< Number of entries in #Tags.
    const char  *Domain;    ///< Match servers in this JetStream domain.

} natsSysEventFilterOptions;

/** \brief Details of one subscription.
 *
 * Returned by CONNZ (per connection) and by SUBSZ (per server), which is why
 * it lives here rather than in either endpoint's header.
 */
typedef struct __natsSysSubDetail
{
    char   *Account; ///< Account the subscription belongs to.
    char   *Subject; ///< Subject subscribed to.
    char   *Queue;   ///< Queue group name (wire key `qgroup`); `NULL` when none.
    char   *Sid;     ///< Subscription ID, as chosen by the client.
    int64_t Msgs;    ///< Messages delivered to this subscription.
    int64_t Max;     ///< Auto-unsubscribe threshold; 0 when unset.
    uint64_t Cid;    ///< Connection ID owning the subscription.

} natsSysSubDetail;

//
// Types shared by more than one endpoint.
//
// Grouping them here keeps the endpoint headers independent of one another:
// `SlowConsumersStats` is used by VARZ but belongs to STATSZ's payload, and
// `JetStreamVarz` the other way round, so declaring each beside its own
// endpoint would make varz.h and statsz.h mutually dependent.
//

/** \brief Counts of connections dropped for being too slow. */
typedef struct __natsSysSlowConsumersStats
{
    uint64_t Clients;  ///< Slow client connections.
    uint64_t Routes;   ///< Slow route connections.
    uint64_t Gateways; ///< Slow gateway connections.
    uint64_t Leafs;    ///< Slow leaf node connections.

} natsSysSlowConsumersStats;

/** \brief Counts of JetStream API calls. */
typedef struct __natsSysJetStreamAPIStats
{
    uint64_t Total;    ///< API requests handled.
    uint64_t Errors;   ///< API requests that failed.
    uint64_t Inflight; ///< API requests currently being handled.

} natsSysJetStreamAPIStats;

/** \brief JetStream resource usage for a server or account.
 *
 * In the JSZ payload these members are flattened into the enclosing object
 * rather than nested.
 */
typedef struct __natsSysJetStreamStats
{
    uint64_t                 Memory;         ///< Memory in use, in bytes.
    uint64_t                 Store;          ///< Storage in use, in bytes.
    uint64_t                 ReservedMemory; ///< Memory reserved, in bytes.
    uint64_t                 ReservedStore;  ///< Storage reserved, in bytes.
    int                      Accounts;       ///< Number of accounts using JetStream.
    int                      HAAssets;       ///< Number of highly-available assets.
    natsSysJetStreamAPIStats API;            ///< API call counts.

} natsSysJetStreamStats;

/** \brief A server's JetStream configuration. */
typedef struct __natsSysJetStreamConfig
{
    int64_t MaxMemory; ///< Maximum memory, in bytes.
    int64_t MaxStore;  ///< Maximum storage, in bytes.
    char   *StoreDir;  ///< Storage directory.

    /** \brief Sync interval in **nanoseconds**.
     *
     * \note The server sends this as an integer count of nanoseconds. Every
     * other timeout in orbit.c is in milliseconds, so convert before comparing.
     */
    int64_t SyncInterval;

    bool  SyncAlways; ///< Whether every write is synced.
    char *Domain;     ///< JetStream domain name.
    bool  CompressOK; ///< Whether the server supports compression.
    char *UniqueTag;  ///< Tag used to keep replicas apart.

} natsSysJetStreamConfig;

/** \brief One peer in a Raft group. */
typedef struct __natsSysPeerInfo
{
    char *Name;    ///< Server name.
    bool  Current; ///< Whether the peer is up to date.
    bool  Offline; ///< Whether the peer is unreachable.

    /** \brief Time since the peer was last seen, in **nanoseconds**. */
    int64_t Active;

    uint64_t Lag;  ///< How far behind the peer is.
    char    *Peer; ///< Opaque peer ID.

} natsSysPeerInfo;

/** \brief The JetStream meta group (the Raft group managing assets). */
typedef struct __natsSysMetaClusterInfo
{
    char              *Name;          ///< Meta cluster name.
    char              *Leader;        ///< Server name of the leader.
    char              *Peer;          ///< This server's peer ID.
    natsSysPeerInfo  **Replicas;      ///< Other peers in the group.
    int                ReplicasCount; ///< Number of entries in #Replicas.
    int                Size;          ///< Number of servers in the group.
    int                Pending;       ///< Pending meta-layer operations.

} natsSysMetaClusterInfo;

/** \brief A server's JetStream state.
 *
 * Each member is `NULL` when the server did not report it.
 */
typedef struct __natsSysJetStreamVarz
{
    natsSysJetStreamConfig *Config; ///< Configuration; `NULL` when absent.
    natsSysJetStreamStats  *Stats;  ///< Resource usage; `NULL` when absent.
    natsSysMetaClusterInfo *Meta;   ///< Meta group; `NULL` when absent.

} natsSysJetStreamVarz;

/** @} */ // end natsSysClientTypesGroup

/** \defgroup natsSysClientFuncGroup Functions
 *
 *  System client functions.
 *  @{
 */

/** \brief Initialises a #natsSysClientOpts to its defaults.
 *
 * Sets #StallInterval to #NATS_SYS_DEFAULT_STALL and #ServerCount to -1
 * (disabled).
 *
 * @param opts the options struct to initialise; cannot be `NULL`.
 * @return #NATS_OK on success, #NATS_INVALID_ARG if `opts` is `NULL`.
 */
NATS_EXTERN natsStatus
natsSysClientOpts_Init(natsSysClientOpts *opts);

/** \brief Creates a system client over an existing connection.
 *
 * The connection must be authenticated to the `$SYS` account and must stay open
 * for the lifetime of the client. It is borrowed, not owned: destroying the
 * client leaves the connection untouched.
 *
 * @param newClient out-param set to the new client, or `NULL` on error.
 * @param nc the NATS connection; borrowed, must not be `NULL`.
 * @param opts the options, or `NULL` for the #natsSysClientOpts_Init defaults.
 * @return #NATS_OK on success, #NATS_INVALID_ARG for `NULL` arguments or for
 * an out-of-range option (a #natsSysClientOpts.ServerCount that is 0 or below
 * -1, or a #natsSysClientOpts.StallInterval below 0), #NATS_NO_MEMORY on
 * allocation failure.
 */
NATS_EXTERN natsStatus
natsSysClient_Create(natsSysClient **newClient, natsConnection *nc,
                     const natsSysClientOpts *opts);

/** \brief Destroys a client created by #natsSysClient_Create.
 *
 * Passing `NULL` is a no-op. The underlying connection is not closed.
 *
 * @param client the client to destroy.
 */
NATS_EXTERN void
natsSysClient_Destroy(natsSysClient *client);

/** \brief Converts an RFC 3339 timestamp from a response into Unix nanoseconds.
 *
 * Timestamps are carried as text rather than a numeric type: orbit.c has no
 * date parser and cnats does not export one, so decoding them in the library
 * would mean shipping a second, unrelated implementation. This helper is
 * provided so callers are not stranded.
 *
 * Accepts `YYYY-MM-DDTHH:MM:SS`, an optional fractional-second part, and either
 * `Z` or a `+HH:MM` / `-HH:MM` offset.
 *
 * @param unixNanos out-param set to nanoseconds since the Unix epoch, UTC.
 * @param rfc3339 the timestamp text, as found in e.g. #natsSysServerInfo.Time.
 * @return #NATS_OK on success, #NATS_INVALID_ARG for `NULL` arguments, text
 * that is not a valid RFC 3339 timestamp, or a timestamp outside
 * 1677-09-21 … 2262-04-11 — the range `int64` nanoseconds can represent, the
 * same window Go documents for `time.Time.UnixNano()`.
 *
 * \note That last case is reachable from real data and shares its status with
 * malformed text: a JetStream stream that has never been written reports
 * `0001-01-01T00:00:00Z`, so "never set" is indistinguishable from garbage.
 */
NATS_EXTERN natsStatus
natsSysTime_Parse(int64_t *unixNanos, const char *rfc3339);

/** @} */ // end natsSysClientFuncGroup
/** @} */ // end natsSysClientGroup

#if defined(__cplusplus)
}
#endif

#endif /* NATS_SYSCLIENT_H_ */
