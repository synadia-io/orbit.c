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
 * Client for the server monitoring endpoints published on the `$SYS`
 * account: `VARZ`, `STATSZ`, `CONNZ`, `SUBSZ`, `HEALTHZ` and `JSZ`. Each can
 * be queried from one server by ID, or from every server by a `PING`
 * scatter-gather. The connection must be on the system account.
 *
 * Response structures follow the nats-server v2.10.23 wire format.
 * @{
 */

/** \defgroup natsSysClientTypesGroup Types
 *
 *  System client types.
 *  @{
 */

/** \brief Request timeout, in milliseconds, used when a call is passed 0. */
#define NATS_SYS_DEFAULT_REQUEST_TIMEOUT (10000)

/** \brief Default stall interval for a scatter-gather, in milliseconds. */
#define NATS_SYS_DEFAULT_STALL (300)

/** \brief A client for the system monitoring endpoints.
 *
 * Holds no mutable state after #natsSysClient_Create, so it may be used from
 * several threads at once.
 */
typedef struct __natsSysClient natsSysClient;

/** \brief Options controlling when a `*Ping` gather stops.
 *
 * Initialize with #natsSysClientOpts_Init. A by-ID request always waits for
 * exactly one reply and ignores these.
 */
typedef struct __natsSysClientOpts
{
    int     ServerCount;   ///< Stop after this many replies; -1 for no limit.
    int64_t StallInterval; ///< Stop when no reply arrives within this many ms; must be > 0.

} natsSysClientOpts;

/** \brief The server that produced a response. */
typedef struct __natsSysServerInfo
{
    char    *Name;
    char    *Host;
    char    *ID;      ///< The value to pass as `serverID`.
    char    *Cluster;
    char    *Domain;
    char    *Version; ///< Wire key `ver`.
    char   **Tags;
    int      TagsCount;
    uint64_t Seq;
    bool     JetStream;
    char    *Time;    ///< RFC 3339; see #natsSysTime_Parse.

} natsSysServerInfo;

/** \brief An error reported in a response envelope.
 *
 * The library does not act on it: a response carrying an error still returns
 * #NATS_OK with a zeroed payload. Check `Code != 0`.
 */
typedef struct __natsSysAPIError
{
    int      Code;    ///< 0 when no error.
    uint16_t ErrCode;
    char    *Description;

} natsSysAPIError;

/** \brief Selects which servers answer a `*Ping` request.
 *
 * A zero-valued field is left out of the request. SUBSZ has no filter.
 */
typedef struct __natsSysEventFilterOptions
{
    const char  *Name;    ///< Wire key `server_name`.
    const char  *Cluster;
    const char  *Host;
    const char **Tags;    ///< All must match; no entry may be `NULL`.
    int          TagsCount;
    const char  *Domain;  ///< JetStream domain.

} natsSysEventFilterOptions;

/** \brief One subscription, as reported by CONNZ and SUBSZ. */
typedef struct __natsSysSubDetail
{
    char    *Account;
    char    *Subject;
    char    *Queue;   ///< Wire key `qgroup`.
    char    *Sid;
    int64_t  Msgs;
    int64_t  Max;     ///< Auto-unsubscribe threshold; 0 when unset.
    uint64_t Cid;

} natsSysSubDetail;

/** \brief Connections dropped for being too slow, by kind. */
typedef struct __natsSysSlowConsumersStats
{
    uint64_t Clients;
    uint64_t Routes;
    uint64_t Gateways;
    uint64_t Leafs;

} natsSysSlowConsumersStats;

/** \brief JetStream API call counts. */
typedef struct __natsSysJetStreamAPIStats
{
    uint64_t Total;
    uint64_t Errors;
    uint64_t Inflight;

} natsSysJetStreamAPIStats;

/** \brief JetStream resource usage for a server or account. */
typedef struct __natsSysJetStreamStats
{
    uint64_t                 Memory;         ///< Bytes.
    uint64_t                 Store;          ///< Bytes (wire key `storage`).
    uint64_t                 ReservedMemory; ///< Bytes.
    uint64_t                 ReservedStore;  ///< Bytes (wire key `reserved_storage`).
    int                      Accounts;
    int                      HAAssets;
    natsSysJetStreamAPIStats API;

} natsSysJetStreamStats;

/** \brief A server's JetStream configuration. */
typedef struct __natsSysJetStreamConfig
{
    int64_t MaxMemory;    ///< Bytes.
    int64_t MaxStore;     ///< Bytes (wire key `max_storage`).
    char   *StoreDir;
    int64_t SyncInterval; ///< Nanoseconds.
    bool    SyncAlways;
    char   *Domain;
    bool    CompressOK;
    char   *UniqueTag;

} natsSysJetStreamConfig;

/** \brief One peer in a Raft group. */
typedef struct __natsSysPeerInfo
{
    char    *Name;
    bool     Current;
    bool     Offline;
    int64_t  Active; ///< Nanoseconds since last seen.
    uint64_t Lag;
    char    *Peer;

} natsSysPeerInfo;

/** \brief The JetStream meta group. */
typedef struct __natsSysMetaClusterInfo
{
    char             *Name;
    char             *Leader;
    char             *Peer;
    natsSysPeerInfo **Replicas;
    int               ReplicasCount;
    int               Size;    ///< Wire key `cluster_size`.
    int               Pending;

} natsSysMetaClusterInfo;

/** \brief A server's JetStream state; each member is `NULL` when not reported. */
typedef struct __natsSysJetStreamVarz
{
    natsSysJetStreamConfig *Config;
    natsSysJetStreamStats  *Stats;
    natsSysMetaClusterInfo *Meta;

} natsSysJetStreamVarz;

/** @} */ // end natsSysClientTypesGroup

/** \defgroup natsSysClientFuncGroup Functions
 *
 *  System client functions.
 *  @{
 */

/** \brief Initializes options to their defaults: no server count, a
 * #NATS_SYS_DEFAULT_STALL stall interval.
 *
 * @param opts the options to initialize.
 */
NATS_EXTERN natsStatus
natsSysClientOpts_Init(natsSysClientOpts *opts);

/** \brief Creates a system client over an existing connection.
 *
 * The connection is borrowed and must stay open for the client's lifetime.
 *
 * @param newClient the location where to store the new client.
 * @param nc a connection on the system account.
 * @param opts the options, or `NULL` for the defaults.
 * @return #NATS_INVALID_ARG for a `ServerCount` of 0 or below -1, or a
 * `StallInterval` of 0 or below.
 */
NATS_EXTERN natsStatus
natsSysClient_Create(natsSysClient **newClient, natsConnection *nc,
                     const natsSysClientOpts *opts);

/** \brief Destroys the client. The connection is left open.
 *
 * @param client the client to destroy; `NULL` is a no-op.
 */
NATS_EXTERN void
natsSysClient_Destroy(natsSysClient *client);

/** \brief Converts an RFC 3339 timestamp to nanoseconds since the Unix epoch.
 *
 * Accepts `YYYY-MM-DDTHH:MM:SS`, an optional fraction, and `Z` or a
 * `+HH:MM`/`-HH:MM` offset.
 *
 * @param unixNanos the location where to store the result.
 * @param rfc3339 the text, as found in e.g. #natsSysServerInfo.Time.
 * @return #NATS_INVALID_ARG for malformed text, or a timestamp outside
 * 1677-09-21 … 2262-04-11 (the `int64` nanosecond range); a never-written
 * JetStream stream reports `0001-01-01T00:00:00Z`.
 */
NATS_EXTERN natsStatus
natsSysTime_Parse(int64_t *unixNanos, const char *rfc3339);

/** @} */ // end natsSysClientFuncGroup
/** @} */ // end natsSysClientGroup

#if defined(__cplusplus)
}
#endif

#endif /* NATS_SYSCLIENT_H_ */
