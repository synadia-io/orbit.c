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

#ifndef NATS_SYSCLIENT_VARZ_H_
#define NATS_SYSCLIENT_VARZ_H_

#include "sysclient.h"

#include <nats/nats.h>
#include <stdbool.h>
#include <stdint.h>

#if defined(__cplusplus)
extern "C" {
#endif

/** \defgroup natsSysVarzGroup VARZ
 *
 * General server information, from the `$SYS.REQ.SERVER.<id>.VARZ` endpoint.
 *
 * \note A zero-valued numeric or boolean member may mean absent,
 * present-and-zero, or `null`; the payload does not distinguish them. A string
 * member is `NULL` when absent.
 * @{
 */

/** \brief Cluster (route) configuration. */
typedef struct __natsSysClusterOptsVarz
{
    char   *Name;        ///< Cluster name.
    char   *Host;        ///< Listen address (wire key `addr`).
    int     Port;        ///< Listen port (wire key `cluster_port`).
    double  AuthTimeout; ///< Route authentication timeout, in seconds.
    char  **URLs;        ///< Advertised route URLs.
    int     URLsCount;   ///< Number of entries in #URLs.
    double  TLSTimeout;  ///< TLS handshake timeout, in seconds.
    bool    TLSRequired; ///< Whether routes require TLS.
    bool    TLSVerify;   ///< Whether route certificates are verified.
    int     PoolSize;    ///< Route connection pool size.

} natsSysClusterOptsVarz;

/** \brief One remote gateway a server is configured to reach. */
typedef struct __natsSysRemoteGatewayOptsVarz
{
    char   *Name;       ///< Remote gateway name.
    double  TLSTimeout; ///< TLS handshake timeout, in seconds.
    char  **URLs;       ///< URLs for the remote gateway.
    int     URLsCount;  ///< Number of entries in #URLs.

} natsSysRemoteGatewayOptsVarz;

/** \brief Gateway configuration. */
typedef struct __natsSysGatewayOptsVarz
{
    char                         *Name;           ///< This gateway's name.
    char                         *Host;           ///< Listen address.
    int                           Port;           ///< Listen port.
    double                        AuthTimeout;    ///< Authentication timeout, in seconds.
    double                        TLSTimeout;     ///< TLS handshake timeout, in seconds.
    bool                          TLSRequired;    ///< Whether gateways require TLS.
    bool                          TLSVerify;      ///< Whether certificates are verified.
    char                         *Advertise;      ///< Advertised address.
    int                           ConnectRetries; ///< Connection retry count.
    natsSysRemoteGatewayOptsVarz *Gateways;       ///< Configured remote gateways.
    int                           GatewaysCount;  ///< Number of entries in #Gateways.

    /** \brief Whether connections from unknown clusters are rejected.
     *
     * The wire key is `reject_unknown`; the server config option was later
     * renamed to `reject_unknown_cluster`.
     */
    bool RejectUnknown;

} natsSysGatewayOptsVarz;

/** \brief Subjects a leaf node connection may not import or export. */
typedef struct __natsSysDenyRules
{
    char **Exports;      ///< Subjects that may not be exported.
    int    ExportsCount; ///< Number of entries in #Exports.
    char **Imports;      ///< Subjects that may not be imported.
    int    ImportsCount; ///< Number of entries in #Imports.

} natsSysDenyRules;

/** \brief One configured remote leaf node connection. */
typedef struct __natsSysRemoteLeafOptsVarz
{
    char             *LocalAccount;      ///< Local account bound to the remote.
    double            TLSTimeout;        ///< TLS handshake timeout, in seconds.
    char            **URLs;              ///< Remote URLs.
    int               URLsCount;         ///< Number of entries in #URLs.
    natsSysDenyRules *Deny;              ///< Deny rules; `NULL` when absent.
    bool              TLSOCSPPeerVerify; ///< Whether OCSP peer verification is on.

} natsSysRemoteLeafOptsVarz;

/** \brief Leaf node configuration. */
typedef struct __natsSysLeafNodeOptsVarz
{
    char                      *Host;              ///< Listen address.
    int                        Port;              ///< Listen port.
    double                     AuthTimeout;       ///< Authentication timeout, in seconds.
    double                     TLSTimeout;        ///< TLS handshake timeout, in seconds.
    bool                       TLSRequired;       ///< Whether leaf nodes require TLS.
    bool                       TLSVerify;         ///< Whether certificates are verified.
    natsSysRemoteLeafOptsVarz *Remotes;           ///< Configured remote leaf nodes.
    int                        RemotesCount;      ///< Number of entries in #Remotes.
    bool                       TLSOCSPPeerVerify; ///< Whether OCSP peer verification is on.

} natsSysLeafNodeOptsVarz;

/** \brief MQTT configuration. */
typedef struct __natsSysMQTTOptsVarz
{
    char   *Host;              ///< Listen address.
    int     Port;              ///< Listen port.
    char   *NoAuthUser;        ///< User applied to unauthenticated connections.
    double  AuthTimeout;       ///< Authentication timeout, in seconds.
    bool    TLSMap;            ///< Whether certificate mapping is on.
    double  TLSTimeout;        ///< TLS handshake timeout, in seconds.
    char  **TLSPinnedCerts;    ///< Pinned certificate fingerprints.
    int     TLSPinnedCertsCount; ///< Number of entries in #TLSPinnedCerts.
    char   *JsDomain;          ///< JetStream domain used for MQTT sessions.

    /** \brief MQTT acknowledgement wait, in **nanoseconds**.
     *
     * \note Nanoseconds on the wire — not the milliseconds used everywhere
     * else in orbit.c.
     */
    int64_t AckWait;

    uint16_t MaxAckPending;     ///< Maximum unacknowledged messages.
    bool     TLSOCSPPeerVerify; ///< Whether OCSP peer verification is on.

} natsSysMQTTOptsVarz;

/** \brief WebSocket configuration. */
typedef struct __natsSysWebsocketOptsVarz
{
    char   *Host;       ///< Listen address.
    int     Port;       ///< Listen port.
    char   *Advertise;  ///< Advertised address.
    char   *NoAuthUser; ///< User applied to unauthenticated connections.
    char   *JWTCookie;  ///< Cookie carrying the user JWT.

    /** \brief WebSocket handshake timeout, in **nanoseconds**. */
    int64_t HandshakeTimeout;

    double AuthTimeout;         ///< Authentication timeout, in seconds.
    bool   NoTLS;               ///< Whether TLS is disabled.
    bool   TLSMap;              ///< Whether certificate mapping is on.
    char **TLSPinnedCerts;      ///< Pinned certificate fingerprints.
    int    TLSPinnedCertsCount; ///< Number of entries in #TLSPinnedCerts.
    bool   SameOrigin;          ///< Whether the Origin header must match.
    char **AllowedOrigins;      ///< Accepted Origin values.
    int    AllowedOriginsCount; ///< Number of entries in #AllowedOrigins.
    bool   Compression;         ///< Whether per-message deflate is on.
    bool   TLSOCSPPeerVerify;   ///< Whether OCSP peer verification is on.

} natsSysWebsocketOptsVarz;

/** \brief OCSP response cache counters. */
typedef struct __natsSysOCSPResponseCacheVarz
{
    char   *Type;      ///< Cache type (wire key `cache_type`).
    int64_t Hits;      ///< Cache hits.
    int64_t Misses;    ///< Cache misses.
    int64_t Responses; ///< Responses currently cached.
    int64_t Revokes;   ///< Cached revoked responses.
    int64_t Goods;     ///< Cached good responses.
    int64_t Unknowns;  ///< Cached unknown responses.

} natsSysOCSPResponseCacheVarz;

/** \brief One entry of the HTTP request counters.
 *
 * The payload carries these as a JSON object keyed by path. C has no map, so
 * the entries are carried as an array in the order the server sent them.
 */
typedef struct __natsSysHTTPReqStat
{
    char    *Path;  ///< The monitoring path, i.e. the JSON key.
    uint64_t Count; ///< Requests served for that path.

} natsSysHTTPReqStat;

/** \brief General information about one server. */
typedef struct __natsSysVarz
{
    char *ID;        ///< Server ID (wire key `server_id`).
    char *Name;      ///< Server name (wire key `server_name`).
    char *Version;   ///< Server version.
    int   Proto;     ///< Protocol version.
    char *GitCommit; ///< Build commit.
    char *GoVersion; ///< Go version the server was built with (wire key `go`).
    char *Host;      ///< Client listen address.
    int   Port;      ///< Client listen port.

    bool AuthRequired;      ///< Whether clients must authenticate.
    bool TLSRequired;       ///< Whether clients must use TLS.
    bool TLSVerify;         ///< Whether client certificates are verified.
    bool TLSOCSPPeerVerify; ///< Whether OCSP peer verification is on.

    char  *IP;                     ///< Advertised client IP.
    char **ClientConnectURLs;      ///< Advertised client URLs (wire key `connect_urls`).
    int    ClientConnectURLsCount; ///< Number of entries in #ClientConnectURLs.
    char **WSConnectURLs;          ///< Advertised WebSocket URLs.
    int    WSConnectURLsCount;     ///< Number of entries in #WSConnectURLs.

    int MaxConn; ///< Maximum client connections (wire key `max_connections`).
    int MaxSubs; ///< Maximum subscriptions (wire key `max_subscriptions`).

    /** \brief Client ping interval, in **nanoseconds**. */
    int64_t PingInterval;

    int     MaxPingsOut;    ///< Missed pings before disconnect (wire key `ping_max`).
    char   *HTTPHost;       ///< Monitoring listen address.
    int     HTTPPort;       ///< Monitoring port.
    char   *HTTPBasePath;   ///< Monitoring base path.
    int     HTTPSPort;      ///< Monitoring TLS port.
    double  AuthTimeout;    ///< Client authentication timeout, in seconds.
    int32_t MaxControlLine; ///< Maximum protocol line length, in bytes.
    int     MaxPayload;     ///< Maximum message payload, in bytes.
    int64_t MaxPending;     ///< Maximum pending bytes per client.

    natsSysClusterOptsVarz   Cluster;   ///< Route configuration.
    natsSysGatewayOptsVarz   Gateway;   ///< Gateway configuration.
    natsSysLeafNodeOptsVarz  LeafNode;  ///< Leaf node configuration (wire key `leaf`).
    natsSysMQTTOptsVarz      MQTT;      ///< MQTT configuration.
    natsSysWebsocketOptsVarz Websocket; ///< WebSocket configuration.
    natsSysJetStreamVarz     JetStream; ///< JetStream state.

    double TLSTimeout; ///< Client TLS handshake timeout, in seconds.

    /** \brief Write deadline, in **nanoseconds**. */
    int64_t WriteDeadline;

    char *Start;  ///< Server start time, RFC 3339; see #natsSysTime_Parse.
    char *Now;    ///< Time the response was produced, RFC 3339.
    char *Uptime; ///< Uptime as the server's own human-readable string.

    int64_t  Mem;              ///< Memory in use, in bytes.
    int      Cores;            ///< CPU cores detected.
    int      MaxProcs;         ///< GOMAXPROCS (wire key `gomaxprocs`).
    double   CPU;              ///< CPU usage, as a percentage.
    int      Connections;      ///< Current client connections.
    uint64_t TotalConnections; ///< Client connections since start.
    int      Routes;           ///< Current route connections.
    int      Remotes;          ///< Configured route remotes.
    int      Leafs;            ///< Current leaf node connections (wire key `leafnodes`).
    int64_t  InMsgs;           ///< Messages received.
    int64_t  OutMsgs;          ///< Messages sent.
    int64_t  InBytes;          ///< Bytes received.
    int64_t  OutBytes;         ///< Bytes sent.
    int64_t  SlowConsumers;    ///< Connections dropped for being too slow.
    uint32_t Subscriptions;    ///< Current subscriptions.

    natsSysHTTPReqStat *HTTPReqStats;      ///< Monitoring request counters.
    int                 HTTPReqStatsCount; ///< Number of entries in #HTTPReqStats.

    char  *ConfigLoadTime;         ///< Config load time, RFC 3339.
    char **Tags;                   ///< Server tags.
    int    TagsCount;              ///< Number of entries in #Tags.
    char **TrustedOperatorsJwt;    ///< Trusted operator JWTs, encoded.
    int    TrustedOperatorsJwtCount; ///< Number of entries in #TrustedOperatorsJwt.

    /** \brief Decoded operator claims, as raw JSON.
     *
     * Each element is a full JWT claims tree. orbit.c ships no JWT claims
     * model and cnats exposes none, so rather than drop the data or decode
     * part of it, each element is carried as the JSON text the server sent.
     * Parse it with any JSON reader; numbers survive the round trip exactly
     * and strings are re-escaped.
     */
    char **TrustedOperatorsClaimJSON;
    int    TrustedOperatorsClaimJSONCount; ///< Number of entries above.

    char    *SystemAccount;     ///< Configured system account.
    uint64_t PinnedAccountFail; ///< Rejected pinned-account connections.

    natsSysOCSPResponseCacheVarz *OCSPResponseCache;  ///< OCSP cache counters; `NULL` when absent.
    natsSysSlowConsumersStats    *SlowConsumersStats; ///< Slow consumer counts; `NULL` when absent.

} natsSysVarz;

/** \brief A VARZ response from one server.
 *
 * \warning #Error is decoded but never acted on. Check `Error.Code != 0`
 * before trusting #Varz.
 */
typedef struct __natsSysVarzResp
{
    natsSysServerInfo Server; ///< Which server answered.
    natsSysVarz       Varz;   ///< The payload (wire key `data`).
    natsSysAPIError   Error;  ///< Server-reported error, if any.

} natsSysVarzResp;

/** \brief The responses gathered by #natsSysClient_VarzPing.
 *
 * Caller-provided, typically on the stack. #natsSysVarzRespList_Destroy
 * releases the contents, not the list object itself.
 */
typedef struct __natsSysVarzRespList
{
    natsSysVarzResp **Resps; ///< One response per server that answered.
    int               Count; ///< Number of entries in #Resps.

} natsSysVarzRespList;

/** \brief Options for a VARZ request.
 *
 * Only the shared server filters apply; VARZ has no options of its own.
 */
typedef struct __natsSysVarzOptions
{
    natsSysEventFilterOptions Filter; ///< Which servers should answer.

} natsSysVarzOptions;

/** \brief Initialises a #natsSysVarzOptions to its defaults (all unset). */
NATS_EXTERN natsStatus
natsSysVarzOptions_Init(natsSysVarzOptions *opts);

/** \brief Requests general information from one server.
 *
 * @param newResp out-param set to the response; destroy with
 * #natsSysVarzResp_Destroy. Set to `NULL` on error.
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
natsSysClient_Varz(natsSysVarzResp **newResp, natsSysClient *client,
                   const char *serverID, const natsSysVarzOptions *opts,
                   int64_t timeout);

/** \brief Requests general information from every server in the cluster.
 *
 * \note As with every `*Ping` call, running out of time is normal termination:
 * a gather that hears nothing returns #NATS_OK with a `Count` of 0. See
 * #natsSysClient_HealthzPing for the full description.
 *
 * @param list out-param populated with the responses; unless the return is
 * #NATS_INVALID_ARG, always release it with #natsSysVarzRespList_Destroy.
 * @param client the system client.
 * @param opts the request options, or `NULL` for the defaults.
 * @param timeout milliseconds to wait, or 0 for
 * #NATS_SYS_DEFAULT_REQUEST_TIMEOUT.
 * @return #NATS_OK on success, #NATS_INVALID_ARG for a bad argument,
 * #NATS_NO_RESPONDERS when nothing is listening on the system subject,
 * #NATS_ERR for a malformed response.
 */
NATS_EXTERN natsStatus
natsSysClient_VarzPing(natsSysVarzRespList *list, natsSysClient *client,
                       const natsSysVarzOptions *opts, int64_t timeout);

/** \brief Destroys a response returned by #natsSysClient_Varz.
 *
 * Passing `NULL` is a no-op.
 */
NATS_EXTERN void
natsSysVarzResp_Destroy(natsSysVarzResp *resp);

/** \brief Releases the contents of a #natsSysVarzRespList.
 *
 * Passing `NULL` is a no-op. The list object itself is not freed.
 */
NATS_EXTERN void
natsSysVarzRespList_Destroy(natsSysVarzRespList *list);

/** @} */ // end natsSysVarzGroup

#if defined(__cplusplus)
}
#endif

#endif /* NATS_SYSCLIENT_VARZ_H_ */
