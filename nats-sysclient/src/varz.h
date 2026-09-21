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
 * General server information, from `$SYS.REQ.SERVER.<id>.VARZ`.
 *
 * Timeouts are in seconds where the server reports a float and in
 * nanoseconds where it reports a `time.Duration`; each is marked below.
 * @{
 */

/** \brief Cluster (route) configuration. */
typedef struct __natsSysClusterOptsVarz
{
    char   *Name;
    char   *Host;        ///< Wire key `addr`.
    int     Port;        ///< Wire key `cluster_port`.
    double  AuthTimeout; ///< Seconds.
    char  **URLs;
    int     URLsCount;
    double  TLSTimeout;  ///< Seconds.
    bool    TLSRequired;
    bool    TLSVerify;
    int     PoolSize;

} natsSysClusterOptsVarz;

/** \brief One configured remote gateway. */
typedef struct __natsSysRemoteGatewayOptsVarz
{
    char   *Name;
    double  TLSTimeout; ///< Seconds.
    char  **URLs;
    int     URLsCount;

} natsSysRemoteGatewayOptsVarz;

/** \brief Gateway configuration. */
typedef struct __natsSysGatewayOptsVarz
{
    char                         *Name;
    char                         *Host;
    int                           Port;
    double                        AuthTimeout; ///< Seconds.
    double                        TLSTimeout;  ///< Seconds.
    bool                          TLSRequired;
    bool                          TLSVerify;
    char                         *Advertise;
    int                           ConnectRetries;
    natsSysRemoteGatewayOptsVarz *Gateways;
    int                           GatewaysCount;
    bool                          RejectUnknown; ///< Wire key `reject_unknown`.

} natsSysGatewayOptsVarz;

/** \brief Subjects a leaf node connection may not import or export. */
typedef struct __natsSysDenyRules
{
    char **Exports;
    int    ExportsCount;
    char **Imports;
    int    ImportsCount;

} natsSysDenyRules;

/** \brief One configured remote leaf node connection. */
typedef struct __natsSysRemoteLeafOptsVarz
{
    char             *LocalAccount;
    double            TLSTimeout; ///< Seconds.
    char            **URLs;
    int               URLsCount;
    natsSysDenyRules *Deny;       ///< `NULL` when absent.
    bool              TLSOCSPPeerVerify;

} natsSysRemoteLeafOptsVarz;

/** \brief Leaf node configuration. */
typedef struct __natsSysLeafNodeOptsVarz
{
    char                      *Host;
    int                        Port;
    double                     AuthTimeout; ///< Seconds.
    double                     TLSTimeout;  ///< Seconds.
    bool                       TLSRequired;
    bool                       TLSVerify;
    natsSysRemoteLeafOptsVarz *Remotes;
    int                        RemotesCount;
    bool                       TLSOCSPPeerVerify;

} natsSysLeafNodeOptsVarz;

/** \brief MQTT configuration. */
typedef struct __natsSysMQTTOptsVarz
{
    char    *Host;
    int      Port;
    char    *NoAuthUser;
    double   AuthTimeout; ///< Seconds.
    bool     TLSMap;
    double   TLSTimeout;  ///< Seconds.
    char   **TLSPinnedCerts;
    int      TLSPinnedCertsCount;
    char    *JsDomain;
    int64_t  AckWait;     ///< Nanoseconds.
    uint16_t MaxAckPending;
    bool     TLSOCSPPeerVerify;

} natsSysMQTTOptsVarz;

/** \brief WebSocket configuration. */
typedef struct __natsSysWebsocketOptsVarz
{
    char   *Host;
    int     Port;
    char   *Advertise;
    char   *NoAuthUser;
    char   *JWTCookie;
    int64_t HandshakeTimeout; ///< Nanoseconds.
    double  AuthTimeout;      ///< Seconds.
    bool    NoTLS;
    bool    TLSMap;
    char  **TLSPinnedCerts;
    int     TLSPinnedCertsCount;
    bool    SameOrigin;
    char  **AllowedOrigins;
    int     AllowedOriginsCount;
    bool    Compression;
    bool    TLSOCSPPeerVerify;

} natsSysWebsocketOptsVarz;

/** \brief OCSP response cache counters. */
typedef struct __natsSysOCSPResponseCacheVarz
{
    char   *Type; ///< Wire key `cache_type`.
    int64_t Hits;
    int64_t Misses;
    int64_t Responses;
    int64_t Revokes;
    int64_t Goods;
    int64_t Unknowns;

} natsSysOCSPResponseCacheVarz;

/** \brief One entry of `http_req_stats`, a JSON object keyed by path. */
typedef struct __natsSysHTTPReqStat
{
    char    *Path;
    uint64_t Count;

} natsSysHTTPReqStat;

/** \brief General information about one server. */
typedef struct __natsSysVarz
{
    char *ID;        ///< Wire key `server_id`.
    char *Name;      ///< Wire key `server_name`.
    char *Version;
    int   Proto;
    char *GitCommit;
    char *GoVersion; ///< Wire key `go`.
    char *Host;
    int   Port;

    bool AuthRequired;
    bool TLSRequired;
    bool TLSVerify;
    bool TLSOCSPPeerVerify;

    char  *IP;
    char **ClientConnectURLs; ///< Wire key `connect_urls`.
    int    ClientConnectURLsCount;
    char **WSConnectURLs;
    int    WSConnectURLsCount;

    int     MaxConn;        ///< Wire key `max_connections`.
    int     MaxSubs;        ///< Wire key `max_subscriptions`.
    int64_t PingInterval;   ///< Nanoseconds.
    int     MaxPingsOut;    ///< Wire key `ping_max`.
    char   *HTTPHost;
    int     HTTPPort;
    char   *HTTPBasePath;
    int     HTTPSPort;
    double  AuthTimeout;    ///< Seconds.
    int32_t MaxControlLine;
    int     MaxPayload;
    int64_t MaxPending;

    natsSysClusterOptsVarz   Cluster;
    natsSysGatewayOptsVarz   Gateway;
    natsSysLeafNodeOptsVarz  LeafNode;  ///< Wire key `leaf`.
    natsSysMQTTOptsVarz      MQTT;
    natsSysWebsocketOptsVarz Websocket;
    natsSysJetStreamVarz     JetStream;

    double  TLSTimeout;    ///< Seconds.
    int64_t WriteDeadline; ///< Nanoseconds.
    char   *Start;         ///< RFC 3339; see #natsSysTime_Parse.
    char   *Now;           ///< RFC 3339.
    char   *Uptime;        ///< The server's own human-readable string.

    int64_t  Mem;      ///< Bytes.
    int      Cores;
    int      MaxProcs; ///< Wire key `gomaxprocs`.
    double   CPU;      ///< Percent.
    int      Connections;
    uint64_t TotalConnections;
    int      Routes;
    int      Remotes;
    int      Leafs;    ///< Wire key `leafnodes`.
    int64_t  InMsgs;
    int64_t  OutMsgs;
    int64_t  InBytes;
    int64_t  OutBytes;
    int64_t  SlowConsumers;
    uint32_t Subscriptions;

    natsSysHTTPReqStat *HTTPReqStats; ///< In the order the server sent them.
    int                 HTTPReqStatsCount;

    char  *ConfigLoadTime; ///< RFC 3339.
    char **Tags;
    int    TagsCount;
    char **TrustedOperatorsJwt;
    int    TrustedOperatorsJwtCount;

    /** The decoded operator claims (`trusted_operators_claim`), each element
     * carried verbatim as the JSON text the server sent. */
    char **TrustedOperatorsClaimJSON;
    int    TrustedOperatorsClaimJSONCount;

    char    *SystemAccount;
    uint64_t PinnedAccountFail; ///< Wire key `pinned_account_fails`.

    natsSysOCSPResponseCacheVarz *OCSPResponseCache;  ///< `NULL` when absent (wire key `ocsp_peer_cache`).
    natsSysSlowConsumersStats    *SlowConsumersStats; ///< `NULL` when absent.

} natsSysVarz;

/** \brief A VARZ response. Check `Error.Code` before reading #Varz. */
typedef struct __natsSysVarzResp
{
    natsSysServerInfo Server;
    natsSysVarz       Varz; ///< Wire key `data`.
    natsSysAPIError   Error;

} natsSysVarzResp;

/** \brief Responses gathered by #natsSysClient_VarzPing; see
 * #natsSysHealthzRespList. */
typedef struct __natsSysVarzRespList
{
    natsSysVarzResp **Resps;
    int               Count;

} natsSysVarzRespList;

/** \brief VARZ request options: only the server filter. */
typedef struct __natsSysVarzOptions
{
    natsSysEventFilterOptions Filter;

} natsSysVarzOptions;

/** \brief Initializes options to their defaults (all unset). */
NATS_EXTERN natsStatus
natsSysVarzOptions_Init(natsSysVarzOptions *opts);

/** \brief Requests general information from one server; see
 * #natsSysClient_Healthz. */
NATS_EXTERN natsStatus
natsSysClient_Varz(natsSysVarzResp **newResp, natsSysClient *client,
                   const char *serverID, const natsSysVarzOptions *opts,
                   int64_t timeout);

/** \brief Requests general information from every server; see
 * #natsSysClient_HealthzPing. */
NATS_EXTERN natsStatus
natsSysClient_VarzPing(natsSysVarzRespList *list, natsSysClient *client,
                       const natsSysVarzOptions *opts, int64_t timeout);

/** \brief Destroys a response; `NULL` is a no-op. */
NATS_EXTERN void
natsSysVarzResp_Destroy(natsSysVarzResp *resp);

/** \brief Destroys the contents of a list, not the list itself. */
NATS_EXTERN void
natsSysVarzRespList_Destroy(natsSysVarzRespList *list);

/** @} */ // end natsSysVarzGroup

#if defined(__cplusplus)
}
#endif

#endif /* NATS_SYSCLIENT_VARZ_H_ */
