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

#ifndef NATS_SYSCLIENT_CONNZ_H_
#define NATS_SYSCLIENT_CONNZ_H_

#include "sysclient.h"

#include <nats/nats.h>
#include <stdbool.h>
#include <stdint.h>

#if defined(__cplusplus)
extern "C" {
#endif

/** \defgroup natsSysConnzGroup CONNZ
 *
 * Client connection details, from the `$SYS.REQ.SERVER.<id>.CONNZ` endpoint.
 *
 * This endpoint paginates: a server with more connections than
 * #natsSysConnzOptions.Limit returns one page at a time. Use
 * #natsSysClient_ConnzEach to walk every page without managing offsets.
 * @{
 */

/**
 * \name Sort orders
 *
 * Values for #natsSysConnzOptions.Sort. Only #NATS_SYS_SORT_CID sorts
 * ascending; every other order is descending.
 * @{
 */
#define NATS_SYS_SORT_CID       "cid"        ///< By connection ID.
#define NATS_SYS_SORT_START     "start"      ///< By start time; same as by CID.
#define NATS_SYS_SORT_SUBS      "subs"       ///< By subscription count.
#define NATS_SYS_SORT_PENDING   "pending"    ///< By bytes waiting to be sent.
#define NATS_SYS_SORT_OUT_MSGS  "msgs_to"    ///< By messages sent.
#define NATS_SYS_SORT_IN_MSGS   "msgs_from"  ///< By messages received.
#define NATS_SYS_SORT_OUT_BYTES "bytes_to"   ///< By bytes sent.
#define NATS_SYS_SORT_IN_BYTES  "bytes_from" ///< By bytes received.
#define NATS_SYS_SORT_LAST      "last"       ///< By last activity.
#define NATS_SYS_SORT_IDLE      "idle"       ///< By idle time.
#define NATS_SYS_SORT_UPTIME    "uptime"     ///< By connection age.
#define NATS_SYS_SORT_STOP      "stop"       ///< By stop time (closed connections).
#define NATS_SYS_SORT_REASON    "reason"     ///< By close reason (closed connections).
/** @} */

/** \brief Which connections a CONNZ request should return. */
typedef enum
{
    natsSysConnOpen = 0, ///< Only open connections.
    natsSysConnClosed,   ///< Only closed connections.
    natsSysConnAll,      ///< Both open and closed.

} natsSysConnState;

/** \brief Summary of a TLS peer certificate. */
typedef struct __natsSysTLSPeerCert
{
    char *Subject;          ///< Certificate subject.
    char *SubjectPKISha256; ///< SHA-256 of the subject public key info.
    char *CertSha256;       ///< SHA-256 of the certificate.

} natsSysTLSPeerCert;

/** \brief Details of one client connection.
 *
 * #Subs and #SubsDetail are only populated when the request asked for them via
 * #natsSysConnzOptions.Subscriptions or
 * #natsSysConnzOptions.SubscriptionsDetail.
 */
typedef struct __natsSysConnInfo
{
    uint64_t Cid;          ///< Connection ID.
    char    *Kind;         ///< Connection kind (Client, Router, Leafnode, ...).
    char    *Type;         ///< Connection type (nats, mqtt, websocket).
    char    *IP;           ///< Client address.
    int      Port;         ///< Client port.
    char    *Start;        ///< Connection start time, RFC 3339.
    char    *LastActivity; ///< Last activity time, RFC 3339.
    char    *Stop;         ///< Close time, RFC 3339; `NULL` while still open.
    char    *Reason;       ///< Close reason; `NULL` while still open.
    char    *RTT;          ///< Last measured round trip, as the server's own string.
    char    *Uptime;       ///< Connection age, as the server's own string.
    char    *Idle;         ///< Idle time, as the server's own string.
    int64_t  Pending;      ///< Bytes waiting to be written (wire key `pending_bytes`).
    int64_t  InMsgs;       ///< Messages received from this connection.
    int64_t  OutMsgs;      ///< Messages sent to this connection.
    int64_t  InBytes;      ///< Bytes received from this connection.
    int64_t  OutBytes;     ///< Bytes sent to this connection.
    uint32_t NumSubs;      ///< Subscription count (wire key `subscriptions`).
    char    *Name;         ///< Client-supplied connection name.
    char    *Lang;         ///< Client language.
    char    *Version;      ///< Client library version.
    char    *TLSVersion;   ///< Negotiated TLS version.
    char    *TLSCipher;    ///< Negotiated cipher suite.

    natsSysTLSPeerCert **TLSPeerCerts;      ///< Peer certificates presented.
    int                  TLSPeerCertsCount; ///< Number of entries in #TLSPeerCerts.

    bool  TLSFirst;       ///< Whether TLS was negotiated before the INFO exchange.
    char *AuthorizedUser; ///< Authenticated user, when the request asked for it.
    char *Account;        ///< Account the connection is bound to.

    char **Subs;      ///< Subscribed subjects (wire key `subscriptions_list`).
    int    SubsCount; ///< Number of entries in #Subs.

    natsSysSubDetail *SubsDetail;      ///< Full subscription details.
    int               SubsDetailCount; ///< Number of entries in #SubsDetail.

    char  *JWT;        ///< Client JWT, when connected with one.
    char  *IssuerKey;  ///< Key that issued the client JWT.
    char  *NameTag;    ///< Name tag from the client JWT.
    char **Tags;       ///< Tags from the client JWT.
    int    TagsCount;  ///< Number of entries in #Tags.
    char  *MQTTClient; ///< MQTT client ID, for MQTT connections.

} natsSysConnInfo;

/** \brief One page of connection details. */
typedef struct __natsSysConnz
{
    char *ID;  ///< Reporting server's ID (wire key `server_id`).
    char *Now; ///< Time the page was produced, RFC 3339.

    int NumConns; ///< Connections in this page (wire key `num_connections`).
    int Total;    ///< Connections matching the request across all pages.
    int Offset;   ///< Offset this page starts at.
    int Limit;    ///< Page size the server applied.

    natsSysConnInfo **Conns;      ///< The connections in this page.
    int               ConnsCount; ///< Number of entries in #Conns.

} natsSysConnz;

/** \brief A CONNZ response from one server.
 *
 * \warning #Error is decoded but never acted on. Check `Error.Code != 0`
 * before trusting #Connz.
 */
typedef struct __natsSysConnzResp
{
    natsSysServerInfo Server; ///< Which server answered.
    natsSysConnz      Connz;  ///< The payload (wire key `data`).
    natsSysAPIError   Error;  ///< Server-reported error, if any.

} natsSysConnzResp;

/** \brief The responses gathered by #natsSysClient_ConnzPing. */
typedef struct __natsSysConnzRespList
{
    natsSysConnzResp **Resps; ///< One response per server that answered.
    int                Count; ///< Number of entries in #Resps.

} natsSysConnzRespList;

/** \brief Options for a CONNZ request.
 *
 * \note Unlike every other endpoint, CONNZ always sends all twelve of its own
 * fields — an otherwise empty request still carries `"sort":""` and
 * `"state":0`. That is visible to the server, so it is preserved rather than
 * tidied away. The #Filter members *are* omitted when unset.
 */
typedef struct __natsSysConnzOptions
{
    const char *Sort;          ///< Sort order; one of the `NATS_SYS_SORT_*` values.
    bool  Username;            ///< Include authenticated user names (wire key `auth`).
    bool  Subscriptions;       ///< Include #natsSysConnInfo.Subs.
    bool  SubscriptionsDetail; ///< Include #natsSysConnInfo.SubsDetail.
    int   Offset;              ///< First connection to return.
    int   Limit;               ///< Maximum connections per page.

    uint64_t         CID;        ///< Return only this connection ID.
    const char      *MQTTClient; ///< Return only this MQTT client ID.
    natsSysConnState State;      ///< Which connection states to return.

    /**
     * \name Filters that require #Username
     * @{
     */
    const char *User;          ///< Return only this user's connections.
    const char *Account;       ///< Return only this account's connections (wire key `acc`).
    const char *FilterSubject; ///< Return only connections interested in this subject.
    /** @} */

    natsSysEventFilterOptions Filter; ///< Which servers should answer.

} natsSysConnzOptions;

/** \brief Invoked once per page of a CONNZ walk.
 *
 * \note The polarity is the opposite of #natsRequestManySentinel in
 * nats-extra: that one is a predicate where `true` stops, this one is a
 * consumer where `true` continues, matching the `yield` it ports from.
 *
 * @param page borrowed, and destroyed by the library as soon as the handler
 * returns. Copy anything you need to keep.
 * @param closure the opaque pointer passed to the walk.
 * @return `true` to fetch the next page, `false` to stop the walk.
 */
typedef bool (*natsSysConnzPageHandler)(const natsSysConnzResp *page, void *closure);

/** \brief An independent, resumable pagination over one server. */
typedef struct __natsSysConnzWalk natsSysConnzWalk;

/** \brief One walk per server that answered a ping.
 *
 * Release with #natsSysConnzWalkList_Destroy, which frees the contents but not
 * the list object itself.
 */
typedef struct __natsSysConnzWalkList
{
    natsSysConnzWalk **Walks; ///< One walk per responding server.
    int                Count; ///< Number of entries in #Walks.

} natsSysConnzWalkList;

/** \brief Initialises a #natsSysConnzOptions to its defaults (all unset). */
NATS_EXTERN natsStatus
natsSysConnzOptions_Init(natsSysConnzOptions *opts);

/** \brief Requests one page of connections from one server.
 *
 * @param newResp out-param set to the response; destroy with
 * #natsSysConnzResp_Destroy. Set to `NULL` on error.
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
natsSysClient_Connz(natsSysConnzResp **newResp, natsSysClient *client,
                    const char *serverID, const natsSysConnzOptions *opts,
                    int64_t timeout);

/** \brief Requests one page of connections from every server in the cluster.
 *
 * \note As with every `*Ping` call, running out of time is normal termination.
 * See #natsSysClient_HealthzPing.
 */
NATS_EXTERN natsStatus
natsSysClient_ConnzPing(natsSysConnzRespList *list, natsSysClient *client,
                        const natsSysConnzOptions *opts, int64_t timeout);

/** \brief Walks every page of connections on one server.
 *
 * Issues requests in a loop, advancing the offset by the size of each page,
 * and stops once the reported total has been reached, the server returns an
 * empty page, or `handler` returns `false`.
 *
 * `timeout` is the budget for the **whole walk**, not for each page.
 *
 * @param client the system client.
 * @param serverID the target server's ID. Cannot be `NULL` or empty.
 * @param opts the request options, or `NULL` for the defaults. Borrowed and
 * never modified; the walk advances its own copy of the offset.
 * @param timeout milliseconds for the whole walk, or 0 for
 * #NATS_SYS_DEFAULT_REQUEST_TIMEOUT.
 * @param handler invoked once per page; cannot be `NULL`.
 * @param closure passed through to `handler`.
 * @return #NATS_OK when the walk finished or the handler stopped it,
 * #NATS_TIMEOUT if the budget ran out mid-walk, or any status
 * #natsSysClient_Connz can return. Pages already delivered stay delivered.
 */
NATS_EXTERN natsStatus
natsSysClient_ConnzEach(natsSysClient *client, const char *serverID,
                        const natsSysConnzOptions *opts, int64_t timeout,
                        natsSysConnzPageHandler handler, void *closure);

/** \brief Pings every server, then hands back one independent walk each.
 *
 * The scatter-gather happens once, here; each returned walk owns the page that
 * ping produced and continues from it with direct by-ID requests. Walks share
 * nothing but the connection, which is thread-safe, so they may be run
 * concurrently.
 *
 * \note Each server's total is taken from its first page and never refreshed.
 *
 * @param list out-param populated with one walk per responding server; unless
 * the return is #NATS_INVALID_ARG, always release it with
 * #natsSysConnzWalkList_Destroy.
 * @param client the system client. Must outlive the walks.
 * @param opts the request options, or `NULL` for the defaults. Copied, so the
 * caller may free its strings as soon as this returns.
 * @param timeout milliseconds for the initial ping, or 0 for
 * #NATS_SYS_DEFAULT_REQUEST_TIMEOUT.
 * @return #NATS_OK on success, #NATS_INVALID_ARG for a bad argument,
 * #NATS_NO_RESPONDERS when nothing is listening on the system subject,
 * #NATS_ERR for a malformed response.
 */
NATS_EXTERN natsStatus
natsSysClient_ConnzPingEach(natsSysConnzWalkList *list, natsSysClient *client,
                            const natsSysConnzOptions *opts, int64_t timeout);

/** \brief Returns the ID of the server a walk covers; borrowed.
 *
 * Never `NULL` for a walk handed back by #natsSysClient_ConnzPingEach — a
 * reply that did not name its sender is rejected there rather than turned
 * into a walk. Returns `NULL` only if `walk` itself is `NULL`.
 */
NATS_EXTERN const char *
natsSysConnzWalk_ServerID(const natsSysConnzWalk *walk);

/** \brief Drives one walk to completion.
 *
 * Delivers the page already fetched by the ping, then continues with by-ID
 * requests until the server's reported total is reached, a page comes back
 * empty, or `handler` returns `false`.
 *
 * A walk that ran to completion, or that was stopped by the handler, delivers
 * nothing on a second run. One that returned #NATS_TIMEOUT is *not* finished:
 * running it again resumes from the page it did not reach.
 *
 * @param walk the walk to run.
 * @param timeout milliseconds for the remainder of this walk, or 0 for
 * #NATS_SYS_DEFAULT_REQUEST_TIMEOUT.
 * @param handler invoked once per page; cannot be `NULL`.
 * @param closure passed through to `handler`.
 * @return #NATS_OK when the walk finished or the handler stopped it,
 * #NATS_TIMEOUT if the budget ran out, #NATS_NOT_FOUND if the server dropped
 * out of the cluster between the ping and the walk.
 */
NATS_EXTERN natsStatus
natsSysConnzWalk_Run(natsSysConnzWalk *walk, int64_t timeout,
                     natsSysConnzPageHandler handler, void *closure);

/** \brief Releases the contents of a #natsSysConnzWalkList.
 *
 * Passing `NULL` is a no-op. The list object itself is not freed.
 */
NATS_EXTERN void
natsSysConnzWalkList_Destroy(natsSysConnzWalkList *list);

/** \brief Destroys a response returned by #natsSysClient_Connz.
 *
 * Passing `NULL` is a no-op.
 */
NATS_EXTERN void
natsSysConnzResp_Destroy(natsSysConnzResp *resp);

/** \brief Releases the contents of a #natsSysConnzRespList.
 *
 * Passing `NULL` is a no-op. The list object itself is not freed.
 */
NATS_EXTERN void
natsSysConnzRespList_Destroy(natsSysConnzRespList *list);

/** @} */ // end natsSysConnzGroup

#if defined(__cplusplus)
}
#endif

#endif /* NATS_SYSCLIENT_CONNZ_H_ */
