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
 * Client connections, from `$SYS.REQ.SERVER.<id>.CONNZ`. The result is
 * paginated; #natsSysClient_ConnzEach walks every page.
 * @{
 */

/**
 * \name Sort orders for #natsSysConnzOptions.Sort
 *
 * Only #NATS_SYS_SORT_CID sorts ascending.
 * @{
 */
#define NATS_SYS_SORT_CID       "cid"
#define NATS_SYS_SORT_START     "start"
#define NATS_SYS_SORT_SUBS      "subs"
#define NATS_SYS_SORT_PENDING   "pending"
#define NATS_SYS_SORT_OUT_MSGS  "msgs_to"
#define NATS_SYS_SORT_IN_MSGS   "msgs_from"
#define NATS_SYS_SORT_OUT_BYTES "bytes_to"
#define NATS_SYS_SORT_IN_BYTES  "bytes_from"
#define NATS_SYS_SORT_LAST      "last"
#define NATS_SYS_SORT_IDLE      "idle"
#define NATS_SYS_SORT_UPTIME    "uptime"
#define NATS_SYS_SORT_STOP      "stop"
#define NATS_SYS_SORT_REASON    "reason"
/** @} */

/** \brief Which connections a CONNZ request returns. */
typedef enum
{
    natsSysConnOpen = 0,
    natsSysConnClosed,
    natsSysConnAll,

} natsSysConnState;

/** \brief Summary of a TLS peer certificate. */
typedef struct __natsSysTLSPeerCert
{
    char *Subject;
    char *SubjectPKISha256; ///< Wire key `spki_sha256`.
    char *CertSha256;

} natsSysTLSPeerCert;

/** \brief One client connection.
 *
 * #Subs and #SubsDetail are only populated when requested through
 * #natsSysConnzOptions.
 */
typedef struct __natsSysConnInfo
{
    uint64_t Cid;
    char    *Kind;         ///< Client, Router, Leafnode, ...
    char    *Type;         ///< nats, mqtt, websocket.
    char    *IP;
    int      Port;
    char    *Start;        ///< RFC 3339.
    char    *LastActivity; ///< RFC 3339.
    char    *Stop;         ///< RFC 3339; `NULL` while open.
    char    *Reason;       ///< `NULL` while open.
    char    *RTT;          ///< The server's own string.
    char    *Uptime;       ///< The server's own string.
    char    *Idle;         ///< The server's own string.
    int64_t  Pending;      ///< Bytes waiting to be written (wire key `pending_bytes`); may exceed 2GB.
    int64_t  InMsgs;
    int64_t  OutMsgs;
    int64_t  InBytes;
    int64_t  OutBytes;
    uint32_t NumSubs;      ///< Wire key `subscriptions`.
    char    *Name;
    char    *Lang;
    char    *Version;
    char    *TLSVersion;
    char    *TLSCipher;    ///< Wire key `tls_cipher_suite`.

    natsSysTLSPeerCert **TLSPeerCerts;
    int                  TLSPeerCertsCount;

    bool  TLSFirst;
    char *AuthorizedUser;
    char *Account;

    char **Subs; ///< Wire key `subscriptions_list`.
    int    SubsCount;

    natsSysSubDetail *SubsDetail; ///< Wire key `subscriptions_list_detail`.
    int               SubsDetailCount;

    char  *JWT;
    char  *IssuerKey;
    char  *NameTag;
    char **Tags;
    int    TagsCount;
    char  *MQTTClient;

} natsSysConnInfo;

/** \brief One page of connections. */
typedef struct __natsSysConnz
{
    char *ID;  ///< Wire key `server_id`.
    char *Now; ///< RFC 3339.

    int NumConns; ///< Connections in this page (wire key `num_connections`).
    int Total;    ///< Matching connections across all pages.
    int Offset;
    int Limit;

    natsSysConnInfo **Conns;
    int               ConnsCount;

} natsSysConnz;

/** \brief A CONNZ response. Check `Error.Code` before reading #Connz. */
typedef struct __natsSysConnzResp
{
    natsSysServerInfo Server;
    natsSysConnz      Connz; ///< Wire key `data`.
    natsSysAPIError   Error;

} natsSysConnzResp;

/** \brief Responses gathered by #natsSysClient_ConnzPing; see
 * #natsSysHealthzRespList. */
typedef struct __natsSysConnzRespList
{
    natsSysConnzResp **Resps;
    int                Count;

} natsSysConnzRespList;

/** \brief CONNZ request options.
 *
 * Every field but #Filter is always sent, so an empty request still carries
 * `"sort":""` and `"state":0`; that is what the server expects.
 */
typedef struct __natsSysConnzOptions
{
    const char *Sort;                ///< A `NATS_SYS_SORT_*` value.
    bool        Username;            ///< Include user names (wire key `auth`).
    bool        Subscriptions;       ///< Include #natsSysConnInfo.Subs.
    bool        SubscriptionsDetail; ///< Include #natsSysConnInfo.SubsDetail.
    int         Offset;
    int         Limit;

    uint64_t         CID;        ///< Return only this connection.
    const char      *MQTTClient; ///< Return only this MQTT client ID.
    natsSysConnState State;

    // Filters that require Username.
    const char *User;
    const char *Account;       ///< Wire key `acc`.
    const char *FilterSubject; ///< Connections with interest in this subject.

    natsSysEventFilterOptions Filter;

} natsSysConnzOptions;

/** \brief Called once per page of a walk.
 *
 * @param page the page; destroyed when the handler returns.
 * @param closure the closure passed to the walk.
 * @return `true` to fetch the next page, `false` to stop.
 */
typedef bool (*natsSysConnzPageHandler)(const natsSysConnzResp *page, void *closure);

/** \brief A resumable pagination over one server. */
typedef struct __natsSysConnzWalk natsSysConnzWalk;

/** \brief One walk per server that answered a ping. */
typedef struct __natsSysConnzWalkList
{
    natsSysConnzWalk **Walks;
    int                Count;

} natsSysConnzWalkList;

/** \brief Initializes options to their defaults (all unset). */
NATS_EXTERN natsStatus
natsSysConnzOptions_Init(natsSysConnzOptions *opts);

/** \brief Requests one page of connections from one server; see
 * #natsSysClient_Healthz. */
NATS_EXTERN natsStatus
natsSysClient_Connz(natsSysConnzResp **newResp, natsSysClient *client,
                    const char *serverID, const natsSysConnzOptions *opts,
                    int64_t timeout);

/** \brief Requests one page of connections from every server; see
 * #natsSysClient_HealthzPing. */
NATS_EXTERN natsStatus
natsSysClient_ConnzPing(natsSysConnzRespList *list, natsSysClient *client,
                        const natsSysConnzOptions *opts, int64_t timeout);

/** \brief Walks every page of connections on one server.
 *
 * Requests pages in a loop, advancing the offset by each page's size, until
 * the reported total is reached, a page comes back empty, or `handler`
 * returns `false`. Pages already delivered stay delivered on error.
 *
 * @param client the system client.
 * @param serverID the server's ID.
 * @param opts the options, or `NULL` for the defaults; the walk keeps its
 * own offset.
 * @param timeout in milliseconds for the whole walk, not per page; 0 for
 * #NATS_SYS_DEFAULT_REQUEST_TIMEOUT.
 * @param handler called once per page.
 * @param closure passed to `handler`.
 * @return #NATS_TIMEOUT when the budget ran out mid-walk, or any status
 * #natsSysClient_Connz returns.
 */
NATS_EXTERN natsStatus
natsSysClient_ConnzEach(natsSysClient *client, const char *serverID,
                        const natsSysConnzOptions *opts, int64_t timeout,
                        natsSysConnzPageHandler handler, void *closure);

/** \brief Pings every server and returns one walk per reply.
 *
 * Each walk holds the page its server returned to the ping and continues
 * from there with by-ID requests. Walks are independent and may run on
 * separate threads. A server's total is taken from its first page and never
 * refreshed. A reply that did not carry a server ID is rejected with
 * #NATS_ERR.
 *
 * @param list the list to fill; always destroy it with
 * #natsSysConnzWalkList_Destroy unless the return is #NATS_INVALID_ARG.
 * @param client the system client; must outlive the walks.
 * @param opts the options, or `NULL`; copied, so the caller's strings may
 * be freed once this returns.
 * @param timeout in milliseconds for the ping; 0 for
 * #NATS_SYS_DEFAULT_REQUEST_TIMEOUT.
 */
NATS_EXTERN natsStatus
natsSysClient_ConnzPingEach(natsSysConnzWalkList *list, natsSysClient *client,
                            const natsSysConnzOptions *opts, int64_t timeout);

/** \brief Returns the ID of the server a walk covers. */
NATS_EXTERN const char *
natsSysConnzWalk_ServerID(const natsSysConnzWalk *walk);

/** \brief Runs a walk to completion.
 *
 * Delivers the page the ping fetched, then every remaining page. A finished
 * walk delivers nothing on a second run; one that returned #NATS_TIMEOUT
 * resumes from the page it did not reach.
 *
 * @param walk the walk.
 * @param timeout in milliseconds for the rest of the walk; 0 for
 * #NATS_SYS_DEFAULT_REQUEST_TIMEOUT.
 * @param handler called once per page.
 * @param closure passed to `handler`.
 * @return #NATS_NOT_FOUND if the server left the cluster since the ping.
 */
NATS_EXTERN natsStatus
natsSysConnzWalk_Run(natsSysConnzWalk *walk, int64_t timeout,
                     natsSysConnzPageHandler handler, void *closure);

/** \brief Destroys the contents of a walk list, not the list itself. */
NATS_EXTERN void
natsSysConnzWalkList_Destroy(natsSysConnzWalkList *list);

/** \brief Destroys a response; `NULL` is a no-op. */
NATS_EXTERN void
natsSysConnzResp_Destroy(natsSysConnzResp *resp);

/** \brief Destroys the contents of a list, not the list itself. */
NATS_EXTERN void
natsSysConnzRespList_Destroy(natsSysConnzRespList *list);

/** @} */ // end natsSysConnzGroup

#if defined(__cplusplus)
}
#endif

#endif /* NATS_SYSCLIENT_CONNZ_H_ */
