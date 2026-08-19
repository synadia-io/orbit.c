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

#ifndef NATS_SYSCLIENT_HEALTHZ_H_
#define NATS_SYSCLIENT_HEALTHZ_H_

#include "sysclient.h"

#include <nats/nats.h>
#include <stdbool.h>
#include <stdint.h>

#if defined(__cplusplus)
extern "C" {
#endif

/** \defgroup natsSysHealthzGroup HEALTHZ
 *
 * Server health, from the `$SYS.REQ.SERVER.<id>.HEALTHZ` endpoint.
 * @{
 */

/** \brief The kind of problem a #natsSysHealthzError describes. */
typedef enum
{
    natsSysHealthzErrorConn = 0,   ///< Connection-level problem.
    natsSysHealthzErrorBadRequest, ///< The health request itself was invalid.
    natsSysHealthzErrorJetStream,  ///< JetStream is unhealthy.
    natsSysHealthzErrorAccount,    ///< An account is unhealthy.
    natsSysHealthzErrorStream,     ///< A stream is unhealthy.
    natsSysHealthzErrorConsumer,   ///< A consumer is unhealthy.

} natsSysHealthzErrorType;

/** \brief One problem reported by a health check.
 *
 * Only returned when #natsSysHealthzOptions.Details was set. A string field is
 * `NULL` when the server did not send it.
 */
typedef struct __natsSysHealthzError
{
    natsSysHealthzErrorType Type;     ///< What kind of thing is unhealthy.
    char                   *Account;  ///< Account involved, if any.
    char                   *Stream;   ///< Stream involved, if any.
    char                   *Consumer; ///< Consumer involved, if any.
    char                   *Error;    ///< Description of the problem.

} natsSysHealthzError;

/** \brief The health of one server. */
typedef struct __natsSysHealthz
{
    char                *Status;      ///< `"ok"` when healthy.
    int                  StatusCode;  ///< HTTP-like status code.
    char                *Error;       ///< Summary error; `NULL` when healthy.
    natsSysHealthzError *Errors;      ///< Detailed problems; see #natsSysHealthzOptions.Details.
    int                  ErrorsCount; ///< Number of entries in #Errors.

} natsSysHealthz;

/** \brief A HEALTHZ response from one server.
 *
 * \warning #Error is decoded but never acted on. Check `Error.Code != 0`
 * before trusting #Healthz.
 */
typedef struct __natsSysHealthzResp
{
    natsSysServerInfo Server;  ///< Which server answered.
    natsSysHealthz    Healthz; ///< The payload (wire key `data`).
    natsSysAPIError   Error;   ///< Server-reported error, if any.

} natsSysHealthzResp;

/** \brief The responses gathered by #natsSysClient_HealthzPing.
 *
 * Caller-provided, typically on the stack. #natsSysHealthzRespList_Destroy
 * releases the contents, not the list object itself. To keep one response past
 * the destroy, set its slot to `NULL` first; do not change #Count.
 */
typedef struct __natsSysHealthzRespList
{
    natsSysHealthzResp **Resps; ///< One response per server that answered.
    int                  Count; ///< Number of entries in #Resps.

} natsSysHealthzRespList;

/** \brief Options for a HEALTHZ request.
 *
 * Initialise with #natsSysHealthzOptions_Init. Every field is optional and a
 * zero-valued one is left out of the request.
 */
typedef struct __natsSysHealthzOptions
{
    bool  JSEnabledOnly; ///< Only check that the server is connected to JetStream.
    bool  JSServerOnly;  ///< Only check server health, skipping JetStream.
    const char *Account; ///< Check this account; required for #Stream or #Consumer.
    const char *Stream;  ///< Check this stream.
    const char *Consumer; ///< Check this consumer.
    bool  Details;       ///< Return #natsSysHealthz.Errors detail.

} natsSysHealthzOptions;

/** \brief Initialises a #natsSysHealthzOptions to its defaults (all unset).
 *
 * @param opts the options struct to initialise; cannot be `NULL`.
 * @return #NATS_OK on success, #NATS_INVALID_ARG if `opts` is `NULL`.
 */
NATS_EXTERN natsStatus
natsSysHealthzOptions_Init(natsSysHealthzOptions *opts);

/** \brief Requests the health of one server.
 *
 * @param newResp out-param set to the response; destroy with
 * #natsSysHealthzResp_Destroy. Set to `NULL` on error.
 * @param client the system client.
 * @param serverID the target server's ID, as found in
 * #natsSysServerInfo.ID. Cannot be `NULL` or empty.
 * @param opts the request options, or `NULL` for the defaults.
 * @param timeout milliseconds to wait, or 0 for
 * #NATS_SYS_DEFAULT_REQUEST_TIMEOUT.
 * @return #NATS_OK on success, #NATS_INVALID_ARG for a bad argument,
 * #NATS_NOT_FOUND when no server with that ID answered, #NATS_TIMEOUT if it
 * did not answer in time, #NATS_ERR for a malformed response.
 */
NATS_EXTERN natsStatus
natsSysClient_Healthz(natsSysHealthzResp **newResp, natsSysClient *client,
                      const char *serverID, const natsSysHealthzOptions *opts,
                      int64_t timeout);

/** \brief Requests the health of every server in the cluster.
 *
 * Scatters to `$SYS.REQ.SERVER.PING.HEALTHZ` and gathers the replies, stopping
 * on whichever comes first of the configured server count, the stall interval
 * elapsing, or `timeout`.
 *
 * \note Running out of time is normal termination, not an error: a gather that
 * hears nothing before `timeout` returns #NATS_OK with a `Count` of 0. That is
 * distinct from #NATS_NO_RESPONDERS, which means the subject had no subscribers
 * at all and usually indicates the connection is not on the system account. If
 * one reply is malformed the whole batch is discarded.
 *
 * @param list out-param populated with the responses; unless the return is
 * #NATS_INVALID_ARG, always release it with #natsSysHealthzRespList_Destroy.
 * @param client the system client.
 * @param opts the request options, or `NULL` for the defaults.
 * @param timeout milliseconds to wait, or 0 for
 * #NATS_SYS_DEFAULT_REQUEST_TIMEOUT.
 * @return #NATS_OK on success, #NATS_INVALID_ARG for a bad argument,
 * #NATS_NO_RESPONDERS when nothing is listening on the system subject,
 * #NATS_ERR for a malformed response.
 */
NATS_EXTERN natsStatus
natsSysClient_HealthzPing(natsSysHealthzRespList *list, natsSysClient *client,
                          const natsSysHealthzOptions *opts, int64_t timeout);

/** \brief Destroys a response returned by #natsSysClient_Healthz.
 *
 * Passing `NULL` is a no-op.
 */
NATS_EXTERN void
natsSysHealthzResp_Destroy(natsSysHealthzResp *resp);

/** \brief Releases the contents of a #natsSysHealthzRespList.
 *
 * Passing `NULL` is a no-op. The list object itself is not freed.
 */
NATS_EXTERN void
natsSysHealthzRespList_Destroy(natsSysHealthzRespList *list);

/** @} */ // end natsSysHealthzGroup

#if defined(__cplusplus)
}
#endif

#endif /* NATS_SYSCLIENT_HEALTHZ_H_ */
