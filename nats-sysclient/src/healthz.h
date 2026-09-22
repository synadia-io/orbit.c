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
 * Server health, from `$SYS.REQ.SERVER.<id>.HEALTHZ`.
 * @{
 */

/** \brief The kind of problem a #natsSysHealthzError describes. */
typedef enum
{
    natsSysHealthzErrorConn = 0,
    natsSysHealthzErrorBadRequest,
    natsSysHealthzErrorJetStream,
    natsSysHealthzErrorAccount,
    natsSysHealthzErrorStream,
    natsSysHealthzErrorConsumer,

} natsSysHealthzErrorType;

/** \brief One problem found by a health check; only reported when
 * #natsSysHealthzOptions.Details is set. */
typedef struct __natsSysHealthzError
{
    natsSysHealthzErrorType Type;
    char                   *Account;
    char                   *Stream;
    char                   *Consumer;
    char                   *Error;

} natsSysHealthzError;

/** \brief The health of one server. */
typedef struct __natsSysHealthz
{
    char                *Status;     ///< `"ok"` when healthy.
    int                  StatusCode; ///< HTTP-like.
    char                *Error;
    natsSysHealthzError *Errors;
    int                  ErrorsCount;

} natsSysHealthz;

/** \brief A HEALTHZ response. Check `Error.Code` before reading #Healthz. */
typedef struct __natsSysHealthzResp
{
    natsSysServerInfo Server;
    natsSysHealthz    Healthz; ///< Wire key `data`.
    natsSysAPIError   Error;

} natsSysHealthzResp;

/** \brief Responses gathered by #natsSysClient_HealthzPing.
 *
 * Typically a stack object; #natsSysHealthzRespList_Destroy frees the
 * responses and the array but not the list itself. To keep a response, set
 * its slot to `NULL` and leave `Count` unchanged.
 */
typedef struct __natsSysHealthzRespList
{
    natsSysHealthzResp **Resps;
    int                  Count;

} natsSysHealthzRespList;

/** \brief HEALTHZ request options; a zero-valued field is left out. */
typedef struct __natsSysHealthzOptions
{
    bool        JSEnabledOnly; ///< Only check that JetStream is enabled.
    bool        JSServerOnly;  ///< Only check the server, not JetStream assets.
    const char *Account;       ///< Required with #Stream or #Consumer.
    const char *Stream;
    const char *Consumer;
    bool        Details;       ///< Populate #natsSysHealthz.Errors.

} natsSysHealthzOptions;

/** \brief Initializes options to their defaults (all unset). */
NATS_EXTERN natsStatus
natsSysHealthzOptions_Init(natsSysHealthzOptions *opts);

/** \brief Requests the health of one server.
 *
 * @param newResp the location where to store the response; destroy with
 * #natsSysHealthzResp_Destroy.
 * @param client the system client.
 * @param serverID the server's ID, from #natsSysServerInfo.ID.
 * @param opts the options, or `NULL` for the defaults.
 * @param timeout in milliseconds; 0 for #NATS_SYS_DEFAULT_REQUEST_TIMEOUT.
 * @return #NATS_NOT_FOUND when no server with that ID answered,
 * #NATS_TIMEOUT when it did not answer in time, #NATS_ERR for a malformed
 * response.
 */
NATS_EXTERN natsStatus
natsSysClient_Healthz(natsSysHealthzResp **newResp, natsSysClient *client,
                      const char *serverID, const natsSysHealthzOptions *opts,
                      int64_t timeout);

/** \brief Requests the health of every server.
 *
 * Scatters to `$SYS.REQ.SERVER.PING.HEALTHZ` and gathers replies until the
 * configured server count, the stall interval or `timeout` is reached.
 * Reaching `timeout` is not an error: a gather that heard nothing returns
 * #NATS_OK with a `Count` of 0. One malformed reply discards the whole batch.
 *
 * @param list the list to fill; always destroy it with
 * #natsSysHealthzRespList_Destroy unless the return is #NATS_INVALID_ARG.
 * @param client the system client.
 * @param opts the options, or `NULL` for the defaults.
 * @param timeout in milliseconds; 0 for #NATS_SYS_DEFAULT_REQUEST_TIMEOUT.
 * @return #NATS_NO_RESPONDERS when nothing is subscribed to the subject,
 * usually because the connection is not on the system account; #NATS_ERR
 * for a malformed response.
 */
NATS_EXTERN natsStatus
natsSysClient_HealthzPing(natsSysHealthzRespList *list, natsSysClient *client,
                          const natsSysHealthzOptions *opts, int64_t timeout);

/** \brief Destroys a response; `NULL` is a no-op. */
NATS_EXTERN void
natsSysHealthzResp_Destroy(natsSysHealthzResp *resp);

/** \brief Destroys the contents of a list, not the list itself. */
NATS_EXTERN void
natsSysHealthzRespList_Destroy(natsSysHealthzRespList *list);

/** @} */ // end natsSysHealthzGroup

#if defined(__cplusplus)
}
#endif

#endif /* NATS_SYSCLIENT_HEALTHZ_H_ */
