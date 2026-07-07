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

#ifndef NATS_REQUEST_MANY_H_
#define NATS_REQUEST_MANY_H_

#include <nats/nats.h>
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

/** \defgroup natsRequestManyGroup Request Many
 *
 * Request-many API: scatter a single request and gather multiple replies.
 * @{
 */

/** \defgroup natsRequestManyTypesGroup Types
 *
 *  Request-many types.
 *  @{
 */

/** \brief Predicate invoked once per received reply to decide whether the
 * gather should stop.
 *
 * Called by the library for each reply as it arrives, before the message is
 * appended to the list. Returning `true` terminates the gather, and the inspected
 * message is destroyed. The sentinel does not take ownership of `msg`.
 *
 * @param msg the reply being inspected; borrowed, do not destroy.
 * @return `true` to stop gathering after this message, `false` to continue.
 */
typedef bool (*natsRequestManySentinel)(natsMsg *msg);

/** \brief Options controlling when a request-many gather terminates.
 *
 * Initialise with #natsRequestManyOpts_Init before setting fields. The gather
 * ends when the first of the configured conditions is met. Leaving a field at
 * its zero value disables that condition, except #timeout, which falls back to
 * a built-in default so the gather never blocks indefinitely.
 */
typedef struct __natsRequestManyOpts
{
    uint64_t                stall;    ///< Stall timer in milliseconds: stop once this long elapses with no new reply. 0 = disabled.
    int                     count;    ///< Stop after this many replies. Unlimited if count is <= 0.
    natsRequestManySentinel sentinel; ///< Optional predicate to stop on a caller-recognised reply. May be `NULL`.
    uint64_t                timeout;  ///< Overall deadline in milliseconds for the entire gather, measured from when the request is sent. 0 = use a built-in default. The gather never blocks indefinitely.

} natsRequestManyOpts;

/** @} */ // end natsRequestManyTypesGroup

/** \defgroup natsRequestManyFuncGroup Functions
 *
 *  Request-many functions.
 *  @{
 */

/** \brief Initialises a #natsRequestManyOpts to its defaults.
 *
 * Zeros every field, disabling the stall, count, and sentinel stop conditions;
 * #timeout falls back to a built-in default. Set the desired fields before
 * passing the struct to a request-many call.
 *
 * @param opts the options struct to initialise; cannot be `NULL`.
 * @return #NATS_OK on success, #NATS_INVALID_ARG if `opts` is `NULL`.
 */
natsStatus
natsRequestManyOpts_Init(natsRequestManyOpts *opts);

/** \brief Synchronously scatters a request and gathers the replies into a list.
 *
 * Publishes `data` to `subj` with a private inbox reply subject and blocks
 * while replies are collected, returning once a stop condition in `opts` is
 * met or an error occurs.
 *
 * On success, `*list` is populated with the replies received. Unless return code is
 * #NATS_INVALID_ARG, always destroy with #natsMsgList_Destroy.
 *
 * @param list out-param populated with the received replies.
 * @param nc the NATS connection. Must be open for the duration of the call.
 * @param subj the subject to publish the request to.
 * @param data the request payload, or `NULL` for an empty body.
 * @param opts the gather options (see #natsRequestManyOpts).
 * @return #NATS_OK on natural completion, #NATS_TIMEOUT if no stop condition
 * was reached (the partial result is still in `*list`), #NATS_INVALID_ARG for
 * malformed arguments, or another #natsStatus for transport errors.
 */
natsStatus
natsRequestMany_RequestMany(natsMsgList *list, natsConnection *nc, const char *subj,
                            const char *data, int dataLen, natsRequestManyOpts *opts);

/** \brief Synchronously scatters a pre-built request message and gathers the
 * replies into a list.
 *
 * Identical to #natsRequestMany_RequestMany but takes a caller-constructed
 * #natsMsg, allowing custom headers on the request. The library overwrites the
 * message's reply subject with its private inbox. Ownership of `msg` remains
 * with the caller.
 *
 * @param list out-param populated with the received replies; unless return code is
 * #NATS_INVALID_ARG, always destroy with #natsMsgList_Destroy
 * @param nc the NATS connection. Must be open for the duration of the call.
 * @param msg the request message to publish; borrowed, not destroyed.
 * @param opts the gather options (see #natsRequestManyOpts).
 * @return #NATS_OK on natural completion, #NATS_TIMEOUT if no stop condition
 * was reached, #NATS_INVALID_ARG for malformed arguments, or another
 * #natsStatus for transport errors.
 */
natsStatus
natsRequestMany_RequestManyMsg(natsMsgList *list, natsConnection *nc, natsMsg *msg,
                               natsRequestManyOpts *opts);

/** @} */ // end natsRequestManyFuncGroup
/** @} */ // end natsRequestManyGroup

#ifdef __cplusplus
}
#endif

#endif /* NATS_REQUEST_MANY_H_ */
