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

#ifndef JETSTREAM_EXTRA_BATCH_FETCH_H_
#define JETSTREAM_EXTRA_BATCH_FETCH_H_

#include <nats/nats.h>
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

/** \def JS_BATCH_FETCH_MAX_BATCH
 *  \brief Server-imposed upper bound on the number of messages a single
 *  batch fetch may return.
 */
#define JS_BATCH_FETCH_MAX_BATCH 1000

/** \def JS_BATCH_FETCH_MAX_SUBJECTS
 *  \brief Server-imposed upper bound on the number of subjects passed to
 *  the multi-last form of a batch fetch.
 */
#define JS_BATCH_FETCH_MAX_SUBJECTS 1024

    /** \defgroup jsBatchFetchGroup Batch Fetch
 *
 * Batch fetch API
 * @{
 */

    /** \defgroup jsBatchFetchTypesGroup Types
 *
 *  Batch fetch types.
 *  @{
 */

/** \brief Options for a JetStream DIRECT.GET batch request.
 *
 * Mirrors the field set described in ADR-31. Pass to #jsBatchFetch_Fetch or
 * #jsBatchFetch_AsyncFetch along with a `jsCtx`.
 *
 * Mutual exclusivity (validated client-side):
 *   - `Sequence` and `StartTime` may not both be set.
 *   - `UpToSeq` and `UpToTime` may not both be set.
 *   - When `MultiLastFor` is set, `MultiLastForLen` must be > 0.
 *
 * Initialise with #jsBatchFetchOptions_Init before setting fields. All
 * pointer fields are borrowed (not copied); the caller must keep them
 * alive for the duration of the call.
 */
typedef struct jsBatchFetchOptions
{
    uint64_t        Sequence;        ///< Start at this sequence (1-based). Mutually exclusive with #StartTime.
    const char      *NextBySubject;  ///< Filter messages by this subject (may include wildcards).
    int             Batch;           ///< Maximum messages to return. Server limit: #JS_BATCH_FETCH_MAX_BATCH.
    int             MaxBytes;        ///< Soft byte cap on the response. 0 = unlimited.
    uint64_t        StartTime;       ///< Start at-or-after this timestamp, in nanoseconds since the Unix epoch (UTC). Serialised as RFC 3339. Mutually exclusive with #Sequence.
    const char      **MultiLastFor;  ///< Subjects to fetch the last message for. Activates multi-last mode.
    int             MultiLastForLen; ///< Number of entries in #MultiLastFor. Server limit: #JS_BATCH_FETCH_MAX_SUBJECTS.
    uint64_t        UpToSeq;         ///< Multi-last cutoff by sequence. Mutually exclusive with #UpToTime.
    uint64_t        UpToTime;        ///< Multi-last cutoff timestamp, in nanoseconds since the Unix epoch (UTC). Serialised as RFC 3339. Mutually exclusive with #UpToSeq.

} jsBatchFetchOptions;

/** @} */ // end jsBatchFetchTypesGroup

/** \defgroup jsBatchFetchCallbacksGroup Callbacks
 *
 *  Batch fetch callbacks.
 *  @{
 */

/** \brief Invoked exactly once when an asynchronous batch fetch terminates.
 *
 * Fires on natural end-of-batch, on timeout, or on error. After this
 * callback returns the underlying subscription has been torn down; no
 * further `msgCB` invocations will follow.
 *
 * @param status #NATS_OK on natural completion (server EOB),
 * #NATS_TIMEOUT if the user-supplied deadline elapsed, or another
 * #natsStatus on transport / protocol error.
 * @param jsErr JetStream error code surfaced by the server, or 0 if not
 * applicable.
 * @param closure the user-supplied pointer passed to #jsBatchFetch_AsyncFetch.
 */
typedef void (*jsBatchFetchCompleteHandler)(natsStatus status,
                                            jsErrCode  jsErr,
                                            void       *closure);

/** @} */ // end jsBatchFetchCallbacksGroup

/** \defgroup jsBatchFetchFuncGroup Functions
 *
 *  Batch fetch functions.
 *  @{
 */

/** \brief Initialises a #jsBatchFetchOptions to its defaults.
 *
 * Zeros every field. Use before setting any specific options and passing
 * the struct to #jsBatchFetch_Fetch or #jsBatchFetch_AsyncFetch.
 *
 * @param opts the options struct to initialise; cannot be `NULL`.
 * @return #NATS_OK on success, #NATS_INVALID_ARG if `opts` is `NULL`.
 */
NATS_EXTERN natsStatus
jsBatchFetchOptions_Init(jsBatchFetchOptions *opts);

/** \brief Synchronously fetches a batch of messages from a JetStream stream.
 *
 * Issues a single `DIRECT.GET.<stream>` request and blocks while the
 * server streams the matching messages back over an inbox subscription.
 * Returns when the server signals end-of-batch, when `timeout`
 * milliseconds have elapsed, or on error.
 *
 * The stream must have been created with `AllowDirect = true` and the
 * server must support batched DIRECT.GET (NATS Server v2.10+).
 *
 * On success, `*list` is populated with the messages received. Always
 * destroy with #natsMsgList_Destroy regardless of return code, since a
 * partial list may have been built before a timeout fired.
 *
 * @param list out-param populated with the received messages.
 * @param nc the NATS connection. Must be open for the duration of the
 * call.
 * @param stream the name of the stream to read from.
 * @param opts optional #jsOptions for API prefix and request timeout
 * overrides. May be `NULL` for defaults (`$JS.API.` prefix).
 * @param bopts the batch-fetch options (see #jsBatchFetchOptions).
 * @param timeout overall fetch deadline, in milliseconds. Must be > 0.
 * @param errCode out-param for the server-reported JetStream error code,
 * or `NULL` if not needed.
 * @return #NATS_OK on natural completion, #NATS_TIMEOUT if the
 * deadline elapsed before EOB (the partial result is still in `*list`),
 * #NATS_NOT_FOUND if no matching messages exist, #NATS_INVALID_ARG
 * for malformed options, #NATS_NO_SERVER_SUPPORT if the server lacks
 * batched DIRECT.GET, or another #natsStatus for transport errors.
 */
NATS_EXTERN natsStatus
jsBatchFetch_Fetch(natsMsgList *list, natsConnection *nc, const char *stream, jsOptions *opts,
                   jsBatchFetchOptions *bopts, int64_t timeout, jsErrCode *errCode);

/** \brief Asynchronously fetches a batch of messages from a JetStream stream.
 *
 * Sets up an inbox subscription, fires the request, and returns
 * immediately. Each matching message is delivered to `msgCB` from a
 * library worker thread. When the batch terminates (server EOB or error),
 * `doneCB` is invoked exactly once and the subscription is torn down.
 *
 * `msgCB` takes ownership of each delivered message and must call
 * #natsMsg_Destroy on it. End-of-batch sentinel messages emitted by the
 * server are intercepted by the library and never delivered to `msgCB`.
 *
 * The async form has no client-side deadline; termination is driven by
 * the server's EOB sentinel. Use the synchronous #jsBatchFetch_Fetch when a
 * timeout is required.
 *
 * @param nc the NATS connection. Must outlive the fetch.
 * @param stream the name of the stream to read from.
 * @param opts optional #jsOptions for API prefix overrides; may be `NULL`.
 * @param bopts the batch-fetch options (see #jsBatchFetchOptions).
 * @param msgCB callback invoked once per received message.
 * @param doneCB callback invoked exactly once when the fetch terminates.
 * @param closure user-supplied pointer forwarded to both callbacks.
 * @return #NATS_OK once the request is in flight,
 * #NATS_INVALID_ARG for malformed options, or another #natsStatus
 * on setup failure.
 */
NATS_EXTERN natsStatus
jsBatchFetch_AsyncFetch(natsConnection *nc, const char *stream, jsOptions *opts,
                        jsBatchFetchOptions *bopts, natsMsgHandler msgCB,
                        jsBatchFetchCompleteHandler doneCB, void *closure);

/** @} */ // end jsBatchFetchFuncGroup
/** @} */ // end jsBatchFetchGroup

#ifdef __cplusplus
}
#endif

#endif /* JETSTREAM_EXTRA_BATCH_FETCH_H_ */
