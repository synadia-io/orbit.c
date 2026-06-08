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

#ifndef JETSTREAM_EXTRA_FAST_PUBLISH_H_
#define JETSTREAM_EXTRA_FAST_PUBLISH_H_

#include <nats/nats.h>
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

/** \defgroup jsFastPublishGroup Fast Publish
 *
 * Fast batch publish API.
 *
 * Streams a batch of messages into a JetStream stream using server-side
 * flow control instead of awaiting an ack per message. The server emits
 * a coalesced flow ack every `Flow` published messages, and the client
 * only stalls when too many flow-ack windows are outstanding.
 *
 * A #jsFastPublishCtx holds one in-progress batch and is NOT safe for
 * concurrent use: all calls on the same context must be made from a
 * single thread.
 * @{
 */

/** \defgroup jsFastPublishTypesGroup Types
 *
 *  Fast publish types.
 *  @{
 */

/** \brief Opaque context for a single fast-publish batch session.
 *
 * Create with #jsFastPublishCtx_Create and release with
 * #jsFastPublish_Destroy. The context becomes closed once
 * #jsFastPublish_Commit, #jsFastPublish_CommitMsg, or
 * #jsFastPublish_Close has been called, after which no further messages
 * may be added; the only legal follow-up call is #jsFastPublish_Destroy.
 */
typedef struct __jsFastPublishCtx jsFastPublishCtx;

/** \brief Per-message acknowledgement returned by #jsFastPublish_Add and
 * #jsFastPublish_AddMsg.
 *
 * `BatchSequence` is the 1-based position of the just-published message
 * within the current batch. `AckSequence` is the highest position in the
 * batch that the server has confirmed via a flow ack at the moment Add
 * returned; messages above it are in flight and may still need to be
 * retried by application code on failure.
 *
 * When the batch was created with `ContinueOnGap = true`,
 * `AckSequence` does NOT imply every preceding message was persisted;
 * gaps may have been tolerated. See #jsFastPublisherOptions.
 */
typedef struct jsFastPubAck
{
    uint64_t BatchSequence; ///< 1-based sequence of this message within the batch.
    uint64_t AckSequence;   ///< Highest in-batch sequence known to be persisted.

} jsFastPubAck;

/** \brief Per-message options for the #jsFastPublish_Add family.
 *
 * Initialise with #jsBatchMsgOpts_Init before setting fields. Pointer
 * fields are borrowed; the caller must keep them alive for the duration
 * of the call.
 *
 * Fields are translated to JetStream message headers when the call
 * executes; unset fields produce no header.
 */
typedef struct jsBatchMsgOpts
{
    int64_t     TTL;                    ///< Per-message TTL, in nanoseconds. 0 = unset.
    const char  *ExpectedStream;        ///< Asserted stream name. `NULL` = unset.
    const char  *ExpectedLastSubject;   ///< Subject for the expected-last-subject-sequence check. `NULL` = unset.
    uint64_t    ExpectedLastSubjSeq;    ///< Expected last sequence for #ExpectedLastSubject; used only when #HasExpectedLastSubjSeq is `true`.
    bool        HasExpectedLastSubjSeq; ///< `true` when #ExpectedLastSubjSeq is set (0 is a valid sequence).
    uint64_t    ExpectedLastSeq;        ///< Expected last sequence of the stream; used only when #HasExpectedLastSeq is `true`.
    bool        HasExpectedLastSeq;     ///< `true` when #ExpectedLastSeq is set (0 is a valid sequence).

} jsBatchMsgOpts;

/** @} */ // end jsFastPublishTypesGroup

/** \defgroup jsFastPublishCallbacksGroup Callbacks
 *
 *  Fast publish callbacks.
 *  @{
 */

/** \brief Asynchronous error handler invoked from the ack-handling
 * thread.
 *
 * Called for server-side signals that cannot naturally surface in a
 * function return: per-message errors reported by the server, gap
 * detections under `ContinueOnGap = true`, and ack-message parse
 * failures. Errors that already cause a call to fail synchronously are
 * NOT forwarded here.
 *
 * The handler is invoked from the ack-handling thread with no internal
 * lock held. It must not block for long and must not call back into the
 * same #jsFastPublishCtx.
 *
 * @param status the underlying #natsStatus when applicable, or #NATS_ERR.
 * @param description human-readable diagnostic; valid only for the duration of the call.
 * @param closure the user-supplied pointer registered on
 *   #jsFastPublisherOptions.
 */
typedef void (*jsFastPublishErrHandler)(natsStatus status,
                                        const char *description,
                                        void       *closure);

/** @} */ // end jsFastPublishCallbacksGroup

/** \defgroup jsFastPublishOptionsGroup Options
 *
 *  Fast publish configuration.
 *  @{
 */

/** \brief Flow-control configuration accepted by #jsFastPublishCtx_Create.
 *
 * Initialise with #jsFastPublisherOptions_Init before overriding fields.
 * All fields are optional; zero (or `NULL`, for pointers) means "use the
 * default".
 */
typedef struct jsFastPublisherOptions
{
    /** \brief Initial flow-ack frequency, in messages.
     *
     * The server emits one flow ack per `Flow` published messages. The
     * server may adjust this value dynamically and the client tracks
     * the change for subsequent stall calculations.
     *
     * Default: 100.
     */
    uint16_t Flow;

    /** \brief Maximum number of unacknowledged flow-ack windows before
     * the publisher stalls.
     *
     * #jsFastPublish_Add blocks once
     * `Flow * MaxOutstandingAcks` messages are outstanding, until a flow
     * ack arrives or #AckTimeout elapses.
     *
     * Default: 2.
     */
    uint16_t MaxOutstandingAcks;

    /** \brief Total wall-clock timeout, in milliseconds, applied to any
     * single stall.
     *
     * 0 = use the built-in default of 5000 ms. The deadline is shared
     * across re-stall cycles within a single Add call; it is not a
     * per-ack budget.
     */
    int64_t  AckTimeout;

    /** \brief Whether the batch should be permitted to continue when the
     * server detects a gap.
     *
     * `false` (default): the server abandons the batch on the first gap
     * and subsequent #jsFastPublish_Add calls fail.
     *
     * `true`: the batch keeps accepting messages; gap detection is
     * surfaced via #ErrHandler instead of failing the call.
     */
    bool     ContinueOnGap;

    /** \brief Optional async error handler. See #jsFastPublishErrHandler. */
    jsFastPublishErrHandler ErrHandler;

    /** \brief Closure forwarded to #ErrHandler. */
    void *ErrHandlerClosure;

} jsFastPublisherOptions;

/** @} */ // end jsFastPublishOptionsGroup

/** \defgroup jsFastPublishFuncGroup Functions
 *
 *  Fast publish functions.
 *  @{
 */

/** \brief Initialises a #jsBatchMsgOpts to its defaults.
 *
 * Zeros every field. Use before setting any specific per-message option.
 *
 * @param opts the options struct to initialise; cannot be `NULL`.
 * @return #NATS_OK on success, #NATS_INVALID_ARG if `opts` is `NULL`.
 */
NATS_EXTERN natsStatus
jsBatchMsgOpts_Init(jsBatchMsgOpts *opts);

/** \brief Initialises a #jsFastPublisherOptions to its defaults.
 *
 * Zeros every field. The library substitutes the documented defaults
 * (Flow = 100, MaxOutstandingAcks = 2, AckTimeout = 5000 ms) for any
 * field left at zero when the options struct is passed to
 * #jsFastPublishCtx_Create.
 *
 * @param opts the options struct to initialise; cannot be `NULL`.
 * @return #NATS_OK on success, #NATS_INVALID_ARG if `opts` is `NULL`.
 */
NATS_EXTERN natsStatus
jsFastPublisherOptions_Init(jsFastPublisherOptions *opts);

/** \brief Creates a new fast-publish context bound to a JetStream context.
 *
 * The returned context owns one in-progress batch. On success `*ctx`
 * receives a newly allocated context that must be released with
 * #jsFastPublish_Destroy.
 *
 * `js` is borrowed and must outlive `*ctx`. `opts` is read fully during
 * the call and may be released immediately afterwards.
 *
 * @param ctx out-param receiving the new context.
 * @param js the JetStream context backing the batch. Must outlive `*ctx`.
 * @param opts optional flow-control configuration; `NULL` selects defaults.
 * @return #NATS_OK on success, #NATS_INVALID_ARG for malformed
 *   arguments, or #NATS_NO_MEMORY on allocation failure.
 */
NATS_EXTERN natsStatus
jsFastPublishCtx_Create(jsFastPublishCtx **ctx, jsCtx *js,
                        jsFastPublisherOptions *opts);

/** \brief Adds a message to the current batch.
 *
 * Publishes the message immediately. The first call on a context blocks
 * until the server's first flow ack arrives or `AckTimeout` elapses;
 * subsequent calls return as soon as the message is on the wire unless
 * the outstanding-ack window is full, in which case the call blocks
 * until the next flow ack or `AckTimeout` elapses.
 *
 * The reported #jsFastPubAck reflects the batch state at the moment Add
 * returns; `AckSequence` may trail `BatchSequence`. Pass `NULL` for
 * `ack` to discard this information.
 *
 * If the batch has already been closed (by commit, fatal gap, or
 * explicit close) the call fails with #NATS_ERR and the context must be
 * destroyed.
 *
 * @param ack out-param receiving the per-message ack, or `NULL`.
 * @param ctx the fast-publish context.
 * @param nc the connection to publish on. Must be the same connection used to create `ctx`.
 * @param subject subject for the message.
 * @param data payload; may be `NULL` when `dataLen` is 0.
 * @param dataLen length of `data`, in bytes.
 * @param opts optional per-message options; `NULL` to send no extra headers.
 * @return #NATS_OK on success, #NATS_TIMEOUT if a stall exceeded
 *   `AckTimeout`, #NATS_INVALID_ARG for malformed arguments, or another
 *   #natsStatus on transport or protocol error.
 */
NATS_EXTERN natsStatus
jsFastPublish_Add(jsFastPubAck *ack, jsFastPublishCtx *ctx, natsConnection *nc,
                  const char *subject, const void *data, int dataLen,
                  jsBatchMsgOpts *opts);

/** \brief Adds a pre-built #natsMsg to the current batch.
 *
 * Behaves identically to #jsFastPublish_Add but accepts a fully formed
 * message, allowing the caller to set arbitrary headers. `msg` is cloned
 * internally; the clone carries the library's ack-inbox reply subject
 * while the caller's `msg` — subject, data, headers, and reply — is left
 * unmodified. `msg` is borrowed for the duration of the call; ownership
 * is not transferred and the caller remains responsible for destroying
 * it.
 *
 * @param ack out-param receiving the per-message ack, or `NULL`.
 * @param ctx the fast-publish context.
 * @param msg the message to add. Cannot be `NULL`.
 * @param opts optional per-message options; applied to the internal
 *   clone, not to `msg`. `NULL` for none.
 * @return same status codes as #jsFastPublish_Add.
 */
NATS_EXTERN natsStatus
jsFastPublish_AddMsg(jsFastPubAck *ack, jsFastPublishCtx *ctx, natsMsg *msg,
                     jsBatchMsgOpts *opts);

/** \brief Publishes the final message of the batch and commits it.
 *
 * Blocks until the server returns the commit ack, until `timeout`
 * milliseconds elapse, or until a transport error occurs.
 *
 * On any return the context is closed; the only legal follow-up call is
 * #jsFastPublish_Destroy.
 *
 * On #NATS_OK `*pubAck` is set to a newly allocated #jsPubAck that the
 * caller must release with `jsPubAck_Destroy()`. Pass `NULL` for
 * `pubAck` to discard the ack. On any non-OK return `*pubAck` is
 * unchanged.
 *
 * @param pubAck out-param receiving the commit ack, or `NULL`.
 * @param ctx the fast-publish context.
 * @param subject subject for the final message.
 * @param data payload; may be `NULL` when `dataLen` is 0.
 * @param dataLen length of `data`, in bytes.
 * @param opts optional per-message options; `NULL` for none.
 * @param timeout commit deadline in milliseconds. Must be > 0.
 * @return #NATS_OK on natural commit, #NATS_TIMEOUT if `timeout`
 *   elapsed before the server replied, #NATS_INVALID_ARG for malformed
 *   arguments, or another #natsStatus on protocol / transport error.
 */
NATS_EXTERN natsStatus
jsFastPublish_Commit(jsPubAck **pubAck, jsFastPublishCtx *ctx,
                     const char *subject, const void *data, int dataLen,
                     jsBatchMsgOpts *opts, int64_t timeout);

/** \brief Publishes a pre-built #natsMsg as the final message of the
 * batch and commits it.
 *
 * Behaves identically to #jsFastPublish_Commit but accepts a fully
 * formed message. `msg` is cloned internally; the clone carries the
 * library's commit reply subject while the caller's `msg` is left
 * unmodified. `msg` is borrowed for the duration of the call; ownership
 * is not transferred.
 *
 * @param pubAck out-param receiving the commit ack, or `NULL`.
 * @param ctx the fast-publish context.
 * @param msg the final message. Cannot be `NULL`.
 * @param opts optional per-message options; applied to the internal
 *   clone, not to `msg`. `NULL` for none.
 * @param timeout commit deadline in milliseconds. Must be > 0.
 * @return same status codes as #jsFastPublish_Commit.
 */
NATS_EXTERN natsStatus
jsFastPublish_CommitMsg(jsPubAck **pubAck, jsFastPublishCtx *ctx, natsMsg *msg,
                        jsBatchMsgOpts *opts, int64_t timeout);

/** \brief Closes the batch without publishing a final message.
 *
 * Sends an end-of-batch commit reusing the subject of the first message
 * in the batch and waits up to `timeout` milliseconds for the server's
 * commit ack.
 *
 * Returns #NATS_ERR if no message has been successfully added to the
 * batch, or if the batch is already closed; `*pubAck` is unchanged in
 * either case.
 *
 * On #NATS_OK `*pubAck` is set to a newly allocated #jsPubAck that the
 * caller must release with `jsPubAck_Destroy()`. Pass `NULL` for
 * `pubAck` to discard the ack.
 *
 * @param pubAck out-param receiving the commit ack, or `NULL`.
 * @param ctx the fast-publish context.
 * @param timeout commit deadline in milliseconds. Must be > 0.
 * @return #NATS_OK on natural commit, #NATS_TIMEOUT if `timeout`
 *   elapsed, #NATS_ERR on an empty or already-closed batch, or another
 *   #natsStatus on transport error.
 */
NATS_EXTERN natsStatus
jsFastPublish_Close(jsPubAck **pubAck, jsFastPublishCtx *ctx, int64_t timeout);

/** \brief Reports whether the batch has been closed.
 *
 * A batch is closed after a successful commit, after a successful
 * #jsFastPublish_Close, after a fatal gap (when `ContinueOnGap` is
 * `false`), or after a transport error that aborted the batch.
 *
 * @param ctx the fast-publish context. `NULL` returns `true`.
 * @return `true` when the batch is no longer accepting messages.
 */
NATS_EXTERN bool
jsFastPublish_IsClosed(jsFastPublishCtx *ctx);

/** \brief Releases all resources owned by `ctx`.
 *
 * Tears down the ack subscription and frees memory. Safe to call on a
 * context that has already been committed or closed. Passing `NULL` is a
 * no-op.
 *
 * Destroying a context whose batch is still open does NOT send a commit
 * or end-of-batch marker on the wire; the in-progress batch is simply
 * abandoned. To seal a batch, call #jsFastPublish_Commit,
 * #jsFastPublish_CommitMsg, or #jsFastPublish_Close before destroying.
 *
 * @param ctx the context to destroy, or `NULL`.
 */
NATS_EXTERN void
jsFastPublish_Destroy(jsFastPublishCtx *ctx);

/** @} */ // end jsFastPublishFuncGroup
/** @} */ // end jsFastPublishGroup

#ifdef __cplusplus
}
#endif

#endif /* JETSTREAM_EXTRA_FAST_PUBLISH_H_ */
