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

#ifndef NATS_COUNTERS_H_
#define NATS_COUNTERS_H_

#include <nats/nats.h>
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/** \defgroup NatsCounters NATS Counters
 *
 * Nats-Counters
 * @{
 */

/** \def NATS_COUNTER_INCREMENT_HDR
 *  \brief Nats-Incr header.
 *
 *  Value: a decimal integer string with an optional sign, e.g. "5", "+5", or
 * "-3".
 */
#define NATS_COUNTER_INCREMENT_HDR "Nats-Incr"

/** \def NATS_COUNTER_SOURCES_HDR
 *  \brief Nats-Counter-Sources header.
 *
 *  Value: JSON object keyed by stream name, each value being an object keyed
 *  by original subject mapping to its last seen counter value string.
 *  Maintained automatically by the server.
 */
#define NATS_COUNTER_SOURCES_HDR "Nats-Counter-Sources"

/** \defgroup counterTypesGroup Types
 *
 *  Counter types.
 *  @{
 */

/** \brief Wraps a JetStream stream configured with allow_msg_counter.
 *
 * Create with #natsCounter_GetFromStream().
 * Destroy with #natsCounter_Destroy().
 */
typedef struct __natsCounter natsCounter;

struct natsCounterSource;

/** \brief Holds a snapshot of a counter at a point in time.
 *
 * Includes its value, last increment, and source-tracking state.
 * Obtain from #natsCounter_Get(); destroy with #natsCounterEntry_Destroy().
 */
typedef struct __natsCounterEntry {
  char *subject;                     ///< Stream subject.
  char *value;                       ///< Decimal string, arbitrary precision.
  char *increment;                   ///< Decimal string; NULL if header absent.
  struct natsCounterSource *sources; ///< Linked list; NULL if header absent.

} natsCounterEntry;

/** \brief A list of counter entries, used for batch retrieval.
 *
 * Used by some APIs which return a list of #natsCounterEntry objects.
 */
typedef struct __natsCounterEntryList {
    natsCounterEntry **Entries;
    int Count;
} natsCounterEntryList;

/** @} */ // end counterTypesGroup

/** \defgroup counterCallbacksGroup Callbacks
 *
 *  Counter callbacks.
 *  @{
 */

/** \brief Callback invoked once per result by #natsCounter_GetMultiple().
 *
 * @param counter the counter that issued the batch request (not owned).
 * @param entry the current result entry (caller must NOT destroy; valid only
 * for the duration of this callback).
 * @param status #NATS_OK on success; any other value signals an
 * error and `entry` will be `NULL`.
 * @param closure the user-supplied pointer passed to
 * #natsCounter_GetMultiple().
 */
typedef void (*natsCounterIterFn)(natsCounter *counter, natsCounterEntry *entry,
                                  natsStatus status, void *closure);

/** \brief Callback invoked once per source contribution by
 * #natsCounterEntry_IterSources().
 *
 * @param stream source stream name.
 * @param subject original pre-transform subject in the source stream.
 * @param value last-seen counter value for that source (string, arbitrary
 * precision).
 * @param closure the user-supplied pointer.
 */
typedef void (*natsCounterSourceIterFn)(const char *stream, const char *subject,
                                        const char *value, void *closure);

/** @} */ // end counterCallbacksGroup

//
// Functions.
//
/** \defgroup counterFuncGroup Functions
 *
 *  Counter functions.
 *  @{
 */

/** \defgroup counterLifecycleGroup Lifecycle
 *
 *  Counter lifecycle management.
 *  @{
 */

/** \brief Creates a counter client for the named stream.
 *
 * The stream must have both `allow_msg_counter` and `allow_direct` enabled.
 *
 * On success, `*counter` is set to a newly allocated #natsCounter; the caller
 * is responsible for destroying it with #natsCounter_Destroy().
 *
 * @param counter receives the newly allocated counter.
 * @param js JetStream context.
 * @param nc the NATS connection that backs `js`. Used by #natsCounter_GetMultiple
 * for batched DIRECT.GET. Must outlive the counter.
 * @param stream name of the stream.
 * @return #NATS_OK on success, #NATS_INVALID_CONFIG if a
 * required stream flag is absent.
 */
NATS_EXTERN natsStatus natsCounter_GetFromStream(natsCounter **counter,
                                                 jsCtx *js,
                                                 natsConnection *nc,
                                                 const char *stream);

/** \brief Releases all resources owned by `counter`.
 *
 * Passing `NULL` is safe and is a no-op.
 *
 * @param counter the counter to destroy, or `NULL`.
 */
NATS_EXTERN void natsCounter_Destroy(natsCounter *counter);

/** @} */ // end counterLifecycleGroup

/** \defgroup counterIntGroup Integer Operations
 *
 *  These functions accept and return `long long` values. Use the `*Str`
 *  variants for counters that may exceed the range of `long long`.
 *  @{
 */

/** \brief Adds `delta` to the counter for `subject`.
 *
 * The server atomically increments the stored value and echoes the new total
 * in the PubAck response. `delta` may be negative (subtraction).
 *
 * @param counter the counter.
 * @param subject the subject to modify.
 * @param delta amount to add (may be negative).
 * @param newValue receives the resulting total.
 * @return #NATS_OK on success, #NATS_ERR if the
 * resulting value exceeds `long long`.
 */
NATS_EXTERN natsStatus natsCounter_Add(natsCounter *counter,
                                       const char *subject, long long delta,
                                       long long *newValue);

/** \brief Adds `delta` to the counter for `subject`, returning the result as
 * an arbitrary-precision decimal string.
 *
 * This is the C equivalent of Go's `AddInt(ctx, subject, value) (*big.Int,
 * error)`. The delta is a convenient `long long`, but the returned value will
 * never overflow because it is a heap-allocated decimal string.
 *
 * @param counter the counter.
 * @param subject the subject to modify.
 * @param delta amount to add (may be negative).
 * @param newValue receives a newly allocated string; caller must `free()`.
 * @return #NATS_OK on success.
 */
NATS_EXTERN natsStatus natsCounter_AddInt(natsCounter *counter,
                                          const char *subject, long long delta,
                                          char **newValue);

/** \brief Adds 1 to the counter for `subject`.
 *
 * @param counter the counter.
 * @param subject the subject to increment.
 * @param newValue receives the resulting total.
 * @return #NATS_OK on success.
 */
NATS_EXTERN natsStatus natsCounter_Increment(natsCounter *counter,
                                             const char *subject,
                                             long long *newValue);

/** \brief Subtracts 1 from the counter for `subject`.
 *
 * @param counter the counter.
 * @param subject the subject to decrement.
 * @param newValue receives the resulting total.
 * @return #NATS_OK on success.
 */
NATS_EXTERN natsStatus natsCounter_Decrement(natsCounter *counter,
                                             const char *subject,
                                             long long *newValue);

/** \brief Reads the current total for `subject` without modifying it.
 *
 * @param counter the counter.
 * @param subject the subject to read.
 * @param value receives the current total. The value will not be modified on
 * error.
 * @return #NATS_OK on success, #NATS_ERR if the
 * value exceeds `long long`.
 */
NATS_EXTERN natsStatus natsCounter_Load(natsCounter *counter,
                                        const char *subject, long long *value);

/** @} */ // end counterIntGroup

/** \defgroup counterStrGroup String Operations
 *
 *  Arbitrary-precision (string) operations.
 *
 *  Delta and returned values are null-terminated decimal strings, optionally
 *  prefixed with '+' or '-'. The caller is responsible for freeing the
 *  returned strings with `free()`.
 *  @{
 */

/** \brief Adds the decimal string `delta` to the counter for `subject`.
 *
 * `*newValue` is set to a newly allocated string; caller must `free()`.
 *
 * @param counter the counter.
 * @param subject the subject to modify.
 * @param delta decimal string delta (e.g. "+5" or "-3").
 * @param newValue receives a newly allocated string; caller must `free()`.
 * @return #NATS_OK on success.
 */
NATS_EXTERN natsStatus natsCounter_AddStr(natsCounter *counter,
                                          const char *subject,
                                          const char *delta, char **newValue);

/** \brief Reads the current total as a decimal string.
 *
 * `*value` is set to a newly allocated string; caller must `free()`.
 *
 * @param counter the counter.
 * @param subject the subject to read.
 * @param value receives a newly allocated string; caller must `free()`.
 * @return #NATS_OK on success.
 */
NATS_EXTERN natsStatus natsCounter_LoadStr(natsCounter *counter,
                                           const char *subject, char **value);

/** @} */ // end counterStrGroup

/** \defgroup counterEntryGroup Entry Operations
 *
 *  Fetch and inspect full counter entries.
 *  @{
 */

/** \brief Fetches the full counter entry for `subject`.
 *
 * Includes the current value, the last increment, and source tracking
 * information.
 *
 * On success, `*entry` is set to a newly allocated #natsCounterEntry; the
 * caller must destroy it with #natsCounterEntry_Destroy().
 *
 * @param counter the counter.
 * @param subject the subject to fetch.
 * @param entry receives the newly allocated entry.
 * @return #NATS_OK on success, #NATS_NOT_FOUND if the
 * subject has never been written.
 */
NATS_EXTERN natsStatus natsCounter_Get(natsCounter *counter,
                                       const char *subject,
                                       natsCounterEntry **entry);

/** \brief Fetches counter entries for multiple subjects in a single batch.
 *
 * Issues one batched DIRECT.GET request (ADR-31 multi-last) for the given
 * subject list and invokes `handler` once per matching entry, then once
 * more with a null entry to signal completion. Non-existent subjects are
 * silently skipped by the server. `subjects` may contain NATS wildcard
 * tokens.
 *
 * The function blocks until the server's end-of-batch sentinel arrives
 * or an internal timeout (5 seconds) elapses; entries are still
 * streamed to `handler` as they are parsed.
 *
 * @param counter the counter.
 * @param subjects array of subject strings.
 * @param numSubjects number of elements in `subjects`. Must not exceed
 * the server's multi-last cap (1024).
 * @param handler callback invoked once per result, then once with a
 * null entry to signal completion. On error, `handler` is invoked with
 * a null entry and a non-OK status; no completion sentinel follows.
 * @param closure user-supplied pointer forwarded to `handler`.
 * @return #NATS_OK on success.
 */
NATS_EXTERN natsStatus
natsCounter_GetMultiple(natsCounterEntryList  *outList,
                        natsCounter           *counter,
                        const char            **subjects,
                        int                   numSubjects,
                        uint64_t              timeout);

/** \brief Returns `true` when the fetched message carries a Nats-Incr header.
 *
 * Always `true` for live counter messages.
 *
 * @param entry the entry.
 * @return `true` if an increment is present.
 */
NATS_EXTERN bool natsCounterEntry_HasIncrement(natsCounterEntry *entry);

/** \brief Returns `true` when the entry carries a Nats-Counter-Sources header.
 *
 * This indicates the stream uses source-based aggregation.
 *
 * @param entry the entry.
 * @return `true` if sources are present.
 */
NATS_EXTERN bool natsCounterEntry_HasSources(natsCounterEntry *entry);

/** \brief Iterates over each per-source contribution in the entry.
 *
 * Invokes `fn` for each source stored in the Nats-Counter-Sources header.
 * Does nothing if the entry has no sources.
 *
 * @param entry the entry.
 * @param fn callback invoked once per source.
 * @param closure user-supplied pointer forwarded to `fn`.
 * @return #NATS_OK on success.
 */
NATS_EXTERN natsStatus natsCounterEntry_IterSources(natsCounterEntry *entry,
                                                    natsCounterSourceIterFn fn,
                                                    void *closure);

/** \brief Releases all resources owned by `entry`.
 *
 * Passing `NULL` is safe and is a no-op.
 *
 * @param entry the entry to destroy, or `NULL`.
 */
NATS_EXTERN void natsCounterEntry_Destroy(natsCounterEntry *entry);

NATS_EXTERN
void natsCounterEntryList_Destroy(natsCounterEntryList *list);

/** @} */ // end counterEntryGroup

/** @} */ // end counterFuncGroup

/** @} */ // end NatsCounters

#ifdef __cplusplus
}
#endif

#endif /* NATS_COUNTERS_H_ */
