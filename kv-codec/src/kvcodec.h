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

#ifndef KV_CODEC_H_
#define KV_CODEC_H_

#include <nats/nats.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

/** \defgroup kvCodecGroup KV Codec
 *
 * Wraps a JetStream KeyValue store (#kvStore) to transparently encode and
 * decode keys and values with pluggable codecs.
 * @{
 */

/** \defgroup kvCodecTypesGroup Types
 *
 *  KV Codec types.
 *  @{
 */

/** \brief An opaque key codec.
 *
 * Create with #kvKeyCodec_Base64, #kvKeyCodec_Path, #kvKeyCodec_NoOp,
 * #kvKeyCodec_New or #kvKeyCodec_Chain; destroy with #kvKeyCodec_Destroy.
 */
typedef struct __kvKeyCodec kvKeyCodec;

/** \brief An opaque value codec.
 *
 * Create with #kvValueCodec_Base64, #kvValueCodec_NoOp, #kvValueCodec_New or
 * #kvValueCodec_Chain; destroy with #kvValueCodec_Destroy.
 */
typedef struct __kvValueCodec kvValueCodec;

/** \brief An opaque codec-enabled wrapper around a #kvStore.
 *
 * Create with #kvCodec_New; destroy with #kvCodec_Destroy.
 */
typedef struct __kvCodec kvCodec;

/** \brief A KeyValue entry with the key and value already decoded.
 *
 * Returned by #kvCodec_Get, #kvCodec_GetRevision, #kvCodec_History and
 * #kvCodecWatcher_Next; destroy with #kvCodecEntry_Destroy.
 */
typedef struct __kvCodecEntry kvCodecEntry;

/** \brief A watcher whose entries are decoded before delivery.
 *
 * Created by #kvCodec_Watch, #kvCodec_WatchMulti and #kvCodec_WatchAll;
 * destroy with #kvCodecWatcher_Destroy.
 */
typedef struct __kvCodecWatcher kvCodecWatcher;

/** \brief A list of decoded key strings.
 *
 * Initialized and filled by #kvCodec_Keys / #kvCodec_KeysWithFilters on a
 * caller-provided (typically stack) object; release the contents with
 * #kvCodecKeysList_Destroy.
 */
typedef struct kvCodecKeysList
{
    char **Keys;  ///< Array of decoded, NUL-terminated key strings.
    int    Count; ///< Number of keys in the array.

} kvCodecKeysList;

/** \brief A list of decoded entries.
 *
 * Initialized and filled by #kvCodec_History on a caller-provided (typically
 * stack) object; release the contents with #kvCodecEntryList_Destroy.
 */
typedef struct kvCodecEntryList
{
    kvCodecEntry **Entries; ///< Array of decoded entries.
    int            Count;   ///< Number of entries in the array.

} kvCodecEntryList;

/** \brief Allocates a buffer for a codec-callback output.
 *
 * Custom codec transforms (#kvKeyTransformF, #kvValueTransformF) must
 * allocate their output buffers with this function so allocation and release
 * both stay inside the library.
 *
 * Behaves like `malloc`: a size of 0 may return `NULL`.
 *
 * @param size the number of bytes to allocate.
 * @return the buffer, or `NULL` on allocation failure.
 */
NATS_EXTERN void *
kvCodec_AllocBuf(size_t size);

/** \brief Releases a buffer obtained from #kvCodec_AllocBuf.
 *
 * Only needed for buffers that were never handed to the library, such as a
 * partial result on a callback's own failure path; outputs returned with
 * #NATS_OK are released by the library.
 *
 * @param buf the buffer to release; `NULL` is a no-op.
 */
NATS_EXTERN void
kvCodec_FreeBuf(void *buf);

/** \brief Key transformation callback for custom key codecs (encode, decode
 * and filter encode).
 *
 * On success, set `*out` to a NUL-terminated string allocated with
 * #kvCodec_AllocBuf and return #NATS_OK. The library takes ownership of
 * `*out` and releases it. On failure return any status other than #NATS_OK;
 * it is propagated verbatim to the caller of the triggering kvCodec call.
 *
 * @param out out-param: the transformed key; allocated with
 * #kvCodec_AllocBuf, released by the library.
 * @param in the key (or filter) to transform; NUL-terminated, borrowed.
 * @param closure the opaque pointer given to #kvKeyCodec_New.
 * @return #NATS_OK on success, any other status to fail the operation.
 */
typedef natsStatus (*kvKeyTransformF)(char **out, const char *in, void *closure);

/** \brief Value transformation callback for custom value codecs (encode and
 * decode).
 *
 * On success, set `*out` to a buffer of `*outLen` bytes allocated with
 * #kvCodec_AllocBuf and return #NATS_OK. The library takes ownership of
 * `*out` and releases it. Values are binary: no NUL termination is required
 * or assumed.
 *
 * @param out out-param: the transformed value; allocated with
 * #kvCodec_AllocBuf, released by the library.
 * @param outLen out-param: the length of `*out` in bytes.
 * @param in the value to transform; may be `NULL` iff `inLen` is 0; borrowed.
 * @param inLen the length of `in` in bytes.
 * @param closure the opaque pointer given to #kvValueCodec_New.
 * @return #NATS_OK on success, any other status to fail the operation.
 */
typedef natsStatus (*kvValueTransformF)(void **out, int *outLen,
                                        const void *in, int inLen, void *closure);

/** \brief Destructor callback for a custom codec's closure.
 *
 * Invoked by #kvKeyCodec_Destroy / #kvValueCodec_Destroy.
 *
 * @param closure the opaque pointer given to the codec constructor.
 */
typedef void (*kvCodecDestroyF)(void *closure);

/** @} */ // end kvCodecTypesGroup

/** \defgroup kvKeyCodecGroup Key codecs
 *
 *  Constructors and destructor for key codecs.
 *  @{
 */

/** \brief Creates a Base64 key codec.
 *
 * Splits the key on '.', encodes each token with URL-safe, unpadded Base64
 * (RFC 4648 section 5, no '=') and re-joins with '.', preserving the NATS
 * subject-token structure. Filterable: tokens that are exactly `*` or `>`
 * are preserved by the filter encoder.
 *
 * \warning Empty tokens encode to empty tokens, and NATS KV rejects keys
 * with leading, trailing or consecutive dots: a key such as `"a..b"` encodes
 * fine but every store operation on it fails with #NATS_INVALID_ARG.
 *
 * @param newCodec out-param: the new codec; destroy with #kvKeyCodec_Destroy.
 * @return #NATS_OK, #NATS_INVALID_ARG if `newCodec` is `NULL`, #NATS_NO_MEMORY.
 */
NATS_EXTERN natsStatus
kvKeyCodec_Base64(kvKeyCodec **newCodec);

/** \brief Creates a path key codec translating `/path/style` keys to NATS
 * subject notation.
 *
 * '/' becomes '.', a leading '/' is encoded as a `_root_` token, `"/"` maps
 * to `_root_`, and a trailing '/' is trimmed. Filterable: `*` and `>`
 * survive the translation.
 *
 * \warning The translation is not injective, matching the Go reference:
 * - a trailing '/' is trimmed, so `"foo/bar/"` decodes back as `"foo/bar"`;
 * - a literal '.' passes through, so `"a.b"` and `"a/b"` store under the
 *   same subject (the later put overwrites the earlier), and listings
 *   report the '/' form;
 * - a caller key literally starting with `_root_` collides with the
 *   sentinel: `"_root_"` is indistinguishable from `"/"`;
 * - doubled slashes (`"//"`, `"a//b"`) produce subjects with empty tokens,
 *   which NATS KV rejects with #NATS_INVALID_ARG.
 *
 * @param newCodec out-param: the new codec; destroy with #kvKeyCodec_Destroy.
 * @return #NATS_OK, #NATS_INVALID_ARG if `newCodec` is `NULL`, #NATS_NO_MEMORY.
 */
NATS_EXTERN natsStatus
kvKeyCodec_Path(kvKeyCodec **newCodec);

/** \brief Creates a no-op (passthrough) key codec. Filterable.
 *
 * Useful as a chain member or wherever a codec object is required.
 *
 * @param newCodec out-param: the new codec; destroy with #kvKeyCodec_Destroy.
 * @return #NATS_OK, #NATS_INVALID_ARG if `newCodec` is `NULL`, #NATS_NO_MEMORY.
 */
NATS_EXTERN natsStatus
kvKeyCodec_NoOp(kvKeyCodec **newCodec);

/** \brief Creates a custom key codec from user callbacks.
 *
 * @param newCodec out-param: the new codec; destroy with #kvKeyCodec_Destroy.
 * @param encodeKey required: encodes a key for storage.
 * @param decodeKey required: decodes a stored key.
 * @param encodeFilter optional: encodes a pattern preserving `*`/`>`
 * wildcards. Pass `NULL` if the codec cannot preserve wildcards; watch/list
 * calls whose patterns contain wildcards will then fail with
 * #NATS_INVALID_SUBJECT.
 * @param destroy optional: called on `closure` by #kvKeyCodec_Destroy; may be `NULL`.
 * @param closure opaque pointer passed to every callback; may be `NULL`.
 * @return #NATS_OK, #NATS_INVALID_ARG if `newCodec`, `encodeKey` or
 * `decodeKey` is `NULL`, #NATS_NO_MEMORY.
 */
NATS_EXTERN natsStatus
kvKeyCodec_New(kvKeyCodec    **newCodec,
               kvKeyTransformF encodeKey,
               kvKeyTransformF decodeKey,
               kvKeyTransformF encodeFilter,
               kvCodecDestroyF destroy,
               void           *closure);

/** \brief Creates a key codec that applies `codecs` in sequence.
 *
 * Encoding applies the codecs first-to-last, decoding last-to-first. The
 * chain is filterable only if every member is; otherwise wildcard operations
 * fail with #NATS_INVALID_SUBJECT.
 *
 * The chain borrows the member codecs: they must outlive the chain and must
 * still be destroyed by the caller (after the chain).
 *
 * @param newCodec out-param: the new codec; destroy with #kvKeyCodec_Destroy.
 * @param codecs array of at least one codec; the array itself is copied.
 * @param numCodecs the number of codecs in `codecs`; must be >= 1.
 * @return #NATS_OK, #NATS_INVALID_ARG if `newCodec` is `NULL`, `numCodecs`
 * is < 1, or any member is `NULL`, #NATS_NO_MEMORY.
 */
NATS_EXTERN natsStatus
kvKeyCodec_Chain(kvKeyCodec **newCodec, kvKeyCodec **codecs, int numCodecs);

/** \brief Destroys a key codec, invoking the custom destroy callback, if any.
 *
 * Must not be called while a #kvCodec, a chain, or a #kvCodecWatcher still
 * references the codec (watchers keep using the wrapper's codecs until they
 * are destroyed).
 *
 * @param codec the codec to destroy; may be `NULL`.
 */
NATS_EXTERN void
kvKeyCodec_Destroy(kvKeyCodec *codec);

/** @} */ // end kvKeyCodecGroup

/** \defgroup kvValueCodecGroup Value codecs
 *
 *  Constructors and destructor for value codecs.
 *  @{
 */

/** \brief Creates a Base64 value codec.
 *
 * Encodes the whole value buffer with URL-safe, unpadded Base64 (RFC 4648
 * section 5, no '=').
 *
 * @param newCodec out-param: the new codec; destroy with #kvValueCodec_Destroy.
 * @return #NATS_OK, #NATS_INVALID_ARG if `newCodec` is `NULL`, #NATS_NO_MEMORY.
 */
NATS_EXTERN natsStatus
kvValueCodec_Base64(kvValueCodec **newCodec);

/** \brief Creates a no-op (passthrough) value codec.
 *
 * @param newCodec out-param: the new codec; destroy with #kvValueCodec_Destroy.
 * @return #NATS_OK, #NATS_INVALID_ARG if `newCodec` is `NULL`, #NATS_NO_MEMORY.
 */
NATS_EXTERN natsStatus
kvValueCodec_NoOp(kvValueCodec **newCodec);

/** \brief Creates a custom value codec from user callbacks.
 *
 * @param newCodec out-param: the new codec; destroy with #kvValueCodec_Destroy.
 * @param encodeValue required: encodes a value for storage.
 * @param decodeValue required: decodes a stored value.
 * @param destroy optional: called on `closure` by #kvValueCodec_Destroy; may be `NULL`.
 * @param closure opaque pointer passed to every callback; may be `NULL`.
 * @return #NATS_OK, #NATS_INVALID_ARG if `newCodec`, `encodeValue` or
 * `decodeValue` is `NULL`, #NATS_NO_MEMORY.
 */
NATS_EXTERN natsStatus
kvValueCodec_New(kvValueCodec    **newCodec,
                 kvValueTransformF encodeValue,
                 kvValueTransformF decodeValue,
                 kvCodecDestroyF   destroy,
                 void             *closure);

/** \brief Creates a value codec that applies `codecs` in sequence.
 *
 * Encoding applies the codecs first-to-last, decoding last-to-first. The
 * chain borrows the member codecs: they must outlive the chain and must
 * still be destroyed by the caller (after the chain).
 *
 * @param newCodec out-param: the new codec; destroy with #kvValueCodec_Destroy.
 * @param codecs array of at least one codec; the array itself is copied.
 * @param numCodecs the number of codecs in `codecs`; must be >= 1.
 * @return #NATS_OK, #NATS_INVALID_ARG if `newCodec` is `NULL`, `numCodecs`
 * is < 1, or any member is `NULL`, #NATS_NO_MEMORY.
 */
NATS_EXTERN natsStatus
kvValueCodec_Chain(kvValueCodec **newCodec, kvValueCodec **codecs, int numCodecs);

/** \brief Destroys a value codec, invoking the custom destroy callback, if any.
 *
 * Must not be called while a #kvCodec, a chain, or a #kvCodecWatcher still
 * references the codec (watchers keep using the wrapper's codecs until they
 * are destroyed).
 *
 * @param codec the codec to destroy; may be `NULL`.
 */
NATS_EXTERN void
kvValueCodec_Destroy(kvValueCodec *codec);

/** @} */ // end kvValueCodecGroup

/** \defgroup kvCodecStoreGroup Codec-enabled KV store
 *
 *  Codec-transparent equivalents of the #kvStore operations.
 *  @{
 */

/** \brief Creates a codec-enabled wrapper around an existing #kvStore.
 *
 * The wrapper borrows all three objects: `kv`, `keyCodec` and `valueCodec`
 * must outlive the wrapper AND any watcher created from it, and are NOT
 * destroyed by #kvCodec_Destroy. Either codec may be `NULL`, meaning
 * passthrough for that dimension.
 *
 * @param newCodec out-param: the new wrapper; destroy with #kvCodec_Destroy.
 * @param kv the underlying store; cannot be `NULL`; borrowed.
 * @param keyCodec the key codec, or `NULL` for no key transformation.
 * @param valueCodec the value codec, or `NULL` for no value transformation.
 * @return #NATS_OK, #NATS_INVALID_ARG, #NATS_NO_MEMORY.
 */
NATS_EXTERN natsStatus
kvCodec_New(kvCodec **newCodec, kvStore *kv,
            kvKeyCodec *keyCodec, kvValueCodec *valueCodec);

/** \brief Destroys the wrapper only; the underlying #kvStore and the codecs
 * are left untouched.
 *
 * @param c the wrapper to destroy; may be `NULL`.
 */
NATS_EXTERN void
kvCodec_Destroy(kvCodec *c);

/** \brief Puts a value under `key`; the key and value are encoded before the
 * underlying #kvStore_Put.
 *
 * @param rev out-param for the new revision; may be `NULL`.
 * @param c the codec-enabled store.
 * @param key the key (in decoded, caller-side form).
 * @param data the value to store, or `NULL` with `len` 0 for an empty value.
 * @param len the length of `data` in bytes.
 * @return the codec's status on encode failure, otherwise the #kvStore_Put status.
 */
NATS_EXTERN natsStatus
kvCodec_Put(uint64_t *rev, kvCodec *c, const char *key, const void *data, int len);

/** \brief Equivalent to #kvCodec_Put with `(int) strlen(data)`.
 *
 * @param rev out-param for the new revision; may be `NULL`.
 * @param c the codec-enabled store.
 * @param key the key (in decoded, caller-side form).
 * @param data the NUL-terminated value to store; cannot be `NULL`.
 * @return the codec's status on encode failure, otherwise the #kvStore_Put status.
 */
NATS_EXTERN natsStatus
kvCodec_PutString(uint64_t *rev, kvCodec *c, const char *key, const char *data);

/** \brief Gets the latest entry for `key`, decoded.
 *
 * The returned entry reports the caller's original `key` from
 * #kvCodecEntry_Key. If decoding fails, the codec's status is returned and
 * no entry is created. #NATS_NOT_FOUND passes through from the underlying
 * store.
 *
 * @param newEntry out-param: the entry; destroy with #kvCodecEntry_Destroy.
 * @param c the codec-enabled store.
 * @param key the key (in decoded, caller-side form).
 * @return #NATS_OK, the codec's status on encode/decode failure, otherwise
 * the #kvStore_Get status (#NATS_NOT_FOUND when the key does not exist).
 */
NATS_EXTERN natsStatus
kvCodec_Get(kvCodecEntry **newEntry, kvCodec *c, const char *key);

/** \brief Gets a specific revision of `key`, decoded. See #kvCodec_Get.
 *
 * @param newEntry out-param: the entry; destroy with #kvCodecEntry_Destroy.
 * @param c the codec-enabled store.
 * @param key the key (in decoded, caller-side form).
 * @param revision the revision to fetch.
 * @return #NATS_OK, the codec's status on encode/decode failure, otherwise
 * the #kvStore_GetRevision status.
 */
NATS_EXTERN natsStatus
kvCodec_GetRevision(kvCodecEntry **newEntry, kvCodec *c, const char *key,
                    uint64_t revision);

/** \brief Creates `key`, failing if it already exists (as #kvStore_Create
 * does); the key and value are encoded first.
 *
 * @param rev out-param for the new revision; may be `NULL`.
 * @param c the codec-enabled store.
 * @param key the key (in decoded, caller-side form).
 * @param data the value to store, or `NULL` with `len` 0 for an empty value.
 * @param len the length of `data` in bytes.
 * @return the codec's status on encode failure, otherwise the #kvStore_Create status.
 */
NATS_EXTERN natsStatus
kvCodec_Create(uint64_t *rev, kvCodec *c, const char *key, const void *data, int len);

/** \brief Equivalent to #kvCodec_Create with `(int) strlen(data)`.
 *
 * @param rev out-param for the new revision; may be `NULL`.
 * @param c the codec-enabled store.
 * @param key the key (in decoded, caller-side form).
 * @param data the NUL-terminated value to store; cannot be `NULL`.
 * @return the codec's status on encode failure, otherwise the #kvStore_Create status.
 */
NATS_EXTERN natsStatus
kvCodec_CreateString(uint64_t *rev, kvCodec *c, const char *key, const char *data);

/** \brief Updates `key` if and only if its latest revision is `last`; the
 * key and value are encoded first.
 *
 * @param rev out-param for the new revision; may be `NULL`.
 * @param c the codec-enabled store.
 * @param key the key (in decoded, caller-side form).
 * @param data the value to store, or `NULL` with `len` 0 for an empty value.
 * @param len the length of `data` in bytes.
 * @param last the expected latest revision of the key.
 * @return the codec's status on encode failure, otherwise the #kvStore_Update status.
 */
NATS_EXTERN natsStatus
kvCodec_Update(uint64_t *rev, kvCodec *c, const char *key, const void *data,
               int len, uint64_t last);

/** \brief Equivalent to #kvCodec_Update with `(int) strlen(data)`.
 *
 * @param rev out-param for the new revision; may be `NULL`.
 * @param c the codec-enabled store.
 * @param key the key (in decoded, caller-side form).
 * @param data the NUL-terminated value to store; cannot be `NULL`.
 * @param last the expected latest revision of the key.
 * @return the codec's status on encode failure, otherwise the #kvStore_Update status.
 */
NATS_EXTERN natsStatus
kvCodec_UpdateString(uint64_t *rev, kvCodec *c, const char *key,
                     const char *data, uint64_t last);

/** \brief Deletes `key` (places a delete marker); the key is encoded first.
 *
 * @param c the codec-enabled store.
 * @param key the key (in decoded, caller-side form).
 * @return the codec's status on encode failure, otherwise the #kvStore_Delete status.
 */
NATS_EXTERN natsStatus
kvCodec_Delete(kvCodec *c, const char *key);

/** \brief Purges all revisions of `key`; the key is encoded first.
 *
 * @param c the codec-enabled store.
 * @param key the key (in decoded, caller-side form).
 * @param opts the pointer to #kvPurgeOptions, possibly `NULL`.
 * @return the codec's status on encode failure, otherwise the #kvStore_Purge status.
 */
NATS_EXTERN natsStatus
kvCodec_Purge(kvCodec *c, const char *key, const kvPurgeOptions *opts);

/** \brief Starts a watcher on `keys` (a subject pattern, may contain
 * wildcards).
 *
 * If the key codec is filterable, the pattern is encoded with its filter
 * encoder (preserving `*` and `>`). Otherwise, patterns containing `*` or
 * `>` are rejected with #NATS_INVALID_SUBJECT (use #kvCodec_WatchAll and
 * filter client-side instead), and wildcard-free patterns are encoded with
 * the key encoder.
 *
 * @param newWatcher out-param: the watcher; destroy with #kvCodecWatcher_Destroy.
 * @param c the codec-enabled store.
 * @param keys the pattern to watch (in decoded, caller-side form).
 * @param opts the pointer to #kvWatchOptions, possibly `NULL`.
 * @return #NATS_OK, #NATS_INVALID_SUBJECT for a wildcard pattern with a
 * non-filterable codec, the codec's status on encode failure, otherwise the
 * #kvStore_Watch status.
 */
NATS_EXTERN natsStatus
kvCodec_Watch(kvCodecWatcher **newWatcher, kvCodec *c, const char *keys,
              const kvWatchOptions *opts);

/** \brief Starts a watcher on several patterns; the wildcard rules of
 * #kvCodec_Watch apply to each pattern.
 *
 * @param newWatcher out-param: the watcher; destroy with #kvCodecWatcher_Destroy.
 * @param c the codec-enabled store.
 * @param keys the array of patterns to watch (in decoded, caller-side form).
 * @param numKeys the number of patterns in `keys`.
 * @param opts the pointer to #kvWatchOptions, possibly `NULL`.
 * @return see #kvCodec_Watch.
 */
NATS_EXTERN natsStatus
kvCodec_WatchMulti(kvCodecWatcher **newWatcher, kvCodec *c,
                   const char **keys, int numKeys, const kvWatchOptions *opts);

/** \brief Starts a watcher on all keys of the bucket (no filter encoding
 * involved).
 *
 * @param newWatcher out-param: the watcher; destroy with #kvCodecWatcher_Destroy.
 * @param c the codec-enabled store.
 * @param opts the pointer to #kvWatchOptions, possibly `NULL`.
 * @return #NATS_OK, #NATS_INVALID_ARG, #NATS_NO_MEMORY, otherwise the
 * #kvStore_WatchAll status.
 */
NATS_EXTERN natsStatus
kvCodec_WatchAll(kvCodecWatcher **newWatcher, kvCodec *c,
                 const kvWatchOptions *opts);

/** \brief Returns all keys of the bucket, decoded.
 *
 * If any stored key fails to decode, the codec's status is returned and
 * `*list` is left empty.
 *
 * @param list caller-provided list object, initialized and filled by this
 * call; release with #kvCodecKeysList_Destroy.
 * @param c the codec-enabled store.
 * @param opts the pointer to #kvWatchOptions, possibly `NULL`.
 * @return #NATS_OK, the codec's status on decode failure, otherwise the
 * #kvStore_Keys status.
 */
NATS_EXTERN natsStatus
kvCodec_Keys(kvCodecKeysList *list, kvCodec *c, const kvWatchOptions *opts);

/** \brief Returns the keys matching the given patterns, decoded.
 *
 * The patterns follow the same wildcard/filterability rules as #kvCodec_Watch.
 *
 * @param list caller-provided list object, initialized and filled by this
 * call; release with #kvCodecKeysList_Destroy.
 * @param c the codec-enabled store.
 * @param filters the array of patterns (in decoded, caller-side form).
 * @param numFilters the number of patterns in `filters`.
 * @param opts the pointer to #kvWatchOptions, possibly `NULL`.
 * @return #NATS_OK, #NATS_INVALID_SUBJECT for a wildcard pattern with a
 * non-filterable codec, the codec's status on encode/decode failure,
 * otherwise the #kvStore_KeysWithFilters status.
 */
NATS_EXTERN natsStatus
kvCodec_KeysWithFilters(kvCodecKeysList *list, kvCodec *c,
                        const char **filters, int numFilters,
                        const kvWatchOptions *opts);

/** \brief Frees the strings and the array held by the list and resets it;
 * the #kvCodecKeysList object itself is not freed.
 *
 * @param list the list to release; may be `NULL`.
 */
NATS_EXTERN void
kvCodecKeysList_Destroy(kvCodecKeysList *list);

/** \brief Returns the full history of `key` as decoded entries.
 *
 * The entries report the caller's original `key`. Delete/purge markers are
 * included (unless filtered via `opts`) and carry a `NULL` value that is
 * never passed to the value codec.
 *
 * @param list caller-provided list object, initialized and filled by this
 * call; release with #kvCodecEntryList_Destroy.
 * @param c the codec-enabled store.
 * @param key the key (in decoded, caller-side form).
 * @param opts the pointer to #kvWatchOptions, possibly `NULL`.
 * @return #NATS_OK, the codec's status on encode/decode failure, otherwise
 * the #kvStore_History status.
 */
NATS_EXTERN natsStatus
kvCodec_History(kvCodecEntryList *list, kvCodec *c, const char *key,
                const kvWatchOptions *opts);

/** \brief Destroys every entry in the list and the array itself and resets
 * it; the #kvCodecEntryList object itself is not freed.
 *
 * @param list the list to release; may be `NULL`.
 */
NATS_EXTERN void
kvCodecEntryList_Destroy(kvCodecEntryList *list);

/** \brief Pass-through to #kvStore_PurgeDeletes (no codec involvement).
 *
 * @param c the codec-enabled store.
 * @param opts the pointer to #kvPurgeOptions, possibly `NULL`.
 * @return #NATS_INVALID_ARG if `c` is `NULL`, otherwise the
 * #kvStore_PurgeDeletes status.
 */
NATS_EXTERN natsStatus
kvCodec_PurgeDeletes(kvCodec *c, const kvPurgeOptions *opts);

/** \brief Pass-through to #kvStore_Bucket. Do not free the returned string.
 *
 * @param c the codec-enabled store.
 * @return the bucket name, or `NULL` if `c` is `NULL`.
 */
NATS_EXTERN const char *
kvCodec_Bucket(const kvCodec *c);

/** \brief Pass-through to #kvStore_Status.
 *
 * @param newStatus out-param: the status; destroy with #kvStatus_Destroy.
 * @param c the codec-enabled store.
 * @return #NATS_INVALID_ARG if `c` is `NULL`, otherwise the #kvStore_Status
 * status.
 */
NATS_EXTERN natsStatus
kvCodec_Status(kvStatus **newStatus, kvCodec *c);

/** @} */ // end kvCodecStoreGroup

/** \defgroup kvCodecWatcherGroup Watcher
 *
 *  Watcher delivering decoded entries.
 *  @{
 */

/** \brief Returns the next decoded entry for this watcher.
 *
 * Mirrors #kvWatcher_Next: `*newEntry` may be set to `NULL` with #NATS_OK to
 * signal that the initial snapshot has been delivered. If decoding an
 * entry's key or value fails, the codec's status is returned, that entry is
 * dropped, and the watcher remains usable for subsequent calls.
 *
 * `*newEntry` is always set to `NULL` first, so it is defined on every
 * return, including #NATS_TIMEOUT.
 *
 * @param newEntry out-param: the entry (destroy with #kvCodecEntry_Destroy),
 * or `NULL` for the initial-values marker.
 * @param w the watcher.
 * @param timeout how long to wait (in milliseconds) for the next entry.
 * @return #NATS_OK, the codec's status on decode failure, otherwise the
 * #kvWatcher_Next status (#NATS_TIMEOUT when no entry arrived in time).
 */
NATS_EXTERN natsStatus
kvCodecWatcher_Next(kvCodecEntry **newEntry, kvCodecWatcher *w, int64_t timeout);

/** \brief Stops the watcher (pass-through to #kvWatcher_Stop).
 *
 * @param w the watcher.
 * @return #NATS_INVALID_ARG if `w` is `NULL`, otherwise the #kvWatcher_Stop
 * status.
 */
NATS_EXTERN natsStatus
kvCodecWatcher_Stop(kvCodecWatcher *w);

/** \brief Destroys the watcher wrapper and the underlying #kvWatcher.
 *
 * @param w the watcher to destroy; may be `NULL`.
 */
NATS_EXTERN void
kvCodecWatcher_Destroy(kvCodecWatcher *w);

/** @} */ // end kvCodecWatcherGroup

/** \defgroup kvCodecEntryGroup Entry
 *
 * Accessors mirror #kvEntry. All returned pointers are owned by the entry
 * and remain valid until #kvCodecEntry_Destroy.
 *  @{
 */

/** \brief Returns the bucket name (pass-through to #kvEntry_Bucket).
 *
 * @param e the entry.
 * @return the bucket name, or `NULL` if `e` is `NULL`. */
NATS_EXTERN const char *
kvCodecEntry_Bucket(const kvCodecEntry *e);

/** \brief Returns the decoded key (the original key for entries obtained via
 * #kvCodec_Get, #kvCodec_GetRevision or #kvCodec_History).
 *
 * @param e the entry.
 * @return the key, or `NULL` if `e` is `NULL`. */
NATS_EXTERN const char *
kvCodecEntry_Key(const kvCodecEntry *e);

/** \brief Returns the decoded value, or `NULL` for delete/purge markers.
 *
 * Zero-length stored values are returned as an empty (non-`NULL`) value
 * without invoking the value codec.
 *
 * @param e the entry.
 * @return the value, or `NULL` for markers or if `e` is `NULL`. */
NATS_EXTERN const void *
kvCodecEntry_Value(const kvCodecEntry *e);

/** \brief Returns the decoded value length in bytes (0 when the value is
 * `NULL`).
 *
 * @param e the entry.
 * @return the length in bytes. */
NATS_EXTERN int
kvCodecEntry_ValueLen(const kvCodecEntry *e);

/** \brief Returns the decoded value as a NUL-terminated string (the decoded
 * buffer always carries a hidden terminating NUL), or `NULL` for markers.
 *
 * @param e the entry.
 * @return the value as a string, or `NULL` for markers or if `e` is `NULL`. */
NATS_EXTERN const char *
kvCodecEntry_ValueString(const kvCodecEntry *e);

/** \brief Pass-through to #kvEntry_Revision.
 *
 * @param e the entry.
 * @return the revision (0 if `e` is `NULL`). */
NATS_EXTERN uint64_t
kvCodecEntry_Revision(const kvCodecEntry *e);

/** \brief Pass-through to #kvEntry_Created (UTC time expressed in nanoseconds).
 *
 * @param e the entry.
 * @return the creation time (0 if `e` is `NULL`). */
NATS_EXTERN int64_t
kvCodecEntry_Created(const kvCodecEntry *e);

/** \brief Pass-through to #kvEntry_Delta.
 *
 * @param e the entry.
 * @return the delta (0 if `e` is `NULL`). */
NATS_EXTERN uint64_t
kvCodecEntry_Delta(const kvCodecEntry *e);

/** \brief Pass-through to #kvEntry_Operation.
 *
 * @param e the entry.
 * @return the operation (#kvOp_Unknown if `e` is `NULL`). */
NATS_EXTERN kvOperation
kvCodecEntry_Operation(const kvCodecEntry *e);

/** \brief Destroys the entry, including the wrapped #kvEntry and the decoded
 * buffers.
 *
 * @param e the entry to destroy; may be `NULL`.
 */
NATS_EXTERN void
kvCodecEntry_Destroy(kvCodecEntry *e);

/** @} */ // end kvCodecEntryGroup
/** @} */ // end kvCodecGroup

#ifdef __cplusplus
}
#endif

#endif /* KV_CODEC_H_ */
