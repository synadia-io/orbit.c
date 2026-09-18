// Copyright 2026 The NATS Authors
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

// A small, dependency-free JSON parser shared across orbit.c sub-libraries.
//
// natsJSON_Parse() builds an in-memory tree (DOM) from a JSON document; the
// tree is inspected with the accessors below and released with
// natsJSON_Destroy(). The parser handles the full RFC 8259 grammar: objects,
// arrays, strings (with \uXXXX escapes and surrogate pairs decoded to UTF-8),
// numbers, booleans and null.
//
// This header is internal to the orbit.c build and is NOT installed.

#ifndef ORBIT_JSON_H_
#define ORBIT_JSON_H_

#include "buf.h"

#include <nats/nats.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

// The type of a JSON value node.
typedef enum
{
    NATS_JSON_NULL = 0,
    NATS_JSON_BOOL,
    NATS_JSON_NUMBER,
    NATS_JSON_STRING,
    NATS_JSON_ARRAY,
    NATS_JSON_OBJECT,

} natsJSONType;

// An opaque JSON value node. Obtain one from natsJSON_Parse() and free the
// whole tree with natsJSON_Destroy(). All borrowed pointers returned by the
// accessors below are owned by the tree and remain valid only until it is
// destroyed.
typedef struct __natsJSON natsJSON;

// Parses 'len' bytes of JSON text at 'data' into a tree rooted at *newJSON.
// 'data' need not be NUL-terminated. The root may be any JSON value, not just
// an object or array.
//
// Returns NATS_INVALID_ARG for NULL arguments or negative length, NATS_ERR for
// malformed input, NATS_NO_MEMORY on allocation failure. On any error *newJSON
// is set to NULL.
natsStatus
natsJSON_Parse(natsJSON **newJSON, const char *data, int len);

// Frees a tree returned by natsJSON_Parse(). Passing NULL is a no-op.
void
natsJSON_Destroy(natsJSON *json);

// Returns the type of 'json', or NATS_JSON_NULL when 'json' is NULL.
natsJSONType
natsJSON_Type(const natsJSON *json);

//
// Scalar accessors — interpret a single node. Each returns NATS_INVALID_ARG
// when 'json' is NULL or is not of the required type.
//

natsStatus
natsJSON_AsBool(const natsJSON *json, bool *out);

natsStatus
natsJSON_AsNumber(const natsJSON *json, double *out);

// Interprets a number node as a 64-bit integer (the integer part; any
// fractional or exponent part is ignored).
// Returns NATS_INVALID_ARG for a non-number, and for a literal too large for
// the type rather than storing a saturated value the server never sent.
natsStatus
natsJSON_AsInt(const natsJSON *json, int64_t *out);

// As natsJSON_AsInt, but unsigned. Use this for wire fields declared uint64:
// natsJSON_AsInt rejects anything above INT64_MAX, so a counter above that
// silently clamped.
natsStatus
natsJSON_AsUInt(const natsJSON *json, uint64_t *out);

// Sets *out to the node's decoded string. The pointer is borrowed from the
// tree and must not be freed.
natsStatus
natsJSON_AsStr(const natsJSON *json, const char **out);

// Returns the span of input text 'json' was parsed from: *text points into
// the 'data' passed to natsJSON_Parse() and *len is its length, with no
// NUL terminator and no surrounding whitespace. Any node type is accepted.
//
// The span borrows from the parse input, not from the tree, so it is valid
// only while that input is alive — copy it out when the tree may outlive the
// buffer it was parsed from. Use this to hand back a subtree verbatim without
// re-serializing it.
natsStatus
natsJSON_Raw(const natsJSON *json, const char **text, int *len);

//
// Object accessors.
//

// Looks up the member named 'key' in an object node and returns the borrowed
// child value in *out. Returns NATS_NOT_FOUND when the key is absent and
// NATS_INVALID_ARG when 'json' is not an object. On a duplicate key the first
// occurrence wins.
natsStatus
natsJSON_Field(const natsJSON *json, const char *key, natsJSON **out);

// Returns the number of members in an object node, or 0 when 'json' is not an
// object.
int
natsJSON_FieldCount(const natsJSON *json);

// Returns the member at index 'idx' (in document order) via the borrowed *key
// and *value. Either output pointer may be NULL if not needed. Returns
// NATS_INVALID_ARG when 'json' is not an object or 'idx' is out of range.
natsStatus
natsJSON_FieldAt(const natsJSON *json, int idx, const char **key, natsJSON **value);

//
// Typed object-field convenience getters. Each looks up 'key' and extracts a
// value of the requested type. A missing key — or a key whose value is JSON
// null — returns NATS_NOT_FOUND and leaves *out untouched, so callers can layer
// these over pre-initialised defaults. A present key of the wrong type returns
// NATS_INVALID_ARG.
//

// On success *out is a heap copy of the string; the caller frees it.
natsStatus
natsJSON_GetStr(const natsJSON *json, const char *key, char **out);

// As natsJSON_GetStr, but moves the string out of the tree instead of copying
// it: *out takes over the tree's own allocation, and the member is left as a
// JSON null, so a later getter on the same key reports NATS_NOT_FOUND. For a
// tree that is decoded once and destroyed this halves the allocations.
natsStatus
natsJSON_TakeStr(natsJSON *json, const char *key, char **out);

natsStatus
natsJSON_GetBool(const natsJSON *json, const char *key, bool *out);

natsStatus
natsJSON_GetNumber(const natsJSON *json, const char *key, double *out);

natsStatus
natsJSON_GetInt(const natsJSON *json, const char *key, int64_t *out);

natsStatus
natsJSON_GetUInt(const natsJSON *json, const char *key, uint64_t *out);

// Extracts a JSON array of strings into a freshly allocated array of heap
// strings. On success *out holds *count entries (or NULL when *count is 0); the
// caller frees each entry and then the array. Returns NATS_INVALID_ARG when the
// field is not an array or contains a non-string element.
natsStatus
natsJSON_GetStrArray(const natsJSON *json, const char *key, char ***out, int *count);

// As natsJSON_GetStrArray, but each entry takes over the tree's own string,
// as natsJSON_TakeStr does. The elements are left as JSON nulls. A non-string
// element is detected before anything is moved, so on NATS_INVALID_ARG the
// tree is untouched.
natsStatus
natsJSON_TakeStrArray(natsJSON *json, const char *key, char ***out, int *count);

//
// Array accessors.
//

// Returns the number of elements in an array node, or 0 when 'json' is not an
// array.
int
natsJSON_ArraySize(const natsJSON *json);

// Returns the borrowed element at index 'idx' in *out. Returns
// NATS_INVALID_ARG when 'json' is not an array or 'idx' is out of range.
natsStatus
natsJSON_ArrayGet(const natsJSON *json, int idx, natsJSON **out);

//
// Serialization.
//

// Appends the JSON text of 'json' to 'out'. Numbers are emitted from the
// literal text captured at parse time, so a parse/write round trip preserves
// them exactly; object members keep their document order. Nothing is written
// on error. Returns NATS_INVALID_ARG for NULL arguments.
//
// Use this to hand back a subtree of a parsed document as raw JSON.
natsStatus
natsJSON_Write(const natsJSON *json, natsBuffer *out);

//
// Writer — builds a JSON object incrementally into a natsBuffer.
//
// Errors are sticky: once a call fails, later calls are no-ops that return the
// original status, so a long run of field appends can be checked once at the
// end with natsJSONWriter_Status(). The writer borrows the buffer and owns
// nothing, so there is nothing to destroy.
//
// Typical use:
//
//     natsBuffer     buf = NATS_EMPTY_BUFFER;
//     natsJSONWriter w;
//
//     natsBuf_Init(&buf, 256);
//     natsJSONWriter_Init(&w, &buf);
//     natsJSONWriter_StartObject(&w);
//     if (!nats_IsStringEmpty(opts->Account))
//         natsJSONWriter_AddStr(&w, "account", opts->Account);
//     if (opts->Details)
//         natsJSONWriter_AddBool(&w, "details", true);
//     natsJSONWriter_EndObject(&w);
//     s = natsJSONWriter_Status(&w);
//
typedef struct __natsJSONWriter
{
    natsBuffer *buf;
    natsStatus  st;        // sticky: first error encountered
    int         depth;     // number of open objects
    bool        needComma; // whether a separator precedes the next member

} natsJSONWriter;

// Binds 'w' to 'buf'. The buffer is not reset, so a writer can append to a
// buffer that already holds data. Returns NATS_INVALID_ARG for NULL arguments.
natsStatus
natsJSONWriter_Init(natsJSONWriter *w, natsBuffer *buf);

// Returns the first error the writer encountered, or NATS_OK.
natsStatus
natsJSONWriter_Status(const natsJSONWriter *w);

// Records 'st' as the writer's error if it has none yet, so a caller that
// rejects its own input mid-run fails the whole document the same way a
// write failure would. Returns the writer's status afterwards.
natsStatus
natsJSONWriter_Fail(natsJSONWriter *w, natsStatus st);

// Opens the root object. A writer produces exactly one value, so this is
// rejected with NATS_ERR (and the writer poisoned) inside an open object or
// after the root has been closed; nested objects go through
// natsJSONWriter_StartObjectKey.
natsStatus
natsJSONWriter_StartObject(natsJSONWriter *w);

// Opens an object as the member 'key' of the innermost open object. Returns
// NATS_ERR when no object is open.
natsStatus
natsJSONWriter_StartObjectKey(natsJSONWriter *w, const char *key);

// Closes the innermost open object. Returns NATS_ERR if none is open.
natsStatus
natsJSONWriter_EndObject(natsJSONWriter *w);

//
// Member appenders. Each writes `"key":<value>`, preceded by a comma when the
// enclosing object already has a member. A NULL 'val' for a string member is
// written as an empty string, which is what an unset C string means on the
// wire; pass the field conditionally to omit it entirely.
//

natsStatus
natsJSONWriter_AddStr(natsJSONWriter *w, const char *key, const char *val);

natsStatus
natsJSONWriter_AddBool(natsJSONWriter *w, const char *key, bool val);

natsStatus
natsJSONWriter_AddInt(natsJSONWriter *w, const char *key, int64_t val);

natsStatus
natsJSONWriter_AddUInt(natsJSONWriter *w, const char *key, uint64_t val);

// Writes an array of strings. A NULL 'vals' or a 'count' of 0 writes `[]`.
natsStatus
natsJSONWriter_AddStrArray(natsJSONWriter *w, const char *key, const char *const *vals, int count);

#ifdef __cplusplus
}
#endif

#endif /* ORBIT_JSON_H_ */
