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
// 'data' need not be NUL-terminated, and may be NULL when 'len' is 0. The root
// may be any JSON value, not just an object or array.
//
// Returns NATS_INVALID_ARG for NULL arguments or negative length, NATS_ERR for
// malformed (including empty) input, NATS_NO_MEMORY on allocation failure. On
// any error *newJSON is set to NULL.
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

// Interprets a number node as a double. Returns NATS_INVALID_ARG for a
// non-number or a literal outside double's range.
natsStatus
natsJSON_AsNumber(const natsJSON *json, double *out);

// Interprets a number node as a 64-bit integer. Returns NATS_INVALID_ARG for
// a non-number, a fraction or exponent, or a value out of range.
natsStatus
natsJSON_AsInt(const natsJSON *json, int64_t *out);

// As natsJSON_AsInt, but unsigned.
natsStatus
natsJSON_AsUInt(const natsJSON *json, uint64_t *out);

// Sets *out to the node's decoded string. The pointer is borrowed from the
// tree and must not be freed.
natsStatus
natsJSON_AsStr(const natsJSON *json, const char **out);

// Returns the input text 'json' was parsed from, not NUL-terminated. It
// borrows from the 'data' passed to natsJSON_Parse(), not from the tree.
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

// As natsJSON_Field, but a JSON null value is reported as NATS_NOT_FOUND.
natsStatus
natsJSON_Lookup(const natsJSON *json, const char *key, natsJSON **out);

//
// Typed object-field convenience getters. Each looks up 'key' with
// natsJSON_Lookup and extracts a value of the requested type. A missing key —
// or a key whose value is JSON null — returns NATS_NOT_FOUND and leaves *out
// untouched, so callers can layer these over pre-initialised defaults. A
// present key of the wrong type returns NATS_INVALID_ARG.
//

// On success *out is a heap copy of the string; the caller frees it.
natsStatus
natsJSON_GetStr(const natsJSON *json, const char *key, char **out);

// As natsJSON_GetStr, but *out borrows the string from the tree.
natsStatus
natsJSON_GetStrRef(const natsJSON *json, const char *key, const char **out);

// As natsJSON_GetStr, but moves the string out of the tree instead of copying
// it; the member is left as a JSON null.
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

// As natsJSON_GetStrArray, but moves the strings out as natsJSON_TakeStr
// does. On NATS_INVALID_ARG the tree is untouched.
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
// Writer — builds a JSON object incrementally into a natsBuffer.
//
// Errors are sticky: after a failure later calls are no-ops, so a run of
// appends is checked once with natsJSONWriter_Status(). The writer borrows the
// buffer and needs no destroy.
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
    bool        open;      // whether the object has been opened and not closed
    bool        needComma; // whether a separator precedes the next member

} natsJSONWriter;

// Binds 'w' to 'buf'. The buffer is not reset, so a writer can append to a
// buffer that already holds data. Returns NATS_INVALID_ARG for NULL arguments.
natsStatus
natsJSONWriter_Init(natsJSONWriter *w, natsBuffer *buf);

// Returns the first error the writer encountered, or NATS_OK.
natsStatus
natsJSONWriter_Status(const natsJSONWriter *w);

// Records 'st' as the writer's error if it has none yet; returns the status.
natsStatus
natsJSONWriter_Fail(natsJSONWriter *w, natsStatus st);

// Opens the object. A writer produces exactly one flat object; a second call
// fails with NATS_ERR.
natsStatus
natsJSONWriter_StartObject(natsJSONWriter *w);

// Closes the object. Returns NATS_ERR if it is not open.
natsStatus
natsJSONWriter_EndObject(natsJSONWriter *w);

//
// Member appenders. Each writes `"key":<value>`; a NULL string is written as
// "".
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
