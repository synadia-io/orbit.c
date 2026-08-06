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
natsStatus
natsJSON_AsInt(const natsJSON *json, int64_t *out);

// Sets *out to the node's decoded string. The pointer is borrowed from the
// tree and must not be freed.
natsStatus
natsJSON_AsStr(const natsJSON *json, const char **out);

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

natsStatus
natsJSON_GetBool(const natsJSON *json, const char *key, bool *out);

natsStatus
natsJSON_GetNumber(const natsJSON *json, const char *key, double *out);

natsStatus
natsJSON_GetInt(const natsJSON *json, const char *key, int64_t *out);

// Extracts a JSON array of strings into a freshly allocated array of heap
// strings. On success *out holds *count entries (or NULL when *count is 0); the
// caller frees each entry and then the array. Returns NATS_INVALID_ARG when the
// field is not an array or contains a non-string element.
natsStatus
natsJSON_GetStrArray(const natsJSON *json, const char *key, char ***out, int *count);

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

#ifdef __cplusplus
}
#endif

#endif /* ORBIT_JSON_H_ */
