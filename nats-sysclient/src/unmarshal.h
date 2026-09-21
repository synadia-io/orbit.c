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

// Internal to nats-sysclient. Not installed.
//
// The responses model ~52 structs and ~364 wire fields. Written as a flat
// chain of accessor calls that is several thousand lines nobody can review;
// written as a table it is one row per wire field, so the whole set can be
// read off against the wire format without following any parsing logic.
//
// Scalar, string and string-array fields go through sysclient_scanFields();
// nested objects and object arrays need a few lines of glue per struct.

#ifndef NATS_SYSCLIENT_UNMARSHAL_H_
#define NATS_SYSCLIENT_UNMARSHAL_H_

#include "json.h"

#include <nats/nats.h>
#include <stddef.h>

typedef enum
{
    SYS_FLD_STR = 0,  ///< char *
    SYS_FLD_STRARRAY, ///< char ** plus an int count
    SYS_FLD_RAWJSON,  ///< char * holding the subtree's input text, verbatim
    SYS_FLD_BOOL,     ///< bool
    SYS_FLD_DOUBLE,   ///< double
    SYS_FLD_INT,      ///< int
    SYS_FLD_I32,      ///< int32_t
    SYS_FLD_I64,      ///< int64_t
    SYS_FLD_U16,      ///< uint16_t
    SYS_FLD_U32,      ///< uint32_t
    SYS_FLD_U64,      ///< uint64_t

} sysFieldKind;

typedef struct
{
    const char  *Key;    ///< JSON key, exactly as it appears on the wire.
    sysFieldKind Kind;   ///< How to decode it.
    size_t       Off;    ///< offsetof() the target member.
    size_t       CntOff; ///< offsetof() the companion count, for SYS_FLD_STRARRAY.

} sysField;

// The C width each kind writes through.
#define SYS_W_SYS_FLD_STR      (sizeof(char *))
#define SYS_W_SYS_FLD_STRARRAY (sizeof(char **))
#define SYS_W_SYS_FLD_RAWJSON  (sizeof(char *))
#define SYS_W_SYS_FLD_BOOL     (sizeof(bool))
#define SYS_W_SYS_FLD_DOUBLE   (sizeof(double))
#define SYS_W_SYS_FLD_INT      (sizeof(int))
#define SYS_W_SYS_FLD_I32      (sizeof(int32_t))
#define SYS_W_SYS_FLD_I64      (sizeof(int64_t))
#define SYS_W_SYS_FLD_U16      (sizeof(uint16_t))
#define SYS_W_SYS_FLD_U32      (sizeof(uint32_t))
#define SYS_W_SYS_FLD_U64      (sizeof(uint64_t))

// Always zero — but only when the member's width matches what its kind writes.
// Otherwise the array type is invalid and the row fails to compile.
//
// offsetof already catches a renamed member; this catches mistagged widths. A
// row reading SYS_F(SYS_FLD_I64, natsSysVarz, Port, "port") would write eight
// bytes through a four-byte int and quietly corrupt whatever follows it, so it
// is rejected where it is written rather than where it goes wrong.
//
// It is a width check, not a type check: kinds of equal width are
// interchangeable as far as this can tell, so on LP64 it will not catch
// SYS_FLD_STR on an int64_t member or SYS_FLD_I32 on a uint32_t one. C99 has no
// _Generic to close that gap, and nothing checks a row against the wire
// format either -- that mapping is maintained by hand.
#define SYS_WCHECK(kind, st, fld) \
    (0 * (int) sizeof(char[(sizeof(((st *) 0)->fld) == SYS_W_##kind) ? 1 : -1]))

// One row per field. SYS_F covers every kind but SYS_FLD_STRARRAY, which needs
// the companion count member and so uses SYS_FA.
#define SYS_F(kind, st, fld, key)                                     \
    {(key), (kind), offsetof(st, fld) + SYS_WCHECK(kind, st, fld), 0}
#define SYS_FA(st, fld, cnt, key)                                                   \
    {(key), SYS_FLD_STRARRAY,                                                       \
     offsetof(st, fld) + SYS_WCHECK(SYS_FLD_STRARRAY, st, fld), offsetof(st, cnt)}
#define SYS_NFIELDS(a) ((int) (sizeof(a) / sizeof((a)[0])))

// Decodes every field in 'fields' from 'obj' into 'dst'.
//
// 'dst' must be zeroed beforehand. String members are moved out of the tree
// rather than copied (natsJSON_TakeStr), so a key is decoded once: a second
// scan of the same key on the same node finds it null. A key that is missing or JSON null leaves
// the member at its zero value. A key present with the wrong type returns
// NATS_INVALID_ARG; the reply decoder in sysclient.c reports that as NATS_ERR.
//
// A row whose member width disagrees with its kind cannot reach here: SYS_F
// rejects it at compile time.
natsStatus
sysclient_scanFields(void *dst, natsJSON *obj, const sysField *fields, int n);

// Releases the heap members that sysclient_scanFields() allocated: SYS_FLD_STR,
// SYS_FLD_RAWJSON and SYS_FLD_STRARRAY. Driving both directions off the same
// table is what keeps the parse and free paths from drifting apart. Safe to
// call on a partly-populated struct, and idempotent.
void
sysclient_freeFields(void *dst, const sysField *fields, int n);

/** Parses one element of an object array into a zeroed 'elem'. */
typedef natsStatus (*sysParseFn)(void *elem, natsJSON *node);

/** Releases the members of one element; never frees 'elem' itself. */
typedef void (*sysFreeFn)(void *elem);

// Decodes the array of objects at 'key' into a freshly allocated contiguous
// block of 'count' elements, each 'elemSize' bytes and zeroed before 'parse'
// runs. A missing, null or empty array leaves *out NULL and *count 0, so an
// empty array is indistinguishable from an absent one.
//
// On failure every element parsed so far is released with 'freeElem' and the
// block is freed, so the caller never sees a half-built array.
natsStatus
sysclient_valueArray(void **out, int *count, natsJSON *obj, const char *key,
                     size_t elemSize, sysParseFn parse, sysFreeFn freeElem);

// Releases an array built by sysclient_valueArray() and zeroes both out-params.
void
sysclient_freeValueArray(void **arr, int *count, size_t elemSize, sysFreeFn freeElem);

// As sysclient_valueArray(), but allocates each element separately and stores
// an array of pointers, for payload arrays whose elements are held by pointer.
// A NULL 'freeElem' means the elements have no members of their own.
natsStatus
sysclient_ptrArray(void ***out, int *count, natsJSON *obj, const char *key,
                   size_t elemSize, sysParseFn parse, sysFreeFn freeElem);

void
sysclient_freePtrArray(void ***arr, int *count, sysFreeFn freeElem);

// Decodes the object at 'key' into 'dst', which is a struct held by value.
// A missing or null key leaves 'dst' untouched, so it keeps its zero value.
natsStatus
sysclient_objectInline(void *dst, natsJSON *obj, const char *key, sysParseFn parse);

// Decodes the object at 'key' into a freshly allocated struct, for a member the
// payload may omit entirely. *out stays NULL when the key is absent or null.
natsStatus
sysclient_objectPtr(void **out, natsJSON *obj, const char *key, size_t size,
                    sysParseFn parse, sysFreeFn freeElem);

void
sysclient_freeObjectPtr(void **out, sysFreeFn freeElem);

// Copies each element of the array at 'key' into its own string, verbatim, for
// arrays of subtrees that have no portable C representation. *out is NULL and
// *count 0 when the key is absent, null or an empty array.
natsStatus
sysclient_rawJSONArray(char ***out, int *count, natsJSON *obj, const char *key);

// Releases an array of strings and zeroes both out-params: a pointer array
// whose elements need no cleanup of their own. Pairs with
// sysclient_rawJSONArray, and is also used for the string arrays copied out of
// caller options.
void
sysclient_freeStrArray(char ***arr, int *count);

#endif /* NATS_SYSCLIENT_UNMARSHAL_H_ */
