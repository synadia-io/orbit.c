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
// Table-driven decoding: one sysField row per wire field. Scalars, strings
// and string arrays go through sysclient_scanFields(); nested objects and
// arrays need a few lines of glue per struct.

#ifndef NATS_SYSCLIENT_UNMARSHAL_H_
#define NATS_SYSCLIENT_UNMARSHAL_H_

#include "json.h"

#include <nats/nats.h>
#include <stddef.h>

typedef enum
{
    SYS_FLD_STR = 0,  // char *
    SYS_FLD_STRARRAY, // char ** plus an int count
    SYS_FLD_RAWJSON,  // char * holding the subtree's input text, verbatim
    SYS_FLD_BOOL,
    SYS_FLD_DOUBLE,
    SYS_FLD_INT,
    SYS_FLD_I32,
    SYS_FLD_I64,
    SYS_FLD_U16,
    SYS_FLD_U32,
    SYS_FLD_U64,

} sysFieldKind;

typedef struct
{
    const char  *Key;
    sysFieldKind Kind;
    size_t       Off;
    size_t       CntOff; // the companion count, for SYS_FLD_STRARRAY

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

// Zero, or a compile error when the member's width does not match its kind,
// so a mistagged row cannot write past its member. Kinds of equal width are
// not distinguished.
#define SYS_WCHECK(kind, st, fld) \
    (0 * (int) sizeof(char[(sizeof(((st *) 0)->fld) == SYS_W_##kind) ? 1 : -1]))

// One row per field; SYS_FA is for SYS_FLD_STRARRAY, which needs its count.
#define SYS_F(kind, st, fld, key)                                     \
    {(key), (kind), offsetof(st, fld) + SYS_WCHECK(kind, st, fld), 0}
#define SYS_FA(st, fld, cnt, key)                                                   \
    {(key), SYS_FLD_STRARRAY,                                                       \
     offsetof(st, fld) + SYS_WCHECK(SYS_FLD_STRARRAY, st, fld), offsetof(st, cnt)}
#define SYS_NFIELDS(a) ((int) (sizeof(a) / sizeof((a)[0])))

// Decodes every field in 'fields' from 'obj' into the zeroed 'dst'. Strings
// are moved out of the tree (natsJSON_TakeStr), so a key decodes once. A
// missing or null key leaves the zero value; a wrong type is
// NATS_INVALID_ARG.
natsStatus
sysclient_scanFields(void *dst, natsJSON *obj, const sysField *fields, int n);

// Releases the members sysclient_scanFields() allocated. Idempotent.
void
sysclient_freeFields(void *dst, const sysField *fields, int n);

// Parses one object into a zeroed 'elem'.
typedef natsStatus (*sysParseFn)(void *elem, natsJSON *node);

// Releases the members of 'elem', not 'elem' itself.
typedef void (*sysFreeFn)(void *elem);

// Decodes the object array at 'key' into a contiguous block of 'count'
// zeroed elements. A missing, null or empty array leaves *out NULL and
// *count 0. On failure nothing is left allocated.
natsStatus
sysclient_valueArray(void **out, int *count, natsJSON *obj, const char *key,
                     size_t elemSize, sysParseFn parse, sysFreeFn freeElem);

void
sysclient_freeValueArray(void **arr, int *count, size_t elemSize, sysFreeFn freeElem);

// As sysclient_valueArray(), but each element is allocated separately and
// held by pointer. A NULL 'freeElem' means the elements have no members to
// release.
natsStatus
sysclient_ptrArray(void ***out, int *count, natsJSON *obj, const char *key,
                   size_t elemSize, sysParseFn parse, sysFreeFn freeElem);

void
sysclient_freePtrArray(void ***arr, int *count, sysFreeFn freeElem);

// Decodes the object at 'key' into 'dst', held by value; a missing or null
// key leaves it untouched.
natsStatus
sysclient_objectInline(void *dst, natsJSON *obj, const char *key, sysParseFn parse);

// Decodes the object at 'key' into a new struct; *out stays NULL when the key
// is absent or null.
natsStatus
sysclient_objectPtr(void **out, natsJSON *obj, const char *key, size_t size,
                    sysParseFn parse, sysFreeFn freeElem);

void
sysclient_freeObjectPtr(void **out, sysFreeFn freeElem);

// Copies each element of the array at 'key' into its own string, verbatim.
natsStatus
sysclient_rawJSONArray(char ***out, int *count, natsJSON *obj, const char *key);

// Releases an array of strings and zeroes both out-params.
void
sysclient_freeStrArray(char ***arr, int *count);

#endif /* NATS_SYSCLIENT_UNMARSHAL_H_ */
