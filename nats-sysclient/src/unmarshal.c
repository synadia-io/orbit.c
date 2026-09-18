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

#include "unmarshal.h"

#include "os_shims.h"

#include <stdint.h>
#include <limits.h>
#include <string.h>

#define FIELD_PTR(dst, f) ((void *) ((char *) (dst) + (f)->Off))
#define COUNT_PTR(dst, f) ((int *) ((char *) (dst) + (f)->CntOff))

// Looks up 'key' and applies the policy every combinator shares: an absent key
// or an explicit null yields no node (and no error), a present key of the wrong
// type is NATS_INVALID_ARG. *node is NULL when there is nothing to decode.
static natsStatus
_lookupTyped(natsJSON **node, natsJSON *obj, const char *key, natsJSONType want)
{
    natsJSON  *found = NULL;
    natsStatus s;

    *node = NULL;

    s = natsJSON_Field(obj, key, &found);
    if (s == NATS_NOT_FOUND)
        return NATS_OK;
    if (s != NATS_OK)
        return s;
    if (natsJSON_Type(found) == NATS_JSON_NULL)
        return NATS_OK;
    if (natsJSON_Type(found) != want)
        return NATS_INVALID_ARG;

    *node = found;
    return NATS_OK;
}

// Copies the input text one node was parsed from into a freshly allocated
// NUL-terminated string. The span borrows from the message the tree was parsed
// from, which sysclient.c keeps alive until the tree is gone, so no
// re-serialization is needed: one malloc and one memcpy per subtree.
static natsStatus
_nodeToStr(char **out, natsJSON *node)
{
    const char *text = NULL;
    int         len  = 0;
    natsStatus  s;

    s = natsJSON_Raw(node, &text, &len);
    if (s != NATS_OK)
        return s;

    *out = (char *) NATS_MALLOC((size_t) len + 1);
    if (*out == NULL)
        return NATS_NO_MEMORY;

    memcpy(*out, text, (size_t) len);
    (*out)[len] = '\0';
    return NATS_OK;
}

natsStatus
sysclient_rawJSONField(char **out, natsJSON *obj, const char *key)
{
    natsJSON  *field = NULL;
    natsStatus s;

    if ((out == NULL) || (key == NULL))
        return NATS_INVALID_ARG;

    *out = NULL;

    // Any node type is acceptable here; these are opaque subtrees.
    s = natsJSON_Field(obj, key, &field);
    if (s == NATS_NOT_FOUND)
        return NATS_OK;
    if (s != NATS_OK)
        return s;
    if (natsJSON_Type(field) == NATS_JSON_NULL)
        return NATS_OK;

    return _nodeToStr(out, field);
}

natsStatus
sysclient_valueArray(void **out, int *count, natsJSON *obj, const char *key,
                     size_t elemSize, sysParseFn parse, sysFreeFn freeElem)
{
    natsStatus s;
    natsJSON  *arrNode = NULL;
    char      *block;
    int        n;
    int        i;

    if ((out == NULL) || (count == NULL) || (obj == NULL) || (key == NULL)
        || (elemSize == 0) || (parse == NULL))
        return NATS_INVALID_ARG;

    *out   = NULL;
    *count = 0;

    s = _lookupTyped(&arrNode, obj, key, NATS_JSON_ARRAY);
    if ((s != NATS_OK) || (arrNode == NULL))
        return s;

    n = natsJSON_ArraySize(arrNode);
    if (n == 0)
        return NATS_OK;

    block = (char *) NATS_CALLOC((size_t) n, elemSize);
    if (block == NULL)
        return NATS_NO_MEMORY;

    for (i = 0; i < n; i++)
    {
        natsJSON *elem = NULL;

        s = natsJSON_ArrayGet(arrNode, i, &elem);
        if (s == NATS_OK)
            s = parse(block + (size_t) i * elemSize, elem);
        if (s != NATS_OK)
        {
            // The caller must never see a partly-populated array. Element i is
            // included in the count: parse() may have failed part-way and left
            // members allocated behind it. Every sysFreeFn tolerates a zeroed
            // element, and the block was calloc'd.
            void *built = block;
            int   n     = i + 1;

            sysclient_freeValueArray(&built, &n, elemSize, freeElem);
            return s;
        }
    }

    *out   = block;
    *count = n;
    return NATS_OK;
}

void
sysclient_freeValueArray(void **arr, int *count, size_t elemSize, sysFreeFn freeElem)
{
    char *block;
    int   i;

    if ((arr == NULL) || (count == NULL))
        return;

    block = (char *) *arr;
    for (i = 0; (block != NULL) && (freeElem != NULL) && (i < *count); i++)
        freeElem(block + (size_t) i * elemSize);

    NATS_FREE(block);
    *arr   = NULL;
    *count = 0;
}

natsStatus
sysclient_ptrArray(void ***out, int *count, natsJSON *obj, const char *key,
                   size_t elemSize, sysParseFn parse, sysFreeFn freeElem)
{
    natsStatus s;
    natsJSON  *arrNode = NULL;
    void     **arr;
    int        n;
    int        i;

    if ((out == NULL) || (count == NULL) || (obj == NULL) || (key == NULL)
        || (elemSize == 0) || (parse == NULL))
        return NATS_INVALID_ARG;

    *out   = NULL;
    *count = 0;

    s = _lookupTyped(&arrNode, obj, key, NATS_JSON_ARRAY);
    if ((s != NATS_OK) || (arrNode == NULL))
        return s;

    n = natsJSON_ArraySize(arrNode);
    if (n == 0)
        return NATS_OK;

    arr = (void **) NATS_CALLOC((size_t) n, sizeof(void *));
    if (arr == NULL)
        return NATS_NO_MEMORY;

    for (i = 0; i < n; i++)
    {
        natsJSON *elem = NULL;

        arr[i] = NATS_CALLOC(1, elemSize);
        if (arr[i] == NULL)
        {
            s = NATS_NO_MEMORY;
        }
        else
        {
            s = natsJSON_ArrayGet(arrNode, i, &elem);
            if (s == NATS_OK)
                s = parse(arr[i], elem);
        }

        if (s != NATS_OK)
        {
            int j;

            // arr[i] may be allocated but unparsed; release it too.
            for (j = 0; j <= i; j++)
            {
                if (arr[j] == NULL)
                    continue;
                if (freeElem != NULL)
                    freeElem(arr[j]);
                NATS_FREE(arr[j]);
            }
            NATS_FREE(arr);
            return s;
        }
    }

    *out   = arr;
    *count = n;
    return NATS_OK;
}

void
sysclient_freePtrArray(void ***arr, int *count, sysFreeFn freeElem)
{
    void **p;
    int    i;

    if ((arr == NULL) || (count == NULL))
        return;

    p = *arr;
    for (i = 0; (p != NULL) && (i < *count); i++)
    {
        if (p[i] == NULL)
            continue;
        if (freeElem != NULL)
            freeElem(p[i]);
        NATS_FREE(p[i]);
    }

    NATS_FREE(p);
    *arr   = NULL;
    *count = 0;
}

natsStatus
sysclient_objectInline(void *dst, natsJSON *obj, const char *key, sysParseFn parse)
{
    natsStatus s;
    natsJSON  *node = NULL;

    if ((dst == NULL) || (obj == NULL) || (key == NULL) || (parse == NULL))
        return NATS_INVALID_ARG;

    s = _lookupTyped(&node, obj, key, NATS_JSON_OBJECT);
    if ((s != NATS_OK) || (node == NULL))
        return s;

    return parse(dst, node);
}

natsStatus
sysclient_objectPtr(void **out, natsJSON *obj, const char *key, size_t size,
                    sysParseFn parse, sysFreeFn freeElem)
{
    natsStatus s;
    natsJSON  *node = NULL;
    void      *box;

    if ((out == NULL) || (obj == NULL) || (key == NULL) || (size == 0) || (parse == NULL))
        return NATS_INVALID_ARG;

    *out = NULL;

    s = _lookupTyped(&node, obj, key, NATS_JSON_OBJECT);
    if ((s != NATS_OK) || (node == NULL))
        return s;

    box = NATS_CALLOC(1, size);
    if (box == NULL)
        return NATS_NO_MEMORY;

    s = parse(box, node);
    if (s != NATS_OK)
    {
        if (freeElem != NULL)
            freeElem(box);
        NATS_FREE(box);
        return s;
    }

    *out = box;
    return NATS_OK;
}

void
sysclient_freeObjectPtr(void **out, sysFreeFn freeElem)
{
    if ((out == NULL) || (*out == NULL))
        return;

    if (freeElem != NULL)
        freeElem(*out);
    NATS_FREE(*out);
    *out = NULL;
}

natsStatus
sysclient_rawJSONArray(char ***out, int *count, natsJSON *obj, const char *key)
{
    natsStatus s = NATS_OK;
    natsJSON  *arrNode = NULL;
    char     **arr;
    int        n;
    int        i;

    if ((out == NULL) || (count == NULL) || (obj == NULL) || (key == NULL))
        return NATS_INVALID_ARG;

    *out   = NULL;
    *count = 0;

    s = _lookupTyped(&arrNode, obj, key, NATS_JSON_ARRAY);
    if ((s != NATS_OK) || (arrNode == NULL))
        return s;

    n = natsJSON_ArraySize(arrNode);
    if (n == 0)
        return NATS_OK;

    arr = (char **) NATS_CALLOC((size_t) n, sizeof(char *));
    if (arr == NULL)
        return NATS_NO_MEMORY;

    for (i = 0; i < n; i++)
    {
        natsJSON *elem = NULL;

        s = natsJSON_ArrayGet(arrNode, i, &elem);
        if (s == NATS_OK)
            s = _nodeToStr(&arr[i], elem);

        if (s != NATS_OK)
        {
            sysclient_freeStrArray(&arr, &n);
            return s;
        }
    }

    *out   = arr;
    *count = n;
    return NATS_OK;
}

void
sysclient_freeStrArray(char ***arr, int *count)
{
    char **p;
    int    i;

    if ((arr == NULL) || (count == NULL))
        return;

    p = *arr;
    for (i = 0; (p != NULL) && (i < *count); i++)
        NATS_FREE(p[i]);

    NATS_FREE(p);
    *arr   = NULL;
    *count = 0;
}

// The narrowing kinds range-check rather than truncate: a value the member
// cannot hold is a malformed response, and silently storing 0 for 4294967296
// would be invisible to the caller. The accessors already reject a literal too
// large for int64/uint64; these two add what only the table knows, the width of
// the member it is about to write.
static natsStatus
_getRanged(natsJSON *obj, const char *key, int64_t lo, int64_t hi, int64_t *out)
{
    natsStatus s = natsJSON_GetInt(obj, key, out);

    return ((s == NATS_OK) && ((*out < lo) || (*out > hi))) ? NATS_INVALID_ARG : s;
}

static natsStatus
_getCapped(natsJSON *obj, const char *key, uint64_t max, uint64_t *out)
{
    natsStatus s = natsJSON_GetUInt(obj, key, out);

    return ((s == NATS_OK) && (*out > max)) ? NATS_INVALID_ARG : s;
}

natsStatus
sysclient_scanFields(void *dst, natsJSON *obj, const sysField *fields, int n)
{
    natsStatus s = NATS_OK;
    int        i;

    if ((dst == NULL) || (obj == NULL) || (fields == NULL))
        return NATS_INVALID_ARG;

    for (i = 0; (i < n) && (s == NATS_OK); i++)
    {
        const sysField *f = &fields[i];
        void           *p = FIELD_PTR(dst, f);
        int64_t         i64;
        uint64_t        u64;


        switch (f->Kind)
        {
            // Moved rather than copied: the tree is private to the reply
            // decoder in sysclient.c and destroyed as soon as the payload is
            // decoded, so nothing else will ever read these strings from it.
            case SYS_FLD_STR:
                s = natsJSON_TakeStr(obj, f->Key, (char **) p);
                break;
            case SYS_FLD_STRARRAY:
                s = natsJSON_TakeStrArray(obj, f->Key, (char ***) p, COUNT_PTR(dst, f));
                break;
            case SYS_FLD_RAWJSON:
                s = sysclient_rawJSONField((char **) p, obj, f->Key);
                break;
            case SYS_FLD_BOOL:
                s = natsJSON_GetBool(obj, f->Key, (bool *) p);
                break;
            case SYS_FLD_DOUBLE:
                s = natsJSON_GetNumber(obj, f->Key, (double *) p);
                break;
            case SYS_FLD_INT:
                if ((s = _getRanged(obj, f->Key, INT_MIN, INT_MAX, &i64)) == NATS_OK)
                    *(int *) p = (int) i64;
                break;
            case SYS_FLD_I32:
                if ((s = _getRanged(obj, f->Key, INT32_MIN, INT32_MAX, &i64)) == NATS_OK)
                    *(int32_t *) p = (int32_t) i64;
                break;
            case SYS_FLD_I64:
                s = natsJSON_GetInt(obj, f->Key, (int64_t *) p);
                break;
            case SYS_FLD_U16:
                if ((s = _getCapped(obj, f->Key, UINT16_MAX, &u64)) == NATS_OK)
                    *(uint16_t *) p = (uint16_t) u64;
                break;
            case SYS_FLD_U32:
                if ((s = _getCapped(obj, f->Key, UINT32_MAX, &u64)) == NATS_OK)
                    *(uint32_t *) p = (uint32_t) u64;
                break;
            case SYS_FLD_U64:
                s = natsJSON_GetUInt(obj, f->Key, (uint64_t *) p);
                break;
        }

        // A key that is absent or explicitly null leaves the zero value in
        // place.
        if (s == NATS_NOT_FOUND)
            s = NATS_OK;
    }

    return s;
}

void
sysclient_freeFields(void *dst, const sysField *fields, int n)
{
    int i;

    if ((dst == NULL) || (fields == NULL))
        return;

    for (i = 0; i < n; i++)
    {
        const sysField *f = &fields[i];
        void           *p = FIELD_PTR(dst, f);

        switch (f->Kind)
        {
            case SYS_FLD_STR:
            case SYS_FLD_RAWJSON:
                NATS_FREE(*(char **) p);
                *(char **) p = NULL;
                break;
            case SYS_FLD_STRARRAY:
                sysclient_freeStrArray((char ***) p, COUNT_PTR(dst, f));
                break;
            default:
                break;
        }
    }
}
