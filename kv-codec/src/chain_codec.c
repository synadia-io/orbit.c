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

#include "kvcodecp.h"
#include "os_shims.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

// Chain closures: the codecs array is a copy owned by the chain, the members
// are borrowed from the caller.
typedef struct
{
    kvKeyCodec **codecs;
    int          count;
} kvKeyChain;

typedef struct
{
    kvValueCodec **codecs;
    int            count;
} kvValueChain;

// Runs the key through each member codec: encoding first-to-last, decoding
// last-to-first, filter encoding first-to-last. Intermediate results are
// freed as the chain advances; on success *out is the final malloc'ed key.
static natsStatus
_keyChainTransform(char **out, const char *in, kvKeyChain *chain, bool decode, bool filter)
{
    natsStatus s   = NATS_OK;
    char      *cur = NULL;
    int        i;

    for (i = 0; i < chain->count; i++)
    {
        kvKeyCodec *kc = chain->codecs[decode ? (chain->count - 1 - i) : i];
        const char *src  = (i == 0) ? in : cur;
        char       *next = NULL;

        if (decode)
            s = kc->decodeKey(&next, src, kc->closure);
        else if (filter)
            s = kc->encodeFilter(&next, src, kc->closure);
        else
            s = kc->encodeKey(&next, src, kc->closure);
        NATS_FREE(cur);
        if (s != NATS_OK)
            return s;

        if (next == NULL)
            return NATS_ERR;
        cur = next;
    }
    *out = cur;
    return NATS_OK;
}

static natsStatus
_keyChainEncode(char **out, const char *in, void *closure)
{
    return _keyChainTransform(out, in, (kvKeyChain *)closure, false, false);
}

static natsStatus
_keyChainDecode(char **out, const char *in, void *closure)
{
    return _keyChainTransform(out, in, (kvKeyChain *)closure, true, false);
}

static natsStatus
_keyChainEncodeFilter(char **out, const char *in, void *closure)
{
    return _keyChainTransform(out, in, (kvKeyChain *)closure, false, true);
}

static void
_keyChainDestroy(void *closure)
{
    kvKeyChain *chain = (kvKeyChain *)closure;

    NATS_FREE(chain->codecs);
    NATS_FREE(chain);
}

natsStatus
kvKeyCodec_Chain(kvKeyCodec **newCodec, kvKeyCodec **codecs, int numCodecs)
{
    natsStatus  s          = NATS_OK;
    kvKeyChain *chain      = NULL;
    bool        filterable = true;
    int         i;

    if ((newCodec == NULL) || (codecs == NULL) || (numCodecs < 1))
        return NATS_INVALID_ARG;
    for (i = 0; i < numCodecs; i++)
    {
        if (codecs[i] == NULL)
            return NATS_INVALID_ARG;
        // Filterability of a chain is static: known as soon as its members
        // are, so it is resolved here rather than at watch/list time.
        if (codecs[i]->encodeFilter == NULL)
            filterable = false;
    }

    chain = (kvKeyChain *)NATS_CALLOC(1, sizeof(kvKeyChain));
    if (chain == NULL)
        return NATS_NO_MEMORY;
    chain->codecs = (kvKeyCodec **)NATS_MALLOC((size_t)numCodecs * sizeof(kvKeyCodec *));
    if (chain->codecs == NULL)
    {
        NATS_FREE(chain);
        return NATS_NO_MEMORY;
    }
    memcpy(chain->codecs, codecs, (size_t)numCodecs * sizeof(kvKeyCodec *));
    chain->count = numCodecs;

    s = kvKeyCodec_New(newCodec, _keyChainEncode, _keyChainDecode,
                       filterable ? _keyChainEncodeFilter : NULL,
                       _keyChainDestroy, chain);
    if (s != NATS_OK)
        _keyChainDestroy(chain);
    return s;
}

static natsStatus
_valueChainTransform(void **out, int *outLen, const void *in, int inLen,
                     kvValueChain *chain, bool decode)
{
    natsStatus s      = NATS_OK;
    void      *cur    = NULL;
    int        curLen = 0;
    int        i;

    for (i = 0; i < chain->count; i++)
    {
        kvValueCodec *vc = chain->codecs[decode ? (chain->count - 1 - i) : i];
        // First iteration only: a stage may return *out == NULL for an empty
        // result (allocating 0 bytes may return NULL), so cur cannot distinguish "first
        // stage" from "previous stage produced empty output".
        const void *src     = (i == 0) ? in : cur;
        int         srcLen  = (i == 0) ? inLen : curLen;
        void       *next    = NULL;
        int         nextLen = 0;

        if (decode)
            s = vc->decodeValue(&next, &nextLen, src, srcLen, vc->closure);
        else
            s = vc->encodeValue(&next, &nextLen, src, srcLen, vc->closure);
        NATS_FREE(cur);
        if (s != NATS_OK)
            return s;
        cur    = next;
        curLen = nextLen;
    }
    *out    = cur;
    *outLen = curLen;
    return NATS_OK;
}

static natsStatus
_valueChainEncode(void **out, int *outLen, const void *in, int inLen, void *closure)
{
    return _valueChainTransform(out, outLen, in, inLen, (kvValueChain *)closure, false);
}

static natsStatus
_valueChainDecode(void **out, int *outLen, const void *in, int inLen, void *closure)
{
    return _valueChainTransform(out, outLen, in, inLen, (kvValueChain *)closure, true);
}

static void
_valueChainDestroy(void *closure)
{
    kvValueChain *chain = (kvValueChain *)closure;

    NATS_FREE(chain->codecs);
    NATS_FREE(chain);
}

natsStatus
kvValueCodec_Chain(kvValueCodec **newCodec, kvValueCodec **codecs, int numCodecs)
{
    natsStatus    s     = NATS_OK;
    kvValueChain *chain = NULL;
    int           i;

    if ((newCodec == NULL) || (codecs == NULL) || (numCodecs < 1))
        return NATS_INVALID_ARG;
    for (i = 0; i < numCodecs; i++)
    {
        if (codecs[i] == NULL)
            return NATS_INVALID_ARG;
    }

    chain = (kvValueChain *)NATS_CALLOC(1, sizeof(kvValueChain));
    if (chain == NULL)
        return NATS_NO_MEMORY;
    chain->codecs = (kvValueCodec **)NATS_MALLOC((size_t)numCodecs * sizeof(kvValueCodec *));
    if (chain->codecs == NULL)
    {
        NATS_FREE(chain);
        return NATS_NO_MEMORY;
    }
    memcpy(chain->codecs, codecs, (size_t)numCodecs * sizeof(kvValueCodec *));
    chain->count = numCodecs;

    s = kvValueCodec_New(newCodec, _valueChainEncode, _valueChainDecode,
                         _valueChainDestroy, chain);
    if (s != NATS_OK)
        _valueChainDestroy(chain);
    return s;
}
