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

#include <stdlib.h>
#include <string.h>

void *
kvCodec_AllocBuf(size_t size)
{
    return NATS_MALLOC(size);
}

void
kvCodec_FreeBuf(void *buf)
{
    NATS_FREE(buf);
}

static natsStatus
_strCopy(char **out, const char *in)
{
    char *copy = (char *)NATS_MALLOC(strlen(in) + 1);

    if (copy == NULL)
        return NATS_NO_MEMORY;
    strcpy(copy, in);
    *out = copy;
    return NATS_OK;
}

static natsStatus
_bufCopy(void **out, int *outLen, const void *in, int inLen)
{
    // Hidden trailing NUL so passthrough values remain usable as strings.
    char *copy = (char *)NATS_MALLOC((size_t)inLen + 1);

    if (copy == NULL)
        return NATS_NO_MEMORY;
    if (inLen > 0)
        memcpy(copy, in, (size_t)inLen);
    copy[inLen] = '\0';
    *out        = copy;
    *outLen     = inLen;
    return NATS_OK;
}

static natsStatus
_noOpKeyTransform(char **out, const char *in, void *closure)
{
    (void)closure;
    return _strCopy(out, in);
}

static natsStatus
_noOpValueTransform(void **out, int *outLen, const void *in, int inLen, void *closure)
{
    (void)closure;
    return _bufCopy(out, outLen, in, inLen);
}

natsStatus
kvKeyCodec_New(kvKeyCodec    **newCodec,
               kvKeyTransformF encodeKey,
               kvKeyTransformF decodeKey,
               kvKeyTransformF encodeFilter,
               kvCodecDestroyF destroy,
               void           *closure)
{
    kvKeyCodec *kc = NULL;

    if ((newCodec == NULL) || (encodeKey == NULL) || (decodeKey == NULL))
        return NATS_INVALID_ARG;

    kc = (kvKeyCodec *)NATS_CALLOC(1, sizeof(kvKeyCodec));
    if (kc == NULL)
        return NATS_NO_MEMORY;

    kc->encodeKey    = encodeKey;
    kc->decodeKey    = decodeKey;
    kc->encodeFilter = encodeFilter;
    kc->destroy      = destroy;
    kc->closure      = closure;

    *newCodec = kc;
    return NATS_OK;
}

natsStatus
kvKeyCodec_NoOp(kvKeyCodec **newCodec)
{
    return kvKeyCodec_New(newCodec, _noOpKeyTransform, _noOpKeyTransform,
                          _noOpKeyTransform, NULL, NULL);
}

void
kvKeyCodec_Destroy(kvKeyCodec *codec)
{
    if (codec == NULL)
        return;
    if (codec->destroy != NULL)
        codec->destroy(codec->closure);
    NATS_FREE(codec);
}

natsStatus
kvValueCodec_New(kvValueCodec    **newCodec,
                 kvValueTransformF encodeValue,
                 kvValueTransformF decodeValue,
                 kvCodecDestroyF   destroy,
                 void             *closure)
{
    kvValueCodec *vc = NULL;

    if ((newCodec == NULL) || (encodeValue == NULL) || (decodeValue == NULL))
        return NATS_INVALID_ARG;

    vc = (kvValueCodec *)NATS_CALLOC(1, sizeof(kvValueCodec));
    if (vc == NULL)
        return NATS_NO_MEMORY;

    vc->encodeValue = encodeValue;
    vc->decodeValue = decodeValue;
    vc->destroy     = destroy;
    vc->closure     = closure;

    *newCodec = vc;
    return NATS_OK;
}

natsStatus
kvValueCodec_NoOp(kvValueCodec **newCodec)
{
    return kvValueCodec_New(newCodec, _noOpValueTransform, _noOpValueTransform,
                            NULL, NULL);
}

void
kvValueCodec_Destroy(kvValueCodec *codec)
{
    if (codec == NULL)
        return;
    if (codec->destroy != NULL)
        codec->destroy(codec->closure);
    NATS_FREE(codec);
}

natsStatus
kvcodec_encodeKey(char **out, kvKeyCodec *kc, const char *key)
{
    if ((out == NULL) || (key == NULL))
        return NATS_INVALID_ARG;
    if (kc == NULL)
        return _strCopy(out, key);
    return kc->encodeKey(out, key, kc->closure);
}

natsStatus
kvcodec_decodeKey(char **out, kvKeyCodec *kc, const char *key)
{
    if ((out == NULL) || (key == NULL))
        return NATS_INVALID_ARG;
    if (kc == NULL)
        return _strCopy(out, key);
    return kc->decodeKey(out, key, kc->closure);
}

natsStatus
kvcodec_encodeFilter(char **out, kvKeyCodec *kc, const char *filter)
{
    if ((out == NULL) || (filter == NULL))
        return NATS_INVALID_ARG;
    if (kc == NULL)
        return _strCopy(out, filter);
    if (kc->encodeFilter != NULL)
        return kc->encodeFilter(out, filter, kc->closure);

    // Not filterable: reject wildcard patterns, encode the rest as plain keys.
    // Character match, not a token check, so a literal like "foo*" is over-
    // rejected; kept deliberately for parity with orbit.go (kv.go uses Contains).
    if (strpbrk(filter, "*>") != NULL)
        return NATS_INVALID_SUBJECT;
    return kc->encodeKey(out, filter, kc->closure);
}

natsStatus
kvcodec_encodeValue(void **out, int *outLen, kvValueCodec *vc, const void *value, int len)
{
    if ((out == NULL) || (outLen == NULL) || (len < 0) || ((value == NULL) && (len > 0)))
        return NATS_INVALID_ARG;
    if (vc == NULL)
        return _bufCopy(out, outLen, value, len);
    return vc->encodeValue(out, outLen, value, len, vc->closure);
}

natsStatus
kvcodec_decodeValue(void **out, int *outLen, kvValueCodec *vc, const void *value, int len)
{
    if ((out == NULL) || (outLen == NULL) || (len < 0) || ((value == NULL) && (len > 0)))
        return NATS_INVALID_ARG;
    if (vc == NULL)
        return _bufCopy(out, outLen, value, len);
    return vc->decodeValue(out, outLen, value, len, vc->closure);
}
