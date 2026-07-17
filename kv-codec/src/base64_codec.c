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

#include <limits.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

// URL-safe alphabet, no padding (RFC 4648 section 5).
// Non-canonical trailing bits in the final group are accepted;
// unlike orbit.go, embedded '\r'/'\n' are rejected rather than skipped.
static const char _b64Chars[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

static int
_b64EncodedLen(int n)
{
    switch (n % 3)
    {
        case 1:
            return (n / 3) * 4 + 2;
        case 2:
            return (n / 3) * 4 + 3;
        default:
            return (n / 3) * 4;
    }
}

// Encodes into 'dst' (must have room for _b64EncodedLen(inLen) bytes, not
// NUL-terminated here) and returns the number of bytes written.
static int
_b64EncodeInto(char *dst, const unsigned char *in, int inLen)
{
    int di = 0;
    int i;

    for (i = 0; i + 2 < inLen; i += 3)
    {
        uint32_t v = ((uint32_t)in[i] << 16) | ((uint32_t)in[i + 1] << 8) | in[i + 2];

        dst[di++] = _b64Chars[(v >> 18) & 0x3F];
        dst[di++] = _b64Chars[(v >> 12) & 0x3F];
        dst[di++] = _b64Chars[(v >> 6) & 0x3F];
        dst[di++] = _b64Chars[v & 0x3F];
    }
    switch (inLen - i)
    {
        case 1:
        {
            uint32_t v = (uint32_t)in[i] << 16;

            dst[di++] = _b64Chars[(v >> 18) & 0x3F];
            dst[di++] = _b64Chars[(v >> 12) & 0x3F];
            break;
        }
        case 2:
        {
            uint32_t v = ((uint32_t)in[i] << 16) | ((uint32_t)in[i + 1] << 8);

            dst[di++] = _b64Chars[(v >> 18) & 0x3F];
            dst[di++] = _b64Chars[(v >> 12) & 0x3F];
            dst[di++] = _b64Chars[(v >> 6) & 0x3F];
            break;
        }
        default:
            break;
    }
    return di;
}

static int
_b64Value(char c)
{
    if ((c >= 'A') && (c <= 'Z'))
        return c - 'A';
    if ((c >= 'a') && (c <= 'z'))
        return c - 'a' + 26;
    if ((c >= '0') && (c <= '9'))
        return c - '0' + 52;
    if (c == '-')
        return 62;
    if (c == '_')
        return 63;
    return -1; // includes '=': padding is invalid in the raw encoding
}

// Decodes into 'dst' (must have room for (inLen/4)*3+2 bytes) and returns the
// number of bytes written, or -1 on invalid input.
static int
_b64DecodeInto(unsigned char *dst, const char *in, int inLen)
{
    int di = 0;
    int i;

    if (inLen % 4 == 1)
        return -1;

    for (i = 0; i + 3 < inLen; i += 4)
    {
        int a = _b64Value(in[i]);
        int b = _b64Value(in[i + 1]);
        int c = _b64Value(in[i + 2]);
        int d = _b64Value(in[i + 3]);

        if ((a < 0) || (b < 0) || (c < 0) || (d < 0))
            return -1;

        dst[di++] = (unsigned char)((a << 2) | (b >> 4));
        dst[di++] = (unsigned char)((b << 4) | (c >> 2));
        dst[di++] = (unsigned char)((c << 6) | d);
    }
    switch (inLen - i)
    {
        case 2:
        {
            int a = _b64Value(in[i]);
            int b = _b64Value(in[i + 1]);

            if ((a < 0) || (b < 0))
                return -1;
            dst[di++] = (unsigned char)((a << 2) | (b >> 4));
            break;
        }
        case 3:
        {
            int a = _b64Value(in[i]);
            int b = _b64Value(in[i + 1]);
            int c = _b64Value(in[i + 2]);

            if ((a < 0) || (b < 0) || (c < 0))
                return -1;
            dst[di++] = (unsigned char)((a << 2) | (b >> 4));
            dst[di++] = (unsigned char)((b << 4) | (c >> 2));
            break;
        }
        default:
            break;
    }
    return di;
}

natsStatus
kvcodec_base64RawURLEncode(char **out, int *outLen, const void *in, int inLen)
{
    int   encLen;
    char *buf;

    // (inLen / 3) * 4 must not exceed INT_MAX.
    if (inLen > (INT_MAX - 3) / 4 * 3)
        return NATS_INVALID_ARG;

    encLen = _b64EncodedLen(inLen);
    buf    = (char *)NATS_MALLOC((size_t)encLen + 1);
    if (buf == NULL)
        return NATS_NO_MEMORY;
    _b64EncodeInto(buf, (const unsigned char *)in, inLen);
    buf[encLen] = '\0';
    *out        = buf;
    *outLen     = encLen;
    return NATS_OK;
}

natsStatus
kvcodec_base64RawURLDecode(void **out, int *outLen, const char *in, int inLen)
{
    unsigned char *buf = (unsigned char *)NATS_MALLOC((inLen / 4) * 3 + 3);
    int            n;

    if (buf == NULL)
        return NATS_NO_MEMORY;
    n = _b64DecodeInto(buf, in, inLen);
    if (n < 0)
    {
        NATS_FREE(buf);
        return NATS_ERR;
    }
    buf[n]  = '\0'; // hidden NUL, not counted in *outLen
    *out    = buf;
    *outLen = n;
    return NATS_OK;
}

// Encodes a dot-delimited key token by token. When 'isFilter' is true, tokens
// that are exactly "*" or ">" are passed through verbatim so NATS wildcard
// semantics survive the encoding.
static natsStatus
_b64TransformKey(char **out, const char *key, bool isFilter)
{
    size_t keyLen = strlen(key);
    // Worst case: every token grows to ceil(len*4/3), plus the dots and NUL.
    char       *buf = (char *)NATS_MALLOC((keyLen / 3 + 1) * 4 + keyLen + 2);
    char       *dst;
    const char *tok = key;

    if (buf == NULL)
        return NATS_NO_MEMORY;

    dst = buf;
    for (;;)
    {
        const char *dot    = strchr(tok, '.');
        int         tokLen = (dot != NULL) ? (int)(dot - tok) : (int)strlen(tok);

        if (isFilter && (tokLen == 1) && ((tok[0] == '*') || (tok[0] == '>')))
            *dst++ = tok[0];
        else
            dst += _b64EncodeInto(dst, (const unsigned char *)tok, tokLen);

        if (dot == NULL)
            break;
        *dst++ = '.';
        tok    = dot + 1;
    }
    *dst = '\0';
    *out = buf;
    return NATS_OK;
}

static natsStatus
_b64EncodeKey(char **out, const char *key, void *closure)
{
    (void)closure;
    return _b64TransformKey(out, key, false);
}

static natsStatus
_b64EncodeFilter(char **out, const char *filter, void *closure)
{
    (void)closure;
    return _b64TransformKey(out, filter, true);
}

static natsStatus
_b64DecodeKey(char **out, const char *key, void *closure)
{
    // Decoding shrinks tokens, so the input size (plus NUL) always fits.
    char       *buf = (char *)NATS_MALLOC(strlen(key) + 1);
    char       *dst;
    const char *tok = key;

    (void)closure;

    if (buf == NULL)
        return NATS_NO_MEMORY;

    dst = buf;
    for (;;)
    {
        const char *dot    = strchr(tok, '.');
        int         tokLen = (dot != NULL) ? (int)(dot - tok) : (int)strlen(tok);
        int         n      = _b64DecodeInto((unsigned char *)dst, tok, tokLen);

        if (n < 0)
        {
            NATS_FREE(buf);
            return NATS_ERR;
        }
        dst += n;

        if (dot == NULL)
            break;
        *dst++ = '.';
        tok    = dot + 1;
    }
    *dst = '\0';
    *out = buf;
    return NATS_OK;
}

static natsStatus
_b64EncodeValue(void **out, int *outLen, const void *in, int inLen, void *closure)
{
    char      *encoded = NULL;
    natsStatus s;

    (void)closure;

    s = kvcodec_base64RawURLEncode(&encoded, outLen, in, inLen);
    if (s != NATS_OK)
        return s;
    *out = encoded;
    return NATS_OK;
}

static natsStatus
_b64DecodeValue(void **out, int *outLen, const void *in, int inLen, void *closure)
{
    (void)closure;
    return kvcodec_base64RawURLDecode(out, outLen, (const char *)in, inLen);
}

natsStatus
kvKeyCodec_Base64(kvKeyCodec **newCodec)
{
    return kvKeyCodec_New(newCodec, _b64EncodeKey, _b64DecodeKey,
                          _b64EncodeFilter, NULL, NULL);
}

natsStatus
kvValueCodec_Base64(kvValueCodec **newCodec)
{
    return kvValueCodec_New(newCodec, _b64EncodeValue, _b64DecodeValue,
                            NULL, NULL);
}
