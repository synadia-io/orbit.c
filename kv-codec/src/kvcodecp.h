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

#ifndef KV_CODEC_P_H_
#define KV_CODEC_P_H_

#include "kvcodec.h"

#include <stdbool.h>

// All allocations go through the NATS_MALLOC / NATS_CALLOC / NATS_REALLOC /
// NATS_FREE shims from os_shims.h, so an alternative allocator can be
// swapped in without touching call sites. Custom codec callbacks allocate
// their outputs with kvCodec_AllocBuf (= NATS_MALLOC), so those buffers live
// in the same allocator and are released with NATS_FREE like every other.

// encodeFilter == NULL means the codec is not filterable: watch/list patterns
// containing wildcards are rejected with NATS_INVALID_SUBJECT.
struct __kvKeyCodec
{
    kvKeyTransformF encodeKey;
    kvKeyTransformF decodeKey;
    kvKeyTransformF encodeFilter;
    kvCodecDestroyF destroy;
    void           *closure;
};

struct __kvValueCodec
{
    kvValueTransformF encodeValue;
    kvValueTransformF decodeValue;
    kvCodecDestroyF   destroy;
    void             *closure;
};

struct __kvCodec
{
    kvStore      *kv;         // borrowed
    kvKeyCodec   *keyCodec;   // borrowed, may be NULL (passthrough)
    kvValueCodec *valueCodec; // borrowed, may be NULL (passthrough)
};

struct __kvCodecWatcher
{
    kvWatcher    *w;          // owned
    kvKeyCodec   *keyCodec;   // borrowed from the kvCodec
    kvValueCodec *valueCodec; // borrowed from the kvCodec
};

struct __kvCodecEntry
{
    kvEntry *entry;    // owned; source of Bucket/Revision/Created/Delta/Operation
    char    *key;      // owned; decoded or original plaintext key
    void    *value;    // owned; decoded value + hidden trailing NUL; NULL for markers
    int      valueLen; // decoded length, excluding the hidden NUL
    bool     rawValue; // no value codec: accessors delegate to 'entry', no copy
};

// codec.c: dispatch helpers. A NULL codec means passthrough, which still
// copies into a freshly allocated buffer so every out-buffer is uniformly
// owned.
natsStatus kvcodec_encodeKey(char **out, kvKeyCodec *kc, const char *key);
natsStatus kvcodec_decodeKey(char **out, kvKeyCodec *kc, const char *key);
// Implements the filter logic: filterable -> encodeFilter; otherwise reject
// patterns containing wildcards (NATS_INVALID_SUBJECT), then encodeKey.
natsStatus kvcodec_encodeFilter(char **out, kvKeyCodec *kc, const char *filter);
natsStatus kvcodec_encodeValue(void **out, int *outLen, kvValueCodec *vc,
                               const void *value, int len);
natsStatus kvcodec_decodeValue(void **out, int *outLen, kvValueCodec *vc,
                               const void *value, int len);

// base64_codec.c: URL-safe alphabet, no padding (RFC 4648 section 5).
natsStatus kvcodec_base64RawURLEncode(char **out, int *outLen, const void *in, int inLen);
natsStatus kvcodec_base64RawURLDecode(void **out, int *outLen,
                                      const char *in, int inLen);

// kv.c: wraps a kvEntry, decoding its key (or copying originalKey when not
// NULL) and value. On success the new entry owns 'entry'.
natsStatus kvcodec_wrapEntry(kvCodecEntry **newEntry, kvEntry *entry,
                             const char *originalKey,
                             kvKeyCodec *kc, kvValueCodec *vc);

#endif // KV_CODEC_P_H_
