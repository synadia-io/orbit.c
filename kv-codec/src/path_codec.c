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

// NATS subjects cannot start with a dot, so a leading '/' is represented by
// this sentinel token to keep round-trip fidelity.
#define ROOT_PREFIX "_root_"

static natsStatus
_pathEncodeKey(char **out, const char *key, void *closure)
{
    size_t keyLen = strlen(key);
    char  *buf    = (char *)NATS_MALLOC(keyLen + sizeof(ROOT_PREFIX) + 1);
    char  *dst;
    size_t i;

    (void)closure;

    if (buf == NULL)
        return NATS_NO_MEMORY;

    dst = buf;
    if (key[0] == '/')
    {
        memcpy(dst, ROOT_PREFIX, sizeof(ROOT_PREFIX) - 1);
        dst += sizeof(ROOT_PREFIX) - 1;
        if (keyLen == 1) // key is exactly "/"
        {
            *dst = '\0';
            *out = buf;
            return NATS_OK;
        }
        *dst++ = '.';
        key++;
        keyLen--;
    }
    // A trailing '/' would produce a subject ending in '.', which is invalid.
    if ((keyLen > 0) && (key[keyLen - 1] == '/'))
        keyLen--;
    for (i = 0; i < keyLen; i++)
        *dst++ = (key[i] == '/') ? '.' : key[i];
    *dst = '\0';
    *out = buf;
    return NATS_OK;
}

static natsStatus
_pathDecodeKey(char **out, const char *key, void *closure)
{
    char *buf = (char *)NATS_MALLOC(strlen(key) + 2);
    char *dst;

    (void)closure;

    if (buf == NULL)
        return NATS_NO_MEMORY;

    dst = buf;
    if (strcmp(key, ROOT_PREFIX) == 0)
    {
        *dst++ = '/';
        key += sizeof(ROOT_PREFIX) - 1;
    }
    else if (strncmp(key, ROOT_PREFIX ".", sizeof(ROOT_PREFIX)) == 0)
    {
        *dst++ = '/';
        key += sizeof(ROOT_PREFIX); // sentinel plus the following '.'
    }
    for (; *key != '\0'; key++)
        *dst++ = (*key == '.') ? '/' : *key;
    *dst = '\0';
    *out = buf;
    return NATS_OK;
}

natsStatus
kvKeyCodec_Path(kvKeyCodec **newCodec)
{
    // '*' and '>' are neither '/' nor '.', so the key encoder already
    // preserves wildcards and doubles as the filter encoder.
    return kvKeyCodec_New(newCodec, _pathEncodeKey, _pathDecodeKey,
                          _pathEncodeKey, NULL, NULL);
}
