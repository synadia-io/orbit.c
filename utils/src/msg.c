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

#include "msg.h"

#include "os_shims.h"

natsStatus
natsMsg_CopyHeaders(natsMsg *dst, natsMsg *src)
{
    const char **keys     = NULL;
    int          keyCount = 0;

    natsStatus s = natsMsgHeader_Keys(src, &keys, &keyCount);
    if (s == NATS_NOT_FOUND)
        return NATS_OK; // src carries no headers
    if (s != NATS_OK)
        return s;

    for (int i = 0; s == NATS_OK && i < keyCount; i++)
    {
        const char **vals     = NULL;
        int          valCount = 0;

        s = natsMsgHeader_Values(src, keys[i], &vals, &valCount);
        for (int j = 0; s == NATS_OK && j < valCount; j++)
            s = natsMsgHeader_Add(dst, keys[i], vals[j]);
        NATS_FREE((void *)vals);
    }
    NATS_FREE((void *)keys);
    return s;
}
