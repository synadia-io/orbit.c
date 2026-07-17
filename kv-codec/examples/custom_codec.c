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

// Custom codec example.
//
// Builds a key codec from plain callbacks: keys are uppercased in storage and
// lowercased on the way out. The codec supplies no filter encoder, so watch
// patterns with wildcards are rejected with NATS_INVALID_SUBJECT while exact
// keys still work.
//
// Prerequisites:
//   A JetStream-enabled NATS server (nats-server -js).
//
// Usage:
//   kv-codec-custom_codec [url]

#include "kvcodec.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define URL "nats://localhost:4222"

// Callbacks allocate the result with kvCodec_AllocBuf; the library takes
// ownership and releases it.
static natsStatus
_upperEncode(char **out, const char *in, void *closure)
{
    char  *buf = (char *)kvCodec_AllocBuf(strlen(in) + 1);
    size_t i;

    (void)closure;
    if (buf == NULL)
        return NATS_NO_MEMORY;
    for (i = 0; in[i] != '\0'; i++)
        buf[i] = (char)((in[i] >= 'a' && in[i] <= 'z') ? in[i] - 32 : in[i]);
    buf[i] = '\0';
    *out   = buf;
    return NATS_OK;
}

static natsStatus
_lowerDecode(char **out, const char *in, void *closure)
{
    char  *buf = (char *)kvCodec_AllocBuf(strlen(in) + 1);
    size_t i;

    (void)closure;
    if (buf == NULL)
        return NATS_NO_MEMORY;
    for (i = 0; in[i] != '\0'; i++)
        buf[i] = (char)((in[i] >= 'A' && in[i] <= 'Z') ? in[i] + 32 : in[i]);
    buf[i] = '\0';
    *out   = buf;
    return NATS_OK;
}

int
main(int argc, char **argv)
{
    natsStatus      s  = NATS_OK;
    natsConnection *nc = NULL;
    jsCtx          *js = NULL;
    kvStore        *kv = NULL;
    kvConfig        kvc;
    kvKeyCodec     *keyCodec = NULL;
    kvCodec        *c        = NULL;
    kvCodecWatcher *w        = NULL;
    kvCodecEntry   *entry    = NULL;
    const char     *url      = (argc > 1) ? argv[1] : URL;

    printf("=== KV custom codec example ===\n\n");

    s = natsConnection_ConnectTo(&nc, url);
    if (s == NATS_OK)
        s = natsConnection_JetStream(&js, nc, NULL);
    if (s == NATS_OK)
    {
        kvConfig_Init(&kvc);
        kvc.Bucket = "CUSTOMEXAMPLE";
        s          = js_CreateKeyValue(&kv, js, &kvc);
    }
    if (s != NATS_OK)
        goto done;

    // No filter encoder (4th argument): the codec is not filterable.
    s = kvKeyCodec_New(&keyCodec, _upperEncode, _lowerDecode, NULL, NULL, NULL);
    if (s == NATS_OK)
        s = kvCodec_New(&c, kv, keyCodec, NULL);
    if (s != NATS_OK)
        goto done;

    printf("Watching \"orders.*\" (wildcard, codec not filterable)...\n");
    s = kvCodec_Watch(&w, c, "orders.*", NULL);
    printf("  -> %s (expected NATS_INVALID_SUBJECT)\n", natsStatus_GetText(s));
    if (s != NATS_INVALID_SUBJECT)
    {
        if (s == NATS_OK)
            s = NATS_ERR; // the watch was expected to fail
        goto done;
    }

    printf("Watching the exact key \"orders.neworder\" instead...\n");
    s = kvCodec_Watch(&w, c, "orders.neworder", NULL);
    if (s == NATS_OK)
        s = kvCodecWatcher_Next(&entry, w, 1000); // initial-values marker (NULL)
    if (s == NATS_OK)
        s = kvCodec_PutString(NULL, c, "orders.neworder", "widget x3");
    if (s == NATS_OK)
        s = kvCodecWatcher_Next(&entry, w, 1000);
    if (s != NATS_OK)
        goto done;

    printf("Watcher delivered: %s = %s (stored as ORDERS.NEWORDER)\n",
           kvCodecEntry_Key(entry), kvCodecEntry_ValueString(entry));

done:
    kvCodecEntry_Destroy(entry);
    kvCodecWatcher_Destroy(w);
    kvCodec_Destroy(c);
    kvKeyCodec_Destroy(keyCodec);
    kvStore_Destroy(kv);
    jsCtx_Destroy(js);
    natsConnection_Destroy(nc);

    if (s != NATS_OK)
    {
        printf("\nError: %s\n", natsStatus_GetText(s));
        return 1;
    }

    printf("\n=== Done ===\n");
    return 0;
}
