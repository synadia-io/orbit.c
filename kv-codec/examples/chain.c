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

// Chain codec example.
//
// Chains the path codec with the Base64 codec for keys (path -> subject
// notation, then Base64 per token) and uses Base64 for values, then watches a
// path wildcard: updates arrive already decoded.
//
// Prerequisites:
//   A JetStream-enabled NATS server (nats-server -js).
//
// Usage:
//   kv-codec-chain [url]

#include "kvcodec.h"
#include <stdio.h>

#define URL "nats://localhost:4222"

int
main(int argc, char **argv)
{
    natsStatus      s  = NATS_OK;
    natsConnection *nc = NULL;
    jsCtx          *js = NULL;
    kvStore        *kv = NULL;
    kvConfig        kvc;
    kvKeyCodec     *path     = NULL;
    kvKeyCodec     *b64      = NULL;
    kvKeyCodec     *keyChain = NULL;
    kvKeyCodec     *members[2];
    kvValueCodec   *valueCodec = NULL;
    kvCodec        *c          = NULL;
    kvCodecWatcher *w          = NULL;
    kvCodecEntry   *entry      = NULL;
    const char     *url        = (argc > 1) ? argv[1] : URL;

    printf("=== KV chain codec example ===\n\n");

    s = natsConnection_ConnectTo(&nc, url);
    if (s == NATS_OK)
        s = natsConnection_JetStream(&js, nc, NULL);
    if (s == NATS_OK)
    {
        kvConfig_Init(&kvc);
        kvc.Bucket = "CHAINEXAMPLE";
        s          = js_CreateKeyValue(&kv, js, &kvc);
    }
    if (s != NATS_OK)
        goto done;

    // Key chain: path first, then Base64 (decode runs in reverse).
    s = kvKeyCodec_Path(&path);
    if (s == NATS_OK)
        s = kvKeyCodec_Base64(&b64);
    if (s == NATS_OK)
    {
        members[0] = path;
        members[1] = b64;
        s          = kvKeyCodec_Chain(&keyChain, members, 2);
    }
    if (s == NATS_OK)
        s = kvValueCodec_Base64(&valueCodec);
    if (s == NATS_OK)
        s = kvCodec_New(&c, kv, keyChain, valueCodec);
    if (s != NATS_OK)
        goto done;

    // Both codecs in the chain are filterable, so path wildcards work.
    printf("Watching \"/config/>\"...\n");
    s = kvCodec_Watch(&w, c, "/config/>", NULL);
    if (s == NATS_OK)
        s = kvCodecWatcher_Next(&entry, w, 1000); // initial-values marker (NULL)
    if (s != NATS_OK)
        goto done;

    s = kvCodec_PutString(NULL, c, "/config/app/setting", "42");
    if (s == NATS_OK)
        s = kvCodecWatcher_Next(&entry, w, 1000);
    if (s != NATS_OK)
        goto done;

    printf("Watcher delivered: %s = %s\n",
           kvCodecEntry_Key(entry), kvCodecEntry_ValueString(entry));

done:
    kvCodecEntry_Destroy(entry);
    kvCodecWatcher_Destroy(w);
    kvCodec_Destroy(c);
    kvValueCodec_Destroy(valueCodec);
    kvKeyCodec_Destroy(keyChain);
    kvKeyCodec_Destroy(b64);
    kvKeyCodec_Destroy(path);
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
