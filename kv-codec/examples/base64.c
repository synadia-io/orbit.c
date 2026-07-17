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

// Basic Base64 codec example.
//
// Wraps a KV bucket with Base64 key and value codecs, stores a key that would
// be invalid in a raw bucket ("Acme Inc." contains a space), then shows both
// the decoded codec view and the encoded form actually held by the bucket.
//
// Prerequisites:
//   A JetStream-enabled NATS server (nats-server -js).
//
// Usage:
//   kv-codec-base64 [url]

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
    kvKeyCodec     *keyCodec   = NULL;
    kvValueCodec   *valueCodec = NULL;
    kvCodec        *c          = NULL;
    kvCodecEntry   *entry      = NULL;
    kvKeysList      rawKeys    = { NULL, 0 };
    const char     *url        = (argc > 1) ? argv[1] : URL;
    const char     *key        = "Acme Inc.contact";
    int             i;

    printf("=== KV Base64 codec example ===\n\n");

    s = natsConnection_ConnectTo(&nc, url);
    if (s == NATS_OK)
        s = natsConnection_JetStream(&js, nc, NULL);
    if (s == NATS_OK)
    {
        kvConfig_Init(&kvc);
        kvc.Bucket = "B64EXAMPLE";
        s          = js_CreateKeyValue(&kv, js, &kvc);
    }
    if (s != NATS_OK)
        goto done;

    // Wrap the bucket: keys and values are Base64-encoded transparently.
    s = kvKeyCodec_Base64(&keyCodec);
    if (s == NATS_OK)
        s = kvValueCodec_Base64(&valueCodec);
    if (s == NATS_OK)
        s = kvCodec_New(&c, kv, keyCodec, valueCodec);
    if (s != NATS_OK)
        goto done;

    printf("Putting key \"%s\" (a space is invalid in a raw KV key)...\n", key);
    s = kvCodec_PutString(NULL, c, key, "info@acme.com");
    if (s != NATS_OK)
        goto done;

    s = kvCodec_Get(&entry, c, key);
    if (s != NATS_OK)
        goto done;
    printf("Codec view:  %s = %s\n", kvCodecEntry_Key(entry), kvCodecEntry_ValueString(entry));

    // Peek under the hood: the bucket itself only ever sees encoded keys.
    s = kvStore_Keys(&rawKeys, kv, NULL);
    if (s != NATS_OK)
        goto done;
    for (i = 0; i < rawKeys.Count; i++)
        printf("Raw storage: %s\n", rawKeys.Keys[i]);

done:
    kvKeysList_Destroy(&rawKeys);
    kvCodecEntry_Destroy(entry);
    kvCodec_Destroy(c);
    kvKeyCodec_Destroy(keyCodec);
    kvValueCodec_Destroy(valueCodec);
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
