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

// Path key codec example.
//
// Uses the path codec (keys only, values untouched) so the bucket can be
// addressed with filesystem-style keys: "/config/app/database" is stored as
// the NATS subject "_root_.config.app.database".
//
// Prerequisites:
//   A JetStream-enabled NATS server (nats-server -js).
//
// Usage:
//   kv-codec-path_keys [url]

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
    kvKeyCodec     *keyCodec = NULL;
    kvCodec        *c        = NULL;
    kvCodecKeysList keys     = { NULL, 0 };
    const char     *url      = (argc > 1) ? argv[1] : URL;
    int             i;

    printf("=== KV path codec example ===\n\n");

    s = natsConnection_ConnectTo(&nc, url);
    if (s == NATS_OK)
        s = natsConnection_JetStream(&js, nc, NULL);
    if (s == NATS_OK)
    {
        kvConfig_Init(&kvc);
        kvc.Bucket = "PATHEXAMPLE";
        s          = js_CreateKeyValue(&kv, js, &kvc);
    }
    if (s != NATS_OK)
        goto done;

    // Keys only: pass NULL for the value codec (values are stored as-is).
    s = kvKeyCodec_Path(&keyCodec);
    if (s == NATS_OK)
        s = kvCodec_New(&c, kv, keyCodec, NULL);
    if (s != NATS_OK)
        goto done;

    printf("Putting path-style keys...\n");
    s = kvCodec_PutString(NULL, c, "/config/app/database", "postgres://localhost");
    if (s == NATS_OK)
        s = kvCodec_PutString(NULL, c, "/config/app/cache", "redis://localhost");
    if (s == NATS_OK)
        s = kvCodec_PutString(NULL, c, "logs/app", "/var/log/app.log");
    if (s != NATS_OK)
        goto done;

    s = kvCodec_Keys(&keys, c, NULL);
    if (s != NATS_OK)
        goto done;
    printf("Decoded keys in the bucket:\n");
    for (i = 0; i < keys.Count; i++)
        printf("  %s\n", keys.Keys[i]);

done:
    kvCodecKeysList_Destroy(&keys);
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
