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

// Connect through a NATS context created by the `nats` CLI.
//
// Resolves the context, opens the connection, and prints the settings that
// are returned for the caller to consume (like the JetStream domain).
//
// Prerequisites:
//   A context, e.g.: nats context add example --server nats://localhost:4222
//
// Usage:
//   nats-context-connect [name]
//
// With no argument the selected context (nats context select) is used.

#include "context.h"
#include <stdio.h>

int
main(int argc, char **argv)
{
    natsStatus           s        = NATS_OK;
    natsConnection      *nc       = NULL;
    natsContextSettings *settings = NULL;
    const char          *name     = (argc > 1) ? argv[1] : NULL;
    char                 url[256];

    printf("=== NATS context connect example ===\n\n");

    s = natsContext_Connect(&nc, &settings, name, NULL);
    if (s != NATS_OK)
    {
        printf("Unable to connect through context '%s': %d - %s\n",
               (name != NULL) ? name : "(selected)", s, natsStatus_GetText(s));
        return 1;
    }

    if (natsConnection_GetConnectedUrl(nc, url, sizeof(url)) == NATS_OK)
        printf("Connected to: %s\n", url);

    printf("Context settings:\n");
    printf("  name:             %s\n", settings->Name != NULL ? settings->Name : "");
    printf("  description:      %s\n", settings->Description != NULL ? settings->Description : "");
    printf("  url:              %s\n", settings->URL != NULL ? settings->URL : "(default)");
    printf("  jetstream domain: %s\n", settings->JSDomain != NULL ? settings->JSDomain : "");

    natsContextSettings_Destroy(settings);
    natsConnection_Destroy(nc);
    return 0;
}
