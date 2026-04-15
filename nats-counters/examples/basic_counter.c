// Copyright 2025 Synadia Communications Inc.
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

// Basic counter operations example.
//
// Demonstrates creating a counter-enabled JetStream stream and performing
// increment, decrement, and read operations.
//
// Prerequisites:
//   A NATS server with JetStream enabled (nats-server -js).
//
// Usage:
//   nats-counters-basic_counter [url]

#include "nats_counters.h"
#include <stdio.h>
#include <stdlib.h>

#define STREAM_NAME "EVENTS_COUNTER"
#define URL         "nats://localhost:4222"

static void
_printSource(const char *stream, const char *subject, const char *value,
             void *closure)
{
    printf("    %s / %s = %s\n", stream, subject, value);
}

int main(int argc, char **argv)
{
    natsStatus           s       = NATS_OK;
    natsConnection      *nc      = NULL;
    jsCtx               *js      = NULL;
    natsCounter         *counter = NULL;
    natsCounterEntry    *entry   = NULL;
    long long            value   = 0;
    const char          *url     = (argc > 1) ? argv[1] : URL;

    printf("=== NATS JetStream Counter Example ===\n\n");

    // Connect
    s = natsConnection_ConnectTo(&nc, url);
    if (s != NATS_OK) { printf("Connect error: %s\n", natsStatus_GetText(s)); return 1; }

    s = natsConnection_JetStream(&js, nc, NULL);
    if (s != NATS_OK) { printf("JetStream error: %s\n", natsStatus_GetText(s)); goto done; }

    // TODO: Create or bind the stream.
    //   jsStreamConfig cfg      = {0};
    //   cfg.Name                = STREAM_NAME;
    //   cfg.Subjects            = (const char *[]){"events.*"};
    //   cfg.SubjectsLen         = 1;
    //   cfg.AllowMsgCounter     = true;
    //   cfg.AllowDirect         = true;
    //   js_AddStream(NULL, js, &cfg, NULL); // ignore error if already exists

    s = natsCounter_GetFromStream(&counter, js, STREAM_NAME);
    if (s != NATS_OK) goto done;

    // Increment counters for different event types
    printf("Recording events...\n");

    s = natsCounter_Add(counter, "events.clicks", 5, &value);
    if (s != NATS_OK) goto done;
    printf("  Clicks: +5 = %lld\n", value);

    s = natsCounter_Add(counter, "events.views", 100, &value);
    if (s != NATS_OK) goto done;
    printf("  Views: +100 = %lld\n", value);

    s = natsCounter_Add(counter, "events.errors", 2, &value);
    if (s != NATS_OK) goto done;
    printf("  Errors: +2 = %lld\n", value);

    printf("\nRecording more events...\n");

    s = natsCounter_Add(counter, "events.clicks", 3, &value);
    if (s != NATS_OK) goto done;
    printf("  Clicks: +3 = %lld\n", value);

    s = natsCounter_Add(counter, "events.views", 50, &value);
    if (s != NATS_OK) goto done;
    printf("  Views: +50 = %lld\n", value);

    // Decrement (correction)
    s = natsCounter_Decrement(counter, "events.errors", &value);
    if (s != NATS_OK) goto done;
    printf("  Errors: -1 = %lld\n", value);

    // Load current totals
    printf("\nCurrent totals:\n");

    s = natsCounter_Load(counter, "events.clicks", &value);
    if (s == NATS_OK) printf("  Total clicks: %lld\n", value);

    s = natsCounter_Load(counter, "events.views", &value);
    if (s == NATS_OK) printf("  Total views:  %lld\n", value);

    s = natsCounter_Load(counter, "events.errors", &value);
    if (s == NATS_OK) printf("  Total errors: %lld\n", value);

    // Get full entry with metadata
    printf("\nDetailed entry for clicks:\n");
    s = natsCounter_Get(counter, "events.clicks", &entry);
    if (s != NATS_OK) goto done;

    printf("  Subject: %s\n", natsCounterEntry_Subject(entry));
    printf("  Value:   %s\n", natsCounterEntry_ValueStr(entry));

    if (natsCounterEntry_HasIncrement(entry))
        printf("  Last increment: %s\n", natsCounterEntry_IncrementStr(entry));

    if (natsCounterEntry_HasSources(entry))
    {
        printf("  Sources:\n");
        natsCounterEntry_IterSources(entry, _printSource, NULL);
    }

done:
    natsCounterEntry_Destroy(entry);
    natsCounter_Destroy(counter);
    if (js != NULL) jsCtx_Destroy(js);
    if (nc != NULL) natsConnection_Destroy(nc);

    if (s != NATS_OK)
    {
        printf("Error: %s\n", natsStatus_GetText(s));
        return 1;
    }

    printf("\n=== Counter operations completed ===\n");
    return 0;
}
