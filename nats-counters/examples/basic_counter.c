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
    if (s != NATS_OK) goto done;

    s = natsConnection_JetStream(&js, nc, NULL);
    if (s != NATS_OK) goto done;

    // Create the stream (ignore error if it already exists).
    {
        jsStreamConfig cfg;
        jsErrCode      jerr = 0;

        jsStreamConfig_Init(&cfg);
        cfg.Name            = STREAM_NAME;
        cfg.Subjects        = (const char *[]){"events_counter.*"};
        cfg.SubjectsLen     = 1;
        cfg.AllowMsgCounter = true;
        cfg.AllowDirect     = true;
        s = js_AddStream(NULL, js, &cfg, NULL, &jerr);
        if (s != NATS_OK && jerr == JSStreamNameExistErr)
            s = NATS_OK;
        if (s != NATS_OK) goto done;
    }

    s = natsCounter_GetFromStream(&counter, js, nc, STREAM_NAME);
    if (s != NATS_OK) goto done;

    // Increment counters for different event types
    printf("Recording events...\n");

    s = natsCounter_Add(counter, "events_counter.clicks", 5, &value);
    if (s != NATS_OK) goto done;
    printf("  Clicks: +5 = %lld\n", value);

    s = natsCounter_Add(counter, "events_counter.views", 100, &value);
    if (s != NATS_OK) goto done;
    printf("  Views: +100 = %lld\n", value);

    s = natsCounter_Add(counter, "events_counter.errors", 2, &value);
    if (s != NATS_OK) goto done;
    printf("  Errors: +2 = %lld\n", value);

    printf("\nRecording more events...\n");

    s = natsCounter_Add(counter, "events_counter.clicks", 3, &value);
    if (s != NATS_OK) goto done;
    printf("  Clicks: +3 = %lld\n", value);

    s = natsCounter_Add(counter, "events_counter.views", 50, &value);
    if (s != NATS_OK) goto done;
    printf("  Views: +50 = %lld\n", value);

    // Decrement (correction)
    s = natsCounter_Decrement(counter, "events_counter.errors", &value);
    if (s != NATS_OK) goto done;
    printf("  Errors: -1 = %lld\n", value);

    // Load current totals
    printf("\nCurrent totals:\n");

    s = natsCounter_Load(counter, "events_counter.clicks", &value);
    if (s == NATS_OK) printf("  Total clicks: %lld\n", value);

    s = natsCounter_Load(counter, "events_counter.views", &value);
    if (s == NATS_OK) printf("  Total views:  %lld\n", value);

    s = natsCounter_Load(counter, "events_counter.errors", &value);
    if (s == NATS_OK) printf("  Total errors: %lld\n", value);

    // Get full entry with metadata
    printf("\nDetailed entry for clicks:\n");
    s = natsCounter_Get(counter, "events_counter.clicks", &entry);
    if (s != NATS_OK) goto done;

    printf("  Subject: %s\n", entry->subject);
    printf("  Value:   %s\n", entry->value);

    if (natsCounterEntry_HasIncrement(entry))
        printf("  Last increment: %s\n", entry->increment);

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
        natsStatus  lastStatus;
        const char *lastErr = nats_GetLastError(&lastStatus);

        printf("Error: %s\n", natsStatus_GetText(s));
        if (lastErr != NULL && lastErr[0] != '\0')
            printf("Detail: %s\n", lastErr);
        return 1;
    }

    printf("\n=== Counter operations completed ===\n");
    return 0;
}
