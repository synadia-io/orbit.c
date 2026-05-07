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

// Distributed counter with source tracking example.
//
// Demonstrates source-based aggregation from ADR-49: regional counter
// streams are sourced into a global stream.  The global stream maintains
// Nats-Counter-Sources headers so each region's contribution can be
// inspected at read time.
//
// Stream topology:
//   REGION_US_EAST  (api.requests, api.errors) ──┐
//   REGION_US_WEST  (api.requests, api.errors) ──┼──► GLOBAL
//   REGION_EU       (api.requests, api.errors) ──┘
//
// Each regional stream listens on its own subject namespace.  The global
// stream sources all three.  Because there are no subject transforms,
// each region's subjects appear separately in the global stream.
//
// Prerequisites:
//   A NATS server (2.12 or later) with JetStream enabled (nats-server -js).
//
// Usage:
//   nats-counters-distributed_counter [url]

#include "nats_counters.h"
#include <stdio.h>
#include <string.h>

#define URL "nats://localhost:4222"

static void
_printSource(const char *stream, const char *subject, const char *value,
             void *closure)
{
    const char *region = stream;
    if (strcmp(stream, "REGION_US_EAST") == 0) region = "US-East";
    else if (strcmp(stream, "REGION_US_WEST") == 0) region = "US-West";
    else if (strcmp(stream, "REGION_EU") == 0)      region = "EU";

    printf("    %-10s: %s  (from %s)\n", region, value, subject);
}

static natsStatus
_printEntryWithSources(natsCounter *counter, const char *subject,
                       const char *label)
{
    natsCounterEntry *entry = NULL;
    natsStatus        s;

    s = natsCounter_Get(counter, subject, &entry);
    if (s != NATS_OK)
        return s;

    printf("%-22s %s\n", label, entry->value);
    if (natsCounterEntry_HasSources(entry))
    {
        printf("  Sources:\n");
        natsCounterEntry_IterSources(entry, _printSource, NULL);
    }
    natsCounterEntry_Destroy(entry);
    return NATS_OK;
}

static natsStatus
_createRegionalStream(jsCtx *js, const char *name, const char *subjectFilter)
{
    jsStreamConfig cfg;

    jsStreamConfig_Init(&cfg);
    cfg.Name            = name;
    cfg.Subjects        = &subjectFilter;
    cfg.SubjectsLen     = 1;
    cfg.AllowMsgCounter = true;
    cfg.AllowDirect     = true;

    return js_AddStream(NULL, js, &cfg, NULL, NULL);
}

static natsStatus
_createGlobalStream(jsCtx *js)
{
    jsStreamConfig cfg;
    jsStreamSource s1, s2, s3;
    jsStreamSource *sources[3];

    jsStreamConfig_Init(&cfg);
    cfg.Name            = "GLOBAL";
    cfg.AllowMsgCounter = true;
    cfg.AllowDirect     = true;

    jsStreamSource_Init(&s1);
    s1.Name = "REGION_US_EAST";
    jsStreamSource_Init(&s2);
    s2.Name = "REGION_US_WEST";
    jsStreamSource_Init(&s3);
    s3.Name = "REGION_EU";

    sources[0]     = &s1;
    sources[1]     = &s2;
    sources[2]     = &s3;
    cfg.Sources    = sources;
    cfg.SourcesLen = 3;

    return js_AddStream(NULL, js, &cfg, NULL, NULL);
}

int main(int argc, char **argv)
{
    natsStatus           s           = NATS_OK;
    natsConnection      *nc          = NULL;
    jsCtx               *js          = NULL;
    natsCounter         *usEast      = NULL;
    natsCounter         *usWest      = NULL;
    natsCounter         *eu          = NULL;
    natsCounter         *global      = NULL;
    long long            value       = 0;
    const char          *url         = (argc > 1) ? argv[1] : URL;

    printf("=== Setting Up Regional Counters ===\n\n");

    s = natsConnection_ConnectTo(&nc, url);
    if (s != NATS_OK) goto done;

    s = natsConnection_JetStream(&js, nc, NULL);
    if (s != NATS_OK) goto done;

    // Create regional streams and the global aggregation stream.
    s = _createRegionalStream(js, "REGION_US_EAST", "us-east.>");
    if (s != NATS_OK) goto done;
    printf("Created US-East regional stream\n");

    s = _createRegionalStream(js, "REGION_US_WEST", "us-west.>");
    if (s != NATS_OK) goto done;
    printf("Created US-West regional stream\n");

    s = _createRegionalStream(js, "REGION_EU", "eu.>");
    if (s != NATS_OK) goto done;
    printf("Created EU regional stream\n");

    s = _createGlobalStream(js);
    if (s != NATS_OK) goto done;
    printf("Created global stream sourcing from all regions\n\n");

    // Get counters for each stream.
    s = natsCounter_GetFromStream(&usEast, js, nc, "REGION_US_EAST");
    if (s != NATS_OK) goto done;
    s = natsCounter_GetFromStream(&usWest, js, nc, "REGION_US_WEST");
    if (s != NATS_OK) goto done;
    s = natsCounter_GetFromStream(&eu, js, nc, "REGION_EU");
    if (s != NATS_OK) goto done;
    s = natsCounter_GetFromStream(&global, js, nc, "GLOBAL");
    if (s != NATS_OK) goto done;

    // Simulate regional traffic.
    printf("=== Simulating Regional Traffic ===\n\n");

    printf("US-East region:\n");
    s = natsCounter_Add(usEast, "us-east.api.requests", 1500, &value);
    if (s != NATS_OK) goto done;
    printf("  API requests: %lld\n", value);
    s = natsCounter_Add(usEast, "us-east.api.errors", 23, &value);
    if (s != NATS_OK) goto done;
    printf("  API errors:   %lld\n\n", value);

    printf("US-West region:\n");
    s = natsCounter_Add(usWest, "us-west.api.requests", 2100, &value);
    if (s != NATS_OK) goto done;
    printf("  API requests: %lld\n", value);
    s = natsCounter_Add(usWest, "us-west.api.errors", 15, &value);
    if (s != NATS_OK) goto done;
    printf("  API errors:   %lld\n\n", value);

    printf("EU region:\n");
    s = natsCounter_Add(eu, "eu.api.requests", 800, &value);
    if (s != NATS_OK) goto done;
    printf("  API requests: %lld\n", value);
    s = natsCounter_Add(eu, "eu.api.errors", 7, &value);
    if (s != NATS_OK) goto done;
    printf("  API errors:   %lld\n\n", value);

    // Allow time for source propagation before reading globals.
    nats_Sleep(500);

    printf("=== Global View (per-region entries) ===\n\n");

    s = _printEntryWithSources(global, "us-east.api.requests", "US-East API Requests:");
    if (s != NATS_OK) goto done;

    s = _printEntryWithSources(global, "us-west.api.requests", "US-West API Requests:");
    if (s != NATS_OK) goto done;

    s = _printEntryWithSources(global, "eu.api.requests", "EU API Requests:");
    if (s != NATS_OK) goto done;

done:
    natsCounter_Destroy(usEast);
    natsCounter_Destroy(usWest);
    natsCounter_Destroy(eu);
    natsCounter_Destroy(global);
    if (js != NULL) jsCtx_Destroy(js);
    if (nc != NULL) natsConnection_Destroy(nc);

    if (s != NATS_OK)
    {
        printf("Error: %s\n", natsStatus_GetText(s));
        return 1;
    }

    return 0;
}
