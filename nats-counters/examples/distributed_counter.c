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

// Distributed counter with source tracking example.
//
// Demonstrates the multi-tier aggregation pattern from ADR-49: regional
// counters are sourced into a global aggregation stream.  The global stream
// maintains Nats-Counter-Sources headers so the contribution of each region
// can be inspected at read time.
//
// Stream topology:
//   METRICS_US_EAST  ──┐
//   METRICS_US_WEST  ──┼──► METRICS_GLOBAL
//   METRICS_EU       ──┘
//
// Prerequisites:
//   A NATS server with JetStream enabled (nats-server -js).
//
// Usage:
//   nats-counters-distributed_counter [url]

#include "nats_counters.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define URL "nats://localhost:4222"

// Source breakdown callback closure
typedef struct { long long total; } SourcePrintCtx;

static void
_printSource(const char *stream, const char *subject, const char *value,
             void *closure)
{
    // Map stream name to a human-readable region label.
    const char *region = stream;
    if (strcmp(stream, "METRICS_US_EAST") == 0) region = "US-East";
    else if (strcmp(stream, "METRICS_US_WEST") == 0) region = "US-West";
    else if (strcmp(stream, "METRICS_EU") == 0)      region = "EU";
    else if (strcmp(stream, "METRICS_GLOBAL") == 0)  region = "Global";

    printf("    %-10s: %s  (from %s)\n", region, value, subject);
}

static natsStatus
_createRegionalStream(jsCtx *js, const char *name, const char *subjectFilter)
{
    // TODO: Build jsStreamConfig with:
    //   Name            = name
    //   Subjects        = [subjectFilter]
    //   AllowMsgCounter = true
    //   AllowDirect     = true
    // Call js_AddStream() or js_UpdateStream() (ignore already-exists error).
    (void)js;
    (void)name;
    (void)subjectFilter;
    return NATS_ERR;
}

static natsStatus
_createGlobalStream(jsCtx *js)
{
    // TODO: Build jsStreamConfig with:
    //   Name            = "METRICS_GLOBAL"
    //   Subjects        = ["metrics.global.>"]
    //   AllowMsgCounter = true
    //   AllowDirect     = true
    //   Sources         = [
    //     { Name: "METRICS_US_EAST",
    //       SubjectTransforms: [{ Src: "metrics.us-east.>", Dest: "metrics.global.>" }] },
    //     { Name: "METRICS_US_WEST",
    //       SubjectTransforms: [{ Src: "metrics.us-west.>", Dest: "metrics.global.>" }] },
    //     { Name: "METRICS_EU",
    //       SubjectTransforms: [{ Src: "metrics.eu.>",      Dest: "metrics.global.>" }] },
    //   ]
    (void)js;
    return NATS_ERR;
}

int main(int argc, char **argv)
{
    natsStatus           ns          = NATS_OK;
    orbitCountersStatus  s           = ORBIT_COUNTERS_OK;
    natsConnection      *nc          = NULL;
    jsCtx               *js          = NULL;
    natsCounter         *usEast      = NULL;
    natsCounter         *usWest      = NULL;
    natsCounter         *eu          = NULL;
    natsCounter         *global      = NULL;
    natsCounterEntry    *entry       = NULL;
    long long            value       = 0;
    const char          *url         = (argc > 1) ? argv[1] : URL;

    printf("=== Setting Up Regional Counters ===\n\n");

    ns = natsConnection_ConnectTo(&nc, url);
    if (ns != NATS_OK) { printf("Connect error: %s\n", natsStatus_GetText(ns)); return 1; }

    ns = natsConnection_JetStream(&js, nc, NULL);
    if (ns != NATS_OK) { printf("JetStream error: %s\n", natsStatus_GetText(ns)); goto done; }

    // Create regional streams and the global aggregation stream.
    ns = _createRegionalStream(js, "METRICS_US_EAST", "metrics.us-east.>");
    if (ns != NATS_OK) goto done;
    printf("Created US-East metrics stream\n");

    ns = _createRegionalStream(js, "METRICS_US_WEST", "metrics.us-west.>");
    if (ns != NATS_OK) goto done;
    printf("Created US-West metrics stream\n");

    ns = _createRegionalStream(js, "METRICS_EU", "metrics.eu.>");
    if (ns != NATS_OK) goto done;
    printf("Created EU metrics stream\n");

    ns = _createGlobalStream(js);
    if (ns != NATS_OK) goto done;
    printf("Created Global aggregation stream with sources from all regions\n\n");

    // Get counters for each stream.
    s = natsCounter_GetFromStream(&usEast, js, "METRICS_US_EAST");
    if (s != ORBIT_COUNTERS_OK) goto done;
    s = natsCounter_GetFromStream(&usWest, js, "METRICS_US_WEST");
    if (s != ORBIT_COUNTERS_OK) goto done;
    s = natsCounter_GetFromStream(&eu, js, "METRICS_EU");
    if (s != ORBIT_COUNTERS_OK) goto done;
    s = natsCounter_GetFromStream(&global, js, "METRICS_GLOBAL");
    if (s != ORBIT_COUNTERS_OK) goto done;

    // Simulate regional traffic.
    printf("=== Simulating Regional Traffic ===\n\n");

    printf("US-East region:\n");
    s = natsCounter_Add(usEast, "metrics.us-east.api.requests", 1500, &value);
    if (s != ORBIT_COUNTERS_OK) goto done;
    printf("  API requests: %lld\n", value);
    s = natsCounter_Add(usEast, "metrics.us-east.api.errors", 23, &value);
    if (s != ORBIT_COUNTERS_OK) goto done;
    printf("  API errors:   %lld\n", value);
    s = natsCounter_Add(usEast, "metrics.us-east.db.queries", 3200, &value);
    if (s != ORBIT_COUNTERS_OK) goto done;
    printf("  DB queries:   %lld\n\n", value);

    printf("US-West region:\n");
    s = natsCounter_Add(usWest, "metrics.us-west.api.requests", 2100, &value);
    if (s != ORBIT_COUNTERS_OK) goto done;
    printf("  API requests: %lld\n", value);
    s = natsCounter_Add(usWest, "metrics.us-west.api.errors", 15, &value);
    if (s != ORBIT_COUNTERS_OK) goto done;
    printf("  API errors:   %lld\n", value);
    s = natsCounter_Add(usWest, "metrics.us-west.db.queries", 4500, &value);
    if (s != ORBIT_COUNTERS_OK) goto done;
    printf("  DB queries:   %lld\n\n", value);

    printf("EU region:\n");
    s = natsCounter_Add(eu, "metrics.eu.api.requests", 800, &value);
    if (s != ORBIT_COUNTERS_OK) goto done;
    printf("  API requests: %lld\n", value);
    s = natsCounter_Add(eu, "metrics.eu.api.errors", 7, &value);
    if (s != ORBIT_COUNTERS_OK) goto done;
    printf("  API errors:   %lld\n", value);
    s = natsCounter_Add(eu, "metrics.eu.db.queries", 1600, &value);
    if (s != ORBIT_COUNTERS_OK) goto done;
    printf("  DB queries:   %lld\n\n", value);

    // TODO: nats_Sleep(500) to allow source propagation before reading globals.

    printf("=== Global Aggregated View ===\n\n");

    s = natsCounter_Get(global, "metrics.global.api.requests", &entry);
    if (s != ORBIT_COUNTERS_OK) goto done;
    printf("Total API Requests: %s (aggregated)\n", natsCounterEntry_ValueStr(entry));
    natsCounterEntry_Destroy(entry); entry = NULL;

    s = natsCounter_Get(global, "metrics.global.api.errors", &entry);
    if (s != ORBIT_COUNTERS_OK) goto done;
    printf("Total API Errors:   %s (aggregated)\n", natsCounterEntry_ValueStr(entry));
    natsCounterEntry_Destroy(entry); entry = NULL;

    s = natsCounter_Get(global, "metrics.global.db.queries", &entry);
    if (s != ORBIT_COUNTERS_OK) goto done;
    printf("Total DB Queries:   %s (aggregated)\n", natsCounterEntry_ValueStr(entry));
    natsCounterEntry_Destroy(entry); entry = NULL;

    // Show source breakdown for API requests.
    printf("\n=== Source Breakdown ===\n\n");

    s = natsCounter_Get(global, "metrics.global.api.requests", &entry);
    if (s != ORBIT_COUNTERS_OK) goto done;

    printf("API Requests by region:\n");
    if (natsCounterEntry_HasSources(entry))
        natsCounterEntry_IterSources(entry, _printSource, NULL);
    else
        printf("  (no source data — stream sourcing may not have propagated yet)\n");

    natsCounterEntry_Destroy(entry); entry = NULL;

done:
    natsCounterEntry_Destroy(entry);
    natsCounter_Destroy(usEast);
    natsCounter_Destroy(usWest);
    natsCounter_Destroy(eu);
    natsCounter_Destroy(global);
    if (js != NULL) jsCtx_Destroy(js);
    if (nc != NULL) natsConnection_Destroy(nc);

    if (s != ORBIT_COUNTERS_OK)
    {
        printf("Error: %s\n", orbitCountersStatus_GetText(s));
        return 1;
    }
    if (ns != NATS_OK)
    {
        printf("Error: %s\n", natsStatus_GetText(ns));
        return 1;
    }

    return 0;
}
