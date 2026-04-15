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

// Batch counter operations example.
//
// Demonstrates fetching multiple counter values in a single batch
// request using natsCounter_GetMultiple().
//
// Prerequisites:
//   A NATS server with JetStream enabled (nats-server -js).
//
// Usage:
//   nats-counters-batch_operations [url]

#include "nats_counters.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define STREAM_NAME "METRICS"
#define URL         "nats://localhost:4222"

// Closure passed to the batch callback to accumulate results.
typedef struct
{
    int         count;
    natsStatus  lastError;
    bool        done;
} BatchCtx;

static void
_onEntry(natsCounter *counter, natsCounterEntry *entry,
         natsStatus status, void *closure)
{
    BatchCtx *ctx = (BatchCtx *)closure;

    if (status != NATS_OK)
    {
        ctx->lastError = status;
        ctx->done      = true;
        return;
    }

    // NULL entry signals end of batch.
    if (entry == NULL)
    {
        ctx->done = true;
        return;
    }

    printf("  %-40s = %s", natsCounterEntry_Subject(entry),
           natsCounterEntry_ValueStr(entry));

    if (natsCounterEntry_HasIncrement(entry))
        printf("  (last incr: %s)", natsCounterEntry_IncrementStr(entry));

    printf("\n");
    ctx->count++;
}

int main(int argc, char **argv)
{
    natsStatus       s       = NATS_OK;
    natsConnection  *nc      = NULL;
    jsCtx           *js      = NULL;
    natsCounter     *counter = NULL;
    long long        value   = 0;
    BatchCtx         ctx     = {0};
    const char      *url     = (argc > 1) ? argv[1] : URL;

    static const char *SERVICES[] = { "auth", "api", "db", "cache" };
    static const char *METRICS[]  = { "requests", "errors", "latency_ms" };
    static const int   N_SVC      = 4;
    static const int   N_MET      = 3;

    // Build subject arrays
    int         numSubjects  = N_SVC * N_MET;
    const char **subjects    = calloc(numSubjects, sizeof(char *));
    char        **subjectBuf = calloc(numSubjects, sizeof(char *));
    int          i, j, idx  = 0;

    if (subjects == NULL || subjectBuf == NULL)
    {
        s = NATS_NO_MEMORY;
        goto done;
    }

    printf("=== Adding Counter Values ===\n\n");

    s = natsConnection_ConnectTo(&nc, url);
    if (s != NATS_OK) { printf("Connect error: %s\n", natsStatus_GetText(s)); return 1; }

    s = natsConnection_JetStream(&js, nc, NULL);
    if (s != NATS_OK) { printf("JetStream error: %s\n", natsStatus_GetText(s)); goto done; }

    // TODO: Create or bind the stream (AllowMsgCounter + AllowDirect).

    s = natsCounter_GetFromStream(&counter, js, STREAM_NAME);
    if (s != NATS_OK) goto done;

    // Populate counters
    for (i = 0; i < N_SVC; i++)
    {
        for (j = 0; j < N_MET; j++)
        {
            char  buf[64];
            long long val;

            snprintf(buf, sizeof(buf), "metrics.%s.%s", SERVICES[i], METRICS[j]);

            if (strcmp(METRICS[j], "requests") == 0)
                val = 100 + (long long)(strlen(SERVICES[i]) * 10);
            else if (strcmp(METRICS[j], "errors") == 0)
                val = (long long)strlen(SERVICES[i]);
            else
                val = 50 + (long long)(strlen(SERVICES[i]) * 5);

            s = natsCounter_Add(counter, buf, val, &value);
            if (s != NATS_OK) goto done;
            printf("  %s = %lld\n", buf, value);

            subjectBuf[idx] = strdup(buf);
            subjects[idx]   = subjectBuf[idx];
            idx++;
        }
    }

    // Batch fetch all counters
    printf("\n=== Fetching Multiple Counters (Batch) ===\n\n");
    printf("Fetched %d subjects in batch:\n\n", numSubjects);

    ctx.count = 0;
    ctx.done  = false;
    s = natsCounter_GetMultiple(counter, subjects, numSubjects, _onEntry, &ctx);
    if (s != NATS_OK) goto done;

    // TODO: In a real implementation natsCounter_GetMultiple() may be
    //       asynchronous; wait for ctx.done before proceeding.

    if (ctx.lastError != NATS_OK)
    {
        s = ctx.lastError;
        goto done;
    }

    printf("\nFetched %d entries.\n", ctx.count);

done:
    for (i = 0; i < numSubjects; i++)
        free(subjectBuf[i]);
    free(subjectBuf);
    free(subjects);

    natsCounter_Destroy(counter);
    if (js != NULL) jsCtx_Destroy(js);
    if (nc != NULL) natsConnection_Destroy(nc);

    if (s != NATS_OK)
    {
        printf("Error: %s\n", natsStatus_GetText(s));
        return 1;
    }

    return 0;
}
