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

// Synchronous batch fetch example.
//
// Creates a JetStream stream with AllowDirect, publishes a few messages,
// then retrieves them in a single DIRECT.GET batch.
//
// Prerequisites:
//   A NATS server with JetStream enabled (nats-server -js).
//
// Usage:
//   jetstream-extra-batch_fetch [url]

#include "batch_fetch.h"
#include <stdio.h>
#include <string.h>

#define STREAM_NAME "EVENTS"
#define URL         "nats://localhost:4222"

int main(int argc, char **argv)
{
    natsStatus           s    = NATS_OK;
    natsConnection      *nc   = NULL;
    jsCtx               *js   = NULL;
    jsStreamConfig       cfg;
    jsBatchFetchOptions  opts;
    natsMsgList          list = {0};
    const char          *url  = (argc > 1) ? argv[1] : URL;
    int                  i;

    printf("=== JetStream batch fetch example ===\n\n");

    s = natsConnection_ConnectTo(&nc, url);
    if (s != NATS_OK) goto done;

    s = natsConnection_JetStream(&js, nc, NULL);
    if (s != NATS_OK) goto done;

    // Create the stream with AllowDirect (idempotent — ignore "exists").
    jsStreamConfig_Init(&cfg);
    cfg.Name        = STREAM_NAME;
    cfg.Subjects    = (const char *[]){"events.>"};
    cfg.SubjectsLen = 1;
    cfg.AllowDirect = true;
    js_AddStream(NULL, js, &cfg, NULL, NULL);

    // Publish a few messages.
    printf("Publishing 5 events...\n");
    for (i = 1; i <= 5; i++)
    {
        char subj[32], body[32];
        snprintf(subj, sizeof(subj), "events.user.%d", i);
        snprintf(body, sizeof(body), "payload-%d", i);
        s = js_Publish(NULL, js, subj, body, (int)strlen(body), NULL, NULL);
        if (s != NATS_OK) goto done;
    }

    // Fetch them all in a single batch.
    printf("\nFetching batch...\n");
    jsBatchFetchOptions_Init(&opts);
    opts.Batch = 100;

    s = jsBatchFetch(&list, nc, STREAM_NAME, NULL, &opts, 5000, NULL);
    if (s != NATS_OK) goto done;

    printf("Received %d messages:\n", list.Count);
    for (i = 0; i < list.Count; i++)
    {
        const char *origSubj = NULL;
        const char *seq      = NULL;
        const char *data     = natsMsg_GetData(list.Msgs[i]);
        int         dlen     = natsMsg_GetDataLength(list.Msgs[i]);

        natsMsgHeader_Get(list.Msgs[i], JSSubject,  &origSubj);
        natsMsgHeader_Get(list.Msgs[i], JSSequence, &seq);

        printf("  seq=%-3s subject=%-22s data=%.*s\n",
               seq      ? seq      : "?",
               origSubj ? origSubj : "?",
               dlen, data ? data : "");
    }

done:
    natsMsgList_Destroy(&list);
    if (js != NULL) jsCtx_Destroy(js);
    if (nc != NULL) natsConnection_Destroy(nc);

    if (s != NATS_OK)
    {
        printf("\nError: %s\n", natsStatus_GetText(s));
        return 1;
    }

    printf("\n=== Done ===\n");
    return 0;
}
