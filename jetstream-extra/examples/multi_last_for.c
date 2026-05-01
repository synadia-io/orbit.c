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

// Multi-last-for example: get the last message on each of N subjects in
// a single round-trip.
//
// Prerequisites:
//   A NATS server with JetStream enabled (nats-server -js).
//
// Usage:
//   jetstream-extra-multi_last_for [url]

#include "batch_fetch.h"
#include <stdio.h>
#include <string.h>

#define STREAM_NAME "DEVICES"
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
    const char          *deviceSubjects[3] = {
        "device.A.state",
        "device.B.state",
        "device.C.state",
    };
    int i;

    printf("=== Multi-last-for example ===\n\n");

    s = natsConnection_ConnectTo(&nc, url);
    if (s != NATS_OK) goto done;

    s = natsConnection_JetStream(&js, nc, NULL);
    if (s != NATS_OK) goto done;

    jsStreamConfig_Init(&cfg);
    cfg.Name        = STREAM_NAME;
    cfg.Subjects    = (const char *[]){"device.>"};
    cfg.SubjectsLen = 1;
    cfg.AllowDirect = true;
    js_AddStream(NULL, js, &cfg, NULL, NULL);

    // Two state updates per device — only the latest should come back.
    printf("Publishing two state updates per device...\n");
    for (i = 0; i < 3; i++)
    {
        const char *subj = deviceSubjects[i];
        s = js_Publish(NULL, js, subj, "stale", 5, NULL, NULL);
        if (s == NATS_OK)
            s = js_Publish(NULL, js, subj, "fresh", 5, NULL, NULL);
        if (s != NATS_OK) goto done;
    }

    printf("\nFetching last message per subject...\n");
    jsBatchFetchOptions_Init(&opts);
    opts.MultiLastFor    = deviceSubjects;
    opts.MultiLastForLen = 3;

    s = jsBatchFetch(&list, nc, STREAM_NAME, NULL, &opts, 5000, NULL);
    if (s != NATS_OK) goto done;

    printf("Received %d messages:\n", list.Count);
    for (i = 0; i < list.Count; i++)
    {
        const char *origSubj = NULL;
        const char *data     = natsMsg_GetData(list.Msgs[i]);
        int         dlen     = natsMsg_GetDataLength(list.Msgs[i]);

        natsMsgHeader_Get(list.Msgs[i], JSSubject, &origSubj);

        printf("  %-22s -> %.*s\n",
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
