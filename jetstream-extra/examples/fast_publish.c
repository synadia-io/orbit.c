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

// Fast batch publish example.
//
// Streams a batch of messages into a JetStream stream using server-side
// flow control: instead of awaiting an ack per message, the server emits
// a coalesced flow ack every `Flow` messages and the publisher only
// stalls when too many flow-ack windows are outstanding. The batch is
// sealed with a single commit.
//
// Prerequisites:
//   A NATS server with JetStream enabled (nats-server -js).
//
// Usage:
//   jetstream-extra-fast_publish [url]

#include "fast_publish.h"
#include <stdio.h>
#include <string.h>

#define STREAM_NAME "ORDERS"
#define URL         "nats://localhost:4222"
#define BATCH_SIZE  1000

int
main(int argc, char **argv)
{
    natsStatus s = NATS_OK;
    natsConnection *nc = NULL;
    jsCtx *js = NULL;
    jsStreamConfig cfg;
    jsFastPublishCtx *fp = NULL;
    jsFastPubAck ack = { 0 };
    jsPubAck *pubAck = NULL;
    const char *url = (argc > 1) ? argv[1] : URL;
    int i;

    printf("=== JetStream fast publish example ===\n\n");

    s = natsConnection_ConnectTo(&nc, url);
    if (s != NATS_OK)
        goto done;

    s = natsConnection_JetStream(&js, nc, NULL);
    if (s != NATS_OK)
        goto done;

    // Fast publishing requires a stream created with AllowBatched
    // (idempotent — ignore "exists").
    jsStreamConfig_Init(&cfg);
    cfg.Name = STREAM_NAME;
    cfg.Subjects = (const char *[]){ "orders.>" };
    cfg.SubjectsLen = 1;
    cfg.AllowBatched = true;
    js_AddStream(NULL, js, &cfg, NULL, NULL);

    // A context with default flow control: one flow ack per 100 messages,
    // up to 2 outstanding ack windows before Add stalls.
    s = jsFastPublishCtx_Create(&fp, js, NULL);
    if (s != NATS_OK)
        goto done;

    // Stream BATCH_SIZE - 1 messages; the final one goes out with Commit.
    printf("Fast publishing %d messages...\n", BATCH_SIZE);
    for (i = 1; i < BATCH_SIZE; i++)
    {
        char subj[32], body[48];
        snprintf(subj, sizeof(subj), "orders.new.%d", i);
        snprintf(body, sizeof(body), "{\"order\":%d}", i);

        s = jsFastPublish_Add(&ack, fp, nc, subj, body, (int)strlen(body), NULL);
        if (s != NATS_OK)
            goto done;
    }
    printf("  last per-message ack: batchSeq=%llu confirmed up to %llu\n",
           (unsigned long long)ack.BatchSequence,
           (unsigned long long)ack.AckSequence);

    // Commit publishes the final message and seals the batch, blocking
    // until the server confirms. The context is closed afterwards.
    printf("\nCommitting batch...\n");
    s = jsFastPublish_Commit(&pubAck, fp, "orders.new.0",
                             "{\"order\":0}", 11, NULL, 5000);
    if (s != NATS_OK)
        goto done;

    printf("  committed: stream=%s sequence=%llu count=%llu\n",
           pubAck->Stream ? pubAck->Stream : "?",
           (unsigned long long)pubAck->Sequence,
           (unsigned long long)pubAck->Count);

done:
    jsPubAck_Destroy(pubAck);
    jsFastPublish_Destroy(fp);
    if (js != NULL)
        jsCtx_Destroy(js);
    if (nc != NULL)
        natsConnection_Destroy(nc);

    if (s != NATS_OK)
    {
        printf("\nError: %s\n", natsStatus_GetText(s));
        return 1;
    }

    printf("\n=== Done ===\n");
    return 0;
}
