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

// Sentinel example.
//
// A sentinel predicate ends the gather on a reply the caller recognises. Here a
// worker replies with data and then, after a short pause, an empty-payload
// "done" marker; the sentinel stops the gather on that marker. The marker
// message is inspected but not appended to the result list.
//
// Prerequisites:
//   A core NATS server (nats-server). JetStream is not required.
//
// Usage:
//   nats-extra-sentinel [url]

#include "requestmany.h"
#include <stdio.h>
#include <string.h>

#define SUBJECT "work"
#define URL     "nats://localhost:4222"

// Streams a few data replies, then an empty terminator.
static void
_worker(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    const char *reply = natsMsg_GetReply(msg);
    int i;
    (void)sub;
    (void)closure;

    for (i = 1; i <= 3; i++)
    {
        char body[32];
        snprintf(body, sizeof(body), "result-%d", i);
        natsConnection_PublishString(nc, reply, body);
    }
    // Empty-payload marker signalling "no more results".
    natsConnection_PublishString(nc, reply, "");
    natsMsg_Destroy(msg);
}

// Stop the gather on the first empty-payload reply.
static bool
_onReply(natsMsg *msg, void *closure)
{
    (void)closure;
    return natsMsg_GetDataLength(msg) == 0;
}

int
main(int argc, char **argv)
{
    natsStatus s = NATS_OK;
    natsConnection *nc = NULL;
    natsSubscription *sub = NULL;
    natsRequestManyOpts opts;
    natsMsgList list = { 0 };
    const char *url = (argc > 1) ? argv[1] : URL;
    int i;

    printf("=== Sentinel example ===\n\n");

    s = natsConnection_ConnectTo(&nc, url);
    if (s != NATS_OK)
        goto done;

    printf("Starting worker on \"%s\"...\n", SUBJECT);
    s = natsConnection_Subscribe(&sub, nc, SUBJECT, _worker, NULL);
    if (s != NATS_OK)
        goto done;

    // The sentinel is the stop condition; timeout is only a safety net.
    natsRequestManyOpts_Init(&opts);
    opts.Sentinel = _onReply;
    opts.Timeout  = 5000;

    printf("\nSending request...\n");
    s = natsRequestMany_Request(&list, nc, SUBJECT, NULL, 0, &opts);
    if (s != NATS_OK)
        goto done;

    // The terminator was consumed by the sentinel, so only data replies remain.
    printf("Gathered %d results (terminator discarded by sentinel):\n", list.Count);
    for (i = 0; i < list.Count; i++)
    {
        const char *data = natsMsg_GetData(list.Msgs[i]);
        int dlen = natsMsg_GetDataLength(list.Msgs[i]);
        printf("  %.*s\n", dlen, data ? data : "");
    }

done:
    natsMsgList_Destroy(&list);
    natsSubscription_Destroy(sub);
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
