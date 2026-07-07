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

// Basic request-many example.
//
// Starts a handful of responders on "help", then scatters one request and
// gathers every reply. The gather ends on the first stop condition met: the
// stall window (quiet period after the last reply), the count cap, or the
// overall timeout.
//
// Prerequisites:
//   A core NATS server (nats-server). JetStream is not required.
//
// Usage:
//   nats-extra-request_many [url]

#include "requestmany.h"
#include <stdio.h>
#include <string.h>

#define SUBJECT "help"
#define URL     "nats://localhost:4222"

#define N_RESPONDERS 5

// Each responder replies once with its identity.
static void
_responder(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    char reply[32];
    (void)sub;
    snprintf(reply, sizeof(reply), "responder-%d", *(int *)closure);
    natsConnection_PublishString(nc, natsMsg_GetReply(msg), reply);
    natsMsg_Destroy(msg);
}

int
main(int argc, char **argv)
{
    natsStatus s = NATS_OK;
    natsConnection *nc = NULL;
    natsSubscription *subs[N_RESPONDERS] = { 0 };
    int ids[N_RESPONDERS];
    natsRequestManyOpts opts;
    natsMsgList list = { 0 };
    const char *url = (argc > 1) ? argv[1] : URL;
    int i;

    printf("=== Request-many example ===\n\n");

    s = natsConnection_ConnectTo(&nc, url);
    if (s != NATS_OK)
        goto done;

    // Start the responders.
    printf("Starting %d responders on \"%s\"...\n", N_RESPONDERS, SUBJECT);
    for (i = 0; i < N_RESPONDERS && s == NATS_OK; i++)
    {
        ids[i] = i;
        s = natsConnection_Subscribe(&subs[i], nc, SUBJECT, _responder, &ids[i]);
    }
    if (s != NATS_OK)
        goto done;

    // Scatter one request and gather the replies.
    natsRequestManyOpts_Init(&opts);
    opts.Stall   = 100;  // stop 100 ms after the last reply arrives
    opts.Count   = 10;   // ...or once 10 replies are gathered
    opts.Timeout = 5000; // ...or after 5 s overall, whichever comes first

    printf("\nSending request...\n");
    s = natsRequestMany_Request(&list, nc, SUBJECT, NULL, 0, &opts);

    // NATS_OK means a stop condition ended the gather; NATS_TIMEOUT means only
    // the overall deadline was reached. Either way the replies are in `list`.
    if (s != NATS_OK && s != NATS_TIMEOUT)
        goto done;
    s = NATS_OK;

    printf("Received %d replies:\n", list.Count);
    for (i = 0; i < list.Count; i++)
    {
        const char *data = natsMsg_GetData(list.Msgs[i]);
        int dlen = natsMsg_GetDataLength(list.Msgs[i]);
        printf("  %.*s\n", dlen, data ? data : "");
    }

done:
    natsMsgList_Destroy(&list);
    for (i = 0; i < N_RESPONDERS; i++)
        natsSubscription_Destroy(subs[i]);
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
