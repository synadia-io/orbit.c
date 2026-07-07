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

// Request-with-headers example.
//
// To attach custom headers to the request, build the message yourself and use
// natsRequestMany_RequestMsg. The library publishes a copy of the message whose
// reply subject is its private inbox, leaving your message untouched. Here a
// responder echoes the request's X-Req header back in its reply, proving the
// header crossed the wire.
//
// Prerequisites:
//   A core NATS server (nats-server). JetStream is not required.
//
// Usage:
//   nats-extra-request_msg [url]

#include "requestmany.h"
#include <stdio.h>
#include <string.h>

#define SUBJECT "echo"
#define URL     "nats://localhost:4222"

// Echoes the request's X-Req header value back as the reply payload.
static void
_echoResponder(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    const char *v = NULL;
    (void)sub;
    (void)closure;
    if ((natsMsgHeader_Get(msg, "X-Req", &v) == NATS_OK) && (v != NULL))
        natsConnection_PublishString(nc, natsMsg_GetReply(msg), v);
    else
        natsConnection_PublishString(nc, natsMsg_GetReply(msg), "NOHEADER");
    natsMsg_Destroy(msg);
}

int
main(int argc, char **argv)
{
    natsStatus s = NATS_OK;
    natsConnection *nc = NULL;
    natsSubscription *sub = NULL;
    natsMsg *req = NULL;
    natsRequestManyOpts opts;
    natsMsgList list = { 0 };
    const char *url = (argc > 1) ? argv[1] : URL;
    int i;

    printf("=== Request-with-headers example ===\n\n");

    s = natsConnection_ConnectTo(&nc, url);
    if (s != NATS_OK)
        goto done;

    printf("Starting echo responder on \"%s\"...\n", SUBJECT);
    s = natsConnection_Subscribe(&sub, nc, SUBJECT, _echoResponder, NULL);
    if (s != NATS_OK)
        goto done;

    // Build the request message and attach a custom header.
    s = natsMsg_Create(&req, SUBJECT, NULL, "ping", 4);
    if (s == NATS_OK)
        s = natsMsgHeader_Set(req, "X-Req", "orbit");
    if (s != NATS_OK)
        goto done;

    // Single responder, so cap the gather at one reply.
    natsRequestManyOpts_Init(&opts);
    opts.count   = 1;
    opts.timeout = 5000;

    printf("\nSending request with header X-Req: orbit\n");
    s = natsRequestMany_RequestMsg(&list, nc, req, &opts);
    if (s != NATS_OK)
        goto done;

    printf("Received %d reply:\n", list.Count);
    for (i = 0; i < list.Count; i++)
    {
        const char *data = natsMsg_GetData(list.Msgs[i]);
        int dlen = natsMsg_GetDataLength(list.Msgs[i]);
        printf("  responder echoed: %.*s\n", dlen, data ? data : "");
    }

done:
    natsMsg_Destroy(req);
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
