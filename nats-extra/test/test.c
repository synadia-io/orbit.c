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

#include "requestmany.h"

#include "test.h"

#define N_HELLO_RESPONDERS 5
#define N_RESPONDERS       (N_HELLO_RESPONDERS + 1) // + sentinel

// Replies immediately, so all five arrive well within any stall window.
static void
_helloResponder(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    (void)sub;
    (void)closure;
    natsConnection_PublishString(nc, natsMsg_GetReply(msg), "hello");
    natsMsg_Destroy(msg);
}

// Replies with an empty payload after a delay, so it always arrives last and
// acts as the sentinel / stall trigger.
static void
_sentinelResponder(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    (void)sub;
    (void)closure;
    nats_Sleep(100);
    natsConnection_PublishString(nc, natsMsg_GetReply(msg), "");
    natsMsg_Destroy(msg);
}

static natsStatus
_startResponders(natsConnection *nc, natsSubscription **subs)
{
    natsStatus s = NATS_OK;
    int i;

    for (i = 0; i < N_HELLO_RESPONDERS && s == NATS_OK; i++)
        s = natsConnection_Subscribe(&subs[i], nc, "foo", _helloResponder, NULL);
    if (s == NATS_OK)
        s = natsConnection_Subscribe(&subs[N_HELLO_RESPONDERS], nc, "foo", _sentinelResponder, NULL);

    return s;
}

static void
_stopResponders(natsSubscription **subs)
{
    int i;

    // Let in-flight handlers finish before tearing down the connection. A
    // gather that stops early (count/stall) returns while responders — notably
    // the sentinel, which sleeps 100ms — are still holding their request
    // message; draining lets them publish and destroy it. Any late reply lands
    // on the already-closed inbox and is dropped server-side.
    nats_Sleep(150);

    for (i = 0; i < N_RESPONDERS; i++)
        natsSubscription_Destroy(subs[i]);
}

// Echoes the request's X-Req header back as the reply payload, so a matching
// reply proves the header crossed the wire.
static void
_echoHeaderResponder(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
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

static bool
_emptySentinel(natsMsg *msg, void *closure)
{
    (void)closure;
    return natsMsg_GetDataLength(msg) == 0;
}

// Echoes the request's payload back, so a gather can assert the request body
// crossed the wire.
static void
_echoDataResponder(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    (void)sub;
    (void)closure;
    natsConnection_Publish(nc, natsMsg_GetReply(msg),
                           natsMsg_GetData(msg), natsMsg_GetDataLength(msg));
    natsMsg_Destroy(msg);
}

//=============================================================================
// Validation tests — no server needed.
//=============================================================================

void
test_RequestManyArgs(void)
{
    natsRequestManyOpts opts;
    natsMsgList list = { 0 };
    natsConnection *nc = (natsConnection *)0x1; // never dereferenced on these paths
    natsStatus s;

    test("Init returns NATS_INVALID_ARG for NULL: ");
    s = natsRequestManyOpts_Init(NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Init zeros all fields: ");
    memset(&opts, 0xff, sizeof(opts));
    s = natsRequestManyOpts_Init(&opts);
    testCond((s == NATS_OK)
             && (opts.Stall == 0)
             && (opts.Count == 0)
             && (opts.Sentinel == NULL)
             && (opts.SentinelClosure == NULL)
             && (opts.Timeout == 0));

    natsRequestManyOpts_Init(&opts);

    test("NULL list rejected: ");
    s = natsRequestMany_Request(NULL, nc, "foo", NULL, 0, &opts);
    testCond(s == NATS_INVALID_ARG);

    test("NULL connection rejected: ");
    s = natsRequestMany_Request(&list, NULL, "foo", NULL, 0, &opts);
    testCond(s == NATS_INVALID_ARG);

    test("NULL subject rejected: ");
    s = natsRequestMany_Request(&list, nc, NULL, NULL, 0, &opts);
    testCond(s == NATS_INVALID_ARG);

    test("NULL opts rejected: ");
    s = natsRequestMany_Request(&list, nc, "foo", NULL, 0, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("NULL msg rejected: ");
    s = natsRequestMany_RequestMsg(&list, nc, NULL, &opts);
    testCond(s == NATS_INVALID_ARG);
}

//=============================================================================
// Integration tests — require a nats-server.
//=============================================================================

// No stop condition but the overall timeout: gather every reply (five "hello"
// plus the empty sentinel, which is NOT filtered without a sentinel predicate)
// and report NATS_TIMEOUT because the deadline is what ended the gather.
void
test_RequestManyOverallTimeout(void)
{
    natsRequestManyOpts opts;
    natsMsgList list = { 0 };
    natsSubscription *subs[N_RESPONDERS] = { 0 };

    CORE_SETUP;

    test("Start responders: ");
    s = _startResponders(nc, subs);
    testCond(s == NATS_OK);

    test("Overall timeout gathers all replies: ");
    natsRequestManyOpts_Init(&opts);
    opts.Timeout = 300; // must exceed the sentinel's 100ms delay
    s = natsRequestMany_Request(&list, nc, "foo", NULL, 0, &opts);
    testCond((s == NATS_TIMEOUT) && (list.Count == N_RESPONDERS));

    natsMsgList_Destroy(&list);
    _stopResponders(subs);
    CORE_TEARDOWN;
}

// Stall shorter than the sentinel delay: the five instant replies arrive, then
// the stall window elapses before the sentinel, ending cleanly with 5 messages.
void
test_RequestManyStall(void)
{
    natsRequestManyOpts opts;
    natsMsgList list = { 0 };
    natsSubscription *subs[N_RESPONDERS] = { 0 };

    CORE_SETUP;

    test("Start responders: ");
    s = _startResponders(nc, subs);
    testCond(s == NATS_OK);

    test("Stall short-circuits after the burst: ");
    natsRequestManyOpts_Init(&opts);
    opts.Stall = 50;     // < the sentinel's 100ms delay
    opts.Timeout = 5000; // large; the stall must be what ends the gather
    s = natsRequestMany_Request(&list, nc, "foo", NULL, 0, &opts);
    testCond((s == NATS_OK) && (list.Count == N_HELLO_RESPONDERS));

    natsMsgList_Destroy(&list);
    _stopResponders(subs);
    CORE_TEARDOWN;
}

// Count cap stops the gather early.
void
test_RequestManyCount(void)
{
    natsRequestManyOpts opts;
    natsMsgList list = { 0 };
    natsSubscription *subs[N_RESPONDERS] = { 0 };

    CORE_SETUP;

    test("Start responders: ");
    s = _startResponders(nc, subs);
    testCond(s == NATS_OK);

    test("Count cap stops early: ");
    natsRequestManyOpts_Init(&opts);
    opts.Count = 3;
    opts.Timeout = 5000;
    s = natsRequestMany_Request(&list, nc, "foo", NULL, 0, &opts);
    testCond((s == NATS_OK) && (list.Count == 3));

    natsMsgList_Destroy(&list);
    _stopResponders(subs);
    CORE_TEARDOWN;
}

// Sentinel: the empty-payload reply ends the gather and is itself discarded, so
// only the five "hello" replies land in the list.
void
test_RequestManySentinel(void)
{
    natsRequestManyOpts opts;
    natsMsgList list = { 0 };
    natsSubscription *subs[N_RESPONDERS] = { 0 };

    CORE_SETUP;

    test("Start responders: ");
    s = _startResponders(nc, subs);
    testCond(s == NATS_OK);

    test("Sentinel ends gather and is discarded: ");
    natsRequestManyOpts_Init(&opts);
    opts.Sentinel = _emptySentinel;
    opts.Timeout = 5000;
    s = natsRequestMany_Request(&list, nc, "foo", NULL, 0, &opts);
    testCond((s == NATS_OK) && (list.Count == N_HELLO_RESPONDERS));

    natsMsgList_Destroy(&list);
    _stopResponders(subs);
    CORE_TEARDOWN;
}

// The msg form must carry the caller's request headers to responders.
void
test_RequestManyMsgHeaders(void)
{
    natsRequestManyOpts opts;
    natsMsgList list = { 0 };
    natsSubscription *sub = NULL;
    natsMsg *req = NULL;
    const char *data = NULL;
    int dataLen = 0;

    CORE_SETUP;

    test("Subscribe echo responder: ");
    s = natsConnection_Subscribe(&sub, nc, "foo", _echoHeaderResponder, NULL);
    testCond(s == NATS_OK);

    test("Build request with a custom header: ");
    s = natsMsg_Create(&req, "foo", NULL, "ping", 4);
    if (s == NATS_OK)
        s = natsMsgHeader_Set(req, "X-Req", "orbit");
    testCond(s == NATS_OK);

    test("RequestManyMsg delivers the header to the responder: ");
    natsRequestManyOpts_Init(&opts);
    opts.Count = 1;
    opts.Timeout = 2000;
    s = natsRequestMany_RequestMsg(&list, nc, req, &opts);
    if ((s == NATS_OK) && (list.Count == 1))
    {
        data = natsMsg_GetData(list.Msgs[0]);
        dataLen = natsMsg_GetDataLength(list.Msgs[0]);
    }
    testCond((s == NATS_OK) && (list.Count == 1) && (data != NULL)
             && (dataLen == 5) && (memcmp(data, "orbit", 5) == 0));

    natsMsg_Destroy(req);
    natsMsgList_Destroy(&list);
    natsSubscription_Destroy(sub);
    CORE_TEARDOWN;
}

// A zero timeout selects the built-in default rather than an immediate
// deadline: the gather must proceed and stop on the count, not time out at 0.
void
test_RequestManyDefaultTimeout(void)
{
    natsRequestManyOpts opts;
    natsMsgList list = { 0 };
    natsSubscription *subs[N_RESPONDERS] = { 0 };

    CORE_SETUP;

    test("Start responders: ");
    s = _startResponders(nc, subs);
    testCond(s == NATS_OK);

    test("Zero timeout falls back to the default, not an immediate deadline: ");
    natsRequestManyOpts_Init(&opts);
    opts.Count = 3;   // ends the gather
    opts.Timeout = 0; // 0 must mean "use the default", not "expire now"
    s = natsRequestMany_Request(&list, nc, "foo", NULL, 0, &opts);
    testCond((s == NATS_OK) && (list.Count == 3));

    natsMsgList_Destroy(&list);
    _stopResponders(subs);
    CORE_TEARDOWN;
}

// The request payload must reach responders: an echo responder mirrors the body
// back, so a matching reply proves the data crossed the wire.
void
test_RequestManyRequestData(void)
{
    natsRequestManyOpts opts;
    natsMsgList list = { 0 };
    natsSubscription *sub = NULL;
    const char *data = NULL;
    int dataLen = 0;

    CORE_SETUP;

    test("Subscribe echo-data responder: ");
    s = natsConnection_Subscribe(&sub, nc, "foo", _echoDataResponder, NULL);
    testCond(s == NATS_OK);

    test("Request delivers the payload to the responder: ");
    natsRequestManyOpts_Init(&opts);
    opts.Count = 1;
    opts.Timeout = 2000;
    s = natsRequestMany_Request(&list, nc, "foo", "ping", 4, &opts);
    if ((s == NATS_OK) && (list.Count == 1))
    {
        data = natsMsg_GetData(list.Msgs[0]);
        dataLen = natsMsg_GetDataLength(list.Msgs[0]);
    }
    testCond((s == NATS_OK) && (list.Count == 1) && (data != NULL)
             && (dataLen == 4) && (memcmp(data, "ping", 4) == 0));

    natsMsgList_Destroy(&list);
    natsSubscription_Destroy(sub);
    CORE_TEARDOWN;
}

// A count larger than the number of replies never trips: another stop condition
// (here the stall) must end the gather with whatever arrived.
void
test_RequestManyCountExceedsReplies(void)
{
    natsRequestManyOpts opts;
    natsMsgList list = { 0 };
    natsSubscription *subs[N_RESPONDERS] = { 0 };

    CORE_SETUP;

    test("Start responders: ");
    s = _startResponders(nc, subs);
    testCond(s == NATS_OK);

    test("Count above the reply total falls through to the stall: ");
    natsRequestManyOpts_Init(&opts);
    opts.Count = 1000;   // far more than the responders will ever send
    opts.Stall = 50;     // < the sentinel's 100ms delay, so it ends the burst
    opts.Timeout = 5000;
    s = natsRequestMany_Request(&list, nc, "foo", NULL, 0, &opts);
    testCond((s == NATS_OK) && (list.Count == N_HELLO_RESPONDERS));

    natsMsgList_Destroy(&list);
    _stopResponders(subs);
    CORE_TEARDOWN;
}

//=============================================================================
// main
//=============================================================================

int
main(int argc, char **argv)
{
    return testMain(argc, argv);
}
