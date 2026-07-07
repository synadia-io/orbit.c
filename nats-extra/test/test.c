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

#include <nats/nats.h>
#include "requestmany.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

// Test framework — same as jetstream-extra / nats-counters.

typedef void (*testFunc)(void);

typedef struct
{
    const char *name;
    testFunc func;
} testInfo;

#define _TEST_PROTO
#include "list.h"
#undef _TEST_PROTO

#define _TEST_LIST
static testInfo allTests[] = {
#include "list.h"
};
#undef _TEST_LIST

static int tests = 0;
static bool failed = false;

static const char *natsServerExe = "nats-server";
static bool keepServerOutput = false;

#define NATS_INVALID_PID (-1)
#define LOGFILE_NAME     "server.log"

#define FAIL(m)                    \
    {                              \
        printf("@@ %s @@\n", (m)); \
        failed = true;             \
        return;                    \
    }

#define CHECK_SERVER_STARTED(p)  \
    if ((p) == NATS_INVALID_PID) \
    FAIL("Unable to start or verify that the server was started!")

#define test(s)                    \
    {                              \
        printf("#%02d ", ++tests); \
        printf("%s", (s));         \
        fflush(stdout);            \
    }
#define testCond(c)                            \
    if (c)                                     \
    {                                          \
        printf("\033[0;32mPASSED\033[0;0m\n"); \
        fflush(stdout);                        \
    }                                          \
    else                                       \
    {                                          \
        printf("\033[0;31mFAILED\033[0;0m\n"); \
        fflush(stdout);                        \
        failed = true;                         \
        return;                                \
    }

// Server lifecycle

typedef pid_t natsPid;

static natsPid g_serverPid = NATS_INVALID_PID;

static void
_stopServer(natsPid pid)
{
    int status = 0;
    if (pid == NATS_INVALID_PID)
        return;
    if (kill(pid, SIGINT) < 0)
    {
        if (kill(pid, SIGKILL) < 0)
            return;
    }
    waitpid(pid, &status, 0);
    if (pid == g_serverPid)
        g_serverPid = NATS_INVALID_PID;
}

static natsStatus
_checkStart(const char *url, int maxAttempts)
{
    natsConnection *nc = NULL;
    natsStatus s = NATS_OK;
    int attempts = 0;

    while ((s = natsConnection_ConnectTo(&nc, url)) != NATS_OK && attempts++ < maxAttempts)
    {
        usleep(200 * 1000);
    }

    if (nc != NULL)
        natsConnection_Destroy(nc);
    return s;
}

static natsPid
_startServer(const char *url, const char *cmdLineOpts, bool checkStart)
{
    natsPid pid = fork();
    if (pid == -1)
        return NATS_INVALID_PID;

    if (pid == 0)
    {
        char combined[2048];
        char *argvPtrs[64];
        int index = 0;
        char *p;

        snprintf(combined, sizeof(combined), "%s%s%s -a 127.0.0.1%s",
                 natsServerExe,
                 (cmdLineOpts != NULL ? " " : ""),
                 (cmdLineOpts != NULL ? cmdLineOpts : ""),
                 (keepServerOutput ? "" : " -l " LOGFILE_NAME));

        p = combined;
        while (*p != '\0')
        {
            while (*p == ' ' || *p == '\t')
                *p++ = '\0';
            if (*p == '\0')
                break;
            argvPtrs[index++] = p;
            while (*p != '\0' && *p != ' ' && *p != '\t')
                p++;
        }
        argvPtrs[index] = NULL;

        execvp(argvPtrs[0], argvPtrs);
        perror("exec failed");
        _exit(1);
    }

    if (checkStart)
    {
        if (_checkStart(url, 10) != NATS_OK)
        {
            _stopServer(pid);
            return NATS_INVALID_PID;
        }
    }

    g_serverPid = pid;
    return pid;
}

// CORE_SETUP declares local variables, starts a plain nats-server, and
// connects. Pair with CORE_TEARDOWN. main() also stops the server as a safety
// net if a test bails early via testCond.

#define CORE_SETUP                                              \
    natsStatus s = NATS_OK;                                     \
    natsConnection *nc = NULL;                                  \
    natsPid pid = NATS_INVALID_PID;                             \
                                                                \
    test("Start server: ");                                     \
    pid = _startServer("nats://127.0.0.1:4222", NULL, true);    \
    CHECK_SERVER_STARTED(pid);                                  \
    testCond(true);                                             \
                                                                \
    test("Connect: ");                                          \
    s = natsConnection_ConnectTo(&nc, "nats://127.0.0.1:4222"); \
    testCond(s == NATS_OK);

#define CORE_TEARDOWN           \
    natsConnection_Destroy(nc); \
    _stopServer(pid);

// Responder fixture — mirrors orbit.go's requestmany_test.go.

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

// Sentinel predicate: an empty-payload reply ends the gather (orbit.go's
// DefaultSentinel).
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
             && (opts.stall == 0)
             && (opts.count == 0)
             && (opts.sentinel == NULL)
             && (opts.sentinelClosure == NULL)
             && (opts.timeout == 0));

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
    opts.timeout = 300; // must exceed the sentinel's 100ms delay
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
    opts.stall = 50;     // < the sentinel's 100ms delay
    opts.timeout = 5000; // large; the stall must be what ends the gather
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
    opts.count = 3;
    opts.timeout = 5000;
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
    opts.sentinel = _emptySentinel;
    opts.timeout = 5000;
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
    opts.count = 1;
    opts.timeout = 2000;
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
    opts.count = 3;   // ends the gather
    opts.timeout = 0; // 0 must mean "use the default", not "expire now"
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
    opts.count = 1;
    opts.timeout = 2000;
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
    opts.count = 1000;   // far more than the responders will ever send
    opts.stall = 50;     // < the sentinel's 100ms delay, so it ends the burst
    opts.timeout = 5000;
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
    const char *envStr;
    const char *testName = NULL;
    testFunc f = NULL;
    int i;

    if (argc != 2)
    {
        printf("@@ Usage: %s [testname]\n", argv[0]);
        return 1;
    }
    testName = argv[1];

    envStr = getenv("NATS_TEST_SERVER_EXE");
    if (envStr != NULL && envStr[0] != '\0')
        natsServerExe = envStr;

    envStr = getenv("NATS_TEST_KEEP_SERVER_OUTPUT");
    if (envStr != NULL && envStr[0] != '\0')
        keepServerOutput = true;

    if (nats_Open(-1) != NATS_OK)
    {
        printf("@@ Unable to run tests: unable to initialize the library!\n");
        return 1;
    }

    for (i = 0; i < (int)(sizeof(allTests) / sizeof(allTests[0])); i++)
    {
        if (strcmp(testName, allTests[i].name) != 0)
            continue;
        printf("\033[0;34m\n== %s ==\n\033[0;0m", allTests[i].name);
        f = allTests[i].func;
        f();
        break;
    }

    if (f == NULL)
    {
        printf("@@ Test '%s' not found!\n", testName);
        return 1;
    }

    if (g_serverPid != NATS_INVALID_PID)
        _stopServer(g_serverPid);
    remove(LOGFILE_NAME);
    nats_CloseAndWait(failed ? 1 : 2000);

    if (failed)
    {
        printf("*** TEST FAILED ***\n");
        return 1;
    }
    printf("ALL PASSED\n");
    return 0;
}
