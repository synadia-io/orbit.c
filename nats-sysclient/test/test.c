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

#include "connz.h"
#include "healthz.h"
#include "jsz.h"
#include "statsz.h"
#include "subsz.h"
#include "sysclient.h"
#include "varz.h"

#include <nats/nats.h>

#include <dirent.h>
#include <limits.h>
#include <time.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

// Test framework — mirrors the other orbit.c sub-library suites.

typedef void (*testFunc)(void);

typedef struct
{
    const char *name;
    testFunc    func;
} testInfo;

#define _TEST_PROTO
#include "list.h"
#undef _TEST_PROTO

#define _TEST_LIST
static testInfo allTests[] = {
#include "list.h"
};
#undef _TEST_LIST

static int  tests  = 0;
static bool failed = false;

static const char *natsServerExe    = "nats-server";
static bool        keepServerOutput = false;

#define NATS_INVALID_PID (-1)
#define LOGFILE_NAME     "server.log"
#define TEST_URL         "nats://127.0.0.1:4222"

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
//
// Unlike the other suites this tracks a set of pids rather than one: the
// cluster tests start three servers, and a test that bails early via testCond
// would otherwise leave the extras running.

typedef pid_t natsPid;

#define MAX_SERVERS (8)

static natsPid g_serverPids[MAX_SERVERS];
static int     g_serverCount = 0;

static void
_forgetServer(natsPid pid)
{
    int i;

    for (i = 0; i < g_serverCount; i++)
    {
        if (g_serverPids[i] != pid)
            continue;
        g_serverPids[i] = g_serverPids[--g_serverCount];
        return;
    }
}

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
    _forgetServer(pid);
}

static void
_stopAllServers(void)
{
    while (g_serverCount > 0)
        _stopServer(g_serverPids[0]);
}

static int64_t
_nowMs(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((int64_t) ts.tv_sec * 1000) + (ts.tv_nsec / 1000000);
}

// Every wait below is a deadline in milliseconds rather than a count of
// attempts, so the poll interval can be chosen for how fast the thing being
// waited on actually settles without also deciding how long we are willing to
// wait. Attempts × interval couples the two, and the probes here are not free:
// a gather that has not converged costs its stall interval before returning.
#define SERVER_POLL_MS  (10)
#define CLUSTER_POLL_MS (50)

// Waits up to 'budgetMs' for a server to accept connections.
//
// The other suites poll this every 200ms, two orders of magnitude coarser than
// the event: a local nats-server binds its port in about 5ms, so a 200ms
// interval sleeps through ~195ms of every server start, and this suite starts
// a server in nearly every test.
static natsStatus
_waitForServer(const char *url, int64_t budgetMs)
{
    natsConnection *nc       = NULL;
    natsStatus      s        = NATS_OK;
    int64_t         deadline = _nowMs() + budgetMs;

    while (((s = natsConnection_ConnectTo(&nc, url)) != NATS_OK) && (_nowMs() < deadline))
    {
        usleep(SERVER_POLL_MS * 1000);
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
        char  combined[2048];
        char *argvPtrs[64];
        int   index = 0;
        char *p;

        snprintf(combined, sizeof(combined), "%s%s%s -a 127.0.0.1%s",
                 natsServerExe,
                 (cmdLineOpts != NULL ? " " : ""),
                 (cmdLineOpts != NULL ? cmdLineOpts : ""),
                 (keepServerOutput ? "" : " -l " LOGFILE_NAME));

        p = combined;
        while (*p != '\0')
        {
            while ((*p == ' ') || (*p == '\t'))
                *p++ = '\0';
            if (*p == '\0')
                break;
            argvPtrs[index++] = p;
            while ((*p != '\0') && (*p != ' ') && (*p != '\t'))
                p++;
        }
        argvPtrs[index] = NULL;

        execvp(argvPtrs[0], argvPtrs);
        perror("exec failed");
        _exit(1);
    }

    if (checkStart)
    {
        int status = 0;

        if (_waitForServer(url, 2000) != NATS_OK)
        {
            _stopServer(pid);
            return NATS_INVALID_PID;
        }

        // Connecting proves *a* server is listening, not that it is ours. If a
        // stray server already held the port, our child died on bind and the
        // connection above went to the stranger — the test would then run
        // against a server it cannot configure or stop, and fail somewhere far
        // from the cause. A child that has already exited says exactly that.
        //
        // The grace is needed because a stray server answers immediately, so
        // the wait above returns before our child has finished failing to bind.
        usleep(SERVER_POLL_MS * 1000);
        if (waitpid(pid, &status, WNOHANG) == pid)
        {
            _forgetServer(pid);
            return NATS_INVALID_PID;
        }
    }

    if (g_serverCount < MAX_SERVERS)
        g_serverPids[g_serverCount++] = pid;
    return pid;
}

// Filesystem helpers — same shape as the jetstream-extra suite, which also
// hands the server a per-run store directory and removes it afterwards.

static void
_rmtree(const char *path)
{
    DIR           *dir;
    struct stat    st;
    struct dirent *entry;

    if (stat(path, &st) != 0)
        return;
    if (!S_ISDIR(st.st_mode))
    {
        unlink(path);
        return;
    }

    dir = opendir(path);
    if (dir == NULL)
        return;

    while ((entry = readdir(dir)) != NULL)
    {
        char fullPath[1024];

        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
            continue;
        snprintf(fullPath, sizeof(fullPath), "%s/%s", path, entry->d_name);
        _rmtree(fullPath);
    }

    closedir(dir);
    rmdir(path);
}

static int _uniqueCounter = 0;

// Paths to sweep on the way out.
//
// testCond returns from the test the moment an assertion fails, so a test that
// bails mid-way never reaches its own teardown and leaves the server's store
// directory and config file behind. Registering them here means main() can
// clean up regardless of how the test ended, the same way g_serverPids lets it
// stop servers a bailing test never stopped.
#define MAX_TMP_PATHS (64)

static char g_tmpPaths[MAX_TMP_PATHS][128];
static int  g_tmpPathCount = 0;

static void
_rememberPath(const char *path)
{
    if (g_tmpPathCount < MAX_TMP_PATHS)
        snprintf(g_tmpPaths[g_tmpPathCount++], sizeof(g_tmpPaths[0]), "%s", path);
}

// _rmtree removes a plain file too, so this covers both configs and stores.
static void
_removeRememberedPaths(void)
{
    while (g_tmpPathCount > 0)
        _rmtree(g_tmpPaths[--g_tmpPathCount]);
}

static void
_makeUniqueDir(char *buf, int bufLen, const char *prefix)
{
    snprintf(buf, bufLen, "%s%d_%d", prefix, (int) getpid(), ++_uniqueCounter);
    _rememberPath(buf);
}

// A stand-in for nats-server's HEALTHZ endpoint.
//
// The real endpoint needs a configured system account and returns whatever the
// server happens to be doing; a stand-in gives byte-exact control over both the
// request seen and the response sent, so these tests can assert the wire format
// in both directions. The end-to-end tests against a real $SYS cluster come
// with the remaining endpoints.
typedef struct
{
    const char *reply;
    char        lastRequest[1024];
} fakeResponder;

static void
_healthzResponder(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    fakeResponder *fr  = (fakeResponder *) closure;
    int            len = natsMsg_GetDataLength(msg);

    (void) sub;

    if (len > (int) sizeof(fr->lastRequest) - 1)
        len = (int) sizeof(fr->lastRequest) - 1;
    memcpy(fr->lastRequest, natsMsg_GetData(msg), (size_t) len);
    fr->lastRequest[len] = '\0';

    natsConnection_PublishString(nc, natsMsg_GetReply(msg), fr->reply);
    natsMsg_Destroy(msg);
}

// A full HEALTHZ envelope, including the optional detail array.
#define HEALTHZ_REPLY                                                     \
    "{\"server\":{\"name\":\"s1\",\"host\":\"127.0.0.1\",\"id\":\"SRV1\"," \
    "\"cluster\":\"C1\",\"ver\":\"2.14.0\",\"tags\":[\"az:a\",\"az:b\"],"  \
    "\"seq\":7,\"jetstream\":true,"                                       \
    "\"time\":\"2026-08-12T10:30:45.123456789Z\"},"                       \
    "\"data\":{\"status\":\"ok\",\"status_code\":200,"                    \
    "\"errors\":[{\"type\":4,\"account\":\"JS\",\"stream\":\"orders\","    \
    "\"error\":\"boom\"}]}}"

// SETUP starts a server, connects, and creates a client with default options.
#define SETUP                                                    \
    natsStatus      s   = NATS_OK;                               \
    natsConnection *nc  = NULL;                                  \
    natsSysClient  *sys = NULL;                                  \
    natsPid         pid = NATS_INVALID_PID;                      \
                                                                 \
    test("Start server: ");                                      \
    pid = _startServer(TEST_URL, NULL, true);                    \
    CHECK_SERVER_STARTED(pid);                                   \
    testCond(true);                                              \
                                                                 \
    test("Connect: ");                                           \
    s = natsConnection_ConnectTo(&nc, TEST_URL);                 \
    testCond(s == NATS_OK);                                      \
                                                                 \
    test("Create client: ");                                     \
    s = natsSysClient_Create(&sys, nc, NULL);                    \
    testCond(s == NATS_OK)

#define TEARDOWN                    \
    natsSysClient_Destroy(sys);     \
    natsConnection_Destroy(nc);     \
    _stopServer(pid)

void
test_ClientCreateArgs(void)
{
    natsSysClient    *sys = NULL;
    natsSysClientOpts opts;
    natsStatus        s;

    test("Opts_Init rejects NULL: ");
    testCond(natsSysClientOpts_Init(NULL) == NATS_INVALID_ARG);

    test("Opts_Init sets the documented defaults: ");
    s = natsSysClientOpts_Init(&opts);
    testCond((s == NATS_OK)
             && (opts.StallInterval == NATS_SYS_DEFAULT_STALL)
             && (opts.ServerCount == -1));

    test("Create rejects NULL out-param and connection: ");
    testCond((natsSysClient_Create(NULL, NULL, NULL) == NATS_INVALID_ARG)
             && (natsSysClient_Create(&sys, NULL, NULL) == NATS_INVALID_ARG)
             && (sys == NULL));

    test("Create rejects a zero server count: ");
    natsSysClientOpts_Init(&opts);
    opts.ServerCount = 0;
    testCond(natsSysClient_Create(&sys, (natsConnection *) &opts, &opts) == NATS_INVALID_ARG);

    test("Create rejects a server count below -1: ");
    natsSysClientOpts_Init(&opts);
    opts.ServerCount = -2;
    testCond(natsSysClient_Create(&sys, (natsConnection *) &opts, &opts) == NATS_INVALID_ARG);

    // See natsSysClient_Create for why zero is rejected rather than meaning
    // "no stall".
    test("Create rejects a non-positive stall interval: ");
    natsSysClientOpts_Init(&opts);
    opts.StallInterval = -1;
    if (natsSysClient_Create(&sys, (natsConnection *) &opts, &opts) != NATS_INVALID_ARG)
        FAIL("a negative stall interval was accepted");
    natsSysClientOpts_Init(&opts);
    opts.StallInterval = 0;
    testCond(natsSysClient_Create(&sys, (natsConnection *) &opts, &opts) == NATS_INVALID_ARG);

    test("A rejected Create clears the out-param, whichever argument was bad: ");
    // Poisoned first: the headers promise NULL on *every* error, and a caller
    // that destroys unconditionally would otherwise free an indeterminate
    // pointer. Initialising to NULL here would make the assertion vacuous.
    {
        natsSysClient *poisoned = (natsSysClient *) (uintptr_t) 0xdeadbeef;

        natsSysClientOpts_Init(&opts);
        testCond((natsSysClient_Create(&poisoned, NULL, &opts) == NATS_INVALID_ARG)
                 && (poisoned == NULL));
    }

    test("Destroy(NULL) is a no-op: ");
    natsSysClient_Destroy(NULL);
    testCond(true);
}

void
test_TimeParse(void)
{
    int64_t ns = 0;

    test("NULL arguments are rejected: ");
    testCond((natsSysTime_Parse(NULL, "2026-08-12T10:30:45Z") == NATS_INVALID_ARG)
             && (natsSysTime_Parse(&ns, NULL) == NATS_INVALID_ARG));

    test("The Unix epoch is zero: ");
    testCond((natsSysTime_Parse(&ns, "1970-01-01T00:00:00Z") == NATS_OK) && (ns == 0));

    test("Whole seconds: ");
    testCond((natsSysTime_Parse(&ns, "2026-08-12T10:30:45Z") == NATS_OK)
             && (ns == 1786530645000000000LL));

    test("Nanosecond precision is kept: ");
    testCond((natsSysTime_Parse(&ns, "2026-08-12T10:30:45.123456789Z") == NATS_OK)
             && (ns == 1786530645123456789LL));

    test("A short fraction is scaled, not truncated: ");
    testCond((natsSysTime_Parse(&ns, "1970-01-01T00:00:00.5Z") == NATS_OK)
             && (ns == 500000000LL));

    test("Digits beyond nanoseconds are dropped: ");
    testCond((natsSysTime_Parse(&ns, "1970-01-01T00:00:00.1234567891234Z") == NATS_OK)
             && (ns == 123456789LL));

    test("A positive offset is subtracted: ");
    testCond((natsSysTime_Parse(&ns, "1970-01-01T01:00:00+01:00") == NATS_OK) && (ns == 0));

    test("A negative offset is added: ");
    testCond((natsSysTime_Parse(&ns, "1969-12-31T23:00:00-01:00") == NATS_OK) && (ns == 0));

    test("A timestamp int64 nanoseconds cannot hold is rejected: ");
    // Go documents the same window for time.Time.UnixNano(). The low end is
    // reachable from real data: a JetStream stream that has never been written
    // reports "0001-01-01T00:00:00Z" for first_ts and last_ts.
    testCond((natsSysTime_Parse(&ns, "0001-01-01T00:00:00Z") == NATS_INVALID_ARG)
             && (natsSysTime_Parse(&ns, "9999-12-31T23:59:59Z") == NATS_INVALID_ARG));

    test("The representable boundaries themselves are accepted: ");
    testCond((natsSysTime_Parse(&ns, "2262-04-11T23:47:16Z") == NATS_OK)
             && (ns == 9223372036000000000LL)
             && (natsSysTime_Parse(&ns, "1677-09-21T00:12:44Z") == NATS_OK)
             && (ns == -9223372036000000000LL));

    test("One second past either boundary is rejected: ");
    testCond((natsSysTime_Parse(&ns, "2262-04-11T23:47:17Z") == NATS_INVALID_ARG)
             && (natsSysTime_Parse(&ns, "1677-09-21T00:12:43Z") == NATS_INVALID_ARG));

    test("Lowercase t and z are accepted: ");
    testCond((natsSysTime_Parse(&ns, "1970-01-01t00:00:00z") == NATS_OK) && (ns == 0));

    test("A pre-epoch time is negative: ");
    testCond((natsSysTime_Parse(&ns, "1969-12-31T23:59:59Z") == NATS_OK)
             && (ns == -1000000000LL));

    test("A leap day is accepted: ");
    testCond(natsSysTime_Parse(&ns, "2024-02-29T00:00:00Z") == NATS_OK);

    test("A non-leap 29 February is rejected: ");
    testCond(natsSysTime_Parse(&ns, "2023-02-29T00:00:00Z") == NATS_INVALID_ARG);

    test("Malformed input is rejected: ");
    testCond((natsSysTime_Parse(&ns, "") == NATS_INVALID_ARG)
             && (natsSysTime_Parse(&ns, "2026-08-12") == NATS_INVALID_ARG)
             && (natsSysTime_Parse(&ns, "2026-08-12T10:30:45") == NATS_INVALID_ARG)
             && (natsSysTime_Parse(&ns, "2026-13-12T10:30:45Z") == NATS_INVALID_ARG)
             && (natsSysTime_Parse(&ns, "2026-08-32T10:30:45Z") == NATS_INVALID_ARG)
             && (natsSysTime_Parse(&ns, "2026-08-12T24:30:45Z") == NATS_INVALID_ARG)
             && (natsSysTime_Parse(&ns, "2026-08-12T10:30:45.Z") == NATS_INVALID_ARG)
             && (natsSysTime_Parse(&ns, "2026-08-12T10:30:45Z ") == NATS_INVALID_ARG)
             && (natsSysTime_Parse(&ns, "2026-08-12T10:30:45+0100") == NATS_INVALID_ARG));
}

void
test_DestroyNull(void)
{
    test("Every destroy tolerates NULL: ");
    natsSysClient_Destroy(NULL);
    natsSysHealthzResp_Destroy(NULL);
    natsSysHealthzRespList_Destroy(NULL);
    testCond(true);
}

void
test_HealthzArgs(void)
{
    natsSysHealthzResp     *resp = NULL;
    natsSysHealthzRespList  list = {NULL, 0};
    natsSysHealthzOptions   opts;

    SETUP;

    test("Options_Init rejects NULL: ");
    testCond(natsSysHealthzOptions_Init(NULL) == NATS_INVALID_ARG);

    test("Options_Init zeroes the struct: ");
    memset(&opts, 0xff, sizeof(opts));
    s = natsSysHealthzOptions_Init(&opts);
    testCond((s == NATS_OK) && !opts.JSEnabledOnly && !opts.Details
             && (opts.Account == NULL));

    test("Healthz rejects a NULL out-param: ");
    testCond(natsSysClient_Healthz(NULL, sys, "SRV1", NULL, 0) == NATS_INVALID_ARG);

    test("Healthz rejects a NULL client: ");
    testCond((natsSysClient_Healthz(&resp, NULL, "SRV1", NULL, 0) == NATS_INVALID_ARG)
             && (resp == NULL));

    test("Healthz rejects a NULL server ID: ");
    testCond(natsSysClient_Healthz(&resp, sys, NULL, NULL, 0) == NATS_INVALID_ARG);

    test("Healthz rejects an empty server ID: ");
    testCond(natsSysClient_Healthz(&resp, sys, "", NULL, 0) == NATS_INVALID_ARG);

    test("Healthz rejects a negative timeout: ");
    testCond(natsSysClient_Healthz(&resp, sys, "SRV1", NULL, -1) == NATS_INVALID_ARG);

    test("HealthzPing rejects a NULL list and client: ");
    testCond((natsSysClient_HealthzPing(NULL, sys, NULL, 0) == NATS_INVALID_ARG)
             && (natsSysClient_HealthzPing(&list, NULL, NULL, 0) == NATS_INVALID_ARG));

    TEARDOWN;
}

void
test_HealthzRequestPayload(void)
{
    natsSubscription      *sub  = NULL;
    natsSysHealthzResp    *resp = NULL;
    natsSysHealthzOptions  opts;
    fakeResponder          fr;

    SETUP;

    memset(&fr, 0, sizeof(fr));
    fr.reply = HEALTHZ_REPLY;

    test("Subscribe the stand-in responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.HEALTHZ",
                                 _healthzResponder, &fr);
    testCond(s == NATS_OK);

    // Every HealthzOptions field is optional, so a zeroed options struct —
    // and a NULL one — must marshal to an empty object.
    test("Default options marshal to {}: ");
    s = natsSysClient_Healthz(&resp, sys, "SRV1", NULL, 2000);
    if (s != NATS_OK)
        FAIL("request failed");
    natsSysHealthzResp_Destroy(resp);
    resp = NULL;
    testCond(strcmp(fr.lastRequest, "{}") == 0);

    test("An explicitly zeroed options struct also marshals to {}: ");
    natsSysHealthzOptions_Init(&opts);
    s = natsSysClient_Healthz(&resp, sys, "SRV1", &opts, 2000);
    if (s != NATS_OK)
        FAIL("request failed");
    natsSysHealthzResp_Destroy(resp);
    resp = NULL;
    testCond(strcmp(fr.lastRequest, "{}") == 0);

    test("Only the set fields are emitted: ");
    natsSysHealthzOptions_Init(&opts);
    opts.JSEnabledOnly = true;
    opts.Account       = "JS";
    opts.Details       = true;
    s                  = natsSysClient_Healthz(&resp, sys, "SRV1", &opts, 2000);
    if (s != NATS_OK)
        FAIL("request failed");
    natsSysHealthzResp_Destroy(resp);
    resp = NULL;
    testCond(strcmp(fr.lastRequest,
                    "{\"js-enabled-only\":true,\"account\":\"JS\",\"details\":true}")
             == 0);

    test("Every field set: ");
    natsSysHealthzOptions_Init(&opts);
    opts.JSEnabledOnly = true;
    opts.JSServerOnly  = true;
    opts.Account       = "JS";
    opts.Stream        = "orders";
    opts.Consumer      = "c1";
    opts.Details       = true;
    s                  = natsSysClient_Healthz(&resp, sys, "SRV1", &opts, 2000);
    if (s != NATS_OK)
        FAIL("request failed");
    natsSysHealthzResp_Destroy(resp);
    resp = NULL;
    testCond(strcmp(fr.lastRequest,
                    "{\"js-enabled-only\":true,\"js-server-only\":true,"
                    "\"account\":\"JS\",\"stream\":\"orders\",\"consumer\":\"c1\","
                    "\"details\":true}")
             == 0);

    test("A value needing escaping is escaped: ");
    natsSysHealthzOptions_Init(&opts);
    opts.Account = "a\"b";
    s            = natsSysClient_Healthz(&resp, sys, "SRV1", &opts, 2000);
    if (s != NATS_OK)
        FAIL("request failed");
    natsSysHealthzResp_Destroy(resp);
    resp = NULL;
    testCond(strcmp(fr.lastRequest, "{\"account\":\"a\\\"b\"}") == 0);

    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_Healthz(void)
{
    natsSubscription   *sub  = NULL;
    natsSysHealthzResp *resp = NULL;
    fakeResponder       fr;
    int64_t             ns = 0;

    SETUP;

    memset(&fr, 0, sizeof(fr));
    fr.reply = HEALTHZ_REPLY;

    test("Subscribe the stand-in responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.HEALTHZ",
                                 _healthzResponder, &fr);
    testCond(s == NATS_OK);

    test("Request: ");
    s = natsSysClient_Healthz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_OK) && (resp != NULL));

    test("Server info decoded: ");
    testCond((strcmp(resp->Server.Name, "s1") == 0)
             && (strcmp(resp->Server.Host, "127.0.0.1") == 0)
             && (strcmp(resp->Server.ID, "SRV1") == 0)
             && (strcmp(resp->Server.Cluster, "C1") == 0)
             && (strcmp(resp->Server.Version, "2.14.0") == 0)
             && (resp->Server.Seq == 7)
             && resp->Server.JetStream);

    test("An absent envelope key stays NULL: ");
    testCond(resp->Server.Domain == NULL);

    test("Tags decoded: ");
    testCond((resp->Server.TagsCount == 2)
             && (strcmp(resp->Server.Tags[0], "az:a") == 0)
             && (strcmp(resp->Server.Tags[1], "az:b") == 0));

    test("The timestamp round-trips through natsSysTime_Parse: ");
    testCond((natsSysTime_Parse(&ns, resp->Server.Time) == NATS_OK)
             && (ns == 1786530645123456789LL));

    test("Payload decoded: ");
    testCond((strcmp(resp->Healthz.Status, "ok") == 0)
             && (resp->Healthz.StatusCode == 200)
             && (resp->Healthz.Error == NULL));

    test("The nested error array decoded: ");
    testCond((resp->Healthz.ErrorsCount == 1)
             && (resp->Healthz.Errors[0].Type == natsSysHealthzErrorStream)
             && (strcmp(resp->Healthz.Errors[0].Account, "JS") == 0)
             && (strcmp(resp->Healthz.Errors[0].Stream, "orders") == 0)
             && (strcmp(resp->Healthz.Errors[0].Error, "boom") == 0)
             && (resp->Healthz.Errors[0].Consumer == NULL));

    test("No error reported: ");
    testCond((resp->Error.Code == 0) && (resp->Error.Description == NULL));

    natsSysHealthzResp_Destroy(resp);
    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_HealthzServerError(void)
{
    natsSubscription   *sub  = NULL;
    natsSysHealthzResp *resp = NULL;
    fakeResponder       fr;

    SETUP;

    memset(&fr, 0, sizeof(fr));
    // An error envelope carries no "data" key at all.
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"error\":{\"code\":400,"
               "\"err_code\":10023,\"description\":\"bad request\"}}";

    test("Subscribe the stand-in responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.HEALTHZ",
                                 _healthzResponder, &fr);
    testCond(s == NATS_OK);

    // The error is decoded but never checked, so the call still succeeds and
    // it is the caller's job to look.
    test("An error envelope still returns NATS_OK: ");
    s = natsSysClient_Healthz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_OK) && (resp != NULL));

    test("The error is decoded: ");
    testCond((resp->Error.Code == 400) && (resp->Error.ErrCode == 10023)
             && (strcmp(resp->Error.Description, "bad request") == 0));

    test("The payload is zeroed: ");
    testCond((resp->Healthz.Status == NULL) && (resp->Healthz.StatusCode == 0)
             && (resp->Healthz.ErrorsCount == 0));

    natsSysHealthzResp_Destroy(resp);
    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_HealthzMalformed(void)
{
    natsSubscription   *sub  = NULL;
    natsSysHealthzResp *resp = NULL;
    fakeResponder       fr;

    SETUP;

    memset(&fr, 0, sizeof(fr));
    fr.reply = "not json at all";

    test("Subscribe the stand-in responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.HEALTHZ",
                                 _healthzResponder, &fr);
    testCond(s == NATS_OK);

    test("A malformed reply is NATS_ERR, not NATS_INVALID_ARG: ");
    s = natsSysClient_Healthz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_ERR) && (resp == NULL));

    test("A reply that is valid JSON but not an object is NATS_ERR: ");
    fr.reply = "[1,2,3]";
    s        = natsSysClient_Healthz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_ERR) && (resp == NULL));

    test("A wrongly-typed payload field is NATS_ERR: ");
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"status\":123}}";
    s        = natsSysClient_Healthz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_ERR) && (resp == NULL));

    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_HealthzInvalidServerID(void)
{
    natsSysHealthzResp *resp = NULL;

    SETUP;

    // Nothing is subscribed, so the server answers with its no-responders
    // control message, which the by-ID path reports as "no such server".
    test("An unknown server ID is NATS_NOT_FOUND: ");
    s = natsSysClient_Healthz(&resp, sys, "NOSUCHSERVER", NULL, 2000);
    testCond((s == NATS_NOT_FOUND) && (resp == NULL));

    TEARDOWN;
}

// The envelope's own edge cases. Each of these decoded wrongly before: a null
// "server" failed the whole response, and a wrongly-typed "error" was dropped
// so the caller's documented `Error.Code != 0` check cleared a bad response.
void
test_EnvelopeEdges(void)
{
    natsSubscription   *sub  = NULL;
    natsSysHealthzResp *resp = NULL;
    fakeResponder       fr;

    SETUP;

    memset(&fr, 0, sizeof(fr));

    test("Subscribe the stand-in responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.HEALTHZ",
                                 _healthzResponder, &fr);
    testCond(s == NATS_OK);

    test("A null \"server\" leaves the zero value, as any other null does: ");
    fr.reply = "{\"server\":null,\"data\":{\"status\":\"ok\"}}";
    s        = natsSysClient_Healthz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_OK) && (resp != NULL) && (resp->Server.ID == NULL)
             && (resp->Healthz.Status != NULL)
             && (strcmp(resp->Healthz.Status, "ok") == 0));
    natsSysHealthzResp_Destroy(resp);
    resp = NULL;

    test("A null \"error\" is absent, not malformed: ");
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"error\":null,\"data\":{\"status\":\"ok\"}}";
    s        = natsSysClient_Healthz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_OK) && (resp != NULL) && (resp->Error.Code == 0));
    natsSysHealthzResp_Destroy(resp);
    resp = NULL;

    // Silently ignoring this is worse than failing: the caller is told to trust
    // the payload once Error.Code is 0, and it would be 0 here.
    test("An \"error\" that is not an object fails the response: ");
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"error\":\"boom\",\"data\":{\"status\":\"ok\"}}";
    s        = natsSysClient_Healthz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_ERR) && (resp == NULL));

    test("A \"server\" that is neither object nor null fails the response: ");
    fr.reply = "{\"server\":7,\"data\":{\"status\":\"ok\"}}";
    s        = natsSysClient_Healthz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_ERR) && (resp == NULL));

    natsSubscription_Destroy(sub);
    TEARDOWN;
}

// Numbers the target member cannot represent are a malformed response, not a
// silent truncation. Go rejects each of these.
void
test_FieldRanges(void)
{
    natsSubscription *sub  = NULL;
    natsSysVarzResp  *varz = NULL;
    natsSysStatszResp *statsz = NULL;
    fakeResponder     fr;

    SETUP;

    memset(&fr, 0, sizeof(fr));

    test("Subscribe the stand-in responders: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.VARZ", _healthzResponder, &fr);
    testCond(s == NATS_OK);

    test("A uint32 field above UINT32_MAX is rejected, not truncated to 0: ");
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"subscriptions\":4294967296}}";
    s        = natsSysClient_Varz(&varz, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_ERR) && (varz == NULL));

    test("An int field above INT_MAX is rejected: ");
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"port\":4294967296}}";
    s        = natsSysClient_Varz(&varz, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_ERR) && (varz == NULL));

    test("A negative value in an unsigned field is rejected, not wrapped: ");
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"total_connections\":-1}}";
    s        = natsSysClient_Varz(&varz, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_ERR) && (varz == NULL));

    test("A uint64 field at its maximum still decodes: ");
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"data\":"
               "{\"total_connections\":18446744073709551615}}";
    s        = natsSysClient_Varz(&varz, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_OK) && (varz != NULL)
             && (varz->Varz.TotalConnections == 18446744073709551615ULL));
    natsSysVarzResp_Destroy(varz);
    varz = NULL;

    natsSubscription_Destroy(sub);

    test("Subscribe a STATSZ stand-in: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.STATSZ", _healthzResponder, &fr);
    testCond(s == NATS_OK);

    test("A uint16 error code above UINT16_MAX is rejected: ");
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"error\":{\"err_code\":70000},\"statsz\":{}}";
    s        = natsSysClient_Statsz(&statsz, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_ERR) && (statsz == NULL));

    test("A route's pending byte count above INT_MAX decodes: ");
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"statsz\":"
               "{\"routes\":[{\"rid\":1,\"pending\":3221225472}]}}";
    s        = natsSysClient_Statsz(&statsz, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_OK) && (statsz != NULL) && (statsz->Statsz.RoutesCount == 1)
             && (statsz->Statsz.Routes[0]->Pending == 3221225472LL));
    natsSysStatszResp_Destroy(statsz);
    statsz = NULL;

    natsSubscription_Destroy(sub);

    test("Subscribe a CONNZ stand-in: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.CONNZ", _healthzResponder, &fr);
    testCond(s == NATS_OK);

    test("A connection's pending byte count above INT_MAX decodes: ");
    {
        natsSysConnzResp *connz = NULL;

        fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"data\":"
                   "{\"connections\":[{\"cid\":1,\"pending_bytes\":3221225472}]}}";
        s        = natsSysClient_Connz(&connz, sys, "SRV1", NULL, 2000);
        testCond((s == NATS_OK) && (connz != NULL) && (connz->Connz.ConnsCount == 1)
                 && (connz->Connz.Conns[0]->Pending == 3221225472LL));
        natsSysConnzResp_Destroy(connz);
    }

    natsSubscription_Destroy(sub);
    TEARDOWN;
}

// An array element that fails part-way through its own parse must not strand
// what it had already allocated. Only a leak checker can see this, so the
// assertion is simply that the call fails cleanly — run under ASan.
void
test_PartialElementCleanup(void)
{
    natsSubscription   *sub  = NULL;
    natsSysHealthzResp *resp = NULL;
    fakeResponder       fr;

    SETUP;

    memset(&fr, 0, sizeof(fr));

    test("Subscribe the stand-in responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.HEALTHZ",
                                 _healthzResponder, &fr);
    testCond(s == NATS_OK);

    // "account" parses and is strdup'd; "stream" then fails on the wrong type,
    // so the element is abandoned mid-build with one member already allocated.
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"status\":\"ok\","
               "\"errors\":[{\"account\":\"LEAKED-IF-BROKEN\",\"stream\":5}]}}";

    // One request is enough: the leak is a single strdup, and LeakSanitizer
    // reports it at exit whatever the count. The assertion is only that the
    // call fails cleanly — run this under ASan for the rest.
    test("An element failing mid-parse is rejected and leaks nothing: ");
    s = natsSysClient_Healthz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_ERR) && (resp == NULL));

    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_HealthzPing(void)
{
    natsSubscription      *subs[3] = {NULL, NULL, NULL};
    fakeResponder          frs[3];
    natsSysHealthzRespList list = {NULL, 0};
    int                    i;
    bool                   ok;

    static const char *replies[3] = {
        "{\"server\":{\"id\":\"SRV1\",\"name\":\"s1\"},\"data\":{\"status\":\"ok\"}}",
        "{\"server\":{\"id\":\"SRV2\",\"name\":\"s2\"},\"data\":{\"status\":\"ok\"}}",
        "{\"server\":{\"id\":\"SRV3\",\"name\":\"s3\"},\"data\":{\"status\":\"ok\"}}",
    };

    SETUP;

    test("Subscribe three stand-in responders: ");
    for (i = 0; (i < 3) && (s == NATS_OK); i++)
    {
        memset(&frs[i], 0, sizeof(frs[i]));
        frs[i].reply = replies[i];
        s = natsConnection_Subscribe(&subs[i], nc, "$SYS.REQ.SERVER.*.HEALTHZ",
                                     _healthzResponder, &frs[i]);
    }
    testCond(s == NATS_OK);

    test("Ping gathers one response per server: ");
    s = natsSysClient_HealthzPing(&list, sys, NULL, 2000);
    testCond((s == NATS_OK) && (list.Count == 3));

    test("Each server is represented exactly once: ");
    ok = true;
    for (i = 0; i < 3; i++)
    {
        int  found = 0;
        int  j;
        char id[8];

        snprintf(id, sizeof(id), "SRV%d", i + 1);
        for (j = 0; j < list.Count; j++)
        {
            if (strcmp(list.Resps[j]->Server.ID, id) == 0)
                found++;
        }
        if (found != 1)
            ok = false;
    }
    testCond(ok);

    test("Each response carries its payload: ");
    ok = true;
    for (i = 0; i < list.Count; i++)
    {
        if ((list.Resps[i]->Healthz.Status == NULL)
            || (strcmp(list.Resps[i]->Healthz.Status, "ok") != 0))
            ok = false;
    }
    testCond(ok);

    natsSysHealthzRespList_Destroy(&list);

    test("Destroying an already-destroyed list is safe: ");
    natsSysHealthzRespList_Destroy(&list);
    testCond((list.Resps == NULL) && (list.Count == 0));

    for (i = 0; i < 3; i++)
        natsSubscription_Destroy(subs[i]);
    TEARDOWN;
}

void
test_HealthzPingNoResponders(void)
{
    natsSysHealthzRespList list = {NULL, 0};

    SETUP;

    // Nothing is subscribed, so the server answers the scatter with its
    // no-responders control message. That is a real failure — in practice it
    // means the connection is not on the system account — so it must not be
    // mistaken for "the cluster is empty".
    test("A ping with nothing listening is NATS_NO_RESPONDERS: ");
    s = natsSysClient_HealthzPing(&list, sys, NULL, 300);
    testCond((s == NATS_NO_RESPONDERS) && (list.Count == 0));

    natsSysHealthzRespList_Destroy(&list);
    TEARDOWN;
}

// Receives the request and deliberately never replies, so the gather runs out
// of time instead of hitting the no-responders path.
static void
_silentResponder(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    (void) nc;
    (void) sub;
    (void) closure;
    natsMsg_Destroy(msg);
}

void
test_HealthzPingSilent(void)
{
    natsSubscription      *sub  = NULL;
    natsSysHealthzRespList list = {NULL, 0};

    SETUP;

    test("Subscribe a responder that never answers: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.HEALTHZ",
                                 _silentResponder, NULL);
    testCond(s == NATS_OK);

    // The deadline expiring is how a gather normally ends: it returns
    // whatever arrived, which here is nothing.
    test("A gather that times out empty succeeds with no responses: ");
    s = natsSysClient_HealthzPing(&list, sys, NULL, 300);
    testCond((s == NATS_OK) && (list.Count == 0) && (list.Resps == NULL));

    natsSysHealthzRespList_Destroy(&list);
    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_HealthzTimeout(void)
{
    natsSubscription *sub  = NULL;
    natsSysHealthzResp *resp = NULL;

    SETUP;

    // Subscribed but never replying: there IS a responder, so the request is
    // not "no such server" — it is a server that did not answer in time. The
    // two are separate documented statuses on every by-ID entry point.
    test("Subscribe a silent responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.HEALTHZ", _silentResponder, NULL);
    if (s == NATS_OK)
        s = natsConnection_Flush(nc);
    testCond(s == NATS_OK);

    test("A by-ID request nobody answers in time is NATS_TIMEOUT: ");
    s = natsSysClient_Healthz(&resp, sys, "SRV1", NULL, 300);
    testCond((s == NATS_TIMEOUT) && (resp == NULL));

    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_HealthzPingServerCount(void)
{
    natsSubscription      *subs[3] = {NULL, NULL, NULL};
    fakeResponder          frs[3];
    natsSysHealthzRespList list = {NULL, 0};
    natsSysClientOpts      opts;
    natsSysClient         *bounded = NULL;
    int                    i;

    static const char *replies[3] = {
        "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"status\":\"ok\"}}",
        "{\"server\":{\"id\":\"SRV2\"},\"data\":{\"status\":\"ok\"}}",
        "{\"server\":{\"id\":\"SRV3\"},\"data\":{\"status\":\"ok\"}}",
    };

    SETUP;

    test("Subscribe three stand-in responders: ");
    for (i = 0; (i < 3) && (s == NATS_OK); i++)
    {
        memset(&frs[i], 0, sizeof(frs[i]));
        frs[i].reply = replies[i];
        s = natsConnection_Subscribe(&subs[i], nc, "$SYS.REQ.SERVER.*.HEALTHZ",
                                     _healthzResponder, &frs[i]);
    }
    testCond(s == NATS_OK);

    test("Create a client bounded to two servers: ");
    natsSysClientOpts_Init(&opts);
    opts.ServerCount = 2;
    s                = natsSysClient_Create(&bounded, nc, &opts);
    testCond(s == NATS_OK);

    test("The gather stops at the configured count: ");
    s = natsSysClient_HealthzPing(&list, bounded, NULL, 2000);
    testCond((s == NATS_OK) && (list.Count == 2));

    natsSysHealthzRespList_Destroy(&list);
    natsSysClient_Destroy(bounded);
    for (i = 0; i < 3; i++)
        natsSubscription_Destroy(subs[i]);
    TEARDOWN;
}

//
// STATSZ
//

// Note the payload key: "statsz", not "data". STATSZ is the one endpoint that
// differs, and test_StatszEnvelopeKey below pins that down.
#define STATSZ_REPLY                                                            \
    "{\"server\":{\"id\":\"SRV1\",\"name\":\"s1\"},"                            \
    "\"statsz\":{\"start\":\"2026-08-12T10:30:45Z\",\"mem\":100,\"cores\":8,"   \
    "\"cpu\":2.5,\"connections\":3,\"total_connections\":99,"                   \
    "\"active_accounts\":2,\"subscriptions\":7,"                                \
    "\"sent\":{\"msgs\":10,\"bytes\":100},"                                     \
    "\"received\":{\"msgs\":20,\"bytes\":200},"                                 \
    "\"slow_consumers\":1,\"active_servers\":3,"                                \
    "\"routes\":[{\"rid\":1,\"name\":\"r1\",\"sent\":{\"msgs\":1,\"bytes\":2}," \
    "\"received\":{\"msgs\":3,\"bytes\":4},\"pending\":5}],"                    \
    "\"gateways\":[{\"gwid\":2,\"name\":\"g1\",\"inbound_connections\":6}],"    \
    "\"jetstream\":{\"stats\":{\"memory\":11,\"api\":{\"total\":7}}}}}"

void
test_Statsz(void)
{
    natsSubscription  *sub  = NULL;
    natsSysStatszResp *resp = NULL;
    fakeResponder      fr;

    SETUP;

    memset(&fr, 0, sizeof(fr));
    fr.reply = STATSZ_REPLY;

    test("Subscribe the stand-in responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.STATSZ",
                                 _healthzResponder, &fr);
    testCond(s == NATS_OK);

    test("Request: ");
    s = natsSysClient_Statsz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_OK) && (resp != NULL));

    test("Scalars decoded: ");
    testCond((resp->Statsz.Mem == 100) && (resp->Statsz.Cores == 8)
             && (resp->Statsz.CPU > 2.4) && (resp->Statsz.CPU < 2.6)
             && (resp->Statsz.Connections == 3)
             && (resp->Statsz.TotalConnections == 99)
             && (resp->Statsz.ActiveAccounts == 2)
             && (resp->Statsz.NumSubs == 7)
             && (resp->Statsz.SlowConsumers == 1)
             && (resp->Statsz.ActiveServers == 3)
             && (strcmp(resp->Statsz.Start, "2026-08-12T10:30:45Z") == 0));

    test("Inline DataStats decoded: ");
    testCond((resp->Statsz.Sent.Msgs == 10) && (resp->Statsz.Sent.Bytes == 100)
             && (resp->Statsz.Received.Msgs == 20)
             && (resp->Statsz.Received.Bytes == 200));

    test("Route array decoded, with its own inline stats: ");
    testCond((resp->Statsz.RoutesCount == 1) && (resp->Statsz.Routes[0]->ID == 1)
             && (strcmp(resp->Statsz.Routes[0]->Name, "r1") == 0)
             && (resp->Statsz.Routes[0]->Sent.Msgs == 1)
             && (resp->Statsz.Routes[0]->Sent.Bytes == 2)
             && (resp->Statsz.Routes[0]->Received.Msgs == 3)
             && (resp->Statsz.Routes[0]->Received.Bytes == 4)
             && (resp->Statsz.Routes[0]->Pending == 5));

    test("Gateway array decoded: ");
    testCond((resp->Statsz.GatewaysCount == 1) && (resp->Statsz.Gateways[0]->ID == 2)
             && (strcmp(resp->Statsz.Gateways[0]->Name, "g1") == 0)
             && (resp->Statsz.Gateways[0]->NumInbound == 6)
             && (resp->Statsz.Gateways[0]->Sent.Msgs == 0));

    test("The shared JetStreamVarz decoded: ");
    testCond((resp->Statsz.JetStream != NULL) && (resp->Statsz.JetStream->Stats != NULL)
             && (resp->Statsz.JetStream->Stats->Memory == 11)
             && (resp->Statsz.JetStream->Stats->API.Total == 7)
             && (resp->Statsz.JetStream->Config == NULL)
             && (resp->Statsz.JetStream->Meta == NULL));

    natsSysStatszResp_Destroy(resp);
    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_StatszEnvelopeKey(void)
{
    natsSubscription  *sub  = NULL;
    natsSysStatszResp *resp = NULL;
    fakeResponder      fr;

    SETUP;

    memset(&fr, 0, sizeof(fr));
    // Same payload, but under "data" — the key every OTHER endpoint uses.
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"mem\":100,\"cores\":8}}";

    test("Subscribe the stand-in responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.STATSZ",
                                 _healthzResponder, &fr);
    testCond(s == NATS_OK);

    test("Request: ");
    s = natsSysClient_Statsz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_OK) && (resp != NULL));

    // If this ever starts passing the payload through, the port has quietly
    // normalised an envelope difference that is real on the wire.
    test("A payload under \"data\" is ignored by STATSZ: ");
    testCond((resp->Statsz.Mem == 0) && (resp->Statsz.Cores == 0));

    natsSysStatszResp_Destroy(resp);
    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_StatszRequestPayload(void)
{
    natsSubscription    *sub  = NULL;
    natsSysStatszResp   *resp = NULL;
    natsSysStatszOptions opts;
    fakeResponder        fr;
    const char          *tags[] = {"az:a", "az:b"};

    SETUP;

    memset(&fr, 0, sizeof(fr));
    fr.reply = STATSZ_REPLY;

    test("Subscribe the stand-in responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.STATSZ",
                                 _healthzResponder, &fr);
    testCond(s == NATS_OK);

    test("Default options marshal to {}: ");
    s = natsSysClient_Statsz(&resp, sys, "SRV1", NULL, 2000);
    if (s != NATS_OK)
        FAIL("request failed");
    natsSysStatszResp_Destroy(resp);
    resp = NULL;
    testCond(strcmp(fr.lastRequest, "{}") == 0);

    test("The event filter is flattened into the request object: ");
    natsSysStatszOptions_Init(&opts);
    opts.Filter.Name      = "s1";
    opts.Filter.Cluster   = "C1";
    opts.Filter.Tags      = tags;
    opts.Filter.TagsCount = 2;
    opts.Filter.Domain    = "hub";
    s                     = natsSysClient_Statsz(&resp, sys, "SRV1", &opts, 2000);
    if (s != NATS_OK)
        FAIL("request failed");
    natsSysStatszResp_Destroy(resp);
    resp = NULL;
    testCond(strcmp(fr.lastRequest,
                    "{\"server_name\":\"s1\",\"cluster\":\"C1\","
                    "\"tags\":[\"az:a\",\"az:b\"],\"domain\":\"hub\"}")
             == 0);

    natsSubscription_Destroy(sub);
    TEARDOWN;
}

//
// VARZ
//

// Exercises every shape the field-table machinery has to handle: each scalar
// width, string arrays, structs embedded by value, structs held by pointer,
// value arrays, pointer arrays, the map field and the raw-JSON field.
#define VARZ_REPLY                                                                  \
    "{\"server\":{\"id\":\"SRV1\",\"name\":\"s1\"},\"data\":{"                      \
    "\"server_id\":\"SRV1\",\"server_name\":\"s1\",\"version\":\"2.14.0\","         \
    "\"proto\":1,\"go\":\"go1.23\",\"host\":\"0.0.0.0\",\"port\":4222,"             \
    "\"auth_required\":true,\"max_control_line\":4096,\"max_payload\":1048576,"     \
    "\"max_pending\":67108864,\"ping_interval\":120000000000,"                      \
    "\"write_deadline\":10000000000,\"auth_timeout\":2.5,\"tls_timeout\":0.5,"      \
    "\"connect_urls\":[\"a:1\",\"b:2\"],"                                           \
    "\"start\":\"2026-08-12T10:30:45Z\",\"now\":\"2026-08-12T11:00:00Z\","          \
    "\"uptime\":\"29m\",\"mem\":12345678,\"cores\":8,\"gomaxprocs\":8,"             \
    "\"cpu\":1.5,\"connections\":3,"                                                \
    "\"total_connections\":18446744073709551615,\"subscriptions\":42,"              \
    "\"cluster\":{\"name\":\"C1\",\"addr\":\"0.0.0.0\",\"cluster_port\":6222,"      \
    "\"urls\":[\"u1\"],\"pool_size\":3},"                                           \
    "\"gateway\":{\"name\":\"G1\",\"port\":7222,\"gateways\":["                     \
    "{\"name\":\"R1\",\"urls\":[\"g1\",\"g2\"]},{\"name\":\"R2\"}]},"               \
    "\"leaf\":{\"port\":7422,\"remotes\":[{\"local_account\":\"A\","                \
    "\"urls\":[\"l1\"],\"deny\":{\"exports\":[\"e1\"],"                             \
    "\"imports\":[\"i1\",\"i2\"]}}]},"                                              \
    "\"mqtt\":{\"port\":1883,\"ack_wait\":30000000000,\"max_ack_pending\":1024},"   \
    "\"websocket\":{\"port\":8080,\"handshake_timeout\":2000000000,"                \
    "\"allowed_origins\":[\"o1\"]},"                                                \
    "\"jetstream\":{\"config\":{\"max_memory\":100,\"max_storage\":200,"            \
    "\"store_dir\":\"/tmp\",\"sync_interval\":5000000000},"                         \
    "\"stats\":{\"memory\":1,\"storage\":2,\"accounts\":3,"                         \
    "\"api\":{\"total\":10,\"errors\":2}},"                                         \
    "\"meta\":{\"name\":\"M\",\"leader\":\"s1\",\"cluster_size\":3,\"replicas\":["   \
    "{\"name\":\"s2\",\"current\":true,\"active\":1000},"                           \
    "{\"name\":\"s3\",\"offline\":true}]}},"                                        \
    "\"http_req_stats\":{\"/varz\":7,\"/connz\":3},"                                \
    "\"tags\":[\"az:a\"],\"trusted_operators_jwt\":[\"jwt1\"],"                     \
    "\"trusted_operators_claim\":[{\"sub\":\"OABC\",\"nats\":{\"type\":\"operator\"}}," \
    "{\"sub\":\"ODEF\"}],"                                                          \
    "\"system_account\":\"$SYS\","                                                  \
    "\"ocsp_peer_cache\":{\"cache_type\":\"local\",\"cache_hits\":5},"              \
    "\"slow_consumer_stats\":{\"clients\":1,\"routes\":2,\"gateways\":3,"           \
    "\"leafs\":4}}}"

void
test_Varz(void)
{
    natsSubscription *sub  = NULL;
    natsSysVarzResp  *resp = NULL;
    natsSysVarz      *v    = NULL;
    fakeResponder     fr;

    SETUP;

    memset(&fr, 0, sizeof(fr));
    fr.reply = VARZ_REPLY;

    test("Subscribe the stand-in responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.VARZ",
                                 _healthzResponder, &fr);
    testCond(s == NATS_OK);

    test("Request: ");
    s = natsSysClient_Varz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_OK) && (resp != NULL));
    v = &resp->Varz;

    test("Strings and ints decoded: ");
    testCond((strcmp(v->ID, "SRV1") == 0) && (strcmp(v->Name, "s1") == 0)
             && (strcmp(v->Version, "2.14.0") == 0) && (v->Proto == 1)
             && (strcmp(v->GoVersion, "go1.23") == 0) && (v->Port == 4222)
             && v->AuthRequired && !v->TLSRequired);

    test("Narrow widths decoded: ");
    testCond((v->MaxControlLine == 4096) && (v->MaxPayload == 1048576)
             && (v->MaxPending == 67108864) && (v->Subscriptions == 42));

    // Nanoseconds, not the milliseconds used everywhere else in orbit.c.
    test("Durations are nanoseconds: ");
    testCond((v->PingInterval == 120000000000LL) && (v->WriteDeadline == 10000000000LL));

    test("Doubles decoded: ");
    testCond((v->AuthTimeout > 2.4) && (v->AuthTimeout < 2.6) && (v->TLSTimeout > 0.4)
             && (v->TLSTimeout < 0.6));

    // Would saturate at INT64_MAX through natsJSON_GetInt.
    test("A uint64 above INT64_MAX survives: ");
    testCond(v->TotalConnections == UINT64_MAX);

    test("String arrays decoded: ");
    testCond((v->ClientConnectURLsCount == 2)
             && (strcmp(v->ClientConnectURLs[0], "a:1") == 0)
             && (strcmp(v->ClientConnectURLs[1], "b:2") == 0)
             && (v->WSConnectURLsCount == 0) && (v->WSConnectURLs == NULL)
             && (v->TagsCount == 1) && (strcmp(v->Tags[0], "az:a") == 0));

    test("A struct embedded by value decoded: ");
    testCond((strcmp(v->Cluster.Name, "C1") == 0) && (v->Cluster.Port == 6222)
             && (v->Cluster.PoolSize == 3) && (v->Cluster.URLsCount == 1)
             && (strcmp(v->Cluster.URLs[0], "u1") == 0));

    test("A value array inside an embedded struct decoded: ");
    testCond((v->Gateway.GatewaysCount == 2)
             && (strcmp(v->Gateway.Gateways[0].Name, "R1") == 0)
             && (v->Gateway.Gateways[0].URLsCount == 2)
             && (strcmp(v->Gateway.Gateways[0].URLs[1], "g2") == 0)
             && (strcmp(v->Gateway.Gateways[1].Name, "R2") == 0)
             && (v->Gateway.Gateways[1].URLsCount == 0));

    test("A pointer struct nested two levels down decoded: ");
    testCond((v->LeafNode.RemotesCount == 1)
             && (strcmp(v->LeafNode.Remotes[0].LocalAccount, "A") == 0)
             && (v->LeafNode.Remotes[0].Deny != NULL)
             && (v->LeafNode.Remotes[0].Deny->ExportsCount == 1)
             && (v->LeafNode.Remotes[0].Deny->ImportsCount == 2)
             && (strcmp(v->LeafNode.Remotes[0].Deny->Imports[1], "i2") == 0));

    test("MQTT and WebSocket decoded: ");
    testCond((v->MQTT.Port == 1883) && (v->MQTT.AckWait == 30000000000LL)
             && (v->MQTT.MaxAckPending == 1024) && (v->Websocket.Port == 8080)
             && (v->Websocket.HandshakeTimeout == 2000000000LL)
             && (v->Websocket.AllowedOriginsCount == 1));

    test("The shared JetStream types decoded: ");
    testCond((v->JetStream.Config != NULL) && (v->JetStream.Config->MaxMemory == 100)
             && (v->JetStream.Config->SyncInterval == 5000000000LL)
             && (strcmp(v->JetStream.Config->StoreDir, "/tmp") == 0)
             && (v->JetStream.Stats != NULL) && (v->JetStream.Stats->Accounts == 3)
             && (v->JetStream.Stats->API.Total == 10)
             && (v->JetStream.Stats->API.Errors == 2));

    test("A pointer array decoded: ");
    testCond((v->JetStream.Meta != NULL) && (v->JetStream.Meta->Size == 3)
             && (v->JetStream.Meta->ReplicasCount == 2)
             && (strcmp(v->JetStream.Meta->Replicas[0]->Name, "s2") == 0)
             && v->JetStream.Meta->Replicas[0]->Current
             && (v->JetStream.Meta->Replicas[0]->Active == 1000)
             && v->JetStream.Meta->Replicas[1]->Offline
             && !v->JetStream.Meta->Replicas[1]->Current);

    // A JSON object on the wire; C keeps the entries in document order.
    test("The map field decoded in document order: ");
    testCond((v->HTTPReqStatsCount == 2)
             && (strcmp(v->HTTPReqStats[0].Path, "/varz") == 0)
             && (v->HTTPReqStats[0].Count == 7)
             && (strcmp(v->HTTPReqStats[1].Path, "/connz") == 0)
             && (v->HTTPReqStats[1].Count == 3));

    // No JWT claims model exists, so the subtrees come back as JSON text.
    test("Operator claims come back as raw JSON: ");
    testCond((v->TrustedOperatorsClaimJSONCount == 2)
             && (strcmp(v->TrustedOperatorsClaimJSON[0],
                        "{\"sub\":\"OABC\",\"nats\":{\"type\":\"operator\"}}")
                 == 0)
             && (strcmp(v->TrustedOperatorsClaimJSON[1], "{\"sub\":\"ODEF\"}") == 0)
             && (v->TrustedOperatorsJwtCount == 1)
             && (strcmp(v->TrustedOperatorsJwt[0], "jwt1") == 0));

    test("Pointer structs decoded: ");
    testCond((v->OCSPResponseCache != NULL)
             && (strcmp(v->OCSPResponseCache->Type, "local") == 0)
             && (v->OCSPResponseCache->Hits == 5)
             && (v->OCSPResponseCache->Misses == 0)
             && (v->SlowConsumersStats != NULL)
             && (v->SlowConsumersStats->Clients == 1)
             && (v->SlowConsumersStats->Leafs == 4));

    natsSysVarzResp_Destroy(resp);
    natsSubscription_Destroy(sub);
    TEARDOWN;
}

// VARZ was the one endpoint with no request-payload test: natsSysVarzOptions
// was never constructed anywhere in the suite, so its whole server filter
// could have been dropped from the marshaller unnoticed.
void
test_VarzRequestPayload(void)
{
    natsSubscription *sub  = NULL;
    natsSysVarzResp  *resp = NULL;
    natsSysVarzOptions opts;
    fakeResponder     fr;
    const char       *tags[] = {"az:a", "az:b"};

    SETUP;

    memset(&fr, 0, sizeof(fr));
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"server_id\":\"SRV1\"}}";

    test("Subscribe the stand-in responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.VARZ", _healthzResponder, &fr);
    testCond(s == NATS_OK);

    test("Zeroed options marshal to an empty object: ");
    natsSysVarzOptions_Init(&opts);
    s = natsSysClient_Varz(&resp, sys, "SRV1", &opts, 2000);
    if (s != NATS_OK)
        FAIL("request failed");
    natsSysVarzResp_Destroy(resp);
    resp = NULL;
    testCond(strcmp(fr.lastRequest, "{}") == 0);

    test("The event filter is flattened into the request object: ");
    natsSysVarzOptions_Init(&opts);
    opts.Filter.Name      = "s1";
    opts.Filter.Cluster   = "C1";
    opts.Filter.Host      = "10.0.0.1";
    opts.Filter.Tags      = tags;
    opts.Filter.TagsCount = 2;
    opts.Filter.Domain    = "hub";
    s                     = natsSysClient_Varz(&resp, sys, "SRV1", &opts, 2000);
    if (s != NATS_OK)
        FAIL("request failed");
    natsSysVarzResp_Destroy(resp);
    resp = NULL;
    testCond(strcmp(fr.lastRequest,
                    "{\"server_name\":\"s1\",\"cluster\":\"C1\",\"host\":\"10.0.0.1\","
                    "\"tags\":[\"az:a\",\"az:b\"],\"domain\":\"hub\"}")
             == 0);

    // A partly filled array is a caller bug. Serialized, the NULL would become
    // "" and match no server: the ping would come back empty and the by-ID
    // request time out, with nothing to say why.
    test("A NULL tag entry is rejected before anything is sent: ");
    {
        const char *holey[] = {"az:a", NULL};

        natsSysVarzOptions_Init(&opts);
        opts.Filter.Tags      = holey;
        opts.Filter.TagsCount = 2;
        fr.lastRequest[0]     = '\0';
        s                     = natsSysClient_Varz(&resp, sys, "SRV1", &opts, 2000);
        testCond((s == NATS_INVALID_ARG) && (resp == NULL) && (fr.lastRequest[0] == '\0'));
    }

    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_VarzDefaults(void)
{
    natsSubscription *sub  = NULL;
    natsSysVarzResp  *resp = NULL;
    fakeResponder     fr;

    SETUP;

    memset(&fr, 0, sizeof(fr));
    // A payload with none of the optional keys: everything must land on its
    // zero value, and every pointer member must stay NULL.
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"server_id\":\"SRV1\"}}";

    test("Subscribe the stand-in responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.VARZ",
                                 _healthzResponder, &fr);
    testCond(s == NATS_OK);

    test("Request: ");
    s = natsSysClient_Varz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_OK) && (resp != NULL));

    test("Absent scalars keep their zero value: ");
    testCond((resp->Varz.Proto == 0) && (resp->Varz.Port == 0)
             && !resp->Varz.AuthRequired && (resp->Varz.PingInterval == 0)
             && (resp->Varz.TotalConnections == 0));

    test("Absent strings are NULL: ");
    testCond((resp->Varz.Name == NULL) && (resp->Varz.Version == NULL)
             && (resp->Varz.Uptime == NULL)
             && (strcmp(resp->Varz.ID, "SRV1") == 0));

    test("Absent arrays are NULL with a count of 0: ");
    testCond((resp->Varz.ClientConnectURLs == NULL)
             && (resp->Varz.ClientConnectURLsCount == 0) && (resp->Varz.Tags == NULL)
             && (resp->Varz.HTTPReqStats == NULL)
             && (resp->Varz.HTTPReqStatsCount == 0)
             && (resp->Varz.TrustedOperatorsClaimJSON == NULL));

    test("Absent pointer structs are NULL: ");
    testCond((resp->Varz.OCSPResponseCache == NULL)
             && (resp->Varz.SlowConsumersStats == NULL)
             && (resp->Varz.JetStream.Config == NULL)
             && (resp->Varz.JetStream.Stats == NULL)
             && (resp->Varz.JetStream.Meta == NULL)
             && (resp->Varz.LeafNode.Remotes == NULL));

    test("Absent value structs are zeroed: ");
    testCond((resp->Varz.Cluster.Name == NULL) && (resp->Varz.Cluster.Port == 0)
             && (resp->Varz.MQTT.Port == 0) && (resp->Varz.Websocket.Port == 0));

    // JSON null must behave exactly like an absent key.
    test("An explicit null behaves like an absent key: ");
    natsSysVarzResp_Destroy(resp);
    resp     = NULL;
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"server_name\":null,"
               "\"port\":null,\"tags\":null,\"cluster\":null,"
               "\"ocsp_peer_cache\":null,\"http_req_stats\":null}}";
    s = natsSysClient_Varz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_OK) && (resp->Varz.Name == NULL) && (resp->Varz.Port == 0)
             && (resp->Varz.Tags == NULL) && (resp->Varz.Cluster.Name == NULL)
             && (resp->Varz.OCSPResponseCache == NULL)
             && (resp->Varz.HTTPReqStats == NULL));

    // http_req_stats is the one map on the wire, so its values are decoded
    // outside the field table. They follow the same null rule regardless.
    test("A null map value leaves a zero count: ");
    natsSysVarzResp_Destroy(resp);
    resp     = NULL;
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"data\":"
               "{\"http_req_stats\":{\"/varz\":7,\"/connz\":null}}}";
    s        = natsSysClient_Varz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_OK) && (resp != NULL) && (resp->Varz.HTTPReqStatsCount == 2)
             && (strcmp(resp->Varz.HTTPReqStats[0].Path, "/varz") == 0)
             && (resp->Varz.HTTPReqStats[0].Count == 7)
             && (strcmp(resp->Varz.HTTPReqStats[1].Path, "/connz") == 0)
             && (resp->Varz.HTTPReqStats[1].Count == 0));

    // The other half of the rule: tolerating null must not mean tolerating a
    // value that is simply the wrong type.
    test("A wrong-typed map value is still rejected: ");
    natsSysVarzResp_Destroy(resp);
    resp     = NULL;
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"data\":"
               "{\"http_req_stats\":{\"/varz\":\"seven\"}}}";
    s        = natsSysClient_Varz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_ERR) && (resp == NULL));

    natsSysVarzResp_Destroy(resp);
    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_VarzPing(void)
{
    natsSubscription   *subs[2] = {NULL, NULL};
    fakeResponder       frs[2];
    natsSysVarzRespList list = {NULL, 0};
    int                 i;

    static const char *replies[2] = {
        "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"server_name\":\"s1\",\"cores\":4}}",
        "{\"server\":{\"id\":\"SRV2\"},\"data\":{\"server_name\":\"s2\",\"cores\":8}}",
    };

    SETUP;

    test("Subscribe two stand-in responders: ");
    for (i = 0; (i < 2) && (s == NATS_OK); i++)
    {
        memset(&frs[i], 0, sizeof(frs[i]));
        frs[i].reply = replies[i];
        s = natsConnection_Subscribe(&subs[i], nc, "$SYS.REQ.SERVER.*.VARZ",
                                     _healthzResponder, &frs[i]);
    }
    testCond(s == NATS_OK);

    test("Ping gathers both: ");
    s = natsSysClient_VarzPing(&list, sys, NULL, 2000);
    testCond((s == NATS_OK) && (list.Count == 2));

    test("Both payloads decoded: ");
    {
        int cores = 0;

        for (i = 0; i < list.Count; i++)
            cores += list.Resps[i]->Varz.Cores;
        testCond(cores == 12);
    }

    natsSysVarzRespList_Destroy(&list);
    for (i = 0; i < 2; i++)
        natsSubscription_Destroy(subs[i]);
    TEARDOWN;
}

//
// CONNZ / SUBSZ
//

// A paging stand-in: serves `total` synthetic connections `limit` at a time,
// reading the offset back out of the request so the client's own paging
// arithmetic is what drives it.
typedef struct
{
    int  total;        ///< Reported total.
    int  perPage;      ///< Connections to put in each page.
    int  forceEmptyAt; ///< Offset at which to return an empty page; -1 to disable.
    int  requests;     ///< Requests served.
    char lastRequest[512];
} pagingResponder;

// A deliberately crude scanner rather than natsJSON_Parse: the stand-in
// responders check what the marshaller *wrote*, so driving them with the same
// parser the library reads with could hide a bug in either.
static int
_jsonInt(const char *json, const char *key)
{
    char        needle[64];
    const char *p;

    snprintf(needle, sizeof(needle), "\"%s\":", key);
    p = strstr(json, needle);
    if (p == NULL)
        return 0;
    return atoi(p + strlen(needle));
}

// Records the request and works out the window this page should carry. The
// three stand-ins below differ only in how they render an element and wrap it
// in an envelope, so everything up to that point lives here.
static int
_pageWindow(pagingResponder *pr, natsMsg *msg, int *offsetOut, int *limitOut)
{
    int offset;
    int limit;
    int n;

    snprintf(pr->lastRequest, sizeof(pr->lastRequest), "%.*s",
             natsMsg_GetDataLength(msg), natsMsg_GetData(msg));
    pr->requests++;

    offset = _jsonInt(pr->lastRequest, "offset");
    limit  = _jsonInt(pr->lastRequest, "limit");
    if (limit <= 0)
        limit = pr->perPage;

    n = pr->total - offset;
    if (n > limit)
        n = limit;
    if (n < 0)
        n = 0;
    if ((pr->forceEmptyAt >= 0) && (offset >= pr->forceEmptyAt))
        n = 0;

    *offsetOut = offset;
    *limitOut  = limit;
    return n;
}

static void
_connzPagingResponder(natsConnection *nc, natsSubscription *sub, natsMsg *msg,
                      void *closure)
{
    pagingResponder *pr = (pagingResponder *) closure;
    char             reply[4096];
    char             conns[3072];
    int              offset;
    int              limit;
    int              n;
    int              i;
    int              off = 0;

    (void) sub;

    n = _pageWindow(pr, msg, &offset, &limit);

    conns[0] = '\0';
    for (i = 0; i < n; i++)
        off += snprintf(conns + off, sizeof(conns) - (size_t) off, "%s{\"cid\":%d}",
                        (i > 0 ? "," : ""), offset + i + 1);

    snprintf(reply, sizeof(reply),
             "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"server_id\":\"SRV1\","
             "\"total\":%d,\"offset\":%d,\"limit\":%d,\"num_connections\":%d,"
             "\"connections\":[%s]}}",
             pr->total, offset, limit, n, conns);

    natsConnection_PublishString(nc, natsMsg_GetReply(msg), reply);
    natsMsg_Destroy(msg);
}

// Tallies a walk: how many pages arrived, how many items in total, and the
// total the server reported on the first page.
typedef struct
{
    int  pages;
    int  items;
    int  total;     ///< Total reported by the first page.
    int  stopAfter; ///< Return false once this many pages have been seen; 0 = never.
    bool sawEmpty;
} pageCounter;

// The body of every page handler in this file. Only the two members holding
// the item count and the total differ between endpoints.
static bool
_countPage(void *closure, int items, int total)
{
    pageCounter *pc = (pageCounter *) closure;

    if (pc->pages == 0)
        pc->total = total;
    pc->pages++;
    pc->items += items;
    if (items == 0)
        pc->sawEmpty = true;

    return (pc->stopAfter == 0) || (pc->pages < pc->stopAfter);
}

static bool
_countConnzPages(const natsSysConnzResp *page, void *closure)
{
    return _countPage(closure, page->Connz.ConnsCount, page->Connz.Total);
}

void
test_ConnzRequestPayload(void)
{
    natsSubscription   *sub  = NULL;
    natsSysConnzResp   *resp = NULL;
    natsSysConnzOptions opts;
    pagingResponder     pr;

    SETUP;

    memset(&pr, 0, sizeof(pr));
    pr.total        = 0;
    pr.perPage      = 10;
    pr.forceEmptyAt = -1;

    test("Subscribe the stand-in responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.CONNZ",
                                 _connzPagingResponder, &pr);
    testCond(s == NATS_OK);

    // CONNZ sends all twelve of its fields whatever their value, so an
    // "empty" request is a full twelve-key object. This is the assertion that
    // pins that down; if it ever starts producing "{}" the library has
    // silently changed what the server sees.
    test("Default options emit every ConnzOptions key: ");
    s = natsSysClient_Connz(&resp, sys, "SRV1", NULL, 2000);
    if (s != NATS_OK)
        FAIL("request failed");
    natsSysConnzResp_Destroy(resp);
    resp = NULL;
    testCond(strcmp(pr.lastRequest,
                    "{\"sort\":\"\",\"auth\":false,\"subscriptions\":false,"
                    "\"subscriptions_detail\":false,\"offset\":0,\"limit\":0,"
                    "\"cid\":0,\"mqtt_client\":\"\",\"state\":0,\"user\":\"\","
                    "\"acc\":\"\",\"filter_subject\":\"\"}")
             == 0);

    test("Set fields are emitted, and unset filter keys are omitted: ");
    natsSysConnzOptions_Init(&opts);
    opts.Sort          = NATS_SYS_SORT_SUBS;
    opts.Username      = true;
    opts.Subscriptions = true;
    opts.Limit         = 5;
    opts.CID           = 7;
    opts.State         = natsSysConnAll;
    opts.Account       = "JS";
    opts.Filter.Name   = "s1";
    s                  = natsSysClient_Connz(&resp, sys, "SRV1", &opts, 2000);
    if (s != NATS_OK)
        FAIL("request failed");
    natsSysConnzResp_Destroy(resp);
    resp = NULL;
    testCond(strcmp(pr.lastRequest,
                    "{\"sort\":\"subs\",\"auth\":true,\"subscriptions\":true,"
                    "\"subscriptions_detail\":false,\"offset\":0,\"limit\":5,"
                    "\"cid\":7,\"mqtt_client\":\"\",\"state\":2,\"user\":\"\","
                    "\"acc\":\"JS\",\"filter_subject\":\"\","
                    "\"server_name\":\"s1\"}")
             == 0);

    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_Connz(void)
{
    natsSubscription *sub  = NULL;
    natsSysConnzResp *resp = NULL;
    fakeResponder     fr;

    SETUP;

    memset(&fr, 0, sizeof(fr));
    fr.reply =
        "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"server_id\":\"SRV1\","
        "\"now\":\"2026-08-12T11:00:00Z\",\"num_connections\":1,\"total\":1,"
        "\"offset\":0,\"limit\":1024,\"connections\":[{"
        "\"cid\":7,\"kind\":\"Client\",\"type\":\"nats\",\"ip\":\"127.0.0.1\","
        "\"port\":54321,\"start\":\"2026-08-12T10:00:00Z\","
        "\"last_activity\":\"2026-08-12T10:59:00Z\",\"rtt\":\"1ms\","
        "\"uptime\":\"1h\",\"idle\":\"1m\",\"pending_bytes\":42,"
        "\"in_msgs\":10,\"out_msgs\":20,\"in_bytes\":100,\"out_bytes\":200,"
        "\"subscriptions\":3,\"name\":\"app\",\"lang\":\"c\",\"version\":\"3.8\","
        "\"tls_version\":\"1.3\",\"tls_cipher_suite\":\"TLS_AES_128_GCM_SHA256\","
        "\"tls_peer_certs\":[{\"subject\":\"CN=a\",\"cert_sha256\":\"abc\"}],"
        "\"tls_first\":true,\"authorized_user\":\"admin\",\"account\":\"JS\","
        "\"subscriptions_list\":[\"foo\",\"bar\"],"
        "\"subscriptions_list_detail\":[{\"account\":\"JS\",\"subject\":\"foo\","
        "\"qgroup\":\"q1\",\"sid\":\"1\",\"msgs\":5,\"max\":10,\"cid\":7}],"
        "\"jwt\":\"ey\",\"issuer_key\":\"OABC\",\"name_tag\":\"tag\","
        "\"tags\":[\"t1\"],\"mqtt_client\":\"m1\"}]}}";

    test("Subscribe the stand-in responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.CONNZ",
                                 _healthzResponder, &fr);
    testCond(s == NATS_OK);

    test("Request: ");
    s = natsSysClient_Connz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_OK) && (resp != NULL));

    test("Page metadata decoded: ");
    testCond((strcmp(resp->Connz.ID, "SRV1") == 0) && (resp->Connz.NumConns == 1)
             && (resp->Connz.Total == 1) && (resp->Connz.Offset == 0)
             && (resp->Connz.Limit == 1024) && (resp->Connz.ConnsCount == 1));

    test("Connection scalars decoded: ");
    testCond((resp->Connz.Conns[0]->Cid == 7)
             && (strcmp(resp->Connz.Conns[0]->Kind, "Client") == 0)
             && (resp->Connz.Conns[0]->Port == 54321)
             && (resp->Connz.Conns[0]->Pending == 42)
             && (resp->Connz.Conns[0]->OutBytes == 200)
             && (resp->Connz.Conns[0]->NumSubs == 3)
             && resp->Connz.Conns[0]->TLSFirst);

    test("An open connection has no stop time or reason: ");
    testCond((resp->Connz.Conns[0]->Stop == NULL)
             && (resp->Connz.Conns[0]->Reason == NULL));

    test("TLS peer certs decoded: ");
    testCond((resp->Connz.Conns[0]->TLSPeerCertsCount == 1)
             && (strcmp(resp->Connz.Conns[0]->TLSPeerCerts[0]->Subject, "CN=a") == 0)
             && (strcmp(resp->Connz.Conns[0]->TLSPeerCerts[0]->CertSha256, "abc") == 0)
             && (resp->Connz.Conns[0]->TLSPeerCerts[0]->SubjectPKISha256 == NULL));

    test("Subscription lists decoded: ");
    testCond((resp->Connz.Conns[0]->SubsCount == 2)
             && (strcmp(resp->Connz.Conns[0]->Subs[1], "bar") == 0)
             && (resp->Connz.Conns[0]->SubsDetailCount == 1)
             && (strcmp(resp->Connz.Conns[0]->SubsDetail[0].Subject, "foo") == 0)
             && (strcmp(resp->Connz.Conns[0]->SubsDetail[0].Queue, "q1") == 0)
             && (resp->Connz.Conns[0]->SubsDetail[0].Msgs == 5)
             && (resp->Connz.Conns[0]->SubsDetail[0].Max == 10)
             && (resp->Connz.Conns[0]->SubsDetail[0].Cid == 7));

    test("JWT fields decoded: ");
    testCond((strcmp(resp->Connz.Conns[0]->IssuerKey, "OABC") == 0)
             && (resp->Connz.Conns[0]->TagsCount == 1)
             && (strcmp(resp->Connz.Conns[0]->Tags[0], "t1") == 0)
             && (strcmp(resp->Connz.Conns[0]->MQTTClient, "m1") == 0));

    natsSysConnzResp_Destroy(resp);
    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_ConnzEach(void)
{
    natsSubscription   *sub = NULL;
    pagingResponder     pr;
    pageCounter         pc;
    natsSysConnzOptions opts;

    SETUP;

    memset(&pr, 0, sizeof(pr));
    pr.total        = 13;
    pr.perPage      = 5;
    pr.forceEmptyAt = -1;

    test("Subscribe the paging responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.CONNZ",
                                 _connzPagingResponder, &pr);
    testCond(s == NATS_OK);

    natsSysConnzOptions_Init(&opts);
    opts.Limit = 5;

    test("13 connections at 5 per page is 3 pages: ");
    memset(&pc, 0, sizeof(pc));
    s = natsSysClient_ConnzEach(sys, "SRV1", &opts, 5000, _countConnzPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 3) && (pc.items == 13));

    test("A handler returning false stops the walk: ");
    memset(&pc, 0, sizeof(pc));
    pc.stopAfter  = 2;
    pr.requests   = 0;
    s             = natsSysClient_ConnzEach(sys, "SRV1", &opts, 5000, _countConnzPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 2) && (pr.requests == 2));

    test("A single full page ends the walk without a second request: ");
    memset(&pc, 0, sizeof(pc));
    pr.total    = 5;
    pr.requests = 0;
    s           = natsSysClient_ConnzEach(sys, "SRV1", &opts, 5000, _countConnzPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 1) && (pr.requests == 1));

    test("No connections still delivers one page: ");
    memset(&pc, 0, sizeof(pc));
    pr.total = 0;
    s        = natsSysClient_ConnzEach(sys, "SRV1", &opts, 5000, _countConnzPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 1) && (pc.items == 0));

    test("Bad arguments are rejected: ");
    testCond((natsSysClient_ConnzEach(NULL, "SRV1", &opts, 0, _countConnzPages, &pc)
              == NATS_INVALID_ARG)
             && (natsSysClient_ConnzEach(sys, "SRV1", &opts, 0, NULL, &pc)
                 == NATS_INVALID_ARG)
             && (natsSysClient_ConnzEach(sys, "SRV1", &opts, -1, _countConnzPages, &pc)
                 == NATS_INVALID_ARG));

    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_ConnzEachEmptyPageGuard(void)
{
    natsSubscription   *sub = NULL;
    pagingResponder     pr;
    pageCounter         pc;
    natsSysConnzOptions opts;

    SETUP;

    // The server claims 100 connections but stops producing them after the
    // first page — the shape of nats-server#7009, and what happens naturally
    // when connections close between pages. Without the empty-page guard the
    // walk never terminates.
    memset(&pr, 0, sizeof(pr));
    pr.total        = 100;
    pr.perPage      = 5;
    pr.forceEmptyAt = 5;

    test("Subscribe the paging responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.CONNZ",
                                 _connzPagingResponder, &pr);
    testCond(s == NATS_OK);

    natsSysConnzOptions_Init(&opts);
    opts.Limit = 5;

    test("An empty page ends the walk instead of looping: ");
    memset(&pc, 0, sizeof(pc));
    s = natsSysClient_ConnzEach(sys, "SRV1", &opts, 5000, _countConnzPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 2) && (pc.items == 5) && pc.sawEmpty);

    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_ConnzPingEach(void)
{
    natsSubscription    *sub = NULL;
    pagingResponder      pr;
    pageCounter          pc;
    natsSysConnzOptions  opts;
    natsSysConnzWalkList walks = {NULL, 0};
    natsSysConnzWalk    *mine  = NULL;
    int                  i;

    SETUP;

    memset(&pr, 0, sizeof(pr));
    pr.total        = 13;
    pr.perPage      = 5;
    pr.forceEmptyAt = -1;

    test("Subscribe the paging responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.CONNZ",
                                 _connzPagingResponder, &pr);
    testCond(s == NATS_OK);

    {
        // Built on the stack and freed before the walks run, to prove the walk
        // copied them rather than borrowing.
        char buf[8];

        snprintf(buf, sizeof(buf), "%s", "cid");
        natsSysConnzOptions_Init(&opts);
        opts.Limit = 5;
        opts.Sort  = buf;

        // A plain nats-server answers $SYS.REQ.SERVER.PING.CONNZ itself even
        // with no system account configured — CONNZ is the only endpoint of
        // the six that does, the others report no responders. So the gather
        // picks up the real server alongside the stand-in, and the walk under
        // test has to be selected by ID rather than assumed to be alone.
        test("Ping produces a walk per responding server: ");
        s = natsSysClient_ConnzPingEach(&walks, sys, &opts, 2000);
        testCond((s == NATS_OK) && (walks.Count >= 1));

        memset(buf, 0xff, sizeof(buf));
        memset(&opts, 0xff, sizeof(opts));
    }

    test("One walk is for the stand-in server: ");
    for (i = 0; i < walks.Count; i++)
    {
        const char *id = natsSysConnzWalk_ServerID(walks.Walks[i]);

        if ((id != NULL) && (strcmp(id, "SRV1") == 0))
            mine = walks.Walks[i];
    }
    testCond(mine != NULL);

    test("Running it yields all 3 pages, starting from the ping's page: ");
    memset(&pc, 0, sizeof(pc));
    s = natsSysConnzWalk_Run(mine, 5000, _countConnzPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 3) && (pc.items == 13));

    // The point of scribbling over the caller's buffer above: the walk's own
    // requests must still carry the sort it was given. CONNZ emits "sort"
    // unconditionally, so a borrowed pointer would show up here as garbage.
    test("The walk's own requests carried the copied sort: ");
    testCond(strstr(pr.lastRequest, "\"sort\":\"cid\"") != NULL);

    test("Re-running a finished walk yields nothing: ");
    memset(&pc, 0, sizeof(pc));
    s = natsSysConnzWalk_Run(mine, 5000, _countConnzPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 0));

    test("Bad arguments are rejected: ");
    testCond((natsSysConnzWalk_Run(NULL, 0, _countConnzPages, &pc) == NATS_INVALID_ARG)
             && (natsSysConnzWalk_Run(mine, 0, NULL, &pc) == NATS_INVALID_ARG)
             && (natsSysConnzWalk_ServerID(NULL) == NULL));

    natsSysConnzWalkList_Destroy(&walks);

    test("Destroying an already-destroyed walk list is safe: ");
    natsSysConnzWalkList_Destroy(&walks);
    testCond((walks.Walks == NULL) && (walks.Count == 0));

    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_SubszRequestPayload(void)
{
    natsSubscription   *sub  = NULL;
    natsSysSubszResp   *resp = NULL;
    natsSysSubszOptions opts;
    fakeResponder       fr;

    SETUP;

    memset(&fr, 0, sizeof(fr));
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"total\":0}}";

    test("Subscribe the stand-in responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.SUBSZ",
                                 _healthzResponder, &fr);
    testCond(s == NATS_OK);

    // SUBSZ is the mixed case: three keys always, two only when set.
    test("Default options emit the three always-sent keys: ");
    s = natsSysClient_Subsz(&resp, sys, "SRV1", NULL, 2000);
    if (s != NATS_OK)
        FAIL("request failed");
    natsSysSubszResp_Destroy(resp);
    resp = NULL;
    testCond(strcmp(fr.lastRequest,
                    "{\"offset\":0,\"limit\":0,\"subscriptions\":false}")
             == 0);

    test("Account and test are appended only when set: ");
    natsSysSubszOptions_Init(&opts);
    opts.Offset        = 10;
    opts.Limit         = 5;
    opts.Subscriptions = true;
    opts.Account       = "JS";
    opts.Test          = "foo.bar";
    s                  = natsSysClient_Subsz(&resp, sys, "SRV1", &opts, 2000);
    if (s != NATS_OK)
        FAIL("request failed");
    natsSysSubszResp_Destroy(resp);
    resp = NULL;
    testCond(strcmp(fr.lastRequest,
                    "{\"offset\":10,\"limit\":5,\"subscriptions\":true,"
                    "\"account\":\"JS\",\"test\":\"foo.bar\"}")
             == 0);

    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_Subsz(void)
{
    natsSubscription *sub  = NULL;
    natsSysSubszResp *resp = NULL;
    fakeResponder     fr;

    SETUP;

    memset(&fr, 0, sizeof(fr));
    // The sublist stats keys sit alongside the SUBSZ fields, not nested.
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"server_id\":\"SRV1\","
               "\"now\":\"2026-08-12T11:00:00Z\",\"num_subscriptions\":12,"
               "\"num_cache\":3,\"num_inserts\":40,\"num_removes\":28,"
               "\"num_matches\":7,\"cache_hit_rate\":0.75,\"max_fanout\":4,"
               "\"avg_fanout\":1.5,\"total\":2,\"offset\":0,\"limit\":1024,"
               "\"subscriptions_list\":["
               "{\"account\":\"JS\",\"subject\":\"foo\",\"sid\":\"1\",\"msgs\":5,"
               "\"cid\":7},"
               "{\"subject\":\"bar\",\"qgroup\":\"q\",\"sid\":\"2\",\"cid\":8}]}}";

    test("Subscribe the stand-in responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.SUBSZ",
                                 _healthzResponder, &fr);
    testCond(s == NATS_OK);

    test("Request: ");
    s = natsSysClient_Subsz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_OK) && (resp != NULL));

    test("Page metadata decoded: ");
    testCond((strcmp(resp->Subsz.ID, "SRV1") == 0) && (resp->Subsz.Total == 2)
             && (resp->Subsz.Offset == 0) && (resp->Subsz.Limit == 1024));

    // The sublist keys are flattened into the SUBSZ object on the wire.
    test("The flattened sublist stats are allocated and decoded: ");
    testCond((resp->Subsz.SublistStats != NULL)
             && (resp->Subsz.SublistStats->NumSubs == 12)
             && (resp->Subsz.SublistStats->NumCache == 3)
             && (resp->Subsz.SublistStats->NumInserts == 40)
             && (resp->Subsz.SublistStats->NumRemoves == 28)
             && (resp->Subsz.SublistStats->NumMatches == 7)
             && (resp->Subsz.SublistStats->CacheHitRate > 0.7)
             && (resp->Subsz.SublistStats->MaxFanout == 4)
             && (resp->Subsz.SublistStats->AvgFanout > 1.4));

    test("Subscription details decoded: ");
    testCond((resp->Subsz.SubsCount == 2)
             && (strcmp(resp->Subsz.Subs[0].Subject, "foo") == 0)
             && (strcmp(resp->Subsz.Subs[0].Account, "JS") == 0)
             && (resp->Subsz.Subs[0].Queue == NULL)
             && (resp->Subsz.Subs[0].Msgs == 5)
             && (strcmp(resp->Subsz.Subs[1].Subject, "bar") == 0)
             && (strcmp(resp->Subsz.Subs[1].Queue, "q") == 0)
             && (resp->Subsz.Subs[1].Account == NULL));

    // With none of the eight keys present the pointer must stay NULL.
    test("Without any sublist key the pointer stays NULL: ");
    natsSysSubszResp_Destroy(resp);
    resp     = NULL;
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"total\":0,\"offset\":0}}";
    s        = natsSysClient_Subsz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_OK) && (resp->Subsz.SublistStats == NULL)
             && (resp->Subsz.Subs == NULL) && (resp->Subsz.SubsCount == 0));

    test("A single sublist key is enough to allocate it: ");
    natsSysSubszResp_Destroy(resp);
    resp     = NULL;
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"max_fanout\":9}}";
    s        = natsSysClient_Subsz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_OK) && (resp->Subsz.SublistStats != NULL)
             && (resp->Subsz.SublistStats->MaxFanout == 9)
             && (resp->Subsz.SublistStats->NumSubs == 0));

    // Every other decoder path treats an explicit null as absent; an all-zero
    // SublistStats here would read as genuine statistics.
    test("An explicit null sublist key counts as absent: ");
    natsSysSubszResp_Destroy(resp);
    resp     = NULL;
    fr.reply = "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"num_subscriptions\":null}}";
    s        = natsSysClient_Subsz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_OK) && (resp->Subsz.SublistStats == NULL));

    natsSysSubszResp_Destroy(resp);
    natsSubscription_Destroy(sub);
    TEARDOWN;
}

// Serves `total` synthetic subscriptions `limit` at a time.
static void
_subszPagingResponder(natsConnection *nc, natsSubscription *sub, natsMsg *msg,
                      void *closure)
{
    pagingResponder *pr = (pagingResponder *) closure;
    char             reply[4096];
    char             subs[3072];
    int              offset;
    int              limit;
    int              n;
    int              i;
    int              off = 0;

    (void) sub;

    n = _pageWindow(pr, msg, &offset, &limit);

    subs[0] = '\0';
    for (i = 0; i < n; i++)
        off += snprintf(subs + off, sizeof(subs) - (size_t) off,
                        "%s{\"subject\":\"foo.%d\",\"sid\":\"%d\",\"cid\":%d}",
                        (i > 0 ? "," : ""), offset + i, offset + i + 1, offset + i + 1);

    snprintf(reply, sizeof(reply),
             "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"server_id\":\"SRV1\","
             "\"total\":%d,\"offset\":%d,\"limit\":%d,\"subscriptions_list\":[%s]}}",
             pr->total, offset, limit, subs);

    natsConnection_PublishString(nc, natsMsg_GetReply(msg), reply);
    natsMsg_Destroy(msg);
}

static bool
_countSubszPages(const natsSysSubszResp *page, void *closure)
{
    return _countPage(closure, page->Subsz.SubsCount, page->Subsz.Total);
}

void
test_SubszEach(void)
{
    natsSubscription   *sub = NULL;
    pagingResponder     pr;
    pageCounter         pc;
    natsSysSubszOptions opts;

    SETUP;

    memset(&pr, 0, sizeof(pr));
    pr.total        = 13;
    pr.perPage      = 5;
    pr.forceEmptyAt = -1;

    test("Subscribe the paging responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.SUBSZ",
                                 _subszPagingResponder, &pr);
    testCond(s == NATS_OK);

    natsSysSubszOptions_Init(&opts);
    opts.Limit         = 5;
    opts.Subscriptions = true;

    test("13 subscriptions at 5 per page is 3 pages: ");
    memset(&pc, 0, sizeof(pc));
    s = natsSysClient_SubszEach(sys, "SRV1", &opts, 5000, _countSubszPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 3) && (pc.items == 13));

    test("Each page asks for the offset the previous one ended at: ");
    testCond(_jsonInt(pr.lastRequest, "offset") == 10);

    test("A handler returning false stops the walk: ");
    memset(&pc, 0, sizeof(pc));
    pc.stopAfter = 2;
    pr.requests  = 0;
    s = natsSysClient_SubszEach(sys, "SRV1", &opts, 5000, _countSubszPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 2) && (pr.requests == 2));

    test("A single full page ends the walk without a second request: ");
    memset(&pc, 0, sizeof(pc));
    pr.total    = 5;
    pr.requests = 0;
    s = natsSysClient_SubszEach(sys, "SRV1", &opts, 5000, _countSubszPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 1) && (pr.requests == 1));

    test("No subscriptions still delivers one page: ");
    memset(&pc, 0, sizeof(pc));
    pr.total = 0;
    s = natsSysClient_SubszEach(sys, "SRV1", &opts, 5000, _countSubszPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 1) && (pc.items == 0));

    // This is the deviation the README documents, on the endpoint that
    // motivated it: nats-server#7009 makes SUBSZ report a total it then
    // declines to produce, so the unguarded loop would never return.
    test("An empty page ends the walk instead of looping: ");
    pr.total        = 100;
    pr.forceEmptyAt = 5;
    memset(&pc, 0, sizeof(pc));
    s = natsSysClient_SubszEach(sys, "SRV1", &opts, 5000, _countSubszPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 2) && (pc.items == 5) && pc.sawEmpty);

    test("Bad arguments are rejected: ");
    testCond((natsSysClient_SubszEach(NULL, "SRV1", &opts, 0, _countSubszPages, &pc)
              == NATS_INVALID_ARG)
             && (natsSysClient_SubszEach(sys, NULL, &opts, 0, _countSubszPages, &pc)
                 == NATS_INVALID_ARG)
             && (natsSysClient_SubszEach(sys, "", &opts, 0, _countSubszPages, &pc)
                 == NATS_INVALID_ARG)
             && (natsSysClient_SubszEach(sys, "SRV1", &opts, 0, NULL, &pc)
                 == NATS_INVALID_ARG)
             && (natsSysClient_SubszEach(sys, "SRV1", &opts, -1, _countSubszPages, &pc)
                 == NATS_INVALID_ARG));

    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_SubszPingEach(void)
{
    natsSubscription    *sub = NULL;
    pagingResponder      pr;
    pageCounter          pc;
    natsSysSubszOptions  opts;
    natsSysSubszWalkList walks = {NULL, 0};
    natsSysSubszWalk    *mine  = NULL;
    int                  i;

    SETUP;

    memset(&pr, 0, sizeof(pr));
    pr.total        = 13;
    pr.perPage      = 5;
    pr.forceEmptyAt = -1;

    test("Subscribe the paging responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.SUBSZ",
                                 _subszPagingResponder, &pr);
    testCond(s == NATS_OK);

    {
        // Built on the stack and overwritten before the walks run, to prove the
        // walk copied the options rather than borrowing them.
        char buf[8];

        snprintf(buf, sizeof(buf), "%s", "ACC");
        natsSysSubszOptions_Init(&opts);
        opts.Limit         = 5;
        opts.Subscriptions = true;
        opts.Account       = buf;

        test("Ping produces a walk per responding server: ");
        s = natsSysClient_SubszPingEach(&walks, sys, &opts, 2000);
        testCond((s == NATS_OK) && (walks.Count == 1));

        memset(buf, 0xff, sizeof(buf));
        memset(&opts, 0xff, sizeof(opts));
    }

    test("The walk is for the stand-in server: ");
    for (i = 0; i < walks.Count; i++)
    {
        const char *id = natsSysSubszWalk_ServerID(walks.Walks[i]);

        if ((id != NULL) && (strcmp(id, "SRV1") == 0))
            mine = walks.Walks[i];
    }
    testCond(mine != NULL);

    test("Running it yields all 3 pages, starting from the ping's page: ");
    memset(&pc, 0, sizeof(pc));
    s = natsSysSubszWalk_Run(mine, 5000, _countSubszPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 3) && (pc.items == 13));

    // Asserted only after the walk has run: before that, lastRequest still
    // holds the ping's own request, which was marshalled from the caller's
    // still-live options and so proves nothing about the walk's copy.
    test("The walk's own requests carried the copied account filter: ");
    testCond(strstr(pr.lastRequest, "\"account\":\"ACC\"") != NULL);

    test("Re-running a finished walk yields nothing: ");
    memset(&pc, 0, sizeof(pc));
    s = natsSysSubszWalk_Run(mine, 5000, _countSubszPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 0));

    test("Bad arguments are rejected: ");
    testCond((natsSysSubszWalk_Run(NULL, 0, _countSubszPages, &pc) == NATS_INVALID_ARG)
             && (natsSysSubszWalk_Run(mine, 0, NULL, &pc) == NATS_INVALID_ARG)
             && (natsSysSubszWalk_ServerID(NULL) == NULL));

    natsSysSubszWalkList_Destroy(&walks);

    test("Destroying an already-destroyed walk list is safe: ");
    natsSysSubszWalkList_Destroy(&walks);
    testCond((walks.Walks == NULL) && (walks.Count == 0));

    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_WalkEdges(void)
{
    natsSubscription    *sub   = NULL;
    natsSysSubszWalkList walks = {NULL, 0};
    pagingResponder      pr;
    pageCounter          pc;
    natsSysSubszOptions  opts;
    fakeResponder        anon;

    SETUP;

    memset(&anon, 0, sizeof(anon));

    memset(&pr, 0, sizeof(pr));
    pr.total        = 13;
    pr.perPage      = 5;
    pr.forceEmptyAt = -1;

    test("Subscribe the paging responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.SUBSZ",
                                 _subszPagingResponder, &pr);
    testCond(s == NATS_OK);

    natsSysSubszOptions_Init(&opts);
    opts.Limit = 5;

    // The natural way to write "no practical limit". Adding it to the current
    // time overflows, and a wrapped deadline lies in the past, so the walk used
    // to end before its first page.
    test("A timeout of INT64_MAX does not abort the walk: ");
    memset(&pc, 0, sizeof(pc));
    s = natsSysClient_SubszEach(sys, "SRV1", &opts, INT64_MAX, _countSubszPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 3) && (pc.items == 13));

    natsSubscription_Destroy(sub);
    sub = NULL;

    // Every page after the first is fetched by server ID, so a reply that did
    // not name its sender cannot be walked. Handing back such a walk would fail
    // every _Run with NATS_INVALID_ARG, a status reserved for the caller's own
    // arguments, and silently strand the other 95 reported subscriptions.
    test("Subscribe a responder that never names itself: ");
    anon.reply = "{\"server\":{\"name\":\"anon\"},\"data\":{\"total\":100,\"offset\":0,"
                 "\"limit\":5,\"subscriptions_list\":[{\"subject\":\"a\"}]}}";
    s          = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.PING.SUBSZ",
                                          _healthzResponder, &anon);
    if (s == NATS_OK)
        s = natsConnection_Flush(nc);
    testCond(s == NATS_OK);

    // Bounded to the one responder that exists: an unbounded gather has no way
    // to know it has heard everyone, so it sits out the full 300ms stall for a
    // reply it already has.
    test("A ping reply with no server ID is rejected, not turned into a walk: ");
    {
        natsSysClient    *bounded = NULL;
        natsSysClientOpts copts;

        natsSysClientOpts_Init(&copts);
        copts.ServerCount = 1;
        s                 = natsSysClient_Create(&bounded, nc, &copts);
        if (s == NATS_OK)
            s = natsSysClient_SubszPingEach(&walks, bounded, &opts, 2000);
        natsSysClient_Destroy(bounded);
    }
    testCond((s != NATS_OK) && (walks.Count == 0) && (walks.Walks == NULL));

    natsSysSubszWalkList_Destroy(&walks);
    natsSubscription_Destroy(sub);
    TEARDOWN;
}

//
// JSZ
//

// Serves `total` synthetic accounts `limit` at a time. The account count is
// reported through the flattened JetStreamStats key "accounts", which is what
// JSZ paginates against.
static void
_jszPagingResponder(natsConnection *nc, natsSubscription *sub, natsMsg *msg,
                    void *closure)
{
    pagingResponder *pr = (pagingResponder *) closure;
    char             reply[4096];
    char             accts[3072];
    int              offset;
    int              limit;
    int              n;
    int              i;
    int              off = 0;

    (void) sub;

    n = _pageWindow(pr, msg, &offset, &limit);

    // Account details only come back when "accounts":true was asked for.
    if (strstr(pr->lastRequest, "\"accounts\":true") == NULL)
        n = 0;

    accts[0] = '\0';
    for (i = 0; i < n; i++)
        off += snprintf(accts + off, sizeof(accts) - (size_t) off,
                        "%s{\"name\":\"ACC%d\",\"memory\":%d}", (i > 0 ? "," : ""),
                        offset + i + 1, offset + i);

    snprintf(reply, sizeof(reply),
             "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"server_id\":\"SRV1\","
             "\"accounts\":%d,\"streams\":2,\"consumers\":3,"
             "\"account_details\":[%s]}}",
             pr->total, accts);

    natsConnection_PublishString(nc, natsMsg_GetReply(msg), reply);
    natsMsg_Destroy(msg);
}

static bool
_countJszPages(const natsSysJszResp *page, void *closure)
{
    // JSZ has no Total; its pagination total is the account count in the
    // flattened stats.
    return _countPage(closure, page->JSInfo.AccountDetailsCount,
                      page->JSInfo.JetStreamStats.Accounts);
}

void
test_JszRequestPayload(void)
{
    natsSubscription *sub  = NULL;
    natsSysJszResp   *resp = NULL;
    natsSysJszOptions opts;
    pagingResponder   pr;

    SETUP;

    memset(&pr, 0, sizeof(pr));
    pr.total        = 0;
    pr.perPage      = 10;
    pr.forceEmptyAt = -1;

    test("Subscribe the stand-in responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.JSZ",
                                 _jszPagingResponder, &pr);
    testCond(s == NATS_OK);

    // Every JszOptions field is optional.
    test("Default options marshal to {}: ");
    s = natsSysClient_Jsz(&resp, sys, "SRV1", NULL, 2000);
    if (s != NATS_OK)
        FAIL("request failed");
    natsSysJszResp_Destroy(resp);
    resp = NULL;
    testCond(strcmp(pr.lastRequest, "{}") == 0);

    test("Only the set fields are emitted: ");
    natsSysJszOptions_Init(&opts);
    opts.Accounts         = true;
    opts.Streams          = true;
    opts.Limit            = 1;
    opts.RaftGroups       = true;
    opts.StreamLeaderOnly = true;
    opts.Filter.Domain    = "hub";
    s                     = natsSysClient_Jsz(&resp, sys, "SRV1", &opts, 2000);
    if (s != NATS_OK)
        FAIL("request failed");
    natsSysJszResp_Destroy(resp);
    resp = NULL;
    testCond(strcmp(pr.lastRequest,
                    "{\"accounts\":true,\"streams\":true,\"limit\":1,\"raft\":true,"
                    "\"stream_leader_only\":true,\"domain\":\"hub\"}")
             == 0);

    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_Jsz(void)
{
    natsSubscription *sub  = NULL;
    natsSysJszResp   *resp = NULL;
    fakeResponder     fr;

    SETUP;

    memset(&fr, 0, sizeof(fr));
    // JetStreamStats is embedded by value in both JSInfo and AccountDetail, so
    // its keys sit alongside their siblings rather than nested.
    fr.reply =
        "{\"server\":{\"id\":\"SRV1\"},\"data\":{\"server_id\":\"SRV1\","
        "\"now\":\"2026-08-12T11:00:00Z\",\"disabled\":false,"
        "\"config\":{\"max_memory\":100,\"max_storage\":200,\"store_dir\":\"/tmp\"},"
        "\"memory\":11,\"storage\":22,\"reserved_memory\":33,"
        "\"reserved_storage\":44,\"accounts\":2,\"ha_assets\":5,"
        "\"api\":{\"total\":7,\"errors\":1,\"inflight\":2},"
        "\"streams\":3,\"consumers\":4,\"messages\":500,\"bytes\":6000,"
        "\"meta_cluster\":{\"name\":\"M\",\"leader\":\"s1\",\"cluster_size\":3,"
        "\"replicas\":[{\"name\":\"s2\",\"current\":true}]},"
        "\"account_details\":[{"
        "\"name\":\"JS\",\"id\":\"AJS\",\"memory\":1,\"storage\":2,\"accounts\":0,"
        "\"api\":{\"total\":9},"
        "\"stream_detail\":[{"
        "\"name\":\"orders\",\"created\":\"2026-08-01T00:00:00Z\","
        "\"cluster\":{\"name\":\"C1\",\"leader\":\"s1\"},"
        "\"config\":{\"name\":\"orders\",\"subjects\":[\"o.>\"],\"max_msgs\":-1},"
        "\"state\":{\"messages\":10,\"bytes\":100},"
        "\"consumer_detail\":[{\"name\":\"c1\"},{\"name\":\"c2\"}],"
        "\"mirror\": {\"name\": \"m\"},"
        "\"sources\":[{\"name\":\"s\"}],"
        "\"stream_raft_group\":\"RG1\","
        "\"consumer_raft_groups\":[{\"name\":\"c1\",\"raft_group\":\"RGC1\"}]}]}]}}";

    test("Subscribe the stand-in responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.JSZ", _healthzResponder,
                                 &fr);
    testCond(s == NATS_OK);

    test("Request: ");
    s = natsSysClient_Jsz(&resp, sys, "SRV1", NULL, 2000);
    testCond((s == NATS_OK) && (resp != NULL));

    test("Top-level scalars decoded: ");
    testCond((strcmp(resp->JSInfo.ID, "SRV1") == 0) && !resp->JSInfo.Disabled
             && (resp->JSInfo.Streams == 3) && (resp->JSInfo.Consumers == 4)
             && (resp->JSInfo.Messages == 500) && (resp->JSInfo.Bytes == 6000));

    test("The nested config object decoded: ");
    testCond((resp->JSInfo.Config.MaxMemory == 100)
             && (resp->JSInfo.Config.MaxStore == 200)
             && (strcmp(resp->JSInfo.Config.StoreDir, "/tmp") == 0));

    // The flattened embed: these keys are siblings of "streams", not nested.
    test("The flattened JetStreamStats decoded: ");
    testCond((resp->JSInfo.JetStreamStats.Memory == 11)
             && (resp->JSInfo.JetStreamStats.Store == 22)
             && (resp->JSInfo.JetStreamStats.ReservedMemory == 33)
             && (resp->JSInfo.JetStreamStats.ReservedStore == 44)
             && (resp->JSInfo.JetStreamStats.Accounts == 2)
             && (resp->JSInfo.JetStreamStats.HAAssets == 5)
             && (resp->JSInfo.JetStreamStats.API.Total == 7)
             && (resp->JSInfo.JetStreamStats.API.Errors == 1)
             && (resp->JSInfo.JetStreamStats.API.Inflight == 2));

    test("The meta cluster decoded: ");
    testCond((resp->JSInfo.Meta != NULL) && (strcmp(resp->JSInfo.Meta->Name, "M") == 0)
             && (resp->JSInfo.Meta->Size == 3)
             && (resp->JSInfo.Meta->ReplicasCount == 1)
             && (strcmp(resp->JSInfo.Meta->Replicas[0]->Name, "s2") == 0));

    test("Account details decoded, with their own flattened stats: ");
    testCond((resp->JSInfo.AccountDetailsCount == 1)
             && (strcmp(resp->JSInfo.AccountDetails[0]->Name, "JS") == 0)
             && (strcmp(resp->JSInfo.AccountDetails[0]->Id, "AJS") == 0)
             && (resp->JSInfo.AccountDetails[0]->JetStreamStats.Memory == 1)
             && (resp->JSInfo.AccountDetails[0]->JetStreamStats.Store == 2)
             && (resp->JSInfo.AccountDetails[0]->JetStreamStats.API.Total == 9));

    test("Stream details decoded: ");
    testCond((resp->JSInfo.AccountDetails[0]->StreamsCount == 1)
             && (strcmp(resp->JSInfo.AccountDetails[0]->Streams[0].Name, "orders") == 0)
             && (strcmp(resp->JSInfo.AccountDetails[0]->Streams[0].RaftGroup, "RG1") == 0)
             && (resp->JSInfo.AccountDetails[0]->Streams[0].Created != NULL));

    // The six subtrees cnats cannot unmarshal come back as JSON text.
    // Verbatim, whitespace and all: the text is sliced out of the reply rather
    // than re-serialized, which is why "mirror" keeps its spaces.
    test("Stream subtrees come back as raw JSON: ");
    {
        natsSysStreamDetail *sd = &resp->JSInfo.AccountDetails[0]->Streams[0];

        testCond((strcmp(sd->ClusterJSON, "{\"name\":\"C1\",\"leader\":\"s1\"}") == 0)
                 && (strcmp(sd->ConfigJSON,
                            "{\"name\":\"orders\",\"subjects\":[\"o.>\"],"
                            "\"max_msgs\":-1}")
                     == 0)
                 && (strcmp(sd->StateJSON, "{\"messages\":10,\"bytes\":100}") == 0)
                 && (strcmp(sd->MirrorJSON, "{\"name\": \"m\"}") == 0)
                 && (sd->ConsumerJSONCount == 2)
                 && (strcmp(sd->ConsumerJSON[0], "{\"name\":\"c1\"}") == 0)
                 && (strcmp(sd->ConsumerJSON[1], "{\"name\":\"c2\"}") == 0)
                 && (sd->SourcesJSONCount == 1)
                 && (strcmp(sd->SourcesJSON[0], "{\"name\":\"s\"}") == 0));
    }

    test("Consumer raft groups decoded: ");
    testCond((resp->JSInfo.AccountDetails[0]->Streams[0].ConsumerRaftGroupsCount == 1)
             && (strcmp(resp->JSInfo.AccountDetails[0]
                            ->Streams[0]
                            .ConsumerRaftGroups[0]
                            ->RaftGroup,
                        "RGC1")
                 == 0));

    natsSysJszResp_Destroy(resp);
    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_JszEach(void)
{
    natsSubscription *sub = NULL;
    pagingResponder   pr;
    pageCounter       pc;
    natsSysJszOptions opts;

    SETUP;

    memset(&pr, 0, sizeof(pr));
    pr.total        = 3;
    pr.perPage      = 1;
    pr.forceEmptyAt = -1;

    test("Subscribe the paging responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.JSZ",
                                 _jszPagingResponder, &pr);
    testCond(s == NATS_OK);

    // Without Accounts there is nothing to page over: one request, one page,
    // however many accounts exist.
    test("Without Accounts it is a single request: ");
    natsSysJszOptions_Init(&opts);
    memset(&pc, 0, sizeof(pc));
    pr.requests = 0;
    s           = natsSysClient_JszEach(sys, "SRV1", &opts, 5000, _countJszPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 1) && (pc.items == 0) && (pr.requests == 1));

    test("With Accounts and a limit of 1, three accounts is three pages: ");
    natsSysJszOptions_Init(&opts);
    opts.Accounts = true;
    opts.Limit    = 1;
    memset(&pc, 0, sizeof(pc));
    pr.requests = 0;
    s           = natsSysClient_JszEach(sys, "SRV1", &opts, 5000, _countJszPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 3) && (pc.items == 3) && (pr.requests == 3));

    test("A handler returning false stops the walk: ");
    memset(&pc, 0, sizeof(pc));
    pc.stopAfter = 2;
    pr.requests  = 0;
    s            = natsSysClient_JszEach(sys, "SRV1", &opts, 5000, _countJszPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 2) && (pr.requests == 2));

    test("An empty page ends the walk instead of looping: ");
    pr.total        = 100;
    pr.forceEmptyAt = 1;
    memset(&pc, 0, sizeof(pc));
    s = natsSysClient_JszEach(sys, "SRV1", &opts, 5000, _countJszPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 2) && (pc.items == 1) && pc.sawEmpty);

    test("Bad arguments are rejected: ");
    testCond((natsSysClient_JszEach(sys, NULL, &opts, 0, _countJszPages, &pc)
              == NATS_INVALID_ARG)
             && (natsSysClient_JszEach(sys, "", &opts, 0, _countJszPages, &pc)
                 == NATS_INVALID_ARG)
             && (natsSysClient_JszEach(sys, "SRV1", &opts, -1, _countJszPages, &pc)
                 == NATS_INVALID_ARG)
             && (natsSysClient_JszEach(NULL, "SRV1", &opts, 0, _countJszPages, &pc)
              == NATS_INVALID_ARG)
             && (natsSysClient_JszEach(sys, "SRV1", &opts, 0, NULL, &pc)
                 == NATS_INVALID_ARG));

    natsSubscription_Destroy(sub);
    TEARDOWN;
}

void
test_JszPingEach(void)
{
    natsSubscription  *sub   = NULL;
    pagingResponder    pr;
    pageCounter        pc;
    natsSysJszOptions  opts;
    natsSysJszWalkList walks = {NULL, 0};

    SETUP;

    memset(&pr, 0, sizeof(pr));
    pr.total        = 3;
    pr.perPage      = 1;
    pr.forceEmptyAt = -1;

    test("Subscribe the paging responder: ");
    s = natsConnection_Subscribe(&sub, nc, "$SYS.REQ.SERVER.*.JSZ",
                                 _jszPagingResponder, &pr);
    testCond(s == NATS_OK);

    natsSysJszOptions_Init(&opts);
    opts.Accounts = true;
    opts.Limit    = 1;

    // A plain nats-server does not answer the JSZ ping, so the stand-in is the
    // only responder here — unlike CONNZ.
    test("Ping produces one walk: ");
    s = natsSysClient_JszPingEach(&walks, sys, &opts, 2000);
    testCond((s == NATS_OK) && (walks.Count == 1));

    test("Running it yields all three pages: ");
    memset(&pc, 0, sizeof(pc));
    s = natsSysJszWalk_Run(walks.Walks[0], 5000, _countJszPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 3) && (pc.items == 3));

    natsSysJszWalkList_Destroy(&walks);

    // needsPagination is decided once, from the ping's page.
    test("Without Accounts a walk yields exactly one page: ");
    natsSysJszOptions_Init(&opts);
    s = natsSysClient_JszPingEach(&walks, sys, &opts, 2000);
    if ((s != NATS_OK) || (walks.Count != 1))
        FAIL("ping failed");
    memset(&pc, 0, sizeof(pc));
    pr.requests = 0;
    s           = natsSysJszWalk_Run(walks.Walks[0], 5000, _countJszPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 1) && (pr.requests == 0));

    natsSysJszWalkList_Destroy(&walks);
    natsSubscription_Destroy(sub);
    TEARDOWN;
}

// A system account cannot be configured from the command line, so the tests
// that need one write a config file and start the server with -c. The name
// matches the */test/conf_* pattern already in .gitignore.
#define SYS_CONF_FILE "conf_sys.conf"
#define SYS_URL       "nats://admin:s3cr3t!@127.0.0.1:4222"

// The system-account counterpart to SETUP: writes 'conf' to 'file', starts a
// server on it with 'args', connects on the system account and creates a
// client. Declares s/nc/sys/pid, exactly as SETUP does.
#define SETUP_SYS(conf, file, args)                        \
    natsStatus      s   = NATS_OK;                         \
    natsConnection *nc  = NULL;                            \
    natsSysClient  *sys = NULL;                            \
    natsPid         pid = NATS_INVALID_PID;                \
                                                           \
    {                                                      \
        FILE *_f;                                          \
                                                           \
        test("Write the server config: ");                 \
        _f = fopen((file), "w");                           \
        if (_f == NULL)                                    \
            FAIL("unable to write the server config");     \
        fputs((conf), _f);                                 \
        fclose(_f);                                        \
        _rememberPath(file);                               \
        testCond(true);                                    \
    }                                                      \
                                                           \
    test("Start the server: ");                            \
    pid = _startServer(SYS_URL, (args), true);             \
    CHECK_SERVER_STARTED(pid);                             \
    testCond(true);                                        \
                                                           \
    test("Connect on the system account: ");               \
    s = natsConnection_ConnectTo(&nc, SYS_URL);            \
    testCond(s == NATS_OK);                                \
                                                           \
    test("Create client: ");                               \
    s = natsSysClient_Create(&sys, nc, NULL);              \
    testCond(s == NATS_OK)

#define TEARDOWN_SYS(file)      \
    natsSysClient_Destroy(sys); \
    natsConnection_Destroy(nc); \
    _stopServer(pid);           \
    remove(file)

static const char *SYS_CONF =
    "port: 4222\n"
    "server_name: sysc1\n"
    "accounts {\n"
    "  $SYS { users = [ { user: \"admin\", pass: \"s3cr3t!\" } ] }\n"
    "}\n";

// The other HEALTHZ tests drive a stand-in responder so they can assert exact
// bytes. This one talks to the real endpoint, which is what actually proves
// the subject layout, the system-account requirement and the by-ID path.
void
test_HealthzRealServer(void)
{
    natsSysHealthzRespList list = {NULL, 0};
    natsSysHealthzResp    *resp = NULL;
    char                   serverID[128];

    SETUP_SYS(SYS_CONF, SYS_CONF_FILE, "-c " SYS_CONF_FILE);


    test("Ping the real HEALTHZ endpoint: ");
    s = natsSysClient_HealthzPing(&list, sys, NULL, 2000);
    testCond((s == NATS_OK) && (list.Count == 1));

    test("The server reports itself healthy: ");
    testCond((list.Resps[0]->Healthz.Status != NULL)
             && (strcmp(list.Resps[0]->Healthz.Status, "ok") == 0)
             && (list.Resps[0]->Server.Name != NULL)
             && (strcmp(list.Resps[0]->Server.Name, "sysc1") == 0)
             && (list.Resps[0]->Server.ID != NULL));

    test("Its reported ID works for a by-ID request: ");
    snprintf(serverID, sizeof(serverID), "%s", list.Resps[0]->Server.ID);
    s = natsSysClient_Healthz(&resp, sys, serverID, NULL, 2000);
    testCond((s == NATS_OK) && (resp != NULL) && (resp->Server.ID != NULL)
             && (strcmp(resp->Server.ID, serverID) == 0));

    natsSysHealthzResp_Destroy(resp);
    resp = NULL;

    test("An unknown server ID is NATS_NOT_FOUND: ");
    s = natsSysClient_Healthz(&resp, sys, "NOTAREALSERVERID", NULL, 2000);
    testCond((s == NATS_NOT_FOUND) && (resp == NULL));

    test("Detailed options are accepted by the real endpoint: ");
    {
        natsSysHealthzOptions opts;

        natsSysHealthzOptions_Init(&opts);
        opts.Details = true;
        s            = natsSysClient_Healthz(&resp, sys, serverID, &opts, 2000);
    }
    testCond((s == NATS_OK) && (resp != NULL));

    natsSysHealthzResp_Destroy(resp);
    natsSysHealthzRespList_Destroy(&list);
    TEARDOWN_SYS(SYS_CONF_FILE);
}

// Decoding a payload the real server actually produced, rather than one this
// suite wrote. Catches key names that were mistyped consistently in both the
// test fixture and the field table.
void
test_VarzStatszRealServer(void)
{
    natsSysVarzRespList   varzes   = {NULL, 0};
    natsSysStatszRespList statszes = {NULL, 0};
    natsSysVarzResp      *varz     = NULL;
    natsSysStatszResp    *statsz   = NULL;
    int64_t               ns       = 0;
    char                  serverID[128];

    SETUP_SYS(SYS_CONF, SYS_CONF_FILE, "-c " SYS_CONF_FILE);


    test("VARZ ping: ");
    s = natsSysClient_VarzPing(&varzes, sys, NULL, 2000);
    testCond((s == NATS_OK) && (varzes.Count == 1));

    test("The real VARZ payload decodes: ");
    testCond((varzes.Resps[0]->Varz.Name != NULL)
             && (strcmp(varzes.Resps[0]->Varz.Name, "sysc1") == 0)
             && (varzes.Resps[0]->Varz.Version != NULL)
             && (varzes.Resps[0]->Varz.ID != NULL)
             && (varzes.Resps[0]->Varz.Port == 4222)
             && (varzes.Resps[0]->Varz.Cores > 0)
             && (varzes.Resps[0]->Varz.MaxPayload > 0));

    test("Its timestamps parse: ");
    testCond((varzes.Resps[0]->Varz.Start != NULL)
             && (natsSysTime_Parse(&ns, varzes.Resps[0]->Varz.Start) == NATS_OK)
             && (ns > 0) && (varzes.Resps[0]->Varz.Now != NULL)
             && (natsSysTime_Parse(&ns, varzes.Resps[0]->Varz.Now) == NATS_OK));

    test("Its ping interval is nanoseconds, not milliseconds: ");
    // The server default is 2 minutes; as nanoseconds that is 1.2e11.
    testCond(varzes.Resps[0]->Varz.PingInterval > 1000000000LL);

    snprintf(serverID, sizeof(serverID), "%s", varzes.Resps[0]->Server.ID);

    test("VARZ by ID: ");
    s = natsSysClient_Varz(&varz, sys, serverID, NULL, 2000);
    testCond((s == NATS_OK) && (varz != NULL) && (varz->Varz.ID != NULL)
             && (strcmp(varz->Varz.ID, serverID) == 0));

    test("STATSZ by ID decodes from the \"statsz\" key: ");
    s = natsSysClient_Statsz(&statsz, sys, serverID, NULL, 2000);
    testCond((s == NATS_OK) && (statsz != NULL) && (statsz->Statsz.Start != NULL)
             && (statsz->Statsz.ActiveServers >= 1) && (statsz->Statsz.Cores > 0));

    test("STATSZ ping: ");
    s = natsSysClient_StatszPing(&statszes, sys, NULL, 2000);
    testCond((s == NATS_OK) && (statszes.Count == 1));

    test("Neither endpoint reported an error: ");
    testCond((varz->Error.Code == 0) && (statsz->Error.Code == 0));

    natsSysStatszResp_Destroy(statsz);
    natsSysVarzResp_Destroy(varz);
    natsSysStatszRespList_Destroy(&statszes);
    natsSysVarzRespList_Destroy(&varzes);
    TEARDOWN_SYS(SYS_CONF_FILE);
}

// Pagination against the server's own paging, rather than a stand-in that
// implements offset/limit the same way this client does.
#define REAL_CONNS (12)

void
test_ConnzSubszRealServer(void)
{
    natsConnection     *extras[REAL_CONNS];
    natsSubscription   *sub   = NULL;
    natsSysConnzResp   *connz = NULL;
    natsSysSubszResp   *subsz = NULL;
    natsSysConnzOptions copts;
    natsSysSubszOptions sopts;
    pageCounter         pc;
    char                serverID[128];
    int                 i;
    int                 total;

    memset(extras, 0, sizeof(extras));

    SETUP_SYS(SYS_CONF, SYS_CONF_FILE, "-c " SYS_CONF_FILE);

    test("Open a dozen more connections and a subscription: ");
    for (i = 0; (i < REAL_CONNS) && (s == NATS_OK); i++)
        s = natsConnection_ConnectTo(&extras[i], SYS_URL);
    if (s == NATS_OK)
        s = natsConnection_SubscribeSync(&sub, nc, "foo.bar");
    if (s == NATS_OK)
        s = natsConnection_Flush(nc);
    testCond(s == NATS_OK);

    test("Learn the server ID: ");
    s = natsSysClient_Connz(&connz, sys, "PING", NULL, 2000);
    // "PING" is a valid target for a by-ID request too: it reaches every
    // server, and with one server that is exactly one reply.
    testCond((s == NATS_OK) && (connz != NULL) && (connz->Connz.ID != NULL));
    snprintf(serverID, sizeof(serverID), "%s", connz->Connz.ID);
    total = connz->Connz.Total;
    natsSysConnzResp_Destroy(connz);
    connz = NULL;

    test("The server sees every connection: ");
    testCond(total >= REAL_CONNS + 1);

    test("Paging five at a time covers them all: ");
    natsSysConnzOptions_Init(&copts);
    copts.Limit = 5;
    memset(&pc, 0, sizeof(pc));
    s = natsSysClient_ConnzEach(sys, serverID, &copts, 5000, _countConnzPages, &pc);
    testCond((s == NATS_OK) && (pc.pages >= 3) && (pc.items >= REAL_CONNS + 1));

    test("A limit larger than the total is a single page: ");
    copts.Limit = 1024;
    memset(&pc, 0, sizeof(pc));
    s = natsSysClient_ConnzEach(sys, serverID, &copts, 5000, _countConnzPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 1) && (pc.items == total));

    test("SUBSZ reports the sublist stats: ");
    natsSysSubszOptions_Init(&sopts);
    sopts.Subscriptions = true;
    s                   = natsSysClient_Subsz(&subsz, sys, serverID, &sopts, 2000);
    testCond((s == NATS_OK) && (subsz != NULL) && (subsz->Subsz.SublistStats != NULL)
             && (subsz->Subsz.SublistStats->NumSubs > 0));

    test("And finds the subscription we made: ");
    {
        bool found = false;

        for (i = 0; i < subsz->Subsz.SubsCount; i++)
        {
            if ((subsz->Subsz.Subs[i].Subject != NULL)
                && (strcmp(subsz->Subsz.Subs[i].Subject, "foo.bar") == 0))
                found = true;
        }
        testCond(found);
    }

    natsSysSubszResp_Destroy(subsz);
    natsSubscription_Destroy(sub);
    for (i = 0; i < REAL_CONNS; i++)
        natsConnection_Destroy(extras[i]);
    TEARDOWN_SYS(SYS_CONF_FILE);
}

// JSZ against a real JetStream server, so the account pagination runs against
// the server's own offset/limit handling rather than a stand-in that
// implements it the same way this client does.
#define JS_CONF_FILE "conf_js.conf"

static const char *JS_CONF =
    "port: 4222\n"
    "server_name: jsc1\n"
    "accounts {\n"
    "  $SYS { users = [ { user: \"admin\", pass: \"s3cr3t!\" } ] }\n"
    "  JS1 { jetstream: enabled, users: [ {user: u1, password: p1} ] }\n"
    "  JS2 { jetstream: enabled, users: [ {user: u2, password: p2} ] }\n"
    "  JS3 { jetstream: enabled, users: [ {user: u3, password: p3} ] }\n"
    "}\n";

void
test_JszRealServer(void)
{
    natsStatus        s;
    natsConnection   *nc        = NULL;
    natsConnection   *accts[3]  = {NULL, NULL, NULL};
    natsSysClient    *sys       = NULL;
    natsPid           pid       = NATS_INVALID_PID;
    natsSysJszResp   *jsz       = NULL;
    natsSysJszOptions opts;
    pageCounter       pc;
    FILE             *f;
    char              cmdline[256];
    char              storeDir[128];
    char              serverID[128];
    int               accounts;
    int               i;

    test("Write a JetStream config with three accounts: ");
    f = fopen(JS_CONF_FILE, "w");
    if (f == NULL)
        FAIL("unable to write the server config");
    _rememberPath(JS_CONF_FILE);
    fputs(JS_CONF, f);
    fclose(f);
    testCond(true);

    test("Start the server: ");
    // The store directory is supplied per run so repeated runs cannot collide,
    // and removed at the end rather than left behind.
    _makeUniqueDir(storeDir, (int) sizeof(storeDir), "datastore_jsz_");
    snprintf(cmdline, sizeof(cmdline), "-c %s -js -sd %s", JS_CONF_FILE, storeDir);
    pid = _startServer(SYS_URL, cmdline, false);
    if (pid == NATS_INVALID_PID)
        FAIL("unable to start the server");
    testCond(_waitForServer(SYS_URL, 2000) == NATS_OK);

    test("Connect and create the client: ");
    s = natsConnection_ConnectTo(&nc, SYS_URL);
    if (s == NATS_OK)
        s = natsSysClient_Create(&sys, nc, NULL);
    testCond(s == NATS_OK);

    test("Activate each JetStream account: ");
    // An account only shows up in JSZ once it has been used, so connect one
    // client to each and create a stream so the account holds an asset.
    for (i = 0; (i < 3) && (s == NATS_OK); i++)
    {
        char        url[128];
        jsCtx      *js  = NULL;
        jsStreamConfig cfg;
        jsErrCode   jerr = 0;

        snprintf(url, sizeof(url), "nats://u%d:p%d@127.0.0.1:4222", i + 1, i + 1);
        s = natsConnection_ConnectTo(&accts[i], url);
        if (s == NATS_OK)
            s = natsConnection_JetStream(&js, accts[i], NULL);
        if (s == NATS_OK)
        {
            jsStreamConfig_Init(&cfg);
            cfg.Name         = "s1";
            cfg.Subjects     = (const char *[]){"sub.>"};
            cfg.SubjectsLen  = 1;
            s                = js_AddStream(NULL, js, &cfg, NULL, &jerr);
        }
        jsCtx_Destroy(js);
    }
    testCond(s == NATS_OK);

    test("Learn the server ID and account count: ");
    natsSysJszOptions_Init(&opts);
    opts.Accounts = true;
    s             = natsSysClient_Jsz(&jsz, sys, "PING", &opts, 2000);
    testCond((s == NATS_OK) && (jsz != NULL) && (jsz->JSInfo.ID != NULL));
    snprintf(serverID, sizeof(serverID), "%s", jsz->JSInfo.ID);
    accounts = jsz->JSInfo.JetStreamStats.Accounts;
    natsSysJszResp_Destroy(jsz);
    jsz = NULL;

    test("All three accounts are reported: ");
    testCond(accounts == 3);

    test("Without Accounts there are no account details: ");
    natsSysJszOptions_Init(&opts);
    s = natsSysClient_Jsz(&jsz, sys, serverID, &opts, 2000);
    testCond((s == NATS_OK) && (jsz->JSInfo.AccountDetailsCount == 0)
             && (jsz->JSInfo.JetStreamStats.Accounts == 3));
    natsSysJszResp_Destroy(jsz);
    jsz = NULL;

    test("Without Accounts the walk is a single page: ");
    memset(&pc, 0, sizeof(pc));
    s = natsSysClient_JszEach(sys, serverID, &opts, 5000, _countJszPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 1) && (pc.items == 0));

    test("Paging one account at a time covers all three: ");
    natsSysJszOptions_Init(&opts);
    opts.Accounts = true;
    opts.Limit    = 1;
    memset(&pc, 0, sizeof(pc));
    s = natsSysClient_JszEach(sys, serverID, &opts, 5000, _countJszPages, &pc);
    testCond((s == NATS_OK) && (pc.pages == 3) && (pc.items == 3));

    test("Streams come back with their config as raw JSON: ");
    natsSysJszOptions_Init(&opts);
    opts.Accounts = true;
    opts.Streams  = true;
    opts.Config   = true;
    s             = natsSysClient_Jsz(&jsz, sys, serverID, &opts, 2000);
    if ((s != NATS_OK) || (jsz->JSInfo.AccountDetailsCount == 0))
        FAIL("no account details");
    {
        bool found = false;

        for (i = 0; i < jsz->JSInfo.AccountDetailsCount; i++)
        {
            natsSysAccountDetail *ad = jsz->JSInfo.AccountDetails[i];
            int                   j;

            for (j = 0; j < ad->StreamsCount; j++)
            {
                if ((ad->Streams[j].Name != NULL)
                    && (strcmp(ad->Streams[j].Name, "s1") == 0)
                    && (ad->Streams[j].ConfigJSON != NULL)
                    && (strstr(ad->Streams[j].ConfigJSON, "\"name\":\"s1\"") != NULL)
                    && (ad->Streams[j].StateJSON != NULL))
                    found = true;
            }
        }
        testCond(found);
    }

    natsSysJszResp_Destroy(jsz);
    for (i = 0; i < 3; i++)
        natsConnection_Destroy(accts[i]);
    natsSysClient_Destroy(sys);
    natsConnection_Destroy(nc);
    _stopServer(pid);
    _rmtree(storeDir);
    remove(JS_CONF_FILE);
}

//
// Cluster
//
// Everything above talks to a single server, so a gather only ever collects one
// reply and a walk list only ever holds one walk. These tests run a real
// three-node cluster, which is the only way to cover the gather actually
// gathering, walks being independent of one another, and a by-ID request
// reaching a server the client is not connected to.

#define CLUSTER_SIZE (3)

// A private port range, so this suite can run beside a developer's own server
// or the Go suite, which hard-codes 4223/5223/6223.
#define CLU_CLIENT_PORT(i) (4322 + (i))
#define CLU_ROUTE_PORT(i)  (4422 + (i))

typedef struct
{
    natsPid         pids[CLUSTER_SIZE];
    char            confs[CLUSTER_SIZE][32];
    char            stores[CLUSTER_SIZE][64];
    natsConnection *nc;  ///< System-account connection to node 1.
    natsSysClient  *sys; ///< Client over that connection.

    /** \brief Client used only by the readiness polls.
     *
     * An unbounded gather cannot know when to stop, so it always waits out the
     * stall interval — 300ms per poll even when all three nodes answered in
     * two. The polls know exactly how many replies they need, so bounding this
     * client by the cluster size lets them return on the last reply. The
     * assertions keep using the unbounded #sys, which is what callers get.
     */
    natsSysClient *poll;

    bool jetstream;
} cluster;

static void
_clusterURL(char *buf, int bufLen, int idx)
{
    snprintf(buf, bufLen, "nats://admin:s3cr3t!@127.0.0.1:%d", CLU_CLIENT_PORT(idx));
}

static bool
_writeClusterConf(cluster *c, int idx)
{
    FILE *f;
    int   i;

    f = fopen(c->confs[idx], "w");
    if (f == NULL)
        return false;
    _rememberPath(c->confs[idx]);

    fprintf(f,
            "port: %d\n"
            "server_name: clu%d\n"
            "accounts {\n"
            "  $SYS { users = [ { user: \"admin\", pass: \"s3cr3t!\" } ] }\n",
            CLU_CLIENT_PORT(idx), idx + 1);

    if (c->jetstream)
    {
        for (i = 1; i <= 3; i++)
            fprintf(f,
                    "  JS%d { jetstream: enabled, users: [ {user: u%d, password: p%d} ] }\n",
                    i, i, i);
    }
    fputs("}\n", f);

    // Each node needs its own store, so they are created per run and removed
    // afterwards rather than left in the test directory.
    if (c->jetstream)
        fprintf(f, "jetstream { store_dir: %s }\n", c->stores[idx]);

    fprintf(f,
            "cluster {\n"
            "  name: SYSC\n"
            "  listen: 127.0.0.1:%d\n"
            "  routes: [\n",
            CLU_ROUTE_PORT(idx));

    for (i = 0; i < CLUSTER_SIZE; i++)
    {
        if (i != idx)
            fprintf(f, "    nats-route://127.0.0.1:%d\n", CLU_ROUTE_PORT(i));
    }
    fputs("  ]\n}\n", f);

    fclose(f);
    return true;
}

// Starts the cluster and connects a system client to node 1. Whatever was
// started is recorded in 'c' even on failure, so _stopCluster still cleans up.
static natsStatus
_startCluster(cluster *c, bool jetstream)
{
    natsStatus s = NATS_OK;
    char       url[128];
    char       cmdline[256];
    int        i;

    memset(c, 0, sizeof(*c));
    c->jetstream = jetstream;
    for (i = 0; i < CLUSTER_SIZE; i++)
        c->pids[i] = NATS_INVALID_PID;

    for (i = 0; i < CLUSTER_SIZE; i++)
    {
        snprintf(c->confs[i], sizeof(c->confs[i]), "conf_cluster%d.conf", i + 1);
        if (jetstream)
            _makeUniqueDir(c->stores[i], (int) sizeof(c->stores[i]), "datastore_clu_");
        if (!_writeClusterConf(c, i))
            return NATS_ERR;
    }

    // Started without waiting on each: every node's routes point at nodes that
    // do not exist yet, so they only converge once all three are up.
    for (i = 0; i < CLUSTER_SIZE; i++)
    {
        snprintf(cmdline, sizeof(cmdline), "-c %s", c->confs[i]);
        c->pids[i] = _startServer(NULL, cmdline, false);
        if (c->pids[i] == NATS_INVALID_PID)
            return NATS_ERR;
    }

    _clusterURL(url, sizeof(url), 0);
    s = _waitForServer(url, 5000);
    if (s == NATS_OK)
        s = natsConnection_ConnectTo(&c->nc, url);
    if (s == NATS_OK)
        s = natsSysClient_Create(&c->sys, c->nc, NULL);
    if (s == NATS_OK)
    {
        natsSysClientOpts opts;

        natsSysClientOpts_Init(&opts);
        opts.ServerCount = CLUSTER_SIZE;
        s                = natsSysClient_Create(&c->poll, c->nc, &opts);
    }
    return s;
}

static void
_stopCluster(cluster *c)
{
    int i;

    natsSysClient_Destroy(c->poll);
    natsSysClient_Destroy(c->sys);
    natsConnection_Destroy(c->nc);
    for (i = 0; i < CLUSTER_SIZE; i++)
    {
        _stopServer(c->pids[i]);
        if (c->confs[i][0] != '\0')
            remove(c->confs[i]);
        if (c->stores[i][0] != '\0')
            _rmtree(c->stores[i]);
    }
}

// Waits for the cluster to converge by asking it, rather than sleeping a fixed
// amount, which also exercises the gather on the way in.
//
// The condition is every node agreeing that all three are active, not simply
// three nodes answering: a node answers on $SYS as soon as its routes are up,
// but active_servers is gossiped and settles a beat later. Waiting on the
// weaker condition makes anything that reads active_servers intermittent.
// 'out' receives the STATSZ gather that proved convergence, so a caller that
// wants to assert on it need not pay for a second round trip. Pass NULL to
// discard it. Untouched unless NATS_OK is returned.
static natsStatus
_waitForCluster(cluster *c, int64_t budgetMs, natsSysStatszRespList *out)
{
    natsSysStatszRespList list     = {NULL, 0};
    natsStatus            s        = NATS_OK;
    int64_t               deadline = _nowMs() + budgetMs;
    bool                  converged;
    int                   j;

    for (;;)
    {
        converged = false;
        s         = natsSysClient_StatszPing(&list, c->poll, NULL, 1000);
        if (s == NATS_OK)
        {
            converged = (list.Count == CLUSTER_SIZE);
            for (j = 0; converged && (j < list.Count); j++)
                converged = (list.Resps[j]->Statsz.ActiveServers == CLUSTER_SIZE);
        }

        if (converged)
        {
            if (out != NULL)
                *out = list;
            else
                natsSysStatszRespList_Destroy(&list);
            return NATS_OK;
        }
        natsSysStatszRespList_Destroy(&list);

        if (_nowMs() >= deadline)
            return (s == NATS_OK) ? NATS_TIMEOUT : s;
        usleep(CLUSTER_POLL_MS * 1000);
    }
}

// Waits for every node to report a meta group of the full size with a leader.
//
// This is necessary but not sufficient for placing a replicated asset — see
// the retry in test_ClusterJszPingEach for why no predicate here is.
static natsStatus
_waitForMetaLeader(cluster *c, int64_t budgetMs)
{
    natsSysJszRespList list     = {NULL, 0};
    natsStatus         s        = NATS_OK;
    int64_t            deadline = _nowMs() + budgetMs;
    bool               led;
    int                j;

    for (;;)
    {
        led = false;
        s   = natsSysClient_JszPing(&list, c->poll, NULL, 2000);
        if (s == NATS_OK)
        {
            led = (list.Count == CLUSTER_SIZE);
            for (j = 0; led && (j < list.Count); j++)
            {
                natsSysMetaClusterInfo *meta = list.Resps[j]->JSInfo.Meta;

                led = (meta != NULL) && (meta->Leader != NULL) && (meta->Leader[0] != '\0')
                      && (meta->Size == CLUSTER_SIZE);
            }
        }
        natsSysJszRespList_Destroy(&list);

        if (led)
            return NATS_OK;
        if (_nowMs() >= deadline)
            return (s == NATS_OK) ? NATS_TIMEOUT : s;
        usleep(CLUSTER_POLL_MS * 1000);
    }
}

// SUBSZ additionally tallies this test's own subjects, by the node index and
// number encoded in "clu.<node>.<n>", counting how often each was reported.
#define CLU_SUBS_MAX (8)

typedef struct
{
    int Seen[CLUSTER_SIZE][CLU_SUBS_MAX];
} subjectTally;

static void
_tallySubs(subjectTally *t, const natsSysSubsz *subsz)
{
    int i;

    for (i = 0; i < subsz->SubsCount; i++)
    {
        const char *subject = subsz->Subs[i].Subject;
        int         node    = 0;
        int         n       = 0;

        if ((subject != NULL) && (sscanf(subject, "clu.%d.%d", &node, &n) == 2)
            && (node >= 0) && (node < CLUSTER_SIZE) && (n >= 0) && (n < CLU_SUBS_MAX))
            t->Seen[node][n]++;
    }
}

// Node index behind a "cluN" server name, or -1 if it is not one of ours.
static int
_nodeIndex(const char *name)
{
    int i;

    if ((name == NULL) || (strlen(name) != 4))
        return -1;

    i = name[3] - '1';
    return ((i >= 0) && (i < CLUSTER_SIZE)) ? i : -1;
}

// True if every walk covered a different server.
static bool
_idsAreDistinct(const char *ids[], int count)
{
    int i;
    int j;

    for (i = 0; i < count; i++)
    {
        if ((ids[i] == NULL) || (ids[i][0] == '\0'))
            return false;
        for (j = i + 1; j < count; j++)
        {
            if (strcmp(ids[i], ids[j]) == 0)
                return false;
        }
    }
    return true;
}

void
test_ClusterPing(void)
{
    cluster                c;
    natsStatus             s;
    natsSysVarzRespList    varzes   = {NULL, 0};
    natsSysStatszRespList  statszes = {NULL, 0};
    natsSysHealthzRespList healths  = {NULL, 0};
    natsSysHealthzResp    *resp     = NULL;
    char                   remoteID[128];
    int                    i;
    int                    j;

    test("Start a three-node cluster: ");
    s = _startCluster(&c, false);
    testCond(s == NATS_OK);

    test("The cluster converges: ");
    s = _waitForCluster(&c, 15000, &statszes);
    testCond(s == NATS_OK);

    test("VARZ ping gathers every node: ");
    s = natsSysClient_VarzPing(&varzes, c.sys, NULL, 5000);
    testCond((s == NATS_OK) && (varzes.Count == CLUSTER_SIZE));

    test("Each node answers exactly once, under its own name: ");
    {
        int counts[CLUSTER_SIZE];

        memset(counts, 0, sizeof(counts));
        for (i = 0; i < varzes.Count; i++)
        {
            j = _nodeIndex(varzes.Resps[i]->Varz.Name);
            if (j >= 0)
                counts[j]++;
        }
        testCond((counts[0] == 1) && (counts[1] == 1) && (counts[2] == 1));
    }

    test("The envelope's server ID agrees with the payload's: ");
    {
        bool ok = true;

        for (i = 0; i < varzes.Count; i++)
            ok = ok && (varzes.Resps[i]->Server.ID != NULL)
                 && (varzes.Resps[i]->Varz.ID != NULL)
                 && (strcmp(varzes.Resps[i]->Server.ID, varzes.Resps[i]->Varz.ID) == 0);
        testCond(ok);
    }

    // Also the only test of a nested-object field table against a real payload:
    // the cluster block is decoded by its own table, and cluster_port lands in
    // a member with a different name.
    test("Every node reports the cluster and both its peers: ");
    {
        bool ok = true;

        for (i = 0; i < varzes.Count; i++)
        {
            natsSysClusterOptsVarz *cl  = &varzes.Resps[i]->Varz.Cluster;
            int                     idx = _nodeIndex(varzes.Resps[i]->Varz.Name);

            ok = ok && (cl->Name != NULL) && (strcmp(cl->Name, "SYSC") == 0)
                 && (cl->Port == CLU_ROUTE_PORT(idx))
                 && (cl->URLsCount == CLUSTER_SIZE - 1)
                 // Not one connection per peer: the server pools routes, and
                 // gives some accounts a dedicated one, so this is a floor.
                 && (varzes.Resps[i]->Varz.Routes >= CLUSTER_SIZE - 1);
        }
        testCond(ok);
    }

    // The gather is over the whole cluster, so pick a node the client is not
    // connected to: reaching it proves the request was routed, not answered
    // locally.
    test("A by-ID request reaches a node we are not connected to: ");
    remoteID[0] = '\0';
    for (i = 0; i < varzes.Count; i++)
    {
        if ((varzes.Resps[i]->Varz.Name != NULL)
            && (strcmp(varzes.Resps[i]->Varz.Name, "clu3") == 0))
            snprintf(remoteID, sizeof(remoteID), "%s", varzes.Resps[i]->Varz.ID);
    }
    s = natsSysClient_Healthz(&resp, c.sys, remoteID, NULL, 5000);
    testCond((remoteID[0] != '\0') && (s == NATS_OK) && (resp != NULL)
             && (resp->Server.Name != NULL) && (strcmp(resp->Server.Name, "clu3") == 0));
    natsSysHealthzResp_Destroy(resp);
    resp = NULL;

    test("An unknown server ID is still NATS_NOT_FOUND in a cluster: ");
    s = natsSysClient_Healthz(&resp, c.sys, "NOTAREALSERVERID", NULL, 2000);
    testCond((s == NATS_NOT_FOUND) && (resp == NULL));

    test("HEALTHZ ping reports every node healthy: ");
    s = natsSysClient_HealthzPing(&healths, c.sys, NULL, 5000);
    if ((s != NATS_OK) || (healths.Count != CLUSTER_SIZE))
        FAIL("healthz ping did not gather the cluster");
    {
        bool ok = true;

        for (i = 0; i < healths.Count; i++)
            ok = ok && (healths.Resps[i]->Healthz.Status != NULL)
                 && (strcmp(healths.Resps[i]->Healthz.Status, "ok") == 0)
                 && (healths.Resps[i]->Healthz.StatusCode == 200);
        testCond(ok);
    }
    natsSysHealthzRespList_Destroy(&healths);

    // Deliberately a fresh gather, not the one _waitForCluster handed back:
    // that list is the poll's own exit condition, so asserting on it would be
    // a tautology and a STATSZ regression would surface as a convergence
    // timeout pointing at the wrong thing.
    natsSysStatszRespList_Destroy(&statszes);
    statszes.Resps = NULL;
    statszes.Count = 0;

    test("STATSZ ping gathers every node and each sees all three: ");
    s = natsSysClient_StatszPing(&statszes, c.sys, NULL, 5000);
    if ((s != NATS_OK) || (statszes.Count != CLUSTER_SIZE))
        FAIL("statsz ping did not gather the cluster");
    {
        bool ok = true;

        for (i = 0; i < statszes.Count; i++)
            ok = ok && (statszes.Resps[i]->Statsz.ActiveServers == CLUSTER_SIZE)
                 && (statszes.Resps[i]->Statsz.Start != NULL)
                 && (statszes.Resps[i]->Statsz.Cores > 0);
        testCond(ok);
    }
    natsSysStatszRespList_Destroy(&statszes);

    test("ServerCount stops the gather short: ");
    {
        natsSysClient    *bounded = NULL;
        natsSysClientOpts opts;

        natsSysClientOpts_Init(&opts);
        opts.ServerCount = CLUSTER_SIZE - 1;
        s                = natsSysClient_Create(&bounded, c.nc, &opts);
        if (s == NATS_OK)
            s = natsSysClient_HealthzPing(&healths, bounded, NULL, 5000);
        testCond((s == NATS_OK) && (healths.Count == CLUSTER_SIZE - 1));
        natsSysHealthzRespList_Destroy(&healths);
        natsSysClient_Destroy(bounded);
    }

    test("A ServerCount above the cluster size still returns on time: ");
    {
        natsSysClient    *bounded = NULL;
        natsSysClientOpts opts;

        // The gather cannot reach its count, so it ends on the stall interval
        // instead. That is normal termination, not an error.
        natsSysClientOpts_Init(&opts);
        opts.ServerCount = CLUSTER_SIZE + 5;
        s                = natsSysClient_Create(&bounded, c.nc, &opts);
        if (s == NATS_OK)
            s = natsSysClient_HealthzPing(&healths, bounded, NULL, 5000);
        testCond((s == NATS_OK) && (healths.Count == CLUSTER_SIZE));
        natsSysHealthzRespList_Destroy(&healths);
        natsSysClient_Destroy(bounded);
    }

    natsSysVarzRespList_Destroy(&varzes);
    _stopCluster(&c);
}

// Connections are spread unevenly on purpose: the walks have to keep separate
// offsets, so a run where every node holds the same number of connections
// would not tell a shared offset from a per-walk one.
#define CLU_CONNS_N1 (6)
#define CLU_CONNS_N2 (2)
#define CLU_CONNS_N3 (1)

void
test_ClusterConnzPingEach(void)
{
    cluster              c;
    natsStatus           s;
    natsConnection      *extras[CLU_CONNS_N1 + CLU_CONNS_N2 + CLU_CONNS_N3];
    natsSysConnzWalkList walks = {NULL, 0};
    natsSysConnzOptions  opts;
    pageCounter          pc[CLUSTER_SIZE];
    const char          *ids[CLUSTER_SIZE];
    const int            perNode[CLUSTER_SIZE] = {CLU_CONNS_N1, CLU_CONNS_N2, CLU_CONNS_N3};
    int                  nextra                = 0;
    int                  i;
    int                  j;

    memset(extras, 0, sizeof(extras));

    test("Start a three-node cluster: ");
    s = _startCluster(&c, false);
    testCond(s == NATS_OK);

    test("The cluster converges: ");
    s = _waitForCluster(&c, 15000, NULL);
    testCond(s == NATS_OK);

    test("Open an uneven spread of connections: ");
    for (i = 0; (i < CLUSTER_SIZE) && (s == NATS_OK); i++)
    {
        char url[128];

        _clusterURL(url, sizeof(url), i);
        for (j = 0; (j < perNode[i]) && (s == NATS_OK); j++)
            s = natsConnection_ConnectTo(&extras[nextra++], url);
    }
    testCond(s == NATS_OK);

    test("Ping produces one walk per node: ");
    natsSysConnzOptions_Init(&opts);
    opts.Limit = 2;
    s          = natsSysClient_ConnzPingEach(&walks, c.sys, &opts, 5000);
    testCond((s == NATS_OK) && (walks.Count == CLUSTER_SIZE));

    test("Each walk covers a different server: ");
    for (i = 0; i < walks.Count; i++)
        ids[i] = natsSysConnzWalk_ServerID(walks.Walks[i]);
    testCond(_idsAreDistinct(ids, walks.Count));

    test("Every walk runs to completion: ");
    memset(pc, 0, sizeof(pc));
    for (i = 0; (i < walks.Count) && (s == NATS_OK); i++)
        s = natsSysConnzWalk_Run(walks.Walks[i], 10000, _countConnzPages, &pc[i]);
    testCond(s == NATS_OK);

    // Checked against each server's own reported total rather than a count
    // hard-coded here, so a connection lingering from a previous step cannot
    // turn a correct walk into a failure.
    test("Each walk delivers exactly its server's total: ");
    {
        bool ok = true;

        for (i = 0; i < walks.Count; i++)
            ok = ok && (pc[i].items == pc[i].total);
        testCond(ok);
    }

    test("Each walk takes the number of pages its own total implies: ");
    {
        bool ok = true;

        for (i = 0; i < walks.Count; i++)
            ok = ok && (pc[i].pages == (pc[i].total + opts.Limit - 1) / opts.Limit);
        testCond(ok);
    }

    test("The cluster's connections are accounted for exactly once: ");
    {
        // One system connection on node 1, plus the spread opened above.
        int want = 1 + nextra;
        int got  = 0;

        for (i = 0; i < walks.Count; i++)
            got += pc[i].items;
        testCond(got == want);
    }

    // Node 1 holds 7 connections (6 plus the system client), node 2 holds 2 and
    // node 3 holds 1, so at 2 per page the walks take 4, 1 and 1 pages. A
    // shared offset would give every walk the same page count.
    test("The busiest node needed more pages than the quietest: ");
    {
        int most  = 0;
        int least = INT_MAX;

        for (i = 0; i < walks.Count; i++)
        {
            if (pc[i].pages > most)
                most = pc[i].pages;
            if (pc[i].pages < least)
                least = pc[i].pages;
        }
        testCond((most >= 4) && (least == 1));
    }

    natsSysConnzWalkList_Destroy(&walks);
    for (i = 0; i < nextra; i++)
        natsConnection_Destroy(extras[i]);
    _stopCluster(&c);
}

// Subscriptions per node, again uneven. Each is on its own subject so the
// server cannot collapse them in the sublist.
#define CLU_SUBS_N1 (7)
#define CLU_SUBS_N2 (3)
#define CLU_SUBS_N3 (1)

void
test_ClusterSubszPingEach(void)
{
    cluster              c;
    natsStatus           s;
    natsConnection      *conns[CLUSTER_SIZE];
    natsSubscription    *subs[CLU_SUBS_N1 + CLU_SUBS_N2 + CLU_SUBS_N3];
    natsSysSubszWalkList walks = {NULL, 0};
    natsSysSubszRespList list  = {NULL, 0};
    natsSysSubszOptions  opts;
    pageCounter          pc[CLUSTER_SIZE];
    const char          *ids[CLUSTER_SIZE];
    const int            perNode[CLUSTER_SIZE] = {CLU_SUBS_N1, CLU_SUBS_N2, CLU_SUBS_N3};
    int                  nsubs                 = 0;
    int                  i;
    int                  j;

    memset(conns, 0, sizeof(conns));
    memset(subs, 0, sizeof(subs));

    test("Start a three-node cluster: ");
    s = _startCluster(&c, false);
    testCond(s == NATS_OK);

    test("The cluster converges: ");
    s = _waitForCluster(&c, 15000, NULL);
    testCond(s == NATS_OK);

    test("Open an uneven spread of subscriptions: ");
    for (i = 0; (i < CLUSTER_SIZE) && (s == NATS_OK); i++)
    {
        char url[128];

        _clusterURL(url, sizeof(url), i);
        s = natsConnection_ConnectTo(&conns[i], url);
        for (j = 0; (j < perNode[i]) && (s == NATS_OK); j++)
        {
            char subject[64];

            snprintf(subject, sizeof(subject), "clu.%d.%d", i, j);
            s = natsConnection_SubscribeSync(&subs[nsubs++], conns[i], subject);
        }
        if (s == NATS_OK)
            s = natsConnection_Flush(conns[i]);
    }
    testCond(s == NATS_OK);

    test("Ping produces one walk per node: ");
    natsSysSubszOptions_Init(&opts);
    opts.Limit         = 2;
    opts.Subscriptions = true;
    s                  = natsSysClient_SubszPingEach(&walks, c.sys, &opts, 5000);
    testCond((s == NATS_OK) && (walks.Count == CLUSTER_SIZE));

    test("Each walk covers a different server: ");
    for (i = 0; i < walks.Count; i++)
        ids[i] = natsSysSubszWalk_ServerID(walks.Walks[i]);
    testCond(_idsAreDistinct(ids, walks.Count));

    test("Every walk runs to completion: ");
    memset(pc, 0, sizeof(pc));
    for (i = 0; (i < walks.Count) && (s == NATS_OK); i++)
        s = natsSysSubszWalk_Run(walks.Walks[i], 10000, _countSubszPages, &pc[i]);
    testCond(s == NATS_OK);

    test("Each walk delivers as many subscriptions as its server claimed: ");
    {
        bool ok = true;

        for (i = 0; i < walks.Count; i++)
            ok = ok && (pc[i].items == pc[i].total);
        testCond(ok);
    }

    test("Each walk takes the number of pages its own total implies: ");
    {
        bool ok = true;

        for (i = 0; i < walks.Count; i++)
            ok = ok
                 && (pc[i].pages
                     == (pc[i].total + opts.Limit - 1) / opts.Limit);
        testCond(ok);
    }

    // Only the page *count* is checked above, never the contents, because a
    // paginated SUBSZ cannot deliver them: nats-server#7009 pages by offset
    // over a sublist with no stable order, so entries are skipped and others
    // repeated while the per-page counts stay consistent. Measured on
    // v2.14.0-RC.1: of seven subscriptions on one node, a limit-2 walk
    // returned one of them twice and three of them not at all. This is the
    // bug the Go suite skips its pagination tests for; nothing above can be
    // tightened until the server is fixed, so completeness is checked below
    // without pagination instead.
    test("An unpaginated ping reports each node's subscriptions exactly once: ");
    natsSysSubszOptions_Init(&opts);
    opts.Subscriptions = true;
    s                  = natsSysClient_SubszPing(&list, c.sys, &opts, 5000);
    if ((s != NATS_OK) || (list.Count != CLUSTER_SIZE))
        FAIL("subsz ping did not gather the cluster");
    {
        bool ok = true;

        for (i = 0; ok && (i < list.Count); i++)
        {
            subjectTally t;
            const char  *name = list.Resps[i]->Server.Name;
            int          node;
            int          k;

            memset(&t, 0, sizeof(t));
            _tallySubs(&t, &list.Resps[i]->Subsz);

            node = _nodeIndex(name);
            ok   = (node >= 0);

            // Its own, once each, and none of any other node's — which also
            // shows SUBSZ reports local interest only, not what the routes
            // carried in.
            for (j = 0; ok && (j < CLUSTER_SIZE); j++)
                for (k = 0; ok && (k < CLU_SUBS_MAX); k++)
                    ok = (t.Seen[j][k] == (((j == node) && (k < perNode[j])) ? 1 : 0));
        }
        testCond(ok);
    }
    natsSysSubszRespList_Destroy(&list);

    natsSysSubszWalkList_Destroy(&walks);
    for (i = 0; i < nsubs; i++)
        natsSubscription_Destroy(subs[i]);
    for (i = 0; i < CLUSTER_SIZE; i++)
        natsConnection_Destroy(conns[i]);
    _stopCluster(&c);
}

void
test_ClusterJszPingEach(void)
{
    cluster            c;
    natsStatus         s;
    natsConnection    *accts[3] = {NULL, NULL, NULL};
    natsSysJszWalkList walks    = {NULL, 0};
    natsSysJszOptions  opts;
    pageCounter          pc[CLUSTER_SIZE];
    const char        *ids[CLUSTER_SIZE];
    int                i;

    test("Start a three-node JetStream cluster: ");
    s = _startCluster(&c, true);
    testCond(s == NATS_OK);

    // No separate convergence wait: this one is strictly stronger, needing all
    // three nodes to answer JSZ *and* a meta leader to exist, and nothing here
    // reads ActiveServers.
    test("The meta group elects a leader: ");
    s = _waitForMetaLeader(&c, 20000);
    testCond(s == NATS_OK);

    // Replicated across all three nodes so every node reports every account,
    // which is what makes the per-walk page counts predictable.
    //
    // Retried rather than issued once: placing a replicated asset is
    // eventually consistent, and the meta group reports a leader and a full
    // membership before it will reliably accept one. Measured on
    // v2.14.0-RC.1, a single attempt straight after that barrier is refused
    // roughly one run in four, and no stronger readiness predicate available
    // through JSZ — including every listed peer being current and online —
    // moved that number.
    test("Give each account a replicated stream: ");
    for (i = 0; (i < 3) && (s == NATS_OK); i++)
    {
        char           url[128];
        jsCtx         *js       = NULL;
        jsErrCode      jerr     = 0;
        int64_t        deadline = _nowMs() + 15000;
        jsStreamConfig cfg;

        snprintf(url, sizeof(url), "nats://u%d:p%d@127.0.0.1:%d", i + 1, i + 1,
                 CLU_CLIENT_PORT(0));
        s = natsConnection_ConnectTo(&accts[i], url);
        if (s == NATS_OK)
            s = natsConnection_JetStream(&js, accts[i], NULL);
        if (s == NATS_OK)
        {
            jsStreamConfig_Init(&cfg);
            cfg.Name        = "s1";
            cfg.Subjects    = (const char *[]){"sub.>"};
            cfg.SubjectsLen = 1;
            cfg.Replicas    = CLUSTER_SIZE;

            for (;;)
            {
                s = js_AddStream(NULL, js, &cfg, NULL, &jerr);
                if ((s == NATS_OK) || (_nowMs() >= deadline))
                    break;
                usleep(CLUSTER_POLL_MS * 1000);
            }
        }
        jsCtx_Destroy(js);
    }
    testCond(s == NATS_OK);

    test("Ping produces one walk per node: ");
    natsSysJszOptions_Init(&opts);
    opts.Accounts = true;
    opts.Limit    = 1;
    s             = natsSysClient_JszPingEach(&walks, c.sys, &opts, 5000);
    testCond((s == NATS_OK) && (walks.Count == CLUSTER_SIZE));

    test("Each walk covers a different server: ");
    for (i = 0; i < walks.Count; i++)
        ids[i] = natsSysJszWalk_ServerID(walks.Walks[i]);
    testCond(_idsAreDistinct(ids, walks.Count));

    test("Every walk runs to completion: ");
    memset(pc, 0, sizeof(pc));
    for (i = 0; (i < walks.Count) && (s == NATS_OK); i++)
        s = natsSysJszWalk_Run(walks.Walks[i], 10000, _countJszPages, &pc[i]);
    testCond(s == NATS_OK);

    test("Every node reports all three accounts, one per page: ");
    {
        bool ok = true;

        for (i = 0; i < walks.Count; i++)
            ok = ok && (pc[i].total == 3) && (pc[i].items == 3) && (pc[i].pages == 3);
        testCond(ok);
    }

    natsSysJszWalkList_Destroy(&walks);
    for (i = 0; i < 3; i++)
        natsConnection_Destroy(accts[i]);
    _stopCluster(&c);
}

int
main(int argc, char **argv)
{
    const char *envStr;
    const char *testName = NULL;
    testFunc    f        = NULL;
    int         i;

    if (argc != 2)
    {
        printf("@@ Usage: %s [testname]\n", argv[0]);
        return 1;
    }
    testName = argv[1];

    envStr = getenv("NATS_TEST_SERVER_EXE");
    if ((envStr != NULL) && (envStr[0] != '\0'))
        natsServerExe = envStr;

    envStr = getenv("NATS_TEST_KEEP_SERVER_OUTPUT");
    if ((envStr != NULL) && (envStr[0] != '\0'))
        keepServerOutput = true;

    if (nats_Open(-1) != NATS_OK)
    {
        printf("@@ Unable to run tests: unable to initialize the library!\n");
        return 1;
    }

    for (i = 0; i < (int) (sizeof(allTests) / sizeof(allTests[0])); i++)
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

    _stopAllServers();
    _removeRememberedPaths();
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
