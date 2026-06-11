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

// jetstream-extra test suite.
//
// Mirrors the framework used in nats-counters/test/test.c.
//
// Run a single test:
//   ./jetstream_extra_testsuite OptionsInitClearsFields
// Run all tests:
//   ctest --test-dir build

#include "batch_fetch.h"
#include "fast_publish.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <dirent.h>

// Test framework — same as nats-counters.

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

// Filesystem helpers

static void
_rmtree(const char *path)
{
    DIR *dir;
    struct stat st;
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

static void
_makeUniqueDir(char *buf, int bufLen, const char *prefix)
{
    snprintf(buf, bufLen, "%s%d_%d", prefix, (int)getpid(), ++_uniqueCounter);
}

// JetStream setup macros

#define JS_SETUP                                                 \
    natsStatus s = NATS_OK;                                      \
    natsConnection *nc = NULL;                                   \
    jsCtx *js = NULL;                                            \
    natsPid pid = NATS_INVALID_PID;                              \
    char datastore[256] = { '\0' };                              \
    char cmdLine[1024] = { '\0' };                               \
                                                                 \
    _makeUniqueDir(datastore, sizeof(datastore), "datastore_");  \
    test("Start JS Server: ");                                   \
    snprintf(cmdLine, sizeof(cmdLine), "-js -sd %s", datastore); \
    pid = _startServer("nats://127.0.0.1:4222", cmdLine, true);  \
    CHECK_SERVER_STARTED(pid);                                   \
    testCond(true);                                              \
                                                                 \
    test("Connect: ");                                           \
    s = natsConnection_ConnectTo(&nc, "nats://127.0.0.1:4222");  \
    testCond(s == NATS_OK);                                      \
                                                                 \
    test("Get context: ");                                       \
    s = natsConnection_JetStream(&js, nc, NULL);                 \
    testCond(s == NATS_OK);

#define JS_TEARDOWN             \
    jsCtx_Destroy(js);          \
    natsConnection_Destroy(nc); \
    _stopServer(pid);           \
    _rmtree(datastore);

static natsStatus
_createDirectStream(jsCtx *js, const char *name, const char *subj)
{
    jsStreamConfig cfg;
    const char *subjects[1];

    subjects[0] = subj;
    jsStreamConfig_Init(&cfg);
    cfg.Name = name;
    cfg.Subjects = subjects;
    cfg.SubjectsLen = 1;
    cfg.AllowDirect = true;

    return js_AddStream(NULL, js, &cfg, NULL, NULL);
}

static natsStatus
_publish(jsCtx *js, const char *subj, const char *body)
{
    return js_Publish(NULL, js, subj, body, (int)strlen(body), NULL, NULL);
}

static natsStatus
_createBatchStream(jsCtx *js, const char *name, const char *subj)
{
    jsStreamConfig cfg;
    const char *subjects[1];

    subjects[0] = subj;
    jsStreamConfig_Init(&cfg);
    cfg.Name = name;
    cfg.Subjects = subjects;
    cfg.SubjectsLen = 1;
    cfg.AllowBatched = true; // enables fast batch publishing

    return js_AddStream(NULL, js, &cfg, NULL, NULL);
}

//=============================================================================
// Validation tests — no server needed.
//=============================================================================

void
test_OptionsInitClearsFields(void)
{
    jsBatchFetchOptions opts;
    natsStatus s;

    test("Init returns NATS_INVALID_ARG for NULL: ");
    s = jsBatchFetchOptions_Init(NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Init zeros all fields: ");
    memset(&opts, 0xff, sizeof(opts));
    s = jsBatchFetchOptions_Init(&opts);
    testCond((s == NATS_OK)
             && (opts.Sequence == 0)
             && (opts.NextBySubject == NULL)
             && (opts.Batch == 0)
             && (opts.MaxBytes == 0)
             && (opts.StartTime == 0)
             && (opts.MultiLastFor == NULL)
             && (opts.MultiLastForLen == 0)
             && (opts.UpToSeq == 0)
             && (opts.UpToTime == 0));
}

void
test_ValidateNullStream(void)
{
    jsBatchFetchOptions opts;
    natsMsgList list = { 0 };
    natsConnection *nc = (natsConnection *)0x1; // never dereferenced
    natsStatus s;

    jsBatchFetchOptions_Init(&opts);

    test("NULL stream rejected: ");
    s = jsBatchFetch_Fetch(&list, nc, NULL, NULL, &opts, 1000, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Empty stream rejected: ");
    s = jsBatchFetch_Fetch(&list, nc, "", NULL, &opts, 1000, NULL);
    testCond(s == NATS_INVALID_ARG);
}

void
test_ValidateBatchTooLarge(void)
{
    jsBatchFetchOptions opts;
    natsMsgList list = { 0 };
    natsConnection *nc = (natsConnection *)0x1;
    natsStatus s;

    jsBatchFetchOptions_Init(&opts);
    opts.Batch = JS_BATCH_FETCH_MAX_BATCH + 1;

    test("Batch > 1000 rejected: ");
    s = jsBatchFetch_Fetch(&list, nc, "S", NULL, &opts, 1000, NULL);
    testCond(s == NATS_INVALID_ARG);
}

void
test_ValidateSequenceAndStartTimeMutex(void)
{
    jsBatchFetchOptions opts;
    natsMsgList list = { 0 };
    natsConnection *nc = (natsConnection *)0x1;
    natsStatus s;

    jsBatchFetchOptions_Init(&opts);
    opts.Sequence = 10;
    opts.StartTime = 1000;

    test("Sequence + StartTime rejected: ");
    s = jsBatchFetch_Fetch(&list, nc, "S", NULL, &opts, 1000, NULL);
    testCond(s == NATS_INVALID_ARG);
}

void
test_ValidateUpToSeqAndUpToTimeMutex(void)
{
    jsBatchFetchOptions opts;
    natsMsgList list = { 0 };
    natsConnection *nc = (natsConnection *)0x1;
    natsStatus s;

    jsBatchFetchOptions_Init(&opts);
    opts.UpToSeq = 10;
    opts.UpToTime = 1000;

    test("UpToSeq + UpToTime rejected: ");
    s = jsBatchFetch_Fetch(&list, nc, "S", NULL, &opts, 1000, NULL);
    testCond(s == NATS_INVALID_ARG);
}

void
test_ValidateMultiLastForLenZero(void)
{
    jsBatchFetchOptions opts;
    const char *subjects[1] = { "a" };
    natsMsgList list = { 0 };
    natsConnection *nc = (natsConnection *)0x1;
    natsStatus s;

    jsBatchFetchOptions_Init(&opts);
    opts.MultiLastFor = subjects;
    opts.MultiLastForLen = 0;

    test("MultiLastFor with length 0 rejected: ");
    s = jsBatchFetch_Fetch(&list, nc, "S", NULL, &opts, 1000, NULL);
    testCond(s == NATS_INVALID_ARG);
}

void
test_ValidateTooManySubjects(void)
{
    jsBatchFetchOptions opts;
    const char **subjects;
    natsMsgList list = { 0 };
    natsConnection *nc = (natsConnection *)0x1;
    natsStatus s;
    int i;
    int n = JS_BATCH_FETCH_MAX_SUBJECTS + 1;

    subjects = (const char **)calloc((size_t)n, sizeof(*subjects));
    for (i = 0; i < n; i++)
        subjects[i] = "x";

    jsBatchFetchOptions_Init(&opts);
    opts.MultiLastFor = subjects;
    opts.MultiLastForLen = n;

    test("> 1024 subjects rejected: ");
    s = jsBatchFetch_Fetch(&list, nc, "S", NULL, &opts, 1000, NULL);
    testCond(s == NATS_INVALID_ARG);

    free(subjects);
}

void
test_ValidateTimeoutZeroSync(void)
{
    jsBatchFetchOptions opts;
    natsMsgList list = { 0 };
    natsConnection *nc = (natsConnection *)0x1;
    natsStatus s;

    jsBatchFetchOptions_Init(&opts);

    test("Sync timeout=0 rejected: ");
    s = jsBatchFetch_Fetch(&list, nc, "S", NULL, &opts, 0, NULL);
    testCond(s == NATS_INVALID_ARG);
}

//=============================================================================
// Integration tests — require a nats-server.
//=============================================================================

void
test_BatchFetchHappyPath(void)
{
    jsBatchFetchOptions opts;
    natsMsgList list = { 0 };
    int i;
    int nMsgs = 50;

    JS_SETUP;

    test("Create direct-allowed stream: ");
    s = _createDirectStream(js, "BATCH", "batch.>");
    testCond(s == NATS_OK);

    test("Publish messages: ");
    for (i = 1; s == NATS_OK && i <= nMsgs; i++)
    {
        char subj[32], body[16];
        snprintf(subj, sizeof(subj), "batch.%d", i);
        snprintf(body, sizeof(body), "v%d", i);
        s = _publish(js, subj, body);
    }
    testCond(s == NATS_OK);

    test("Fetch full batch: ");
    jsBatchFetchOptions_Init(&opts);
    opts.Batch = nMsgs;
    s = jsBatchFetch_Fetch(&list, nc, "BATCH", NULL, &opts, 5000, NULL);
    testCond((s == NATS_OK) && (list.Count == nMsgs));

    natsMsgList_Destroy(&list);
    JS_TEARDOWN;
}

void
test_BatchFetchByteCap(void)
{
    jsBatchFetchOptions opts;
    natsMsgList list = { 0 };
    int i;

    JS_SETUP;

    test("Create direct-allowed stream: ");
    s = _createDirectStream(js, "BCAP", "bcap.>");
    testCond(s == NATS_OK);

    test("Publish 20 messages with small bodies: ");
    for (i = 0; s == NATS_OK && i < 20; i++)
    {
        char subj[32];
        snprintf(subj, sizeof(subj), "bcap.%d", i);
        s = _publish(js, subj, "0123456789"); // 10 bytes payload
    }
    testCond(s == NATS_OK);

    test("Fetch with MaxBytes=50 returns truncated batch: ");
    jsBatchFetchOptions_Init(&opts);
    opts.Batch = 20;
    opts.MaxBytes = 50;
    s = jsBatchFetch_Fetch(&list, nc, "BCAP", NULL, &opts, 5000, NULL);
    // Server should stop short of returning all 20.
    testCond((s == NATS_OK) && (list.Count > 0) && (list.Count < 20));

    natsMsgList_Destroy(&list);
    JS_TEARDOWN;
}

void
test_BatchFetchMultiLastFor(void)
{
    jsBatchFetchOptions opts;
    natsMsgList list = { 0 };
    const char *subjects[3] = { "ml.a", "ml.b", "ml.c" };
    int i;
    bool seenA = false, seenB = false, seenC = false;

    JS_SETUP;

    test("Create direct-allowed stream: ");
    s = _createDirectStream(js, "MLAST", "ml.>");
    testCond(s == NATS_OK);

    test("Publish two values per subject: ");
    for (i = 0; s == NATS_OK && i < 3; i++)
    {
        s = _publish(js, subjects[i], "first");
        if (s == NATS_OK)
            s = _publish(js, subjects[i], "last");
    }
    testCond(s == NATS_OK);

    test("MultiLastFor returns one msg per subject: ");
    jsBatchFetchOptions_Init(&opts);
    opts.MultiLastFor = subjects;
    opts.MultiLastForLen = 3;
    s = jsBatchFetch_Fetch(&list, nc, "MLAST", NULL, &opts, 5000, NULL);

    if (s == NATS_OK)
    {
        for (i = 0; i < list.Count; i++)
        {
            const char *origSubj = NULL;
            const char *data = natsMsg_GetData(list.Msgs[i]);
            int dlen = natsMsg_GetDataLength(list.Msgs[i]);

            // DIRECT.GET responses carry the original subject in a header.
            natsMsgHeader_Get(list.Msgs[i], JSSubject, &origSubj);

            if (origSubj == NULL || data == NULL || dlen != 4)
                continue;
            if (memcmp(data, "last", 4) != 0)
                continue;
            if (strcmp(origSubj, "ml.a") == 0)
                seenA = true;
            if (strcmp(origSubj, "ml.b") == 0)
                seenB = true;
            if (strcmp(origSubj, "ml.c") == 0)
                seenC = true;
        }
    }
    testCond((s == NATS_OK) && (list.Count == 3) && seenA && seenB && seenC);

    natsMsgList_Destroy(&list);
    JS_TEARDOWN;
}

typedef struct
{
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    int             msgCount;
    int             doneCount;
    natsStatus      doneStatus;

} _asyncTestCtx;

static void
_asyncMsg(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    _asyncTestCtx *c = (_asyncTestCtx *)closure;
    (void)nc;
    (void)sub;
    pthread_mutex_lock(&c->mu);
    c->msgCount++;
    pthread_mutex_unlock(&c->mu);
    natsMsg_Destroy(msg);
}

static void
_asyncDone(natsStatus s, jsErrCode je, void *closure)
{
    _asyncTestCtx *c = (_asyncTestCtx *)closure;
    (void)je;
    pthread_mutex_lock(&c->mu);
    c->doneCount++;
    c->doneStatus = s;
    pthread_cond_signal(&c->cv);
    pthread_mutex_unlock(&c->mu);
}

void
test_BatchFetchAsyncHappyPath(void)
{
    jsBatchFetchOptions opts;
    _asyncTestCtx ctx = { 0 };
    int i;
    int nMsgs = 25;
    int waitedMs = 0;

    JS_SETUP;

    pthread_mutex_init(&ctx.mu, NULL);
    pthread_cond_init(&ctx.cv, NULL);

    test("Create direct-allowed stream: ");
    s = _createDirectStream(js, "ASYNC", "async.>");
    testCond(s == NATS_OK);

    test("Publish messages: ");
    for (i = 1; s == NATS_OK && i <= nMsgs; i++)
    {
        char subj[32], body[16];
        snprintf(subj, sizeof(subj), "async.%d", i);
        snprintf(body, sizeof(body), "v%d", i);
        s = _publish(js, subj, body);
    }
    testCond(s == NATS_OK);

    test("Async fetch dispatches and completes: ");
    jsBatchFetchOptions_Init(&opts);
    opts.Batch = nMsgs;
    s = jsBatchFetch_AsyncFetch(nc, "ASYNC", NULL, &opts, _asyncMsg, _asyncDone, &ctx);
    if (s == NATS_OK)
    {
        // Wait up to 5s for doneCB.
        pthread_mutex_lock(&ctx.mu);
        while (ctx.doneCount == 0 && waitedMs < 5000)
        {
            struct timespec until;
            clock_gettime(CLOCK_REALTIME, &until);
            until.tv_sec += 0;
            until.tv_nsec += 100 * 1000 * 1000;
            if (until.tv_nsec >= 1000000000)
            {
                until.tv_sec++;
                until.tv_nsec -= 1000000000;
            }
            pthread_cond_timedwait(&ctx.cv, &ctx.mu, &until);
            waitedMs += 100;
        }
        pthread_mutex_unlock(&ctx.mu);
    }
    testCond((s == NATS_OK)
             && (ctx.doneCount == 1)
             && (ctx.doneStatus == NATS_OK)
             && (ctx.msgCount == nMsgs));

    pthread_mutex_destroy(&ctx.mu);
    pthread_cond_destroy(&ctx.cv);
    JS_TEARDOWN;
}

void
test_BatchFetchStartTime(void)
{
    jsBatchFetchOptions opts;
    natsMsgList list = { 0 };
    struct timespec ts;
    uint64_t startNs = 0;
    int i;
    bool allAfter;

    JS_SETUP;

    test("Create direct-allowed stream: ");
    s = _createDirectStream(js, "STIME", "stime.>");
    testCond(s == NATS_OK);

    test("Publish 5 messages BEFORE the cutoff: ");
    for (i = 0; s == NATS_OK && i < 5; i++)
    {
        char subj[32];
        snprintf(subj, sizeof(subj), "stime.before.%d", i);
        s = _publish(js, subj, "old");
    }
    testCond(s == NATS_OK);

    nats_Sleep(50);
    clock_gettime(CLOCK_REALTIME, &ts);
    startNs = (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
    nats_Sleep(50);

    test("Publish 5 messages AFTER the cutoff: ");
    for (i = 0; s == NATS_OK && i < 5; i++)
    {
        char subj[32];
        snprintf(subj, sizeof(subj), "stime.after.%d", i);
        s = _publish(js, subj, "new");
    }
    testCond(s == NATS_OK);

    test("Fetch with StartTime returns only post-cutoff messages: ");
    jsBatchFetchOptions_Init(&opts);
    opts.StartTime = startNs;
    opts.NextBySubject = "stime.>";
    opts.Batch = 50;
    s = jsBatchFetch_Fetch(&list, nc, "STIME", NULL, &opts, 5000, NULL);

    allAfter = (s == NATS_OK) && (list.Count == 5);
    for (i = 0; allAfter && i < list.Count; i++)
    {
        const char *origSubj = NULL;
        natsMsgHeader_Get(list.Msgs[i], JSSubject, &origSubj);
        if (origSubj == NULL || strncmp(origSubj, "stime.after.", 12) != 0)
            allAfter = false;
    }
    testCond(allAfter);

    natsMsgList_Destroy(&list);
    JS_TEARDOWN;
}

void
test_BatchFetchUnsupportedStream(void)
{
    jsBatchFetchOptions opts;
    natsMsgList list = { 0 };
    jsStreamConfig cfg;
    const char *subjects[1] = { "nd.>" };

    JS_SETUP;

    test("Create stream WITHOUT AllowDirect: ");
    jsStreamConfig_Init(&cfg);
    cfg.Name = "NODIRECT";
    cfg.Subjects = subjects;
    cfg.SubjectsLen = 1;
    cfg.AllowDirect = false;
    s = js_AddStream(NULL, js, &cfg, NULL, NULL);
    testCond(s == NATS_OK);

    test("Publish a message: ");
    s = _publish(js, "nd.x", "hi");
    testCond(s == NATS_OK);

    test("Fetch surfaces server rejection (timeout or status): ");
    jsBatchFetchOptions_Init(&opts);
    opts.Batch = 10;
    s = jsBatchFetch_Fetch(&list, nc, "NODIRECT", NULL, &opts, 1500, NULL);
    // The DIRECT.GET subject won't be served. Expect either timeout or a
    // non-OK status — but never NATS_OK with messages.
    testCond((s != NATS_OK) || (list.Count == 0));

    natsMsgList_Destroy(&list);
    JS_TEARDOWN;
}

//=============================================================================
// Fast publish — validation tests, no server needed.
//=============================================================================

void
test_FastPublishOptionsInit(void)
{
    jsFastPublisherOptions opts;
    natsStatus s;

    test("Init returns NATS_INVALID_ARG for NULL: ");
    s = jsFastPublisherOptions_Init(NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Init zeros all fields: ");
    memset(&opts, 0xff, sizeof(opts));
    s = jsFastPublisherOptions_Init(&opts);
    testCond((s == NATS_OK)
             && (opts.Flow == 0)
             && (opts.MaxOutstandingAcks == 0)
             && (opts.AckTimeout == 0)
             && (opts.ContinueOnGap == false)
             && (opts.ErrHandler == NULL)
             && (opts.ErrHandlerClosure == NULL));
}

void
test_FastPublishBatchMsgOptsInit(void)
{
    jsBatchMsgOpts opts;
    natsStatus s;

    test("Init returns NATS_INVALID_ARG for NULL: ");
    s = jsBatchMsgOpts_Init(NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Init zeros all fields: ");
    memset(&opts, 0xff, sizeof(opts));
    s = jsBatchMsgOpts_Init(&opts);
    testCond((s == NATS_OK)
             && (opts.TTL == 0)
             && (opts.ExpectedStream == NULL)
             && (opts.ExpectedLastSubject == NULL)
             && (opts.ExpectedLastSubjSeq == 0)
             && (opts.HasExpectedLastSubjSeq == false)
             && (opts.ExpectedLastSeq == 0)
             && (opts.HasExpectedLastSeq == false));
}

void
test_FastPublishCreateValidation(void)
{
    jsFastPublishCtx *ctx = NULL;
    jsCtx *js = (jsCtx *)0x1; // Create only stores js, never dereferences it
    natsStatus s;

    test("NULL out-param rejected: ");
    s = jsFastPublishCtx_Create(NULL, js, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("NULL js rejected: ");
    s = jsFastPublishCtx_Create(&ctx, NULL, NULL);
    testCond((s == NATS_INVALID_ARG) && (ctx == NULL));
}

void
test_FastPublishAddValidation(void)
{
    jsFastPublishCtx *fakeCtx = (jsFastPublishCtx *)0x1; // never dereferenced
    natsConnection *fakeNc = (natsConnection *)0x1;
    natsStatus s;

    test("Add NULL ctx rejected: ");
    s = jsFastPublish_Add(NULL, NULL, fakeNc, "s", "d", 1, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Add NULL connection rejected: ");
    s = jsFastPublish_Add(NULL, fakeCtx, NULL, "s", "d", 1, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Add NULL subject rejected: ");
    s = jsFastPublish_Add(NULL, fakeCtx, fakeNc, NULL, "d", 1, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("AddMsg NULL ctx rejected: ");
    s = jsFastPublish_AddMsg(NULL, NULL, (natsMsg *)0x1, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("AddMsg NULL msg rejected: ");
    s = jsFastPublish_AddMsg(NULL, fakeCtx, NULL, NULL);
    testCond(s == NATS_INVALID_ARG);
}

void
test_FastPublishCommitValidation(void)
{
    jsFastPublishCtx *fakeCtx = (jsFastPublishCtx *)0x1; // never dereferenced
    natsMsg *fakeMsg = (natsMsg *)0x1;
    natsStatus s;

    test("Commit NULL ctx rejected: ");
    s = jsFastPublish_Commit(NULL, NULL, "s", "d", 1, NULL, 1000);
    testCond(s == NATS_INVALID_ARG);

    test("Commit NULL subject rejected: ");
    s = jsFastPublish_Commit(NULL, fakeCtx, NULL, "d", 1, NULL, 1000);
    testCond(s == NATS_INVALID_ARG);

    test("CommitMsg NULL ctx rejected: ");
    s = jsFastPublish_CommitMsg(NULL, NULL, fakeMsg, NULL, 1000);
    testCond(s == NATS_INVALID_ARG);

    test("CommitMsg NULL msg rejected: ");
    s = jsFastPublish_CommitMsg(NULL, fakeCtx, NULL, NULL, 1000);
    testCond(s == NATS_INVALID_ARG);

    test("Close NULL ctx rejected: ");
    s = jsFastPublish_Close(NULL, NULL, 1000);
    testCond(s == NATS_INVALID_ARG);
}

void
test_FastPublishLifecycleNoServer(void)
{
    jsFastPublishCtx *ctx = NULL;
    jsCtx *js = (jsCtx *)0x1; // Create only stores js, never dereferences it
    natsMsg *msg = NULL;
    natsStatus s;

    test("IsClosed(NULL) returns true: ");
    testCond(jsFastPublish_IsClosed(NULL) == true);

    test("Destroy(NULL) is a no-op: ");
    jsFastPublish_Destroy(NULL);
    testCond(true);

    test("Create succeeds with default options: ");
    s = jsFastPublishCtx_Create(&ctx, js, NULL);
    testCond((s == NATS_OK) && (ctx != NULL));

    test("Fresh batch reports not closed: ");
    testCond(jsFastPublish_IsClosed(ctx) == false);

    test("AddMsg before first Add is rejected (no connection): ");
    s = natsMsg_Create(&msg, "test.x", NULL, "hi", 2);
    if (s == NATS_OK)
        s = jsFastPublish_AddMsg(NULL, ctx, msg, NULL);
    testCond(s == NATS_INVALID_ARG);
    natsMsg_Destroy(msg);

    test("Close on an empty batch returns NATS_ERR: ");
    s = jsFastPublish_Close(NULL, ctx, 1000);
    testCond(s == NATS_ERR);

    jsFastPublish_Destroy(ctx);
}

//=============================================================================
// Fast publish — integration tests, require a nats-server.
//=============================================================================

void
test_FastPublishHappyPath(void)
{
    jsFastPublishCtx *fp = NULL;
    jsFastPubAck fpAck = { 0 };
    jsPubAck *pubAck = NULL;
    natsMsg *msg = NULL;

    JS_SETUP;

    test("Create batch-publish stream: ");
    s = _createBatchStream(js, "FAST", "fast.>");
    testCond(s == NATS_OK);

    test("Create fast publisher: ");
    s = jsFastPublishCtx_Create(&fp, js, NULL);
    testCond((s == NATS_OK) && (fp != NULL));

    test("Add first message: ");
    s = jsFastPublish_Add(&fpAck, fp, nc, "fast.1", "message 1", 9, NULL);
    testCond((s == NATS_OK) && (fpAck.BatchSequence == 1));

    test("AddMsg second message: ");
    s = natsMsg_Create(&msg, "fast.2", NULL, "message 2", 9);
    if (s == NATS_OK)
        s = jsFastPublish_AddMsg(&fpAck, fp, msg, NULL);
    testCond((s == NATS_OK) && (fpAck.BatchSequence == 2));
    natsMsg_Destroy(msg);

    test("Commit batch with final message: ");
    s = jsFastPublish_Commit(&pubAck, fp, "fast.3", "message 3", 9, NULL, 5000);
    testCond((s == NATS_OK)
             && (pubAck != NULL)
             && (pubAck->Stream != NULL)
             && (strcmp(pubAck->Stream, "FAST") == 0));
    jsPubAck_Destroy(pubAck);

    test("Batch reports closed after commit: ");
    testCond(jsFastPublish_IsClosed(fp) == true);

    test("Add after commit is rejected: ");
    s = jsFastPublish_Add(NULL, fp, nc, "fast.4", "message 4", 9, NULL);
    testCond(s == NATS_ERR);

    jsFastPublish_Destroy(fp);
    JS_TEARDOWN;
}

void
test_FastPublishCloseEndsBatch(void)
{
    jsFastPublishCtx *fp = NULL;

    JS_SETUP;

    test("Create batch-publish stream: ");
    s = _createBatchStream(js, "FASTC", "fastc.>");
    testCond(s == NATS_OK);

    test("Create fast publisher: ");
    s = jsFastPublishCtx_Create(&fp, js, NULL);
    testCond((s == NATS_OK) && (fp != NULL));

    test("Add a message: ");
    s = jsFastPublish_Add(NULL, fp, nc, "fastc.1", "only", 4, NULL);
    testCond(s == NATS_OK);

    test("Close commits the batch without a final message: ");
    s = jsFastPublish_Close(NULL, fp, 5000);
    testCond(s == NATS_OK);

    test("Batch reports closed after Close: ");
    testCond(jsFastPublish_IsClosed(fp) == true);

    jsFastPublish_Destroy(fp);
    JS_TEARDOWN;
}

void
test_FastPublishLargeBatchStall(void)
{
    jsFastPublishCtx *fp = NULL;
    jsFastPubAck fpAck = { 0 };
    jsPubAck *pubAck = NULL;
    int i;
    int nMsgs = 250; // exceeds the default 200-message ack window
    uint64_t maxGap = 0;
    bool seqOk = true;

    JS_SETUP;

    test("Create batch-publish stream: ");
    s = _createBatchStream(js, "FBIG", "fbig.>");
    testCond(s == NATS_OK);

    test("Create fast publisher (default flow control): ");
    s = jsFastPublishCtx_Create(&fp, js, NULL);
    testCond((s == NATS_OK) && (fp != NULL));

    // With the default Flow=100 / MaxOutstandingAcks=2, the publisher must
    // stall and resume across a 200-message window while sending 250.
    test("Add 250 messages across the stall window: ");
    for (i = 1; s == NATS_OK && i <= nMsgs; i++)
    {
        char subj[32], body[32];
        snprintf(subj, sizeof(subj), "fbig.%d", i);
        snprintf(body, sizeof(body), "m%d", i);
        s = jsFastPublish_Add(&fpAck, fp, nc, subj, body, (int)strlen(body), NULL);
        if (s == NATS_OK)
        {
            if (fpAck.AckSequence > fpAck.BatchSequence)
                seqOk = false; // AckSequence must never lead BatchSequence
            if (fpAck.BatchSequence - fpAck.AckSequence > maxGap)
                maxGap = fpAck.BatchSequence - fpAck.AckSequence;
        }
    }
    // maxGap > 0 proves the publisher outran the flow acks (messages were
    // in flight), i.e. the stall/window path was actually exercised.
    testCond((s == NATS_OK) && seqOk && (maxGap > 0));

    test("Commit seals the full batch: ");
    s = jsFastPublish_Commit(&pubAck, fp, "fbig.commit", "done", 4, NULL, 5000);
    testCond((s == NATS_OK)
             && (pubAck != NULL)
             && (pubAck->Count == (uint64_t)(nMsgs + 1)));
    jsPubAck_Destroy(pubAck);

    jsFastPublish_Destroy(fp);
    JS_TEARDOWN;
}

void
test_FastPublishMsgHeaders(void)
{
    jsFastPublishCtx *fp = NULL;
    jsPubAck *pubAck = NULL;
    natsSubscription *sub = NULL;
    natsMsg *rcv = NULL;
    natsMsg *um = NULL;
    jsStreamConfig cfg;
    jsBatchMsgOpts mo;
    const char *subjects[1] = { "fhdr.>" };
    const char *v = NULL;
    bool ttlOk = false;
    bool streamOk = false;

    JS_SETUP;

    test("Create batch+TTL stream: ");
    jsStreamConfig_Init(&cfg);
    cfg.Name = "FHDR";
    cfg.Subjects = subjects;
    cfg.SubjectsLen = 1;
    cfg.AllowBatched = true;
    cfg.AllowMsgTTL = true; // accept the per-message Nats-TTL header
    s = js_AddStream(NULL, js, &cfg, NULL, NULL);
    testCond(s == NATS_OK);

    // A core subscriber on the subject observes exactly what the publisher
    // puts on the wire, so we can assert the option/header translation.
    test("Core-subscribe to observe wire headers: ");
    s = natsConnection_SubscribeSync(&sub, nc, "fhdr.>");
    testCond(s == NATS_OK);

    test("Create fast publisher: ");
    s = jsFastPublishCtx_Create(&fp, js, NULL);
    testCond((s == NATS_OK) && (fp != NULL));

    test("Add applies TTL and ExpectedStream as headers: ");
    jsBatchMsgOpts_Init(&mo);
    mo.TTL = 60000000000LL; // 60s, in nanoseconds
    mo.ExpectedStream = "FHDR";
    s = jsFastPublish_Add(NULL, fp, nc, "fhdr.1", "one", 3, &mo);
    if (s == NATS_OK)
        s = natsSubscription_NextMsg(&rcv, sub, 2000);
    if (s == NATS_OK)
    {
        natsMsgHeader_Get(rcv, "Nats-TTL", &v);
        ttlOk = (v != NULL) && (strcmp(v, "60000000000ns") == 0);
        v = NULL;
        natsMsgHeader_Get(rcv, "Nats-Expected-Stream", &v);
        streamOk = (v != NULL) && (strcmp(v, "FHDR") == 0);
    }
    natsMsg_Destroy(rcv);
    rcv = NULL;
    testCond((s == NATS_OK) && ttlOk && streamOk);

    test("AddMsg copies caller headers onto the wire: ");
    s = natsMsg_Create(&um, "fhdr.2", NULL, "two", 3);
    if (s == NATS_OK)
        s = natsMsgHeader_Set(um, "X-Test", "custom");
    if (s == NATS_OK)
        s = jsFastPublish_AddMsg(NULL, fp, um, NULL);
    if (s == NATS_OK)
        s = natsSubscription_NextMsg(&rcv, sub, 2000);
    v = NULL;
    if (s == NATS_OK)
        natsMsgHeader_Get(rcv, "X-Test", &v);
    natsMsg_Destroy(rcv);
    rcv = NULL;
    natsMsg_Destroy(um);
    um = NULL;
    testCond((s == NATS_OK) && (v != NULL) && (strcmp(v, "custom") == 0));

    test("Commit seals the batch: ");
    s = jsFastPublish_Commit(&pubAck, fp, "fhdr.3", "three", 5, NULL, 5000);
    testCond(s == NATS_OK);
    jsPubAck_Destroy(pubAck);

    natsSubscription_Destroy(sub);
    jsFastPublish_Destroy(fp);
    JS_TEARDOWN;
}

void
test_FastPublishCommitMsg(void)
{
    jsFastPublishCtx *fp = NULL;
    jsPubAck *pubAck = NULL;
    natsMsg *msg = NULL;

    JS_SETUP;

    test("Create batch-publish stream: ");
    s = _createBatchStream(js, "FCM", "fcm.>");
    testCond(s == NATS_OK);

    test("Create fast publisher: ");
    s = jsFastPublishCtx_Create(&fp, js, NULL);
    testCond((s == NATS_OK) && (fp != NULL));

    test("Add a message: ");
    s = jsFastPublish_Add(NULL, fp, nc, "fcm.1", "first", 5, NULL);
    testCond(s == NATS_OK);

    test("CommitMsg seals the batch with a prebuilt message: ");
    s = natsMsg_Create(&msg, "fcm.2", NULL, "last", 4);
    if (s == NATS_OK)
        s = jsFastPublish_CommitMsg(&pubAck, fp, msg, NULL, 5000);
    testCond((s == NATS_OK)
             && (pubAck != NULL)
             && (pubAck->Stream != NULL)
             && (strcmp(pubAck->Stream, "FCM") == 0)
             && (pubAck->Count == 2));
    natsMsg_Destroy(msg);
    jsPubAck_Destroy(pubAck);

    test("Batch reports closed after CommitMsg: ");
    testCond(jsFastPublish_IsClosed(fp) == true);

    jsFastPublish_Destroy(fp);
    JS_TEARDOWN;
}

typedef struct
{
    pthread_mutex_t mu;
    int             count;
    natsStatus      lastStatus;

} _fpErrCtx;

static void
_fpOnErr(natsStatus status, const char *description, void *closure)
{
    _fpErrCtx *c = (_fpErrCtx *)closure;
    (void)description;
    pthread_mutex_lock(&c->mu);
    c->count++;
    c->lastStatus = status;
    pthread_mutex_unlock(&c->mu);
}

void
test_FastPublishRejectedBatch(void)
{
    jsFastPublishCtx *fp = NULL;
    jsFastPublisherOptions opts;
    jsBatchMsgOpts mo;
    _fpErrCtx ectx = { 0 };
    int waitedMs = 0;
    int errCount = 0;

    JS_SETUP;

    pthread_mutex_init(&ectx.mu, NULL);

    test("Create batch-publish stream: ");
    s = _createBatchStream(js, "FREJ", "frej.>");
    testCond(s == NATS_OK);

    test("Create fast publisher with an error handler: ");
    jsFastPublisherOptions_Init(&opts);
    opts.ErrHandler = _fpOnErr;
    opts.ErrHandlerClosure = &ectx;
    s = jsFastPublishCtx_Create(&fp, js, &opts);
    testCond((s == NATS_OK) && (fp != NULL));

    // The stream is empty, so asserting a last sequence of 999 must be
    // rejected by the server. The rejection is delivered asynchronously on
    // the ack inbox and surfaced through the error handler — the Add call
    // itself may have already returned by then.
    test("Add with a bogus ExpectedLastSeq is rejected asynchronously: ");
    jsBatchMsgOpts_Init(&mo);
    mo.HasExpectedLastSeq = true;
    mo.ExpectedLastSeq = 999;
    jsFastPublish_Add(NULL, fp, nc, "frej.1", "x", 1, &mo);

    while (waitedMs < 5000)
    {
        pthread_mutex_lock(&ectx.mu);
        errCount = ectx.count;
        pthread_mutex_unlock(&ectx.mu);
        if (errCount > 0 && jsFastPublish_IsClosed(fp))
            break;
        nats_Sleep(100);
        waitedMs += 100;
    }
    testCond((errCount > 0) && (jsFastPublish_IsClosed(fp) == true));

    test("Add after a rejected batch fails: ");
    s = jsFastPublish_Add(NULL, fp, nc, "frej.2", "y", 1, NULL);
    testCond(s != NATS_OK);

    pthread_mutex_destroy(&ectx.mu);
    jsFastPublish_Destroy(fp);
    JS_TEARDOWN;
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
