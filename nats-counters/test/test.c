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

// nats-counters test suite.
//
// Each test function is registered in list_test.txt as _test(<Name>).
// The list.h two-pass macro system generates forward declarations and the
// dispatch table (same pattern as nats.c/test).
//
// Run a single test:
//   ./testsuite ParseValueValid
// Run all tests:
//   ctest --test-dir build

#include "nats_counters.h"
#include "parser.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <dirent.h>

// Test framework — mirrors nats.c/test conventions.

typedef void (*testFunc)(void);

typedef struct
{
    const char *name;
    testFunc   func;

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

// Server lifecycle — simplified from nats.c/test/test.h.
//
// We cannot use nats.c internal APIs (natsMutex, natsHash, etc.) so we track
// at most one server PID at a time via a simple global.

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

// _checkStart attempts a TCP connection to url to verify the server is up.
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
        // Child — build argv from "nats-server <cmdLineOpts> -a 127.0.0.1 [-l server.log]"
        char combined[2048];
        char *argvPtrs[64];
        int index = 0;

        snprintf(combined, sizeof(combined), "%s%s%s -a 127.0.0.1%s",
                 natsServerExe,
                 (cmdLineOpts != NULL ? " " : ""),
                 (cmdLineOpts != NULL ? cmdLineOpts : ""),
                 (keepServerOutput ? "" : " -l " LOGFILE_NAME));

        // Tokenize in-place.
        char *p = combined;
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

    // Parent — optionally wait for the server to become reachable.
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

// Generate a unique directory name using PID and an incrementing counter.
static int _uniqueCounter = 0;

static void
_makeUniqueDir(char *buf, int bufLen, const char *prefix)
{
    snprintf(buf, bufLen, "%s%d_%d", prefix, (int)getpid(), ++_uniqueCounter);
}

// JetStream setup / teardown macros
//
// JS_SETUP declares local variables, starts a nats-server with -js, connects,
// and creates a JetStream context.
//
// JS_TEARDOWN cleans up all resources.

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

// Stream helper — creates a counter-ready stream for the given subject.

static natsStatus
_createStream(jsCtx *js, const char *name, const char *subj)
{
    jsStreamConfig cfg;
    const char *subjects[1];

    subjects[0] = subj;
    jsStreamConfig_Init(&cfg);
    cfg.Name = name;
    cfg.Subjects = subjects;
    cfg.SubjectsLen = 1;
    cfg.AllowDirect = true;
    cfg.AllowMsgCounter = true;

    return js_AddStream(NULL, js, &cfg, NULL, NULL);
}

//=============================================================================
// Unit tests — parser (no server needed)
//=============================================================================

void
test_ParseValueValid(void)
{
    natsStatus s;
    char *value = NULL;
    const unsigned char body[] = "{\"val\":\"123\"}";

    test("Parse valid body: ");
    s = natsCounterParser_ParseValue(body, sizeof(body) - 1, &value);
    testCond((s == NATS_OK) && (value != NULL) && (strcmp(value, "123") == 0));
    free(value);
}

void
test_ParseValueNegative(void)
{
    natsStatus s;
    char *value = NULL;
    const unsigned char body[] = "{\"val\":\"-456\"}";

    test("Parse negative value: ");
    s = natsCounterParser_ParseValue(body, sizeof(body) - 1, &value);
    testCond((s == NATS_OK) && (value != NULL) && (strcmp(value, "-456") == 0));
    free(value);
}

void
test_ParseValueLarge(void)
{
    natsStatus s;
    char *value = NULL;
    const unsigned char body[] = "{\"val\":\"999999999999999999999999999999\"}";

    test("Parse value larger than int64 range: ");
    s = natsCounterParser_ParseValue(body, sizeof(body) - 1, &value);
    testCond((s == NATS_OK) && (value != NULL) && (strcmp(value, "999999999999999999999999999999") == 0));
    free(value);
}

void
test_ParseValueEmpty(void)
{
    natsStatus s;
    char *value = NULL;

    test("Parse NULL data returns error: ");
    s = natsCounterParser_ParseValue(NULL, 0, &value);
    testCond(s == NATS_ERR);
}

void
test_ParseValueInvalidJSON(void)
{
    natsStatus s;
    char *value = NULL;
    const unsigned char body[] = "not json";

    test("Parse invalid JSON returns error: ");
    s = natsCounterParser_ParseValue(body, sizeof(body) - 1, &value);
    testCond(s == NATS_ERR);
}

void
test_ParseValueInvalidNumber(void)
{
    natsStatus s;
    char *value = NULL;
    const unsigned char body[] = "{\"val\":\"not_a_number\"}";

    test("Parse non-numeric value returns error: ");
    s = natsCounterParser_ParseValue(body, sizeof(body) - 1, &value);
    testCond(s == NATS_ERR);
}

void
test_ParsePubAckValue(void)
{
    natsStatus s;
    char *value = NULL;

    test("Parse PubAck value: ");
    s = natsCounterParser_ParsePubAckValue("42", &value);
    testCond((s == NATS_OK) && (value != NULL) && (strcmp(value, "42") == 0));
    free(value);
}

void
test_ParsePubAckValueNone(void)
{
    natsStatus s;
    char *value = NULL;

    test("Parse PubAck NULL returns NOT_FOUND: ");
    s = natsCounterParser_ParsePubAckValue(NULL, &value);
    testCond(s == NATS_NOT_FOUND);
}

void
test_ParseSourcesEmpty(void)
{
    natsStatus s;
    natsCounterSource *sources = NULL;

    test("Parse NULL sources header returns empty list: ");
    s = natsCounterParser_ParseSources(NULL, &sources);
    testCond((s == NATS_OK) && (sources == NULL));
}

void
test_ParseSourcesSingle(void)
{
    natsStatus s;
    natsCounterSource *sources = NULL;
    const char *header = "{\"STREAM_A\":{\"subject.a\":\"100\"}}";

    test("Parse single source: ");
    s = natsCounterParser_ParseSources(header, &sources);
    testCond((s == NATS_OK) && (sources != NULL) && (strcmp(sources->stream, "STREAM_A") == 0) && (strcmp(sources->subject, "subject.a") == 0) && (strcmp(sources->value, "100") == 0) && (sources->next == NULL));
    natsCounterParser_FreeSources(sources);
}

void
test_ParseSourcesMultipleStreams(void)
{
    natsStatus s;
    natsCounterSource *sources = NULL;
    const char *header = "{\"S1\":{\"sub1\":\"10\"},\"S2\":{\"sub2\":\"20\"}}";
    int count = 0;
    bool foundS1 = false;
    bool foundS2 = false;

    test("Parse multiple sources: ");
    s = natsCounterParser_ParseSources(header, &sources);
    if (s == NATS_OK)
    {
        for (natsCounterSource *n = sources; n != NULL; n = n->next)
        {
            count++;
            if (strcmp(n->stream, "S1") == 0 && strcmp(n->subject, "sub1") == 0 && strcmp(n->value, "10") == 0)
                foundS1 = true;
            if (strcmp(n->stream, "S2") == 0 && strcmp(n->subject, "sub2") == 0 && strcmp(n->value, "20") == 0)
                foundS2 = true;
        }
    }
    testCond((s == NATS_OK) && (count == 2) && foundS1 && foundS2);
    natsCounterParser_FreeSources(sources);
}

void
test_ParseIncrementPositive(void)
{
    natsStatus s;
    char *incr = NULL;

    test("Parse positive increment: ");
    s = natsCounterParser_ParseIncrement("+42", &incr);
    testCond((s == NATS_OK) && (incr != NULL) && (strcmp(incr, "+42") == 0));
    free(incr);
}

void
test_ParseIncrementNegative(void)
{
    natsStatus s;
    char *incr = NULL;

    test("Parse negative increment: ");
    s = natsCounterParser_ParseIncrement("-10", &incr);
    testCond((s == NATS_OK) && (incr != NULL) && (strcmp(incr, "-10") == 0));
    free(incr);
}

void
test_ParseIncrementAbsent(void)
{
    natsStatus s;
    char *incr = NULL;

    test("Parse NULL increment returns NOT_FOUND: ");
    s = natsCounterParser_ParseIncrement(NULL, &incr);
    testCond(s == NATS_NOT_FOUND);
}

void
test_ParseIncrementInvalid(void)
{
    natsStatus s;
    char *incr = NULL;

    test("Parse invalid increment returns error: ");
    s = natsCounterParser_ParseIncrement("not_a_number", &incr);
    testCond(s == NATS_ERR);
}

//=============================================================================
// Integration tests — require a nats-server with JetStream.
//
// Each test uses JS_SETUP / JS_TEARDOWN to start its own server instance.
//=============================================================================

void
test_CounterGetFromStreamDirectNotEnabled(void)
{
    natsStatus s = NATS_OK;
    jsStreamConfig cfg;
    natsCounter *c = NULL;

    natsConnection *nc = NULL;
    jsCtx *js = NULL;
    natsPid pid = NATS_INVALID_PID;
    char datastore[256] = { '\0' };
    char cmdLine[1024] = { '\0' };

    _makeUniqueDir(datastore, sizeof(datastore), "datastore_");
    test("Start JS Server: ");
    snprintf(cmdLine, sizeof(cmdLine), "-js -sd %s", datastore);
    pid = _startServer("nats://127.0.0.1:4222", cmdLine, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Connect: ");
    s = natsConnection_ConnectTo(&nc, "nats://127.0.0.1:4222");
    testCond(s == NATS_OK);

    test("Get context: ");
    s = natsConnection_JetStream(&js, nc, NULL);
    testCond(s == NATS_OK);

    test("Create stream without AllowDirect: ");
    jsStreamConfig_Init(&cfg);
    cfg.Name = "NO_DIRECT";
    cfg.Subjects = (const char *[1]){ "nd.>" };
    cfg.SubjectsLen = 1;
    cfg.AllowDirect = false;
    s = js_AddStream(NULL, js, &cfg, NULL, NULL);
    testCond(s == NATS_OK);

    test("GetFromStream fails when AllowDirect is false: ");
    s = natsCounter_GetFromStream(&c, js, nc, "NO_DIRECT");
    testCond(s == NATS_INVALID_CONFIG);

    JS_TEARDOWN;
}

void
test_CounterGetFromStreamCounterNotEnabled(void)
{
    natsStatus s = NATS_OK;
    jsStreamConfig cfg;
    natsCounter *c = NULL;

    natsConnection *nc = NULL;
    jsCtx *js = NULL;
    natsPid pid = NATS_INVALID_PID;
    char datastore[256] = { '\0' };
    char cmdLine[1024] = { '\0' };

    _makeUniqueDir(datastore, sizeof(datastore), "datastore_");
    test("Start JS Server: ");
    snprintf(cmdLine, sizeof(cmdLine), "-js -sd %s", datastore);
    pid = _startServer("nats://127.0.0.1:4222", cmdLine, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Connect: ");
    s = natsConnection_ConnectTo(&nc, "nats://127.0.0.1:4222");
    testCond(s == NATS_OK);

    test("Get context: ");
    s = natsConnection_JetStream(&js, nc, NULL);
    testCond(s == NATS_OK);

    test("Create stream without AllowMsgCounter: ");
    jsStreamConfig_Init(&cfg);
    cfg.Name = "NO_COUNTER";
    cfg.Subjects = (const char *[1]){ "nc.>" };
    cfg.SubjectsLen = 1;
    cfg.AllowDirect = true;
    cfg.AllowMsgCounter = false;
    s = js_AddStream(NULL, js, &cfg, NULL, NULL);
    testCond(s == NATS_OK);

    test("GetFromStream fails when AllowMsgCounter is false: ");
    s = natsCounter_GetFromStream(&c, js, nc, "NO_COUNTER");
    testCond(s == NATS_INVALID_CONFIG);

    JS_TEARDOWN;
}

void
test_CounterAddIncrementsValue(void)
{
    natsCounter *c = NULL;
    long long val = 0;

    JS_SETUP;

    test("Create counter stream: ");
    s = _createStream(js, "ADD", "add.>");
    testCond(s == NATS_OK);

    test("Get counter: ");
    s = natsCounter_GetFromStream(&c, js, nc, "ADD");
    testCond(s == NATS_OK);

    test("Add 5: ");
    s = natsCounter_Add(c, "add.hits", 5, &val);
    testCond((s == NATS_OK) && (val == 5));

    test("Add 3 more: ");
    s = natsCounter_Add(c, "add.hits", 3, &val);
    testCond((s == NATS_OK) && (val == 8));

    natsCounter_Destroy(c);
    JS_TEARDOWN;
}

void
test_CounterAddNegativeDecrementsValue(void)
{
    natsCounter *c = NULL;
    long long val = 0;

    JS_SETUP;

    test("Create counter stream: ");
    s = _createStream(js, "ADDNEG", "addneg.>");
    testCond(s == NATS_OK);

    test("Get counter: ");
    s = natsCounter_GetFromStream(&c, js, nc, "ADDNEG");
    testCond(s == NATS_OK);

    test("Add 10: ");
    s = natsCounter_Add(c, "addneg.x", 10, &val);
    testCond((s == NATS_OK) && (val == 10));

    test("Add -3: ");
    s = natsCounter_Add(c, "addneg.x", -3, &val);
    testCond((s == NATS_OK) && (val == 7));

    natsCounter_Destroy(c);
    JS_TEARDOWN;
}

void
test_CounterAddIntReturnsString(void)
{
    natsCounter *c = NULL;
    char *val = NULL;

    JS_SETUP;

    test("Create counter stream: ");
    s = _createStream(js, "ADDINT", "addint.>");
    testCond(s == NATS_OK);

    test("Get counter: ");
    s = natsCounter_GetFromStream(&c, js, nc, "ADDINT");
    testCond(s == NATS_OK);

    test("AddInt 5 returns string \"5\": ");
    s = natsCounter_AddInt(c, "addint.x", 5, &val);
    testCond((s == NATS_OK) && (val != NULL) && (strcmp(val, "5") == 0));
    free(val);
    val = NULL;

    test("AddInt 3 returns string \"8\": ");
    s = natsCounter_AddInt(c, "addint.x", 3, &val);
    testCond((s == NATS_OK) && (val != NULL) && (strcmp(val, "8") == 0));
    free(val);
    val = NULL;

    test("AddInt -2 returns string \"6\": ");
    s = natsCounter_AddInt(c, "addint.x", -2, &val);
    testCond((s == NATS_OK) && (val != NULL) && (strcmp(val, "6") == 0));
    free(val);

    natsCounter_Destroy(c);
    JS_TEARDOWN;
}

void
test_CounterIncrementByOne(void)
{
    natsCounter *c = NULL;
    long long val = 0;

    JS_SETUP;

    test("Create counter stream: ");
    s = _createStream(js, "INCR", "incr.>");
    testCond(s == NATS_OK);

    test("Get counter: ");
    s = natsCounter_GetFromStream(&c, js, nc, "INCR");
    testCond(s == NATS_OK);

    test("Increment returns 1: ");
    s = natsCounter_Increment(c, "incr.a", &val);
    testCond((s == NATS_OK) && (val == 1));

    natsCounter_Destroy(c);
    JS_TEARDOWN;
}

void
test_CounterDecrementByOne(void)
{
    natsCounter *c = NULL;
    long long val = 0;

    JS_SETUP;

    test("Create counter stream: ");
    s = _createStream(js, "DECR", "decr.>");
    testCond(s == NATS_OK);

    test("Get counter: ");
    s = natsCounter_GetFromStream(&c, js, nc, "DECR");
    testCond(s == NATS_OK);

    test("Seed with 10: ");
    s = natsCounter_Add(c, "decr.a", 10, &val);
    testCond((s == NATS_OK) && (val == 10));

    test("Decrement returns 9: ");
    s = natsCounter_Decrement(c, "decr.a", &val);
    testCond((s == NATS_OK) && (val == 9));

    natsCounter_Destroy(c);
    JS_TEARDOWN;
}

void
test_CounterLoadReturnsCurrentValue(void)
{
    natsCounter *c = NULL;
    long long val = 0;

    JS_SETUP;

    test("Create counter stream: ");
    s = _createStream(js, "LOAD", "load.>");
    testCond(s == NATS_OK);

    test("Get counter: ");
    s = natsCounter_GetFromStream(&c, js, nc, "LOAD");
    testCond(s == NATS_OK);

    test("Seed with 42: ");
    s = natsCounter_Add(c, "load.x", 42, &val);
    testCond((s == NATS_OK) && (val == 42));

    test("Load returns 42: ");
    val = 0;
    s = natsCounter_Load(c, "load.x", &val);
    testCond((s == NATS_OK) && (val == 42));

    natsCounter_Destroy(c);
    JS_TEARDOWN;
}

void
test_CounterGetReturnsEntry(void)
{
    natsCounter *c = NULL;
    natsCounterEntry *entry = NULL;
    long long val = 0;

    JS_SETUP;

    test("Create counter stream: ");
    s = _createStream(js, "GETENTRY", "ge.>");
    testCond(s == NATS_OK);

    test("Get counter: ");
    s = natsCounter_GetFromStream(&c, js, nc, "GETENTRY");
    testCond(s == NATS_OK);

    test("Seed with 7: ");
    s = natsCounter_Add(c, "ge.x", 7, &val);
    testCond((s == NATS_OK) && (val == 7));

    test("Get entry: ");
    s = natsCounter_Get(c, "ge.x", &entry);
    testCond((s == NATS_OK) && (entry != NULL)
             && (strcmp(entry->subject, "ge.x") == 0)
             && (strcmp(entry->value, "7") == 0)
             && natsCounterEntry_HasIncrement(entry));
    natsCounterEntry_Destroy(entry);

    natsCounter_Destroy(c);
    JS_TEARDOWN;
}

void
test_CounterGetMissingSubject(void)
{
    natsCounter *c = NULL;
    natsCounterEntry *entry = NULL;

    JS_SETUP;

    test("Create counter stream: ");
    s = _createStream(js, "GETMISS", "gm.>");
    testCond(s == NATS_OK);

    test("Get counter: ");
    s = natsCounter_GetFromStream(&c, js, nc, "GETMISS");
    testCond(s == NATS_OK);

    test("Get missing subject returns NOT_FOUND: ");
    s = natsCounter_Get(c, "gm.nonexistent", &entry);
    testCond(s == NATS_NOT_FOUND);

    natsCounter_Destroy(c);
    JS_TEARDOWN;
}

void
test_CounterGetMultipleBatch(void)
{
    natsCounter *c = NULL;
    long long val = 0;

    JS_SETUP;

    test("Create counter stream: ");
    s = _createStream(js, "BATCH", "batch.>");
    testCond(s == NATS_OK);

    test("Get counter: ");
    s = natsCounter_GetFromStream(&c, js, nc, "BATCH");
    testCond(s == NATS_OK);

    test("Populate 3 subjects: ");
    s = natsCounter_Add(c, "batch.a", 1, &val);
    if (s == NATS_OK)
        s = natsCounter_Add(c, "batch.b", 2, &val);
    if (s == NATS_OK)
        s = natsCounter_Add(c, "batch.c", 3, &val);
    testCond(s == NATS_OK);

    test("GetMultiple returns 3 entries: ");
    {
        const char *subjects[] = { "batch.a", "batch.b", "batch.c" };
        natsCounterEntryList list = { 0 };
        s = natsCounter_GetMultiple(&list, c, subjects, 3, 5000);
        testCond((s == NATS_OK) && (list.Count == 3)
                 && (list.Entries != NULL)
                 && (list.Entries[0] != NULL)
                 && (list.Entries[1] != NULL)
                 && (list.Entries[2] != NULL));
        natsCounterEntryList_Destroy(&list);
    }

    natsCounter_Destroy(c);
    JS_TEARDOWN;
}

void
test_CounterGetMultipleEmpty(void)
{
    natsCounter *c = NULL;

    JS_SETUP;

    test("Create counter stream: ");
    s = _createStream(js, "BATCHEM", "batchem.>");
    testCond(s == NATS_OK);

    test("Get counter: ");
    s = natsCounter_GetFromStream(&c, js, nc, "BATCHEM");
    testCond(s == NATS_OK);

    test("GetMultiple with 0 subjects returns empty list: ");
    {
        natsCounterEntryList list = { 0 };
        s = natsCounter_GetMultiple(&list, c, NULL, 0, 5000);
        testCond((s == NATS_OK) && (list.Count == 0) && (list.Entries == NULL));
        natsCounterEntryList_Destroy(&list);
    }

    natsCounter_Destroy(c);
    JS_TEARDOWN;
}

void
test_CounterGetMultipleSkipsMissing(void)
{
    natsCounter *c = NULL;
    long long val = 0;

    JS_SETUP;

    test("Create counter stream: ");
    s = _createStream(js, "BATCHMISS", "bm.>");
    testCond(s == NATS_OK);

    test("Get counter: ");
    s = natsCounter_GetFromStream(&c, js, nc, "BATCHMISS");
    testCond(s == NATS_OK);

    test("Populate 1 subject: ");
    s = natsCounter_Add(c, "bm.exists", 1, &val);
    testCond(s == NATS_OK);

    test("GetMultiple skips missing: ");
    {
        const char *subjects[] = { "bm.exists", "bm.missing" };
        natsCounterEntryList list = { 0 };
        s = natsCounter_GetMultiple(&list, c, subjects, 2, 5000);
        testCond((s == NATS_OK) && (list.Count == 1)
                 && (list.Entries != NULL)
                 && (list.Entries[0] != NULL)
                 && (strcmp(list.Entries[0]->subject, "bm.exists") == 0));
        natsCounterEntryList_Destroy(&list);
    }

    natsCounter_Destroy(c);
    JS_TEARDOWN;
}

// Source iteration callback closure.
typedef struct
{
    int   count;
    char  streams[8][64];
    char  subjects[8][64];
    char  values[8][64];

} SourceTestCtx;

static void
_sourceTestHandler(const char *stream, const char *subject, const char *value,
                   void *closure)
{
    SourceTestCtx *ctx = (SourceTestCtx *)closure;
    if (ctx->count < 8)
    {
        snprintf(ctx->streams[ctx->count], 64, "%s", stream);
        snprintf(ctx->subjects[ctx->count], 64, "%s", subject);
        snprintf(ctx->values[ctx->count], 64, "%s", value);
    }
    ctx->count++;
}

void
test_CounterSourceTracking(void)
{
    natsCounter *regional = NULL;
    natsCounter *global = NULL;
    natsCounterEntry *entry = NULL;
    long long val = 0;
    SourceTestCtx srcCtx = { 0 };
    jsStreamConfig cfg;
    jsStreamSource src;
    jsStreamSource *srcArr[1];

    JS_SETUP;

    // Create a regional counter stream.
    test("Create regional stream: ");
    s = _createStream(js, "REGION_A", "region.a.>");
    testCond(s == NATS_OK);

    // Create a global stream that sources from REGION_A.
    // The sourced messages arrive with Nats-Stream-Source header,
    // causing the server to populate Nats-Counter-Sources.
    test("Create global stream sourcing from REGION_A: ");
    jsStreamConfig_Init(&cfg);
    cfg.Name = "GLOBAL";
    cfg.AllowDirect = true;
    cfg.AllowMsgCounter = true;
    jsStreamSource_Init(&src);
    src.Name = "REGION_A";
    srcArr[0] = &src;
    cfg.Sources = srcArr;
    cfg.SourcesLen = 1;
    s = js_AddStream(NULL, js, &cfg, NULL, NULL);
    testCond(s == NATS_OK);

    // Publish to regional counter.
    test("Get regional counter: ");
    s = natsCounter_GetFromStream(&regional, js, nc, "REGION_A");
    testCond(s == NATS_OK);

    test("Add 42 to regional counter: ");
    s = natsCounter_Add(regional, "region.a.hits", 42, &val);
    testCond((s == NATS_OK) && (val == 42));

    // Wait for sourcing to propagate.
    nats_Sleep(500);

    // Read global entry — should have source tracking.
    test("Get global counter: ");
    s = natsCounter_GetFromStream(&global, js, nc, "GLOBAL");
    testCond(s == NATS_OK);

    test("Global entry has value 42: ");
    s = natsCounter_Get(global, "region.a.hits", &entry);
    testCond((s == NATS_OK) && (entry != NULL)
             && (strcmp(entry->value, "42") == 0));

    test("Global entry has sources: ");
    testCond(natsCounterEntry_HasSources(entry));

    test("IterSources yields REGION_A contribution: ");
    s = natsCounterEntry_IterSources(entry, _sourceTestHandler, &srcCtx);
    testCond((s == NATS_OK) && (srcCtx.count == 1)
             && (strcmp(srcCtx.streams[0], "REGION_A") == 0)
             && (strcmp(srcCtx.subjects[0], "region.a.hits") == 0)
             && (strcmp(srcCtx.values[0], "42") == 0));

    natsCounterEntry_Destroy(entry);
    natsCounter_Destroy(regional);
    natsCounter_Destroy(global);
    JS_TEARDOWN;
}

void
test_CounterAddStrAndLoadStr(void)
{
    natsCounter *c = NULL;
    char *val = NULL;
    char *loaded = NULL;

    JS_SETUP;

    test("Create counter stream: ");
    s = _createStream(js, "STROPS", "strops.>");
    testCond(s == NATS_OK);

    test("Get counter: ");
    s = natsCounter_GetFromStream(&c, js, nc, "STROPS");
    testCond(s == NATS_OK);

    test("AddStr \"+100\" returns \"100\": ");
    s = natsCounter_AddStr(c, "strops.x", "+100", &val);
    testCond((s == NATS_OK) && (val != NULL) && (strcmp(val, "100") == 0));
    free(val);
    val = NULL;

    test("AddStr \"-30\" returns \"70\": ");
    s = natsCounter_AddStr(c, "strops.x", "-30", &val);
    testCond((s == NATS_OK) && (val != NULL) && (strcmp(val, "70") == 0));
    free(val);
    val = NULL;

    test("LoadStr returns \"70\": ");
    s = natsCounter_LoadStr(c, "strops.x", &loaded);
    testCond((s == NATS_OK) && (loaded != NULL) && (strcmp(loaded, "70") == 0));
    free(loaded);

    natsCounter_Destroy(c);
    JS_TEARDOWN;
}

void
test_CounterLargeValueExceedsLongLong(void)
{
    natsCounter *c = NULL;
    natsCounterEntry *entry = NULL;
    char *val = NULL;
    // A value larger than LLONG_MAX (9223372036854775807).
    const char *bigDelta = "+99999999999999999999999999999999";

    JS_SETUP;

    test("Create counter stream: ");
    s = _createStream(js, "BIGVAL", "bigval.>");
    testCond(s == NATS_OK);

    test("Get counter: ");
    s = natsCounter_GetFromStream(&c, js, nc, "BIGVAL");
    testCond(s == NATS_OK);

    test("AddStr with huge value succeeds: ");
    s = natsCounter_AddStr(c, "bigval.x", bigDelta, &val);
    testCond((s == NATS_OK) && (val != NULL)
             && (strcmp(val, "99999999999999999999999999999999") == 0));
    free(val);

    test("ValueStr returns the large value: ");
    s = natsCounter_Get(c, "bigval.x", &entry);
    testCond((s == NATS_OK) && (entry != NULL)
             && (strcmp(entry->value, "99999999999999999999999999999999") == 0));

    natsCounterEntry_Destroy(entry);
    natsCounter_Destroy(c);
    JS_TEARDOWN;
}

void
test_CounterDestroyNull(void)
{
    test("natsCounter_Destroy(NULL) does not crash: ");
    natsCounter_Destroy(NULL);
    testCond(true);
}

//=============================================================================
// main — dispatch a single test by name (same as nats.c/test).
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

    // Kill any leftover server.
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
