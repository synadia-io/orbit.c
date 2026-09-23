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

// Test framework shared by the orbit.c sub-library suites.
//
// Each suite registers its tests in list_test.txt as _test(<Name>), includes
// this header once from test.c, and hands main() over to testMain().
// ORBIT_TEST_LIST must name the suite's list file (orbit_add_testsuite takes
// care of that).

#ifndef ORBIT_TEST_H_
#define ORBIT_TEST_H_

#include <nats/nats.h>

#include <arpa/inet.h>
#include <dirent.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

typedef void (*testFunc)(void);

typedef struct
{
    const char *name;
    testFunc    func;
} testInfo;

#define _test(name) void test_##name(void);
#include ORBIT_TEST_LIST
#undef _test

static testInfo allTests[] = {
#define _test(name) { #name, test_##name },
#include ORBIT_TEST_LIST
#undef _test
};

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

// Child processes (servers, proxies). Every one started is tracked so that a
// testCond bail-out does not leave any running.

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
_rememberServer(natsPid pid)
{
    if ((pid != NATS_INVALID_PID) && (g_serverCount < MAX_SERVERS))
        g_serverPids[g_serverCount++] = pid;
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
    return ((int64_t)ts.tv_sec * 1000) + (ts.tv_nsec / 1000000);
}

#define SERVER_POLL_MS (10)

// Waits up to 'budgetMs' for a server at 'url' to accept connections.
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

// Reports whether something already listens on 127.0.0.1 at the port of 'url'.
static bool
_portInUse(const char *url)
{
    const char        *colon = strrchr(url, ':');
    struct sockaddr_in addr;
    bool               inUse;
    int                fd;

    if (colon == NULL)
        return false;

    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons((uint16_t)atoi(colon + 1));
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return false;
    inUse = (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0);
    close(fd);
    return inUse;
}

// Starts "nats-server <cmdLineOpts> -a 127.0.0.1 [-l server.log]". Fails when
// a stray server already holds the port of 'url', so tests never run against it.
static natsPid
_startServer(const char *url, const char *cmdLineOpts, bool checkStart)
{
    natsPid pid;

    if ((url != NULL) && _portInUse(url))
    {
        printf("@@ A server is already listening at %s @@\n", url);
        return NATS_INVALID_PID;
    }

    pid = fork();
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

    if (checkStart && (_waitForServer(url, 2000) != NATS_OK))
    {
        _stopServer(pid);
        return NATS_INVALID_PID;
    }

    _rememberServer(pid);
    return pid;
}

// Handles a test has open, released newest first by testMain() when a
// testCond bail-out skips the test's own teardown. Newest first means a handle
// is released before anything it borrows (a client before its connection).

typedef struct
{
    void *h;
    void (*destroy)(void *);
} testHandle;

#define MAX_HANDLES (32)

static testHandle g_handles[MAX_HANDLES];
static int        g_handleCount = 0;

static void
_track(void *h, void (*destroy)(void *))
{
    if ((h == NULL) || (g_handleCount >= MAX_HANDLES))
        return;
    g_handles[g_handleCount].h       = h;
    g_handles[g_handleCount].destroy = destroy;
    g_handleCount++;
}

static void
_untrack(void *h)
{
    int i;

    for (i = g_handleCount - 1; i >= 0; i--)
    {
        if (g_handles[i].h != h)
            continue;
        memmove(&g_handles[i], &g_handles[i + 1], (size_t)(g_handleCount - i - 1) * sizeof(g_handles[0]));
        g_handleCount--;
        return;
    }
}

static void
_releaseAllHandles(void)
{
    while (g_handleCount > 0)
    {
        testHandle th = g_handles[--g_handleCount];
        th.destroy(th.h);
    }
}

static void
_connDestroyer(void *h)
{
    natsConnection_Destroy((natsConnection *)h);
}

static void
_rememberConn(natsConnection *nc)
{
    _track(nc, _connDestroyer);
}

// Destroys a connection and drops it from the registry; NULL is a no-op.
static void
_destroyConn(natsConnection *nc)
{
    _untrack(nc);
    natsConnection_Destroy(nc);
}

// natsConnection_ConnectTo, with the connection tracked.
static natsStatus
_connect(natsConnection **nc, const char *url)
{
    natsStatus s = natsConnection_ConnectTo(nc, url);

    if (s == NATS_OK)
        _rememberConn(*nc);
    return s;
}

// Filesystem helpers.

// Removes 'path', recursively when it is a directory.
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

// Config files and store directories, removed by testMain() however the test
// ended.

#define MAX_TMP_PATHS (64)

static char g_tmpPaths[MAX_TMP_PATHS][256];
static int  g_tmpPathCount = 0;

// A path that does not fit is not remembered: removing a truncated one could
// delete an unrelated file or directory.
static void
_rememberPath(const char *path)
{
    if ((g_tmpPathCount >= MAX_TMP_PATHS) || (strlen(path) >= sizeof(g_tmpPaths[0])))
    {
        printf("@@ Not removing '%s' at exit @@\n", path);
        return;
    }
    snprintf(g_tmpPaths[g_tmpPathCount++], sizeof(g_tmpPaths[0]), "%s", path);
}

static void
_removeRememberedPaths(void)
{
    while (g_tmpPathCount > 0)
        _rmtree(g_tmpPaths[--g_tmpPathCount]);
}

static bool
_writeFile(const char *path, const char *content)
{
    FILE *f = fopen(path, "wb");

    if (f == NULL)
        return false;
    fputs(content, f);
    fclose(f);
    return true;
}

static int _uniqueCounter = 0;

// Writes "<prefix><pid>_<n>" into 'buf' and remembers it for removal.
static void
_uniqueTmpPath(char *buf, int bufLen, const char *prefix)
{
    snprintf(buf, bufLen, "%s%d_%d", prefix, (int)getpid(), ++_uniqueCounter);
    _rememberPath(buf);
}

// CORE_SETUP starts a plain server and connects. Pair with CORE_TEARDOWN.

#define CORE_SETUP                            \
    natsStatus      s   = NATS_OK;            \
    natsConnection *nc  = NULL;               \
    natsPid         pid = NATS_INVALID_PID;   \
                                              \
    test("Start server: ");                   \
    pid = _startServer(TEST_URL, NULL, true); \
    CHECK_SERVER_STARTED(pid);                \
    testCond(true);                           \
                                              \
    test("Connect: ");                        \
    s = _connect(&nc, TEST_URL);              \
    testCond(s == NATS_OK)

#define CORE_TEARDOWN \
    _destroyConn(nc); \
    _stopServer(pid)

// JS_SETUP starts a JetStream server on a fresh store directory, connects and
// creates a JetStream context. Pair with JS_TEARDOWN.

#define JS_SETUP                                                 \
    natsStatus      s              = NATS_OK;                    \
    natsConnection *nc             = NULL;                       \
    jsCtx          *js             = NULL;                       \
    natsPid         pid            = NATS_INVALID_PID;           \
    char            datastore[256] = { '\0' };                   \
    char            cmdLine[1024]  = { '\0' };                   \
                                                                 \
    _uniqueTmpPath(datastore, sizeof(datastore), "datastore_");  \
    test("Start JS Server: ");                                   \
    snprintf(cmdLine, sizeof(cmdLine), "-js -sd %s", datastore); \
    pid = _startServer(TEST_URL, cmdLine, true);                 \
    CHECK_SERVER_STARTED(pid);                                   \
    testCond(true);                                              \
                                                                 \
    test("Connect: ");                                           \
    s = _connect(&nc, TEST_URL);                                 \
    testCond(s == NATS_OK);                                      \
                                                                 \
    test("Get context: ");                                       \
    s = natsConnection_JetStream(&js, nc, NULL);                 \
    testCond(s == NATS_OK)

#define JS_TEARDOWN    \
    jsCtx_Destroy(js); \
    _destroyConn(nc);  \
    _stopServer(pid)

// Runs the test named by argv[1], then releases whatever it left behind.
static int
testMain(int argc, char **argv)
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

    _releaseAllHandles();
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

#endif /* ORBIT_TEST_H_ */
