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

// nats-context test suite.
//
// Each test function is registered in list_test.txt as _test(<Name>). The
// list.h two-pass macro system generates the forward declarations and the
// dispatch table (same pattern as the other orbit.c sub-libraries).
//
// Every test starts its own nats-server (which must be on PATH), builds a
// throwaway context config tree under a unique XDG_CONFIG_HOME, and connects
// through natsContext_Connect.
//
// Run a single test:   ./nats_context_testsuite connect_by_path
// Run all tests:       ctest --test-dir build

#include "context.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <dirent.h>

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

// Server lifecycle — one server at a time, tracked via a global PID.

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

// _checkStart polls a TCP connection to url to verify the server is up.
static natsStatus
_checkStart(const char *url, int maxAttempts)
{
    natsConnection *nc = NULL;
    natsStatus      s = NATS_OK;
    int             attempts = 0;

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
        // Child — "nats-server <cmdLineOpts> -a 127.0.0.1 [-l server.log]"
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

// Filesystem / config-tree helpers.

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

static bool
_mkdirOk(const char *path)
{
    return (mkdir(path, 0755) == 0) || (errno == EEXIST);
}

// Creates a unique config base holding nats/context/ and points
// XDG_CONFIG_HOME at it so contexts resolve there.
static bool
_makeConfigTree(char *base, int baseLen)
{
    char path[1024];

    snprintf(base, baseLen, "/tmp/orbitctx_%d_%d", (int) getpid(), ++_uniqueCounter);
    if (!_mkdirOk(base))
        return false;

    snprintf(path, sizeof(path), "%s/nats", base);
    if (!_mkdirOk(path))
        return false;

    snprintf(path, sizeof(path), "%s/nats/context", base);
    if (!_mkdirOk(path))
        return false;

    setenv("XDG_CONFIG_HOME", base, 1);
    return true;
}

static bool
_writeFile(const char *path, const char *content)
{
    FILE *f = fopen(path, "wb");
    if (f == NULL)
        return false;
    fwrite(content, 1, strlen(content), f);
    fclose(f);
    return true;
}

// Writes <base>/nats/context/<name>.json.
static bool
_writeContext(const char *base, const char *name, const char *json)
{
    char path[1024];
    snprintf(path, sizeof(path), "%s/nats/context/%s.json", base, name);
    return _writeFile(path, json);
}

// Writes <base>/nats/context.txt naming the selected context.
static bool
_writeSelected(const char *base, const char *name)
{
    char path[1024];
    snprintf(path, sizeof(path), "%s/nats/context.txt", base);
    return _writeFile(path, name);
}

#define SERVER_URL "nats://127.0.0.1:4222"

// Common teardown. Every argument is safe when unused: Destroy tolerates NULL,
// _stopServer ignores NATS_INVALID_PID, and base may be NULL.
static void
_cleanup(natsContextSettings *settings, natsConnection *nc, natsPid pid, const char *base)
{
    natsContextSettings_Destroy(settings);
    natsConnection_Destroy(nc);
    _stopServer(pid);
    if (base != NULL)
        _rmtree(base);
}

// Tests.

void
test_connect_by_path(void)
{
    natsStatus           s;
    natsPid              pid;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 ctxPath[1024];
    const char          *json = "{ \"url\": \"" SERVER_URL "\", \"description\": \"by path\" }";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Write context file: ");
    testCond(_writeContext(base, "bypath", json));
    snprintf(ctxPath, sizeof(ctxPath), "%s/nats/context/bypath.json", base);

    test("Start server: ");
    pid = _startServer(SERVER_URL, NULL, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Connect by absolute path: ");
    s = natsContext_Connect(&nc, &settings, ctxPath, NULL);
    testCond((s == NATS_OK) && (nc != NULL));

    test("Status connected: ");
    testCond(natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED);

    test("Settings populated: ");
    testCond((settings != NULL) && (settings->URL != NULL) && (settings->Description != NULL) &&
             (strcmp(settings->Description, "by path") == 0));

    _cleanup(settings, nc, pid, base);
}

void
test_connect_by_name(void)
{
    natsStatus           s;
    natsPid              pid;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 name[] = "byname";
    const char          *json = "{ \"url\": \"" SERVER_URL "\", \"user\": \"\" }";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Write context file: ");
    testCond(_writeContext(base, "byname", json));

    test("Start server: ");
    pid = _startServer(SERVER_URL, NULL, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Connect by name: ");
    s = natsContext_Connect(&nc, &settings, name, NULL);
    testCond((s == NATS_OK) && (nc != NULL) &&
             (natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED));

    _cleanup(settings, nc, pid, base);
}

void
test_connect_selected(void)
{
    natsStatus           s;
    natsPid              pid;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 empty[] = "";
    const char          *json = "{ \"url\": \"" SERVER_URL "\" }";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Write context + selection: ");
    testCond(_writeContext(base, "selected", json) && _writeSelected(base, "selected"));

    test("Start server: ");
    pid = _startServer(SERVER_URL, NULL, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    // Empty name -> resolve the selected context from nats/context.txt.
    test("Connect selected context: ");
    s = natsContext_Connect(&nc, &settings, empty, NULL);
    testCond((s == NATS_OK) && (nc != NULL) &&
             (natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED));

    _cleanup(settings, nc, pid, base);
}

void
test_connect_no_context(void)
{
    natsStatus           s;
    natsPid              pid;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 empty[] = "";

    // Config tree with no context files and no nats/context.txt selection.
    test("Setup empty config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Start default server: ");
    pid = _startServer(SERVER_URL, NULL, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    // Empty name with nothing selected mirrors Go: connect with defaults.
    test("Connect with no context: ");
    s = natsContext_Connect(&nc, &settings, empty, NULL);
    testCond((s == NATS_OK) && (nc != NULL) &&
             (natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED));

    test("Settings are empty defaults: ");
    testCond((settings != NULL) && (settings->URL == NULL) && (settings->User == NULL));

    _cleanup(settings, nc, pid, base);
}

void
test_connect_userpass(void)
{
    natsStatus           s;
    natsPid              pid;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 okName[]  = "auth";
    char                 badName[] = "authbad";
    const char          *okJson =
        "{ \"url\": \"" SERVER_URL "\", \"user\": \"testuser\", \"password\": \"testpass\" }";
    const char *badJson =
        "{ \"url\": \"" SERVER_URL "\", \"user\": \"testuser\", \"password\": \"wrong\" }";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Write context files: ");
    testCond(_writeContext(base, "auth", okJson) && _writeContext(base, "authbad", badJson));

    // Auth server: the plain readiness probe would be rejected, so start
    // without the built-in check and probe with credentials ourselves.
    test("Start auth server: ");
    pid = _startServer(SERVER_URL, "--user testuser --pass testpass", false);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Server ready: ");
    testCond(_checkStart("nats://testuser:testpass@127.0.0.1:4222", 20) == NATS_OK);

    test("Connect with correct password: ");
    s = natsContext_Connect(&nc, &settings, okName, NULL);
    testCond((s == NATS_OK) && (nc != NULL) &&
             (natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED));

    natsContextSettings_Destroy(settings);
    natsConnection_Destroy(nc);
    settings = NULL;
    nc       = NULL;

    test("Connect with wrong password fails: ");
    s = natsContext_Connect(&nc, &settings, badName, NULL);
    testCond((s != NATS_OK) && (nc == NULL));

    _cleanup(settings, nc, pid, base);
}

void
test_connect_token(void)
{
    natsStatus           s;
    natsPid              pid;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 name[] = "tok";
    const char          *json = "{ \"url\": \"" SERVER_URL "\", \"token\": \"s3cr3t\" }";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Write context file: ");
    testCond(_writeContext(base, "tok", json));

    test("Start token server: ");
    pid = _startServer(SERVER_URL, "--auth s3cr3t", false);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Server ready: ");
    testCond(_checkStart("nats://s3cr3t@127.0.0.1:4222", 20) == NATS_OK);

    test("Connect with token: ");
    s = natsContext_Connect(&nc, &settings, name, NULL);
    testCond((s == NATS_OK) && (nc != NULL) &&
             (natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED));

    _cleanup(settings, nc, pid, base);
}

// Unsupported settings must make natsContext_Connect fail (no server needed:
// the options builder rejects them before any connection attempt).

void
test_unsupported_nkey(void)
{
    natsStatus           s;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 name[] = "nkeyctx";
    const char          *json = "{ \"url\": \"" SERVER_URL "\", \"nkey\": \"/tmp/user.nk\" }";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Write context file: ");
    testCond(_writeContext(base, "nkeyctx", json));

    test("nkey context is rejected: ");
    s = natsContext_Connect(&nc, &settings, name, NULL);
    testCond((s != NATS_OK) && (nc == NULL) && (settings == NULL));

    _cleanup(settings, nc, NATS_INVALID_PID, base);
}

void
test_unsupported_socks(void)
{
    natsStatus           s;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 name[] = "socksctx";
    const char          *json =
        "{ \"url\": \"" SERVER_URL "\", \"socks_proxy\": \"socks5://127.0.0.1:1080\" }";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Write context file: ");
    testCond(_writeContext(base, "socksctx", json));

    test("socks_proxy context is rejected: ");
    s = natsContext_Connect(&nc, &settings, name, NULL);
    testCond((s != NATS_OK) && (nc == NULL) && (settings == NULL));

    _cleanup(settings, nc, NATS_INVALID_PID, base);
}

void
test_unknown_context(void)
{
    natsStatus           s;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 name[] = "ghost";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    // No context file written for this name.
    test("Unknown context is rejected: ");
    s = natsContext_Connect(&nc, &settings, name, NULL);
    testCond((s != NATS_OK) && (nc == NULL) && (settings == NULL));

    _cleanup(settings, nc, NATS_INVALID_PID, base);
}

int
main(int argc, char **argv)
{
    const char *envStr;
    const char *testName = NULL;
    testFunc    f = NULL;
    int         i;

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
