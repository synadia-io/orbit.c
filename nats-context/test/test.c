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
#include <stdint.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
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

// Writes an executable <base>/bin/<name> holding 'script' and puts that
// directory first on PATH, so a child process finds it ahead of any real one.
static bool
_writeFakeExe(const char *base, const char *name, const char *script)
{
    char        dir[1024];
    char        path[1100];
    char        pathEnv[4096];
    const char *current = getenv("PATH");

    snprintf(dir, sizeof(dir), "%s/bin", base);
    if (!_mkdirOk(dir))
        return false;

    snprintf(path, sizeof(path), "%s/%s", dir, name);
    if (!_writeFile(path, script) || (chmod(path, 0755) != 0))
        return false;

    snprintf(pathEnv, sizeof(pathEnv), "%s:%s", dir, (current != NULL) ? current : "");
    return (setenv("PATH", pathEnv, 1) == 0);
}

// Reads at most 'outLen'-1 bytes of the file at 'path' into 'out'. Returns
// false when the file does not exist.
static bool
_readFile(const char *path, char *out, size_t outLen)
{
    FILE  *f = fopen(path, "rb");
    size_t n;

    if (f == NULL)
        return false;

    n      = fread(out, 1, outLen - 1, f);
    out[n] = '\0';
    fclose(f);
    return true;
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

// A minimal SOCKS5 proxy, forked like the server is, so that a context
// carrying socks_proxy is exercised over a real handshake. It handles one
// connection at a time: RFC 1928 method negotiation, RFC 1929
// username/password when the test asks for it, then it dials the requested
// target and copies bytes both ways until either side hangs up.
//
// A completed handshake writes "<host>:<port>" — the target as it came off the
// wire — to a marker file, which is how a test tells a proxied connection from
// one that went straight to the server.

static bool
_readFull(int fd, void *buf, size_t len)
{
    size_t done = 0;

    while (done < len)
    {
        ssize_t n = read(fd, (char *) buf + done, len - done);

        if ((n < 0) && (errno == EINTR))
            continue;
        if (n <= 0)
            return false;

        done += (size_t) n;
    }

    return true;
}

static bool
_writeFull(int fd, const void *buf, size_t len)
{
    size_t done = 0;

    while (done < len)
    {
        ssize_t n = write(fd, (const char *) buf + done, len - done);

        if ((n < 0) && (errno == EINTR))
            continue;
        if (n <= 0)
            return false;

        done += (size_t) n;
    }

    return true;
}

// Connects to the target of a CONNECT request. Only numeric IPv4 and the name
// "localhost" are understood, which keeps this side of the fork to plain
// syscalls: forking a process that already runs the nats.c threads makes
// anything that takes a lock (a resolver allocating, say) a hazard.
static int
_dialTarget(const char *host, int port)
{
    struct sockaddr_in addr;
    int                fd;

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons((uint16_t) port);

    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1)
    {
        if (strcmp(host, "localhost") != 0)
            return -1;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    }

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    if (connect(fd, (struct sockaddr *) &addr, sizeof(addr)) != 0)
    {
        close(fd);
        return -1;
    }

    return fd;
}

static void
_writeMarker(const char *path, const char *host, int port)
{
    char content[300];
    int  len = snprintf(content, sizeof(content), "%s:%d", host, port);
    int  fd  = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0644);

    if (fd < 0)
        return;

    (void) _writeFull(fd, content, (size_t) len);
    close(fd);
}

// Runs the handshake on an accepted connection and returns the socket to the
// target it asked for, or -1.
static int
_socksHandshake(int fd, const char *user, const char *pass, const char *marker)
{
    unsigned char buf[256];
    char          host[256];
    int           methods;
    int           i;
    int           port;
    int           target;
    bool          wantAuth = (user != NULL);
    bool          offered  = false;

    if (!_readFull(fd, buf, 2) || (buf[0] != 0x05))
        return -1;

    methods = buf[1];
    if (!_readFull(fd, buf, (size_t) methods))
        return -1;

    for (i = 0; i < methods; i++)
    {
        if (buf[i] == (wantAuth ? 0x02 : 0x00))
            offered = true;
    }

    buf[0] = 0x05;
    buf[1] = (unsigned char) (offered ? (wantAuth ? 0x02 : 0x00) : 0xFF);
    if (!_writeFull(fd, buf, 2) || !offered)
        return -1;

    if (wantAuth)
    {
        char gotUser[256];
        char gotPass[256];
        int  len;

        if (!_readFull(fd, buf, 2) || (buf[0] != 0x01))
            return -1;

        len = buf[1];
        if (!_readFull(fd, gotUser, (size_t) len))
            return -1;
        gotUser[len] = '\0';

        if (!_readFull(fd, buf, 1))
            return -1;

        len = buf[0];
        if (!_readFull(fd, gotPass, (size_t) len))
            return -1;
        gotPass[len] = '\0';

        buf[0] = 0x01;
        buf[1] = ((strcmp(gotUser, user) == 0) &&
                  (strcmp(gotPass, (pass != NULL) ? pass : "") == 0))
                     ? 0x00
                     : 0x01;

        if (!_writeFull(fd, buf, 2) || (buf[1] != 0x00))
            return -1;
    }

    // Request: VER CMD RSV ATYP DST.ADDR DST.PORT
    if (!_readFull(fd, buf, 4) || (buf[0] != 0x05) || (buf[1] != 0x01))
        return -1;

    switch (buf[3])
    {
        case 0x01:
            if (!_readFull(fd, buf, 4) ||
                (inet_ntop(AF_INET, buf, host, sizeof(host)) == NULL))
                return -1;
            break;
        case 0x03:
            if (!_readFull(fd, buf, 1))
                return -1;
            i = buf[0];
            if (!_readFull(fd, host, (size_t) i))
                return -1;
            host[i] = '\0';
            break;
        case 0x04:
            if (!_readFull(fd, buf, 16) ||
                (inet_ntop(AF_INET6, buf, host, sizeof(host)) == NULL))
                return -1;
            break;
        default:
            return -1;
    }

    if (!_readFull(fd, buf, 2))
        return -1;
    port = (buf[0] << 8) | buf[1];

    target = _dialTarget(host, port);

    // Reply, with a 0.0.0.0:0 bound address.
    memset(buf, 0, 10);
    buf[0] = 0x05;
    buf[1] = (unsigned char) ((target < 0) ? 0x05 : 0x00);
    buf[3] = 0x01;

    if (!_writeFull(fd, buf, 10) || (target < 0))
    {
        if (target >= 0)
            close(target);
        return -1;
    }

    _writeMarker(marker, host, port);
    return target;
}

static void
_socksPump(int a, int b)
{
    struct pollfd fds[2];
    char          buf[4096];

    fds[0].fd = a;
    fds[1].fd = b;

    for (;;)
    {
        int i;

        for (i = 0; i < 2; i++)
        {
            fds[i].events  = POLLIN;
            fds[i].revents = 0;
        }

        if (poll(fds, 2, -1) < 0)
        {
            if (errno == EINTR)
                continue;
            return;
        }

        for (i = 0; i < 2; i++)
        {
            if ((fds[i].revents & POLLIN) != 0)
            {
                ssize_t n = read(fds[i].fd, buf, sizeof(buf));

                if (n <= 0)
                    return;
                if (!_writeFull(fds[1 - i].fd, buf, (size_t) n))
                    return;
            }
            else if ((fds[i].revents & (POLLHUP | POLLERR)) != 0)
                return;
        }
    }
}

// Starts the proxy on a free port of the loopback, reported through *port.
// 'user' non-NULL makes it demand those credentials.
static natsPid
_startSocksProxy(int *port, const char *user, const char *pass, const char *marker)
{
    struct sockaddr_in addr;
    socklen_t          len = sizeof(addr);
    natsPid            pid;
    int                yes = 1;
    int                lfd = socket(AF_INET, SOCK_STREAM, 0);

    if (lfd < 0)
        return NATS_INVALID_PID;

    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0;

    if ((bind(lfd, (struct sockaddr *) &addr, sizeof(addr)) != 0) ||
        (listen(lfd, 8) != 0) ||
        (getsockname(lfd, (struct sockaddr *) &addr, &len) != 0))
    {
        close(lfd);
        return NATS_INVALID_PID;
    }

    *port = ntohs(addr.sin_port);

    pid = fork();
    if (pid < 0)
    {
        close(lfd);
        return NATS_INVALID_PID;
    }

    if (pid == 0)
    {
        for (;;)
        {
            int client = accept(lfd, NULL, NULL);
            int target;

            if (client < 0)
            {
                if (errno == EINTR)
                    continue;
                _exit(1);
            }

            target = _socksHandshake(client, user, pass, marker);
            if (target >= 0)
            {
                _socksPump(client, target);
                close(target);
            }
            close(client);
        }
    }

    close(lfd);
    return pid;
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

    // Empty name with nothing selected is not an error: connect with defaults.
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

// socks_proxy. The proxy runs in a forked child and records the target it was
// asked for, so these tests can tell a connection that went through it from one
// that reached the server on its own.

void
test_socks_proxy(void)
{
    natsStatus           s;
    natsPid              pid;
    natsPid              proxyPid;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 marker[512];
    char                 target[256];
    char                 json[512];
    char                 name[] = "socksctx";
    int                  port = 0;

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    snprintf(marker, sizeof(marker), "%s/proxy.target", base);

    test("Start SOCKS5 proxy: ");
    proxyPid = _startSocksProxy(&port, NULL, NULL, marker);
    testCond(proxyPid != NATS_INVALID_PID);

    // A name for the server, not an address: the proxy is the one that has to
    // resolve it, so this also covers the domain form of a CONNECT request.
    snprintf(json, sizeof(json),
             "{ \"url\": \"nats://localhost:4222\", \"socks_proxy\": \"socks5://127.0.0.1:%d\" }",
             port);

    test("Write context file: ");
    testCond(_writeContext(base, name, json));

    test("Start server: ");
    pid = _startServer(SERVER_URL, NULL, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Connect through the proxy: ");
    s = natsContext_Connect(&nc, &settings, name, NULL);
    testCond((s == NATS_OK) && (nc != NULL));

    test("Status connected: ");
    testCond(natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED);

    test("Proxy carried the connection: ");
    testCond(_readFile(marker, target, sizeof(target)) &&
             (strcmp(target, "localhost:4222") == 0));

    test("Settings report the proxy: ");
    testCond((settings != NULL) && (settings->SocksProxy != NULL) &&
             (strstr(settings->SocksProxy, "socks5://127.0.0.1:") == settings->SocksProxy));

    _stopServer(proxyPid);
    _cleanup(settings, nc, pid, base);
}

void
test_socks_proxy_auth(void)
{
    natsStatus           s;
    natsPid              pid;
    natsPid              proxyPid;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 marker[512];
    char                 target[256];
    char                 json[512];
    char                 name[] = "socksauth";
    int                  port = 0;

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    snprintf(marker, sizeof(marker), "%s/proxy.target", base);

    test("Start SOCKS5 proxy demanding credentials: ");
    proxyPid = _startSocksProxy(&port, "u$er", "p@ss", marker);
    testCond(proxyPid != NATS_INVALID_PID);

    // The credentials are percent-escaped, the way the `nats` CLI writes a
    // password holding a '@'.
    snprintf(json, sizeof(json),
             "{ \"url\": \"" SERVER_URL "\", "
             "\"socks_proxy\": \"socks5://u%%24er:p%%40ss@127.0.0.1:%d\" }",
             port);

    test("Write context file: ");
    testCond(_writeContext(base, name, json));

    test("Start server: ");
    pid = _startServer(SERVER_URL, NULL, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Connect through the authenticated proxy: ");
    s = natsContext_Connect(&nc, &settings, name, NULL);
    testCond((s == NATS_OK) && (nc != NULL));

    test("Proxy carried the connection: ");
    testCond(_readFile(marker, target, sizeof(target)) &&
             (strcmp(target, "127.0.0.1:4222") == 0));

    _stopServer(proxyPid);
    _cleanup(settings, nc, pid, base);
}

void
test_socks_proxy_bad_credentials(void)
{
    natsStatus           s;
    natsPid              pid;
    natsPid              proxyPid;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 marker[512];
    char                 json[512];
    char                 name[] = "socksbadauth";
    int                  port = 0;

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    snprintf(marker, sizeof(marker), "%s/proxy.target", base);

    test("Start SOCKS5 proxy demanding credentials: ");
    proxyPid = _startSocksProxy(&port, "user", "right", marker);
    testCond(proxyPid != NATS_INVALID_PID);

    snprintf(json, sizeof(json),
             "{ \"url\": \"" SERVER_URL "\", "
             "\"socks_proxy\": \"socks5://user:wrong@127.0.0.1:%d\" }",
             port);

    test("Write context file: ");
    testCond(_writeContext(base, name, json));

    test("Start server: ");
    pid = _startServer(SERVER_URL, NULL, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    // The server is up and reachable directly: only the refused proxy stands
    // between the two, and the connect has to fail on it.
    test("Rejected credentials fail the connect: ");
    s = natsContext_Connect(&nc, &settings, name, NULL);
    testCond((s != NATS_OK) && (nc == NULL) && (settings == NULL));

    _stopServer(proxyPid);
    _cleanup(settings, nc, pid, base);
}

void
test_socks_proxy_unreachable(void)
{
    natsStatus           s;
    natsPid              pid;
    natsPid              proxyPid;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 marker[512];
    char                 json[512];
    char                 name[] = "socksdown";
    int                  port = 0;

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    snprintf(marker, sizeof(marker), "%s/proxy.target", base);

    // Started only to be handed a port nothing is listening on afterwards.
    test("Reserve a proxy port: ");
    proxyPid = _startSocksProxy(&port, NULL, NULL, marker);
    testCond(proxyPid != NATS_INVALID_PID);
    _stopServer(proxyPid);

    snprintf(json, sizeof(json),
             "{ \"url\": \"" SERVER_URL "\", \"socks_proxy\": \"socks5://127.0.0.1:%d\" }",
             port);

    test("Write context file: ");
    testCond(_writeContext(base, name, json));

    test("Start server: ");
    pid = _startServer(SERVER_URL, NULL, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    // The server would be reachable directly, so a connection here would mean
    // the proxy had been quietly skipped.
    test("Dead proxy fails the connect: ");
    s = natsContext_Connect(&nc, &settings, name, NULL);
    testCond((s != NATS_OK) && (nc == NULL) && (settings == NULL));

    _cleanup(settings, nc, pid, base);
}

void
test_socks_proxy_bad_url(void)
{
    natsStatus           s;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 name[] = "socksbadurl";
    const char          *json =
        "{ \"url\": \"" SERVER_URL "\", \"socks_proxy\": \"http://127.0.0.1:8080\" }";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Write context file: ");
    testCond(_writeContext(base, name, json));

    // No server needed: a proxy that is not SOCKS5 is rejected while the
    // options are built, before anything is dialed, and reported as the
    // malformed context content it is.
    test("Non-SOCKS proxy URL is rejected: ");
    s = natsContext_Connect(&nc, &settings, name, NULL);
    testCond((s == NATS_ERR) && (nc == NULL) && (settings == NULL));

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
    test("Unknown context is NATS_NOT_FOUND: ");
    s = natsContext_Connect(&nc, &settings, name, NULL);
    testCond((s == NATS_NOT_FOUND) && (nc == NULL) && (settings == NULL));

    _cleanup(settings, nc, NATS_INVALID_PID, base);
}

void
test_invalid_name(void)
{
    natsStatus           s;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 dotdot[] = "../evil";
    char                 sep[]    = "sub/ctx";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Name with '..' is NATS_INVALID_ARG: ");
    s = natsContext_Connect(&nc, &settings, dotdot, NULL);
    testCond((s == NATS_INVALID_ARG) && (nc == NULL) && (settings == NULL));

    test("Name with a separator is NATS_INVALID_ARG: ");
    s = natsContext_Connect(&nc, &settings, sep, NULL);
    testCond((s == NATS_INVALID_ARG) && (nc == NULL) && (settings == NULL));

    _cleanup(settings, nc, NATS_INVALID_PID, base);
}

void
test_connect_null_name(void)
{
    natsStatus           s;
    natsPid              pid;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];

    // Regression: a NULL name must behave like "" (selected context), not
    // crash in strdup.
    test("Setup empty config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Start default server: ");
    pid = _startServer(SERVER_URL, NULL, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Connect with NULL name: ");
    s = natsContext_Connect(&nc, &settings, NULL, NULL);
    testCond((s == NATS_OK) && (nc != NULL) &&
             (natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED));

    _cleanup(settings, nc, pid, base);
}

void
test_connect_multi_server(void)
{
    natsStatus           s;
    natsPid              pid;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 name[] = "multi";
    // One live server, one dead entry, plus whitespace/empty entries that
    // the splitter must skip.
    const char          *json =
        "{ \"url\": \"" SERVER_URL ", ,nats://127.0.0.1:9999,\" }";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Write context file: ");
    testCond(_writeContext(base, "multi", json));

    test("Start server: ");
    pid = _startServer(SERVER_URL, NULL, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Connect with comma-separated URL list: ");
    s = natsContext_Connect(&nc, &settings, name, NULL);
    testCond((s == NATS_OK) && (nc != NULL) &&
             (natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED));

    test("Settings return the URL list verbatim: ");
    testCond((settings != NULL) && (settings->URL != NULL) &&
             (strchr(settings->URL, ',') != NULL));

    _cleanup(settings, nc, pid, base);
}

void
test_connect_with_caller_opts(void)
{
    natsStatus           s;
    natsPid              pid;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    natsOptions         *opts = NULL;
    char                 base[256];
    char                 name[] = "withopts";
    const char          *json = "{ \"url\": \"" SERVER_URL "\" }";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Write context file: ");
    testCond(_writeContext(base, "withopts", json));

    test("Start server: ");
    pid = _startServer(SERVER_URL, NULL, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Create caller options: ");
    s = natsOptions_Create(&opts);
    if (s == NATS_OK)
        s = natsOptions_SetName(opts, "orbit-ctx-test");
    testCond(s == NATS_OK);

    test("Connect with caller-supplied opts: ");
    s = natsContext_Connect(&nc, &settings, name, opts);
    testCond((s == NATS_OK) && (nc != NULL) &&
             (natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED));

    test("Settings the context does not cover are kept: ");
    testCond((natsConnection_GetName(nc) != NULL) &&
             (strcmp(natsConnection_GetName(nc), "orbit-ctx-test") == 0));

    // The opts object is borrowed: it remains the caller's to destroy, and
    // destroying it must not affect the live connection.
    test("Caller still owns opts: ");
    natsOptions_Destroy(opts);
    testCond(natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED);

    _cleanup(settings, nc, pid, base);
}

// Context values are layered on top of a caller-supplied opts, so they
// overwrite it where the two overlap. This is the opposite of orbit.go, where
// caller options are appended last and win; nats.c has no way to read a
// natsOptions back, so they cannot be merged the other way round.
void
test_context_opts_win(void)
{
    natsStatus           s;
    natsPid              pid;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    natsOptions         *opts = NULL;
    char                 base[256];
    char                 name[] = "override";
    // The context carries the credentials the server wants.
    const char          *json =
        "{ \"url\": \"" SERVER_URL "\", \"user\": \"testuser\","
        " \"password\": \"testpass\" }";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Write context file: ");
    testCond(_writeContext(base, "override", json));

    test("Start auth server: ");
    pid = _startServer(SERVER_URL, "--user testuser --pass testpass", false);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Server ready: ");
    testCond(_checkStart("nats://testuser:testpass@127.0.0.1:4222", 20) == NATS_OK);

    // Credentials the server would reject, plus a server list pointing nowhere:
    // the context has to replace both.
    test("Create conflicting caller options: ");
    s = natsOptions_Create(&opts);
    if (s == NATS_OK)
        s = natsOptions_SetUserInfo(opts, "wronguser", "wrongpass");
    if (s == NATS_OK)
    {
        const char *dead[] = { "nats://127.0.0.1:9999" };
        s = natsOptions_SetServers(opts, dead, 1);
    }
    testCond(s == NATS_OK);

    test("Context overwrites the caller's credentials and servers: ");
    s = natsContext_Connect(&nc, &settings, name, opts);
    testCond((s == NATS_OK) && (nc != NULL) &&
             (natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED));

    natsOptions_Destroy(opts);
    _cleanup(settings, nc, pid, base);
}

static void
_connectedCb(natsConnection *nc, void *closure)
{
    (void) nc;
    (void) closure;
}

// NATS_NOT_YET_CONNECTED is a live connection that is still retrying in the
// background, not a failure: it must be handed to the caller, not torn down.
void
test_connect_retry_not_yet_connected(void)
{
    natsStatus           s;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    natsOptions         *opts = NULL;
    char                 base[256];
    char                 name[] = "retryctx";
    const char          *json = "{ \"url\": \"" SERVER_URL "\" }";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Write context file: ");
    testCond(_writeContext(base, "retryctx", json));

    // A non-NULL connected callback is what makes nats.c return
    // NATS_NOT_YET_CONNECTED instead of blocking.
    test("Create retrying options: ");
    s = natsOptions_Create(&opts);
    if (s == NATS_OK)
        s = natsOptions_SetRetryOnFailedConnect(opts, true, _connectedCb, NULL);
    testCond(s == NATS_OK);

    // No server is started, so the first connect attempt cannot succeed.
    test("Retrying connect reports NATS_NOT_YET_CONNECTED: ");
    s = natsContext_Connect(&nc, &settings, name, opts);
    natsOptions_Destroy(opts);
    testCond(s == NATS_NOT_YET_CONNECTED);

    test("Connection is returned, not destroyed: ");
    testCond(nc != NULL);

    test("Settings are returned too: ");
    testCond((settings != NULL) && (settings->URL != NULL));

    _cleanup(settings, nc, NATS_INVALID_PID, base);
}

void
test_null_connection_arg(void)
{
    natsStatus           s;
    natsContextSettings *settings = (natsContextSettings *) (uintptr_t) 0x1;
    char                 base[256];
    char                 name[] = "whatever";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    // A NULL 'nc' must be rejected, and 'settings' cleared like every other
    // error path does.
    test("NULL connection out-param is NATS_INVALID_ARG: ");
    s = natsContext_Connect(NULL, &settings, name, NULL);
    testCond((s == NATS_INVALID_ARG) && (settings == NULL));

    _cleanup(NULL, NULL, NATS_INVALID_PID, base);
}

// A context carrying TLS material makes TLS mandatory. Against a server with no
// TLS at all that is reported as NATS_SECURE_CONNECTION_WANTED; without it the
// connection would silently fall back to plain text.

#define CERT_DIR "certs"

void
test_tls_material_requires_tls(void)
{
    natsStatus           s;
    natsPid              pid;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 cwd[1024];
    char                 caJson[1400];
    char                 certJson[2400];
    char                 caName[]   = "cactx";
    char                 certName[] = "certctx";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    // The test runs from the source test/ directory, so certs/ is relative to
    // it; contexts need absolute paths.
    test("Locate certs: ");
    testCond(getcwd(cwd, sizeof(cwd)) != NULL);

    test("Write context files: ");
    snprintf(caJson, sizeof(caJson),
             "{ \"url\": \"" SERVER_URL "\", \"ca\": \"%s/" CERT_DIR "/ca.pem\" }", cwd);
    snprintf(certJson, sizeof(certJson),
             "{ \"url\": \"" SERVER_URL "\", \"cert\": \"%s/" CERT_DIR "/client-cert.pem\","
             " \"key\": \"%s/" CERT_DIR "/client-key.pem\" }", cwd, cwd);
    testCond(_writeContext(base, "cactx", caJson) &&
             _writeContext(base, "certctx", certJson));

    test("Start plain (non-TLS) server: ");
    pid = _startServer(SERVER_URL, NULL, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("A 'ca' context refuses a plain-text server: ");
    s = natsContext_Connect(&nc, &settings, caName, NULL);
    testCond((s == NATS_SECURE_CONNECTION_WANTED) && (nc == NULL) && (settings == NULL));

    test("A 'cert'/'key' context refuses a plain-text server: ");
    s = natsContext_Connect(&nc, &settings, certName, NULL);
    testCond((s == NATS_SECURE_CONNECTION_WANTED) && (nc == NULL) && (settings == NULL));

    _cleanup(settings, nc, pid, base);
}

void
test_connect_expansion(void)
{
    natsStatus           s;
    natsPid              pid;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 envName[]   = "expandenv";
    char                 tildeName[] = "expandtilde";
    // User/password win the auth precedence, so the creds file is never
    // opened — the test only observes the expansion in the returned settings.
    const char          *envJson =
        "{ \"url\": \"" SERVER_URL "\", \"user\": \"testuser\", \"password\": \"testpass\","
        " \"creds\": \"$ORBIT_TEST_CREDS_DIR/user.creds\" }";
    const char          *tildeJson =
        "{ \"url\": \"" SERVER_URL "\", \"user\": \"testuser\", \"password\": \"testpass\","
        " \"creds\": \"~/user.creds\" }";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Write context files: ");
    setenv("ORBIT_TEST_CREDS_DIR", "/tmp/orbit-test-creds", 1);
    testCond(_writeContext(base, "expandenv", envJson) &&
             _writeContext(base, "expandtilde", tildeJson));

    test("Start auth server: ");
    pid = _startServer(SERVER_URL, "--user testuser --pass testpass", false);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Server ready: ");
    testCond(_checkStart("nats://testuser:testpass@127.0.0.1:4222", 20) == NATS_OK);

    test("Creds is env-expanded in settings: ");
    s = natsContext_Connect(&nc, &settings, envName, NULL);
    testCond((s == NATS_OK) && (settings != NULL) && (settings->Creds != NULL) &&
             (strcmp(settings->Creds, "/tmp/orbit-test-creds/user.creds") == 0));

    natsContextSettings_Destroy(settings);
    natsConnection_Destroy(nc);
    settings = NULL;
    nc       = NULL;

    // Tilde expansion happens only when the creds path is turned into a
    // connection option; the settings report the file verbatim.
    test("Creds is not tilde-expanded in settings: ");
    s = natsContext_Connect(&nc, &settings, tildeName, NULL);
    testCond((s == NATS_OK) && (settings != NULL) && (settings->Creds != NULL) &&
             (settings->Creds[0] == '~'));

    unsetenv("ORBIT_TEST_CREDS_DIR");
    _cleanup(settings, nc, pid, base);
}

static char g_lastRequestReply[256];

static void
_echoCb(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    const char *reply = natsMsg_GetReply(msg);

    (void) sub;
    (void) closure;

    if (reply != NULL)
    {
        snprintf(g_lastRequestReply, sizeof(g_lastRequestReply), "%s", reply);
        natsConnection_PublishString(nc, reply, "ok");
    }
    natsMsg_Destroy(msg);
}

void
test_connect_inbox_prefix(void)
{
    natsStatus           s;
    natsPid              pid;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    natsSubscription    *sub = NULL;
    natsMsg             *reply = NULL;
    char                 base[256];
    char                 name[] = "inbox";
    const char          *json =
        "{ \"url\": \"" SERVER_URL "\", \"inbox_prefix\": \"_CTXINBOX\" }";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Write context file: ");
    testCond(_writeContext(base, "inbox", json));

    test("Start server: ");
    pid = _startServer(SERVER_URL, NULL, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Connect: ");
    s = natsContext_Connect(&nc, &settings, name, NULL);
    testCond((s == NATS_OK) && (nc != NULL) &&
             (natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED));

    test("Request uses the custom inbox prefix: ");
    g_lastRequestReply[0] = '\0';
    s = natsConnection_Subscribe(&sub, nc, "ctx.echo", _echoCb, NULL);
    if (s == NATS_OK)
        s = natsConnection_RequestString(&reply, nc, "ctx.echo", "hi", 2000);
    testCond((s == NATS_OK) &&
             (strncmp(g_lastRequestReply, "_CTXINBOX.", 10) == 0));

    natsMsg_Destroy(reply);
    natsSubscription_Destroy(sub);
    _cleanup(settings, nc, pid, base);
}

void
test_connect_null_json_fields(void)
{
    natsStatus           s;
    natsPid              pid;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 name[] = "nulls";
    // A JSON null must read as "absent", leaving the field at its zero value.
    const char          *json =
        "{ \"url\": \"" SERVER_URL "\", \"user\": null, \"password\": null,"
        " \"tls_first\": null, \"windows_ca_certs_match\": null }";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Write context file: ");
    testCond(_writeContext(base, "nulls", json));

    test("Start server: ");
    pid = _startServer(SERVER_URL, NULL, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("Connect with null JSON fields: ");
    s = natsContext_Connect(&nc, &settings, name, NULL);
    testCond((s == NATS_OK) && (nc != NULL) &&
             (natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED));

    test("Null fields left at defaults: ");
    testCond((settings != NULL) && (settings->User == NULL) &&
             (settings->TLSFirst == false) &&
             (settings->WinCertStoreCaMatch == NULL));

    _cleanup(settings, nc, pid, base);
}

// A context holding `nsc` is resolved by running `nsc generate profile <value>`.
// The tests below stand a fake `nsc` up on PATH: the real one is not needed, and
// the script records how it was invoked so the arguments can be checked.

#define NSC_LOOKUP "nsc://operator/account/user"

// Records its arguments in $XDG_CONFIG_HOME/nsc_args and prints a profile.
static const char *fakeNscOk =
    "#!/bin/sh\n"
    "echo \"$@\" > \"$XDG_CONFIG_HOME/nsc_args\"\n"
    "echo '{\"user_creds\":\"/tmp/fake.creds\",\"operator\":{\"service\":"
    "[\"nats://127.0.0.1:4222\",\"nats://127.0.0.1:4223\"]}}'\n";

void
test_connect_nsc_lookup(void)
{
    natsStatus           s;
    natsPid              pid;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 argsPath[1024];
    char                 args[256] = {0};
    char                 name[] = "nscctx";
    const char          *json =
        "{ \"url\": \"" SERVER_URL "\", \"nsc\": \"" NSC_LOOKUP "\" }";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Install fake nsc: ");
    testCond(_writeFakeExe(base, "nsc", fakeNscOk));

    test("Write context file: ");
    testCond(_writeContext(base, "nscctx", json));

    test("Start server: ");
    pid = _startServer(SERVER_URL, NULL, true);
    CHECK_SERVER_STARTED(pid);
    testCond(true);

    test("nsc context connects: ");
    s = natsContext_Connect(&nc, &settings, name, NULL);
    testCond((s == NATS_OK) && (nc != NULL) &&
             (natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED));

    test("nsc value parsed into settings: ");
    testCond((settings != NULL) && (settings->NSCLookup != NULL) &&
             (strcmp(settings->NSCLookup, NSC_LOOKUP) == 0));

    // The lookup value must reach nsc as a single argument, not as text pasted
    // into a shell command line.
    test("nsc invoked with the lookup: ");
    snprintf(argsPath, sizeof(argsPath), "%s/nsc_args", base);
    testCond(_readFile(argsPath, args, sizeof(args)) &&
             (strcmp(args, "generate profile " NSC_LOOKUP "\n") == 0));

    _cleanup(settings, nc, pid, base);
}

void
test_nsc_not_found(void)
{
    natsStatus           s;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 emptyDir[1024];
    char                 name[] = "nscctx";
    const char          *json =
        "{ \"url\": \"" SERVER_URL "\", \"nsc\": \"" NSC_LOOKUP "\" }";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Write context file: ");
    testCond(_writeContext(base, "nscctx", json));

    // No server is started: the lookup fails before any connection is made.
    test("Empty PATH: ");
    snprintf(emptyDir, sizeof(emptyDir), "%s/bin", base);
    testCond(_mkdirOk(emptyDir) && (setenv("PATH", emptyDir, 1) == 0));

    test("nsc not on PATH reported: ");
    s = natsContext_Connect(&nc, &settings, name, NULL);
    testCond((s == NATS_NOT_FOUND) && (nc == NULL) && (settings == NULL));

    _cleanup(settings, nc, NATS_INVALID_PID, base);
}

void
test_nsc_invoke_failed(void)
{
    natsStatus           s;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 name[] = "nscctx";
    const char          *script = "#!/bin/sh\necho 'nsc: no such account' >&2\nexit 1\n";
    const char          *json =
        "{ \"url\": \"" SERVER_URL "\", \"nsc\": \"" NSC_LOOKUP "\" }";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Install failing nsc: ");
    testCond(_writeFakeExe(base, "nsc", script));

    test("Write context file: ");
    testCond(_writeContext(base, "nscctx", json));

    test("Failed nsc invocation reported: ");
    s = natsContext_Connect(&nc, &settings, name, NULL);
    testCond((s == NATS_ERR) && (nc == NULL) && (settings == NULL));

    _cleanup(settings, nc, NATS_INVALID_PID, base);
}

void
test_nsc_bad_output(void)
{
    natsStatus           s;
    natsConnection      *nc = NULL;
    natsContextSettings *settings = NULL;
    char                 base[256];
    char                 name[] = "nscctx";
    const char          *script = "#!/bin/sh\necho 'not json'\n";
    const char          *json =
        "{ \"url\": \"" SERVER_URL "\", \"nsc\": \"" NSC_LOOKUP "\" }";

    test("Setup config dir: ");
    testCond(_makeConfigTree(base, sizeof(base)));

    test("Install babbling nsc: ");
    testCond(_writeFakeExe(base, "nsc", script));

    test("Write context file: ");
    testCond(_writeContext(base, "nscctx", json));

    test("Unparsable nsc output reported: ");
    s = natsContext_Connect(&nc, &settings, name, NULL);
    testCond((s == NATS_ERR) && (nc == NULL) && (settings == NULL));

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
