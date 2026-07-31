// Copyright 2026 The NATS Authors
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

#include "../os_shims.h"

#include <stdio.h>

static BOOL CALLBACK
_onceCb(PINIT_ONCE once, PVOID param, PVOID *context)
{
    void (*cb)(void) = (void (*)(void)) param;

    cb();
    return TRUE;
}

void
natsSys_Once(natsSysOnce *once, void (*cb)(void))
{
    InitOnceExecuteOnce(once, _onceCb, (PVOID) cb, NULL);
}

// Waits for 'fd' to become readable, or writable when 'forWrite'. An
// interrupted wait is resumed with the time that is left, so that NATS_OK
// means the socket really is ready and not that the wait was cut short.
//
// select() rather than WSAPoll(), and with the exception set: a non-blocking
// connect that failed is only ever reported there on Windows, never as
// writable, which is the same reason nats.c waits this way.
static natsStatus
_waitReady(natsSock fd, bool forWrite, int64_t deadline)
{
    for (;;)
    {
        struct timeval timeout;
        fd_set         fdSet;
        fd_set         errSet;
        int64_t        remaining = deadline - nats_Now();
        int            res;

        if (remaining <= 0)
            return NATS_TIMEOUT;

        FD_ZERO(&fdSet);
        FD_SET(fd, &fdSet);
        FD_ZERO(&errSet);
        FD_SET(fd, &errSet);

        timeout.tv_sec  = (long) (remaining / 1000);
        timeout.tv_usec = (long) ((remaining % 1000) * 1000);

        // The first argument is ignored on Windows.
        if (forWrite)
            res = select(0, NULL, &fdSet, &errSet, &timeout);
        else
            res = select(0, &fdSet, NULL, &errSet, &timeout);

        if (res > 0)
            return NATS_OK;
        if (res == 0)
            return NATS_TIMEOUT;
        if (WSAGetLastError() != WSAEINTR)
            return NATS_IO_ERROR;
    }
}

static natsStatus
_setCommonOptions(natsSock fd)
{
    struct linger l;
    int           yes  = 1;
    u_long        mode = 1;

    if (ioctlsocket(fd, FIONBIO, &mode) != 0)
        return NATS_SYS_ERROR;

    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (const char *) &yes, sizeof(yes)) == SOCKET_ERROR)
        return NATS_SYS_ERROR;

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (const char *) &yes, sizeof(yes)) == SOCKET_ERROR)
        return NATS_SYS_ERROR;

    l.l_onoff  = 1;
    l.l_linger = 0;

    if (setsockopt(fd, SOL_SOCKET, SO_LINGER, (const char *) &l, sizeof(l)) == SOCKET_ERROR)
        return NATS_SYS_ERROR;

    return NATS_OK;
}

// Connects an already non-blocking socket, waiting out an in-progress connect
// until the deadline.
static natsStatus
_connect(natsSock fd, const struct addrinfo *addr, int64_t deadline)
{
    natsStatus s         = NATS_OK;
    int        error     = 0;
    int        len       = sizeof(error);
    int        lastError = 0;

    if (connect(fd, addr->ai_addr, (int) addr->ai_addrlen) != SOCKET_ERROR)
        return NATS_OK;

    // A connect on a non-blocking socket reports back through the socket
    // becoming writable (or excepted) rather than by finishing here; retrying
    // it instead would only be told it is already under way.
    lastError = WSAGetLastError();
    if ((lastError != WSAEWOULDBLOCK) && (lastError != WSAEINPROGRESS) &&
        (lastError != WSAEINTR) && (lastError != WSAEALREADY))
        return NATS_NO_SERVER;

    s = _waitReady(fd, true, deadline);
    if (s != NATS_OK)
        return s;

    // Being signalled says the connect finished, not that it succeeded.
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, (char *) &error, &len) == SOCKET_ERROR)
        return NATS_SYS_ERROR;

    return (error == 0) ? NATS_OK : NATS_NO_SERVER;
}

natsStatus
natsSys_SockConnect(natsSock *fd, const char *host, int port, int64_t deadline)
{
    struct addrinfo  hints;
    struct addrinfo *servinfo = NULL;
    struct addrinfo *p;
    char             sport[6];
    natsStatus       s = NATS_NO_SERVER;

    if ((fd == NULL) || nats_IsStringEmpty(host) || (port <= 0) || (port > 65535))
        return NATS_INVALID_ARG;

    *fd = NATS_SYS_SOCK_INVALID;
    snprintf(sport, sizeof(sport), "%d", port);

    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(host, sport, &hints, &servinfo) != 0)
        return NATS_NO_SERVER;

    for (p = servinfo; p != NULL; p = p->ai_next)
    {
        natsSock sock = socket(p->ai_family, p->ai_socktype, p->ai_protocol);

        if (sock == NATS_SYS_SOCK_INVALID)
        {
            s = NATS_SYS_ERROR;
            continue;
        }

        // The options go on before the connect, matching what nats.c does with
        // its own sockets, and leave the socket non-blocking for the caller.
        s = _setCommonOptions(sock);
        if (s == NATS_OK)
            s = _connect(sock, p, deadline);

        if (s == NATS_OK)
        {
            *fd = sock;
            break;
        }

        natsSys_SockClose(sock);

        // Another address is worth trying, more time is not.
        if (s == NATS_TIMEOUT)
            break;
    }

    freeaddrinfo(servinfo);
    return s;
}

natsStatus
natsSys_SockWrite(natsSock fd, const void *data, size_t len, int64_t deadline)
{
    const char *buf  = (const char *) data;
    size_t      done = 0;

    while (done < len)
    {
        int n = send(fd, buf + done, (int) (len - done), 0);

        if (n > 0)
        {
            done += (size_t) n;
            continue;
        }

        if (n == SOCKET_ERROR)
        {
            int lastError = WSAGetLastError();

            if (lastError == WSAEWOULDBLOCK)
            {
                natsStatus s = _waitReady(fd, true, deadline);
                if (s != NATS_OK)
                    return s;
                continue;
            }
            if (lastError == WSAEINTR)
                continue;
            if ((lastError == WSAECONNRESET) || (lastError == WSAESHUTDOWN))
                return NATS_CONNECTION_CLOSED;
        }

        return NATS_IO_ERROR;
    }

    return NATS_OK;
}

natsStatus
natsSys_SockRead(natsSock fd, void *data, size_t len, int64_t deadline)
{
    char  *buf  = (char *) data;
    size_t done = 0;

    while (done < len)
    {
        int n = recv(fd, buf + done, (int) (len - done), 0);

        if (n > 0)
        {
            done += (size_t) n;
            continue;
        }

        // A peer that hangs up mid-message leaves the caller with a partial
        // read it has no way to complete.
        if (n == 0)
            return NATS_CONNECTION_CLOSED;

        if (n == SOCKET_ERROR)
        {
            int lastError = WSAGetLastError();

            if (lastError == WSAEWOULDBLOCK)
            {
                natsStatus s = _waitReady(fd, false, deadline);
                if (s != NATS_OK)
                    return s;
                continue;
            }
            if (lastError == WSAEINTR)
                continue;
            if (lastError == WSAECONNRESET)
                return NATS_CONNECTION_CLOSED;
        }

        return NATS_IO_ERROR;
    }

    return NATS_OK;
}

natsStatus
natsSys_SockParseIP(const char *host, unsigned char *ip, int *len)
{
    struct in_addr  v4;
    struct in6_addr v6;

    if (nats_IsStringEmpty(host) || (ip == NULL) || (len == NULL))
        return NATS_INVALID_ARG;

    if (inet_pton(AF_INET, host, &v4) == 1)
    {
        memcpy(ip, &v4.s_addr, 4);
        *len = 4;
        return NATS_OK;
    }

    if (inet_pton(AF_INET6, host, &v6) == 1)
    {
        memcpy(ip, v6.s6_addr, 16);
        *len = 16;
        return NATS_OK;
    }

    return NATS_NOT_FOUND;
}

void
natsSys_SockClose(natsSock fd)
{
    if (fd != NATS_SYS_SOCK_INVALID)
        closesocket(fd);
}
