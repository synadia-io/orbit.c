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

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

// send() must never take the process down with it: the peer here is a proxy
// that can drop the connection at any point of the exchange.
#ifdef MSG_NOSIGNAL
#define SOCK_SEND_FLAGS MSG_NOSIGNAL
#else
#define SOCK_SEND_FLAGS 0
#endif

void
natsSys_Once(natsSysOnce *once, void (*cb)(void))
{
    pthread_once(once, cb);
}

// Waits for 'fd' to become readable, or writable when 'forWrite'. An
// interrupted wait is resumed with the time that is left, so that NATS_OK
// means the socket really is ready and not that the wait was cut short.
static natsStatus
_waitReady(natsSock fd, bool forWrite, int64_t deadline)
{
    struct pollfd p;

    p.fd     = fd;
    p.events = (short) (forWrite ? POLLOUT : POLLIN);

    for (;;)
    {
        int64_t remaining = deadline - nats_Now();
        int     res;

        if (remaining <= 0)
            return NATS_TIMEOUT;

        p.revents = 0;

        // An error on the socket comes back as POLLERR/POLLHUP, which counts
        // as ready here: the operation the caller retries is what reports it.
        res = poll(&p, 1, (int) remaining);
        if (res > 0)
            return NATS_OK;
        if (res == 0)
            return NATS_TIMEOUT;
        if (errno != EINTR)
            return NATS_IO_ERROR;
    }
}

static natsStatus
_setCommonOptions(natsSock fd)
{
    struct linger l;
    int           yes   = 1;
    int           flags = fcntl(fd, F_GETFL, 0);

    if ((flags == -1) || (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1))
        return NATS_SYS_ERROR;

#ifdef SO_NOSIGPIPE
    if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, (void *) &yes, sizeof(yes)) == -1)
        return NATS_SYS_ERROR;
#endif

    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (const char *) &yes, sizeof(yes)) == -1)
        return NATS_SYS_ERROR;

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (const char *) &yes, sizeof(yes)) == -1)
        return NATS_SYS_ERROR;

    l.l_onoff  = 1;
    l.l_linger = 0;

    if (setsockopt(fd, SOL_SOCKET, SO_LINGER, (void *) &l, sizeof(l)) == -1)
        return NATS_SYS_ERROR;

    return NATS_OK;
}

// Connects an already non-blocking socket, waiting out an in-progress connect
// until the deadline.
static natsStatus
_connect(natsSock fd, const struct addrinfo *addr, int64_t deadline)
{
    natsStatus s     = NATS_OK;
    int        error = 0;
    socklen_t  len   = sizeof(error);

    if (connect(fd, addr->ai_addr, addr->ai_addrlen) == 0)
        return NATS_OK;

    // A connect on a non-blocking socket reports back through the socket
    // becoming writable rather than by finishing here; retrying it instead
    // would only be told it is already under way.
    if ((errno != EINPROGRESS) && (errno != EINTR) && (errno != EALREADY))
        return NATS_NO_SERVER;

    s = _waitReady(fd, true, deadline);
    if (s != NATS_OK)
        return s;

    // Writable says the connect finished, not that it succeeded.
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, (void *) &error, &len) != 0)
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
        ssize_t n = send(fd, buf + done, len - done, SOCK_SEND_FLAGS);

        if (n > 0)
        {
            done += (size_t) n;
            continue;
        }

        if ((n < 0) && ((errno == EAGAIN) || (errno == EWOULDBLOCK)))
        {
            natsStatus s = _waitReady(fd, true, deadline);
            if (s != NATS_OK)
                return s;
            continue;
        }

        if ((n < 0) && (errno == EINTR))
            continue;

        // A stream send of a non-empty buffer does not return 0, so anything
        // left here failed and errno says how.
        if (n == 0)
            return NATS_IO_ERROR;

        return (errno == EPIPE) ? NATS_CONNECTION_CLOSED : NATS_IO_ERROR;
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
        ssize_t n = recv(fd, buf + done, len - done, 0);

        if (n > 0)
        {
            done += (size_t) n;
            continue;
        }

        // A peer that hangs up mid-message leaves the caller with a partial
        // read it has no way to complete.
        if (n == 0)
            return NATS_CONNECTION_CLOSED;

        if ((errno == EAGAIN) || (errno == EWOULDBLOCK))
        {
            natsStatus s = _waitReady(fd, false, deadline);
            if (s != NATS_OK)
                return s;
            continue;
        }

        if (errno == EINTR)
            continue;

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
        close(fd);
}
