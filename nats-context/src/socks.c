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

// SOCKS5 (RFC 1928) client for the `socks_proxy` context setting, plugged into
// nats.c as a natsProxyConnHandler: the handler dials the proxy, walks the
// handshake, and hands the connected socket back for the NATS protocol (and
// TLS, when the context asks for it) to run over.

#include "socks.h"
#include "os_shims.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>

#define IFOK(s, c)      if (s == NATS_OK) { s = (c); }

// The handshake needs a budget of its own: the connect timeout in natsOptions
// bounds the dialing nats.c does itself, and nats.c does not apply it around a
// proxy handler, so nothing else limits how long an unresponsive proxy can
// hold up a connect.
#define SOCKS_TIMEOUT       (5000)

#define SOCKS_VERSION       ((unsigned char) 0x05)
#define SOCKS_AUTH_VERSION  ((unsigned char) 0x01)
#define SOCKS_AUTH_NONE     ((unsigned char) 0x00)
#define SOCKS_AUTH_USERPASS ((unsigned char) 0x02)
#define SOCKS_AUTH_NOMATCH  ((unsigned char) 0xFF)
#define SOCKS_CMD_CONNECT   ((unsigned char) 0x01)
#define SOCKS_RESERVED      ((unsigned char) 0x00)
#define SOCKS_REPLY_OK      ((unsigned char) 0x00)
#define SOCKS_ATYP_IPV4     ((unsigned char) 0x01)
#define SOCKS_ATYP_DOMAIN   ((unsigned char) 0x03)
#define SOCKS_ATYP_IPV6     ((unsigned char) 0x04)

#define SOCKS_DEFAULT_PORT  (1080)

// A domain name, a user name and a password are each length-prefixed with a
// single byte on the wire.
#define SOCKS_MAX_STR       (255)

// Largest message either side sends: a CONNECT request for the longest domain
// name, and a reply carrying the longest bound address.
#define SOCKS_MAX_MSG       (4 + 1 + SOCKS_MAX_STR + 2)

typedef struct __natsSocksProxy
{
    char *url;
    char *host;
    int   port;
    char *user;
    char *pass;

    struct __natsSocksProxy *next;

} natsSocksProxy;

// nats.c keeps the handler's closure in the connection's options and never
// hands it back: a reconnect can call the handler long after
// natsContext_Connect returned, and after the settings the proxy was read from
// were destroyed, so there is no point at which a per-connection closure could
// be freed. Proxy definitions are therefore interned here, one entry per
// distinct URL, and live as long as the process — bounded by the number of
// distinct proxies connected through, not by the number of connections.
static natsSysOnce     _socksOnce = NATS_SYS_ONCE_INIT;
static natsMutex      *_socksMutex = NULL;
static natsSocksProxy *_socksProxies = NULL;

static void
_initSocks(void)
{
    if (natsMutex_Create(&_socksMutex) != NATS_OK)
        _socksMutex = NULL;
}

static int
_hexVal(char c)
{
    if ((c >= '0') && (c <= '9'))
        return c - '0';
    if ((c >= 'a') && (c <= 'f'))
        return c - 'a' + 10;
    if ((c >= 'A') && (c <= 'F'))
        return c - 'A' + 10;

    return -1;
}

static bool
_eqNoCase(const char *s, size_t len, const char *lit)
{
    size_t i;

    if (strlen(lit) != len)
        return false;

    for (i = 0; i < len; i++)
    {
        if (tolower((unsigned char) s[i]) != lit[i])
            return false;
    }

    return true;
}

// Copies 'len' bytes of 'src' with %XX escapes resolved, the way the `nats`
// CLI's Go URL parsing reads them, so that a password holding a '@' or a ':'
// survives the round trip.
static natsStatus
_pctDecode(const char *src, size_t len, char **out)
{
    char  *res = (char *) NATS_MALLOC(len + 1);
    size_t i   = 0;
    size_t n   = 0;

    *out = NULL;
    if (res == NULL)
        return NATS_NO_MEMORY;

    while (i < len)
    {
        if (src[i] == '%')
        {
            int hi = (i + 2 < len) ? _hexVal(src[i + 1]) : -1;
            int lo = (i + 2 < len) ? _hexVal(src[i + 2]) : -1;

            if ((hi < 0) || (lo < 0))
            {
                NATS_FREE(res);
                return NATS_INVALID_ARG;
            }

            res[n++] = (char) ((hi << 4) | lo);
            i += 3;
            continue;
        }

        res[n++] = src[i++];
    }

    res[n] = '\0';
    *out   = res;
    return NATS_OK;
}

static natsStatus
_dupRange(const char *start, const char *end, char **out)
{
    size_t len = (size_t) (end - start);

    *out = (char *) NATS_MALLOC(len + 1);
    if (*out == NULL)
        return NATS_NO_MEMORY;

    memcpy(*out, start, len);
    (*out)[len] = '\0';
    return NATS_OK;
}

static natsStatus
_parsePort(const char *start, const char *end, int *port)
{
    long        val = 0;
    const char *p;

    if (start == end)
        return NATS_INVALID_ARG;

    for (p = start; p < end; p++)
    {
        if (!isdigit((unsigned char) *p))
            return NATS_INVALID_ARG;

        val = (val * 10) + (*p - '0');
        if (val > 65535)
            return NATS_INVALID_ARG;
    }

    if (val == 0)
        return NATS_INVALID_ARG;

    *port = (int) val;
    return NATS_OK;
}

// Splits the credentials out of a [user[:password]@] prefix.
static natsStatus
_parseUserInfo(natsSocksProxy *proxy, const char *start, const char *end)
{
    const char *colon = start;
    natsStatus  s;

    while ((colon < end) && (*colon != ':'))
        colon++;

    if (colon == start)
        return NATS_INVALID_ARG; // a password with no user to go with it

    s = _pctDecode(start, (size_t) (colon - start), &proxy->user);
    if ((s == NATS_OK) && (colon < end))
        s = _pctDecode(colon + 1, (size_t) (end - colon - 1), &proxy->pass);

    if (s != NATS_OK)
        return s;

    if ((strlen(proxy->user) > SOCKS_MAX_STR) ||
        ((proxy->pass != NULL) && (strlen(proxy->pass) > SOCKS_MAX_STR)))
        return NATS_INVALID_ARG;

    return NATS_OK;
}

// Parses [socks5://][user[:password]@]host[:port]. The scheme is optional
// because the `nats` CLI does not require one; anything other than socks5(h)
// is rejected rather than quietly dialed as SOCKS5.
static natsStatus
_parseURL(natsSocksProxy *proxy, const char *url)
{
    const char *p    = url;
    const char *sep  = strstr(url, "://");
    const char *end;
    const char *at   = NULL;
    const char *host;
    const char *scan;
    natsStatus  s    = NATS_OK;

    if (sep != NULL)
    {
        if (!_eqNoCase(url, (size_t) (sep - url), "socks5") &&
            !_eqNoCase(url, (size_t) (sep - url), "socks5h"))
            return NATS_INVALID_ARG;

        p = sep + 3;
    }

    // Everything past the authority (a stray trailing '/', say) is not ours to
    // interpret.
    end = p + strcspn(p, "/?#");

    for (scan = p; scan < end; scan++)
    {
        if (*scan == '@')
            at = scan;
    }

    if (at != NULL)
    {
        s = _parseUserInfo(proxy, p, at);
        if (s != NATS_OK)
            return s;

        p = at + 1;
    }

    host = p;

    if ((p < end) && (*p == '['))
    {
        // Bracketed IPv6 literal: [::1]:1080
        const char *close = p;

        while ((close < end) && (*close != ']'))
            close++;

        if (close == end)
            return NATS_INVALID_ARG;

        s = _dupRange(p + 1, close, &proxy->host);
        if (s != NATS_OK)
            return s;
        if (nats_IsStringEmpty(proxy->host))
            return NATS_INVALID_ARG;

        p = close + 1;
        if (p == end)
            return NATS_OK;
        if (*p != ':')
            return NATS_INVALID_ARG;

        return _parsePort(p + 1, end, &proxy->port);
    }

    for (scan = p; scan < end; scan++)
    {
        if (*scan == ':')
            p = scan;
    }

    if ((p < end) && (*p == ':'))
    {
        s = _dupRange(host, p, &proxy->host);
        IFOK(s, _parsePort(p + 1, end, &proxy->port));
    }
    else
    {
        s = _dupRange(host, end, &proxy->host);
    }

    if (s != NATS_OK)
        return s;

    return nats_IsStringEmpty(proxy->host) ? NATS_INVALID_ARG : NATS_OK;
}

static void
_destroyProxy(natsSocksProxy *proxy)
{
    if (proxy == NULL)
        return;

    NATS_FREE(proxy->url);
    NATS_FREE(proxy->host);
    NATS_FREE(proxy->user);
    NATS_FREE(proxy->pass);
    NATS_FREE(proxy);
}

static natsStatus
_createProxy(natsSocksProxy **newProxy, const char *url)
{
    natsSocksProxy *proxy = NULL;
    natsStatus      s     = NATS_OK;

    *newProxy = NULL;

    proxy = (natsSocksProxy *) NATS_CALLOC(1, sizeof(natsSocksProxy));
    if (proxy == NULL)
        return NATS_NO_MEMORY;

    proxy->port = SOCKS_DEFAULT_PORT;
    proxy->url  = NATS_STRDUP(url);
    if (proxy->url == NULL)
        s = NATS_NO_MEMORY;

    IFOK(s, _parseURL(proxy, url));

    if (s != NATS_OK)
    {
        _destroyProxy(proxy);
        return s;
    }

    *newProxy = proxy;
    return NATS_OK;
}

// Username/password authentication, RFC 1929. The reply's version byte is not
// checked: servers are not consistent about echoing 0x01 there, and the status
// byte is what says whether the credentials were taken.
static natsStatus
_authenticate(natsSock fd, const natsSocksProxy *proxy, int64_t deadline)
{
    unsigned char buf[3 + (2 * SOCKS_MAX_STR)];
    size_t        userLen = strlen(proxy->user);
    size_t        passLen = (proxy->pass == NULL) ? 0 : strlen(proxy->pass);
    size_t        n       = 0;
    natsStatus    s;

    buf[n++] = SOCKS_AUTH_VERSION;
    buf[n++] = (unsigned char) userLen;
    memcpy(buf + n, proxy->user, userLen);
    n += userLen;
    buf[n++] = (unsigned char) passLen;
    if (passLen > 0)
    {
        memcpy(buf + n, proxy->pass, passLen);
        n += passLen;
    }

    s = natsSys_SockWrite(fd, buf, n, deadline);
    IFOK(s, natsSys_SockRead(fd, buf, 2, deadline));
    if (s != NATS_OK)
        return s;

    return (buf[1] == 0) ? NATS_OK : NATS_NOT_PERMITTED;
}

// Method negotiation: offer "no authentication", plus username/password when
// the URL carried credentials, and go along with what the proxy picks.
static natsStatus
_negotiate(natsSock fd, const natsSocksProxy *proxy, int64_t deadline)
{
    unsigned char buf[4];
    size_t        n = 0;
    natsStatus    s;

    buf[n++] = SOCKS_VERSION;
    buf[n++] = (proxy->user != NULL) ? 2 : 1;
    buf[n++] = SOCKS_AUTH_NONE;
    if (proxy->user != NULL)
        buf[n++] = SOCKS_AUTH_USERPASS;

    s = natsSys_SockWrite(fd, buf, n, deadline);
    IFOK(s, natsSys_SockRead(fd, buf, 2, deadline));
    if (s != NATS_OK)
        return s;

    if (buf[0] != SOCKS_VERSION)
        return NATS_PROTOCOL_ERROR;

    switch (buf[1])
    {
        case SOCKS_AUTH_NONE:
            return NATS_OK;
        case SOCKS_AUTH_USERPASS:
            // Picking a method that was never offered is the proxy's error.
            if (proxy->user == NULL)
                return NATS_PROTOCOL_ERROR;
            return _authenticate(fd, proxy, deadline);
        case SOCKS_AUTH_NOMATCH:
            // Typically a proxy that wants credentials the context has none of.
            return NATS_NOT_PERMITTED;
        default:
            return NATS_PROTOCOL_ERROR;
    }
}

// RFC 1928 reply codes, mapped onto the closest nats.c status. nats.c reports a
// failed proxy connect to the caller as NATS_NO_SERVER whatever comes back
// here, so this only has to be honest, not load-bearing.
static natsStatus
_replyStatus(unsigned char reply)
{
    switch (reply)
    {
        case 0x02: return NATS_NOT_PERMITTED;  // not allowed by ruleset
        case 0x03:                             // network unreachable
        case 0x04:                             // host unreachable
        case 0x05: return NATS_NO_SERVER;      // connection refused
        case 0x06: return NATS_TIMEOUT;        // TTL expired
        default:   return NATS_ERR;            // general failure, or a request
                                               // the proxy does not support
    }
}

// Asks the proxy to connect to 'host' on 'port' and reads its reply.
static natsStatus
_requestConnect(natsSock fd, const char *host, int port, int64_t deadline)
{
    unsigned char buf[SOCKS_MAX_MSG];
    unsigned char ip[16];
    size_t        hostLen = strlen(host);
    size_t        bound   = 0;
    size_t        n       = 0;
    int           ipLen   = 0;
    natsStatus    s;

    buf[n++] = SOCKS_VERSION;
    buf[n++] = SOCKS_CMD_CONNECT;
    buf[n++] = SOCKS_RESERVED;

    if (natsSys_SockParseIP(host, ip, &ipLen) == NATS_OK)
    {
        buf[n++] = (ipLen == 4) ? SOCKS_ATYP_IPV4 : SOCKS_ATYP_IPV6;
        memcpy(buf + n, ip, (size_t) ipLen);
        n += (size_t) ipLen;
    }
    else
    {
        if (hostLen > SOCKS_MAX_STR)
            return NATS_INVALID_ARG;

        // Names are passed on for the proxy to resolve, which is the point of
        // proxying: the name may only mean something on its side.
        buf[n++] = SOCKS_ATYP_DOMAIN;
        buf[n++] = (unsigned char) hostLen;
        memcpy(buf + n, host, hostLen);
        n += hostLen;
    }

    buf[n++] = (unsigned char) ((port >> 8) & 0xFF);
    buf[n++] = (unsigned char) (port & 0xFF);

    s = natsSys_SockWrite(fd, buf, n, deadline);
    IFOK(s, natsSys_SockRead(fd, buf, 4, deadline));
    if (s != NATS_OK)
        return s;

    if (buf[0] != SOCKS_VERSION)
        return NATS_PROTOCOL_ERROR;
    if (buf[1] != SOCKS_REPLY_OK)
        return _replyStatus(buf[1]);

    switch (buf[3])
    {
        case SOCKS_ATYP_IPV4:
            bound = 4;
            break;
        case SOCKS_ATYP_IPV6:
            bound = 16;
            break;
        case SOCKS_ATYP_DOMAIN:
            s = natsSys_SockRead(fd, buf, 1, deadline);
            if (s != NATS_OK)
                return s;
            bound = buf[0];
            break;
        default:
            return NATS_PROTOCOL_ERROR;
    }

    // The bound address is of no use here, but it has to come off the socket:
    // whatever is left would be read as the start of the server's INFO.
    return natsSys_SockRead(fd, buf, bound + 2, deadline);
}

static natsStatus
_socksConnect(natsSock *fd, char *host, int port, void *closure)
{
    natsSocksProxy *proxy    = (natsSocksProxy *) closure;
    natsSock        sock     = NATS_SYS_SOCK_INVALID;
    int64_t         deadline = nats_Now() + SOCKS_TIMEOUT;
    natsStatus      s;

    if ((fd == NULL) || nats_IsStringEmpty(host) || (proxy == NULL))
        return NATS_INVALID_ARG;

    s = natsSys_SockConnect(&sock, proxy->host, proxy->port, deadline);
    IFOK(s, _negotiate(sock, proxy, deadline));
    IFOK(s, _requestConnect(sock, host, port, deadline));

    if (s != NATS_OK)
    {
        natsSys_SockClose(sock);
        return s;
    }

    *fd = sock;
    return NATS_OK;
}

natsStatus
natsSocks_ProxyHandler(const char *url, natsProxyConnHandler *handler, void **closure)
{
    natsSocksProxy *proxy = NULL;
    natsStatus      s     = NATS_OK;

    if ((url == NULL) || (handler == NULL) || (closure == NULL))
        return NATS_INVALID_ARG;

    *handler = NULL;
    *closure = NULL;

    natsSys_Once(&_socksOnce, _initSocks);
    if (_socksMutex == NULL)
        return NATS_NO_MEMORY;

    natsMutex_Lock(_socksMutex);

    for (proxy = _socksProxies; proxy != NULL; proxy = proxy->next)
    {
        if (strcmp(proxy->url, url) == 0)
            break;
    }

    if (proxy == NULL)
    {
        s = _createProxy(&proxy, url);
        if (s == NATS_OK)
        {
            proxy->next   = _socksProxies;
            _socksProxies = proxy;
        }
    }

    natsMutex_Unlock(_socksMutex);

    if (s != NATS_OK)
        return s;

    *handler = _socksConnect;
    *closure = (void *) proxy;
    return NATS_OK;
}
