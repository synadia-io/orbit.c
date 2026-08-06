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

// This file mirrors nats.c's natsp.h os agnostic functions

#ifndef ORBIT_OS_SHIMS_H_
#define ORBIT_OS_SHIMS_H_

#if defined(_WIN32)
#include "include/n-win.h"
#else
#include "include/n-unix.h"
#endif

#include <nats/nats.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#ifdef __cplusplus
extern "C"
{
#endif

#define NATS_MALLOC(s)      malloc((s))
#define NATS_CALLOC(c,s)    calloc((c), (s))
#define NATS_REALLOC(p, s)  realloc((p), (s))
#ifdef _WIN32
#define NATS_STRDUP(s)      _strdup((s))
#else
#define NATS_STRDUP(s)      strdup((s))
#endif
#define NATS_FREE(p)        free((p))

#if defined(_WIN32)
#define NATS_PATH_SEP '\\'
#define NATS_SYS_SOCK_INVALID INVALID_SOCKET
typedef INIT_ONCE natsSysOnce;
#define NATS_SYS_ONCE_INIT INIT_ONCE_STATIC_INIT
#else
#define NATS_PATH_SEP '/'
#define NATS_SYS_SOCK_INVALID (-1)
typedef pthread_once_t natsSysOnce;
#define NATS_SYS_ONCE_INIT PTHREAD_ONCE_INIT
#endif

#define nats_IsStringEmpty(s) ((((s) == NULL) || ((s)[0] == '\0')) ? true : false)

// Returns the current user's home directory as an allocated string.
natsStatus natsSys_GetHomeDir(char **homeDir);

// Reports whether 'path' exists. A path that cannot be inspected for any
// reason other than not being there (a permission error, say) is reported as
// existing, so that opening it reports the real problem.
bool natsSys_PathExists(const char *path);

// Reports whether 'path' is absolute, following the current platform's rules.
bool natsSys_IsAbsPath(const char *path);

// Runs a program without going through a shell and captures its combined
// stdout and stderr. 'args' is a NULL-terminated argument vector whose first
// entry names the program; it is looked up in the user's PATH when it holds no
// path separator.
//
// On NATS_OK *out is the NUL-terminated output, owned by the caller, and
// *exitCode is the program's exit status: a program that ran and failed is
// reported there rather than as an error status. Returns NATS_NOT_FOUND when
// the program is not on the PATH, NATS_ERR when it cannot be started or dies
// on a signal.
natsStatus natsSys_RunCommand(const char *const *args, char **out, int *exitCode);

// Runs 'cb' on the first call for 'once', with any other thread reaching the
// same 'once' held until that call returns. 'once' must be a variable of static
// storage duration initialized to NATS_SYS_ONCE_INIT.
void natsSys_Once(natsSysOnce *once, void (*cb)(void));

// Client TCP sockets, for reaching a host the library has to talk to before
// nats.c takes the connection over (a SOCKS proxy, say).
//
// Winsock startup is left to nats.c: nats_Open() has already run by the time
// anything here is reachable, and calling WSAStartup() again would only add a
// WSACleanup() that no one is in a position to make.
//
// 'deadline' is an absolute time on the nats_Now() clock, so that one budget
// can span a whole exchange; a call that cannot finish before it returns
// NATS_TIMEOUT.

// Connects to 'host' on 'port' and returns the socket in *fd, left
// non-blocking and carrying the same options nats.c sets on the sockets it
// connects itself (TCP_NODELAY, SO_REUSEADDR and an abortive SO_LINGER), so
// that a socket handed over to it is indistinguishable from one of its own.
natsStatus natsSys_SockConnect(natsSock *fd, const char *host, int port, int64_t deadline);

// Sends, or receives, exactly 'len' bytes, waiting on the socket as needed.
// A peer that closes mid-exchange is reported as NATS_CONNECTION_CLOSED.
natsStatus natsSys_SockWrite(natsSock fd, const void *data, size_t len, int64_t deadline);
natsStatus natsSys_SockRead(natsSock fd, void *data, size_t len, int64_t deadline);

// Parses 'host' as a numeric IPv4 or IPv6 address, writing its network-order
// bytes to 'ip' (which must hold 16) and their count to *len. Returns
// NATS_NOT_FOUND when 'host' is not a numeric address, i.e. is a name.
natsStatus natsSys_SockParseIP(const char *host, unsigned char *ip, int *len);

// Closes 'fd'; NATS_SYS_SOCK_INVALID is a no-op.
void natsSys_SockClose(natsSock fd);

natsStatus natsMutex_Create(natsMutex **newMutex);
bool       natsMutex_TryLock(natsMutex *m);
void       natsMutex_Lock(natsMutex *m);
void       natsMutex_Unlock(natsMutex *m);
void       natsMutex_Destroy(natsMutex *m);

natsStatus natsCondition_Create(natsCondition **cond);
void       natsCondition_Wait(natsCondition *cond, natsMutex *mutex);
natsStatus natsCondition_TimedWait(natsCondition *cond, natsMutex *mutex, int64_t timeout);
natsStatus natsCondition_AbsoluteTimedWait(natsCondition *cond, natsMutex *mutex, int64_t absoluteTime);
void       natsCondition_Signal(natsCondition *cond);
void       natsCondition_Broadcast(natsCondition *cond);
void       natsCondition_Destroy(natsCondition *cond);

#ifdef __cplusplus
}
#endif

#endif /* ORBIT_OS_SHIMS_H_ */
