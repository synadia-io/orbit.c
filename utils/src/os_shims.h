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

#ifdef __cplusplus
extern "C"
{
#endif

typedef void (*natsThreadCb)(void *arg);
typedef void (*natsInitOnceCb)(void);
typedef struct __natsSockCtx natsSockCtx;

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

bool       nats_InitOnce(natsInitOnceType *control, natsInitOnceCb cb);
natsStatus natsThread_Create(natsThread **thread, natsThreadCb cb, void *arg);
void       natsThread_Join(natsThread *t);
void       natsThread_Detach(natsThread *t);
bool       natsThread_IsCurrent(natsThread *t);
void       natsThread_Yield(void);
void       natsThread_Destroy(natsThread *t);
natsStatus natsThreadLocal_CreateKey(natsThreadLocal *tl, void (*destructor)(void *));
void      *natsThreadLocal_Get(natsThreadLocal tl);
natsStatus natsThreadLocal_SetEx(natsThreadLocal tl, const void *value, bool setErr);
void       natsThreadLocal_DestroyKey(natsThreadLocal tl);

void       nats_initForOS(void);
natsStatus natsSock_WaitReady(int waitMode, natsSockCtx *ctx);
natsStatus natsSock_SetBlocking(natsSock fd, bool blocking);
bool       natsSock_IsConnected(natsSock fd);
natsStatus natsSock_Flush(natsSock fd);

#if defined(_WIN32)
int   nats_asprintf(char **newStr, const char *fmt, ...);
char *nats_strcasestr(const char *haystack, const char *needle);
#if _MSC_VER < 1900
int   nats_snprintf(char *buffer, size_t countszt, char *format, ...);
#endif
#endif

#ifdef __cplusplus
}
#endif

#endif /* ORBIT_OS_SHIMS_H_ */
