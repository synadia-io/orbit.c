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
#else
#define NATS_PATH_SEP '/'
#endif

#define nats_IsStringEmpty(s) ((((s) == NULL) || ((s)[0] == '\0')) ? true : false)

// Returns the current user's home directory as an allocated string.
natsStatus natsSys_GetHomeDir(char **homeDir);

// Reports whether 'c' is a path separator on the current platform.
bool natsSys_IsPathSep(char c);

// Reports whether 'path' is absolute, following the current platform's rules
// (mirrors Go's filepath.IsAbs).
bool natsSys_IsAbsPath(const char *path);

// Runs a program without going through a shell and captures its combined
// stdout and stderr, mirroring Go's exec.Command(...).CombinedOutput().
// 'args' is a NULL-terminated argument vector whose first entry names the
// program; it is looked up in the user's PATH when it holds no path separator.
//
// On NATS_OK *out is the NUL-terminated output, owned by the caller, and
// *exitCode is the program's exit status: a program that ran and failed is
// reported there rather than as an error status. Returns NATS_NOT_FOUND when
// the program is not on the PATH, NATS_ERR when it cannot be started or dies
// on a signal.
natsStatus natsSys_RunCommand(const char *const *args, char **out, int *exitCode);

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
