// Copyright 2015-2018 The NATS Authors
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

natsStatus
orbitMutex_Create(orbitMutex **newMutex)
{
    orbitMutex *m = calloc(1, sizeof(orbitMutex));

    if (m == NULL)
        return NATS_NO_MEMORY;

    // cnats keeps a global spin count (gLockSpinCount), but that symbol is
    // internal and not exported from nats.dll, so just use a plain critical
    // section here.
    InitializeCriticalSection(m);
    *newMutex = m;

    return NATS_OK;
}

bool
orbitMutex_TryLock(orbitMutex *m)
{
    if (TryEnterCriticalSection(m) == 0)
        return false;

    return true;
}

void
orbitMutex_Lock(orbitMutex *m)
{
    EnterCriticalSection(m);
}

void
orbitMutex_Unlock(orbitMutex *m)
{
    LeaveCriticalSection(m);
}

void
orbitMutex_Destroy(orbitMutex *m)
{
    if (m == NULL)
        return;

    DeleteCriticalSection(m);
    free(m);
}
