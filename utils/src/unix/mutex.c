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
natsMutex_Create(natsMutex **newMutex)
{
    natsStatus          s = NATS_OK;
    pthread_mutexattr_t attr;
    natsMutex           *m = calloc(1, sizeof(natsMutex));

    if (m == NULL)
        return NATS_NO_MEMORY;

    if (pthread_mutexattr_init(&attr) != 0)
    {
        free(m);
        return NATS_SYS_ERROR;
    }

    if (pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE) != 0)
    {
        s = NATS_SYS_ERROR;
    }

    if ((s == NATS_OK)
        && (pthread_mutex_init(m, &attr) != 0))
    {
        s = NATS_SYS_ERROR;
    }

    if (s == NATS_OK)
        *newMutex = m;
    else
    {
        free(m);
        pthread_mutexattr_destroy(&attr);
    }

    return s;
}

bool
natsMutex_TryLock(natsMutex *m)
{
    if (pthread_mutex_trylock(m) == 0)
        return true;

    return false;
}

void
natsMutex_Lock(natsMutex *m)
{
    if (pthread_mutex_lock(m))
        abort();
}


void
natsMutex_Unlock(natsMutex *m)
{
    if (pthread_mutex_unlock(m))
        abort();
}

void
natsMutex_Destroy(natsMutex *m)
{
    if (m == NULL)
        return;

    pthread_mutex_destroy(m);
    free(m);
}
