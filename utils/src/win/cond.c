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
orbitCondition_Create(orbitCondition **cond)
{
    orbitCondition   *c = (orbitCondition*) calloc(1, sizeof(orbitCondition));
    natsStatus      s  = NATS_OK;

    if (c == NULL)
        return NATS_NO_MEMORY;

    InitializeConditionVariable(c);
    *cond = c;

    return s;
}

void
orbitCondition_Wait(orbitCondition *cond, orbitMutex *mutex)
{
    if (SleepConditionVariableCS(cond, mutex, INFINITE) == 0)
        abort();
}

natsStatus
orbitCondition_TimedWait(orbitCondition *cond, orbitMutex *mutex, int64_t timeout)
{
    if (timeout <= 0)
        return NATS_TIMEOUT;

    if (SleepConditionVariableCS(cond, mutex, (DWORD) timeout) == 0)
    {
        if (GetLastError() == ERROR_TIMEOUT)
            return NATS_TIMEOUT;

        return NATS_SYS_ERROR;
    }

    return NATS_OK;
}

natsStatus
orbitCondition_AbsoluteTimedWait(orbitCondition *cond, orbitMutex *mutex, int64_t absoluteTime)
{
    int64_t now = nats_Now();;
    int64_t sleepTime = absoluteTime - now;

    if (sleepTime <= 0)
        return NATS_TIMEOUT;

    if (SleepConditionVariableCS(cond, mutex, (DWORD) sleepTime) == 0)
    {
        if (GetLastError() == ERROR_TIMEOUT)
            return NATS_TIMEOUT;

        // nats_setError() is internal to cnats and not exported from
        // nats.dll; return the plain status like the unix implementation.
        return NATS_SYS_ERROR;
    }

    return NATS_OK;
}

void
orbitCondition_Signal(orbitCondition *cond)
{
    WakeConditionVariable(cond);
}

void
orbitCondition_Broadcast(orbitCondition *cond)
{
    WakeAllConditionVariable(cond);
}

void
orbitCondition_Destroy(orbitCondition *cond)
{
    if (cond == NULL)
        return;

    free(cond);
}
