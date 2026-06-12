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
natsCondition_Create(natsCondition **cond)
{
    natsCondition   *c = (natsCondition*) calloc(1, sizeof(natsCondition));
    natsStatus      s  = NATS_OK;

    if (c == NULL)
        return NATS_NO_MEMORY;

    if (pthread_cond_init(c, NULL) != 0)
        s = NATS_SYS_ERROR;

    if (s == NATS_OK)
        *cond = c;
    else
        free(c);

    return s;
}

void
natsCondition_Wait(natsCondition *cond, natsMutex *mutex)
{
    if (pthread_cond_wait(cond, mutex) != 0)
        abort();
}

static natsStatus
_timedWait(natsCondition *cond, natsMutex *mutex, bool isAbsolute, int64_t timeout)
{
    int     r;
    struct  timespec ts;
    int64_t target;

    if (timeout <= 0)
        return NATS_TIMEOUT;

    // nats_setTargetTime() is internal to cnats and not exported on all
    // platforms; nats_Now() (ms since epoch) is public, so compute the
    // absolute target time directly.
    target = (isAbsolute ? timeout : nats_Now() + timeout);

    ts.tv_sec = target / 1000;
    ts.tv_nsec = (target % 1000) * 1000000;

    if (ts.tv_nsec >= 1000000000L)
    {
        ts.tv_sec++;
        ts.tv_nsec -= 1000000000L;
    }

    r = pthread_cond_timedwait(cond, mutex, &ts);

    if (r == 0)
        return NATS_OK;

    if (r == ETIMEDOUT)
        return NATS_TIMEOUT;

    return NATS_SYS_ERROR;
}

natsStatus
natsCondition_TimedWait(natsCondition *cond, natsMutex *mutex, int64_t timeout)
{
    return _timedWait(cond, mutex, false, timeout);
}

natsStatus
natsCondition_AbsoluteTimedWait(natsCondition *cond, natsMutex *mutex, int64_t absoluteTime)
{
    return _timedWait(cond, mutex, true, absoluteTime);
}

void
natsCondition_Signal(natsCondition *cond)
{
    if (pthread_cond_signal(cond) != 0)
      abort();
}

void
natsCondition_Broadcast(natsCondition *cond)
{
    if (pthread_cond_broadcast(cond) != 0)
      abort();
}

void
natsCondition_Destroy(natsCondition *cond)
{
    if (cond == NULL)
        return;

    pthread_cond_destroy(cond);
    free(cond);
}
