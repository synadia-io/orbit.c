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

#include "requestmany.h"

#define INITIAL_LIST_CAP 16

// Default overall deadline, in milliseconds, used when opts->timeout is 0.
#define DEFAULT_REQUEST_MANY_TIMEOUT 5000

static int64_t
_nowMs(void)
{
    return nats_NowMonotonicInNanoSeconds() / 1000000;
}

natsStatus
natsRequestManyOpts_Init(natsRequestManyOpts *opts)
{
    if (opts == NULL)
        return NATS_INVALID_ARG;
    opts->stall = 0;
    opts->count = 0;
    opts->sentinel = NULL;
    opts->sentinelClosure = NULL;
    opts->timeout = 0;

    return NATS_OK;
}

static natsStatus
_listAppend(natsMsgList *list, natsMsg *msg, int *cap)
{
    int newCap;
    natsMsg **p;

    if (list->Count == *cap)
    {
        newCap = (*cap == 0) ? INITIAL_LIST_CAP : *cap * 2;
        p = (natsMsg **)realloc(list->Msgs, (size_t)newCap * sizeof(natsMsg *));
        if (p == NULL)
            return NATS_NO_MEMORY;

        list->Msgs = p;
        *cap = newCap;
    }
    list->Msgs[list->Count++] = msg;
    return NATS_OK;
}

static natsStatus
_copyHeaders(natsMsg *dst, natsMsg *src)
{
    const char **keys     = NULL;
    int          keyCount = 0;

    natsStatus s = natsMsgHeader_Keys(src, &keys, &keyCount);
    if (s == NATS_NOT_FOUND)
        return NATS_OK; // src carries no headers
    if (s != NATS_OK)
        return s;

    for (int i = 0; s == NATS_OK && i < keyCount; i++)
    {
        const char **vals     = NULL;
        int          valCount = 0;

        s = natsMsgHeader_Values(src, keys[i], &vals, &valCount);
        for (int j = 0; s == NATS_OK && j < valCount; j++)
            s = natsMsgHeader_Add(dst, keys[i], vals[j]);
        free((void *)vals);
    }
    free((void *)keys);
    return s;
}

static natsStatus
_requestManyInternal(natsMsgList *list, natsConnection *nc, natsMsg *msg, const char *subj,
                            const char *data, int dataLen, natsRequestManyOpts *opts)
{
    natsStatus s = NATS_OK;
    natsInbox *inbox = NULL;
    natsSubscription *sub = NULL;
    natsMsg *startMsg = NULL;
    natsMsg *nextMsg = NULL;
    int cap = 0;
    uint64_t count;
    int64_t deadline;
    uint64_t timeout;

    if (list == NULL || nc == NULL || opts == NULL)
        return NATS_INVALID_ARG;

    list->Count = 0;
    list->Msgs = NULL;

    s = natsInbox_Create(&inbox);
    if (s != NATS_OK)
        return s;
    s = natsConnection_SubscribeSync(&sub, nc, inbox);
    if (s != NATS_OK)
        goto clean;

    if (msg == NULL)
        s = natsMsg_Create(&startMsg, subj, inbox, data, dataLen);
    else
    {
        s = natsMsg_Create(&startMsg, natsMsg_GetSubject(msg), inbox,
                           natsMsg_GetData(msg), natsMsg_GetDataLength(msg));
        if (s == NATS_OK)
            s = _copyHeaders(startMsg, msg);
    }

    if (s != NATS_OK)
        goto clean;
    s = natsConnection_PublishMsg(nc, startMsg);
    natsMsg_Destroy(startMsg);
    startMsg = NULL;

    count    = opts->count;
    timeout  = (opts->timeout == 0) ? DEFAULT_REQUEST_MANY_TIMEOUT : opts->timeout;
    deadline = _nowMs() + (int64_t)timeout;

    while (s == NATS_OK)
    {
        int64_t leftMs = deadline - _nowMs();
        bool    stallBound;

        if (leftMs <= 0)
        {
            s = NATS_TIMEOUT;
            break;
        }

        // The stall timer measures the gap since the last reply, so it only
        // applies once at least one reply has arrived. Until then the wait is
        // bounded solely by the overall deadline.
        stallBound = (opts->stall > 0 && list->Count > 0 && (int64_t)opts->stall < leftMs);

        s = natsSubscription_NextMsg(&nextMsg, sub, stallBound ? (int64_t)opts->stall : leftMs);
        if (s != NATS_OK)
        {
            // A stall-window timeout is a normal completion; a deadline
            // timeout means no stop condition was reached.
            if (s == NATS_TIMEOUT)
                s = stallBound ? NATS_OK : NATS_TIMEOUT;
            break;
        }

        if (opts->sentinel != NULL && opts->sentinel(nextMsg, opts->sentinelClosure))
        {
            natsMsg_Destroy(nextMsg);
            break;
        }
        s = _listAppend(list, nextMsg, &cap);
        if (s != NATS_OK)
        {
            natsMsg_Destroy(nextMsg);
            break;
        }

        if (count > 0)
        {
            count--;
            if (count == 0)
                break;
        }
    }

clean:
    natsMsg_Destroy(startMsg);
    natsSubscription_Destroy(sub);
    natsInbox_Destroy(inbox);
    return s;
}

natsStatus
natsRequestMany_Request(natsMsgList *list, natsConnection *nc, const char *subj,
                        const char *data, int dataLen, natsRequestManyOpts *opts)
{
    if (subj == NULL)
        return NATS_INVALID_ARG;

    return _requestManyInternal(list, nc, NULL, subj, data, dataLen, opts);
}

natsStatus
natsRequestMany_RequestMsg(natsMsgList *list, natsConnection *nc, natsMsg *msg,
                           natsRequestManyOpts *opts)
{
    if (msg == NULL)
        return NATS_INVALID_ARG;

    return _requestManyInternal(list, nc, msg, NULL, NULL, 0, opts);
}
