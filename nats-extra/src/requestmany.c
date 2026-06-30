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

natsStatus
natsRequestManyOpts_Init(natsRequestManyOpts *opts)
{
    if (opts == NULL)
        return NATS_INVALID_ARG;
    opts->stall = 0;
    opts->count = 0;
    opts->sentinel = NULL;

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
    natsMsgList outList;
    int count;

    if (list == NULL || nc == NULL || opts == NULL)
        return NATS_INVALID_ARG;

    list->Count = 0;
    list->Msgs = NULL;
    outList.Count = 0;
    outList.Msgs = NULL;

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
    s = natsConnection_PublishMsg(nc, msg);
    natsMsg_Destroy(msg);

    count = opts->count;
    while (s == NATS_OK)
    {
        s = natsSubscription_NextMsg(&nextMsg, sub, opts->stall == 0 ? 10000 : opts->stall);
        if (s != NATS_OK)
        {
            if (s == NATS_TIMEOUT)
            {
                s = NATS_OK;
                if (opts->stall == 0)
                    continue;
                else
                    break;
            }
            break;
        }

        s = _listAppend(&outList, nextMsg, &cap);
        if (s != NATS_OK)
        {
            natsMsg_Destroy(nextMsg);
            break;
        }

        if (opts->sentinel != NULL && opts->sentinel(nextMsg))
            break;

        if (count > 0) {
            count--;
            if (count == 0)
                break;
        }
    }
    list->Count = outList.Count;
    list->Msgs = outList.Msgs;

clean:
    natsSubscription_Destroy(sub);
    natsInbox_Destroy(inbox);
    return s;
}

natsStatus
natsRequestMany_RequestMany(natsMsgList *list, natsConnection *nc, const char *subj,
                            const char *data, int dataLen, natsRequestManyOpts *opts)
{
    return _requestManyInternal(list, nc, NULL, subj, data, dataLen, opts);
}

natsStatus
natsRequestMany_RequestManyMsg(natsMsgList *list, natsConnection *nc, natsMsg *msg,
                               natsRequestManyOpts *opts)
{
    if (msg == NULL)
        return NATS_INVALID_ARG;

    return natsRequestMany_RequestMany(list, nc, natsMsg_GetSubject(msg), natsMsg_GetData(msg),
                                       natsMsg_GetDataLength(msg), opts);
}

typedef struct _natsRequestManyCtx {
    natsMsgHandler                 userMsgCB;
    natsRequestManyCompleteHandler userDoneCB;
    void                           *userClosure;
    natsRequestManyOpts            *opts;

} natsRequestManyCtx;

void _requestManyMsgCb(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *ctx)
{
    natsRequestManyCtx *rctx = (natsRequestManyCtx *) ctx;
    if (rctx->opts->sentinel != NULL && rctx->opts->sentinel(msg))
    {
        if (rctx->userDoneCB != NULL)
            rctx->userDoneCB(NATS_OK, rctx->userClosure);
        return;
    }

    if (rctx->userMsgCB != NULL)
        rctx->userMsgCB(nc, sub, msg, rctx->userClosure);
}

void _requestManyDoneCb(natsStatus s, void *ctx)
{
    natsRequestManyCtx *rctx = (natsRequestManyCtx *) ctx;
    if (rctx->userDoneCB != NULL)
        rctx->userDoneCB(s, rctx->userClosure);
}

natsStatus
natsRequestMany_RequestManyAsync(natsConnection *nc, const char *subj, const char *data, int dataLen,
                                 natsRequestManyOpts *opts, natsMsgHandler msgCB,
                                 natsRequestManyCompleteHandler doneCB, void *closure)
{
    natsStatus s = NATS_OK;
    natsMsg *msg = NULL;

    s = natsMsg_Create(&msg, subj, NULL, data, dataLen);
    if (s != NATS_OK)
        return s;
    return natsRequestMany_RequestManyMsgAsync(nc, msg, opts, msgCB, doneCB, closure);
}

natsStatus
natsRequestMany_RequestManyMsgAsync(natsConnection *nc, natsMsg *msg,
                                    natsRequestManyOpts *opts, natsMsgHandler msgCB,
                                    natsRequestManyCompleteHandler doneCB, void *closure)
{
    if (nc == NULL || msg == NULL || opts == NULL || msgCB == NULL)
        return NATS_INVALID_ARG;

    return NATS_OK;
}
