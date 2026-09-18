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

#include "batch_fetch.h"

#include "buf.h"
#include "json.h"
#include "os_shims.h" // nats_gmtime + public nats time API

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>
#include <inttypes.h>
#include <time.h>

#define DEFAULT_API_PREFIX "$JS.API."
#define DIRECT_GET_INFIX   "DIRECT.GET."
#define NATS_NUM_PENDING   "Nats-Num-Pending"
#define NATS_UPTO_SEQUENCE "Nats-UpTo-Sequence"
#define HDR_STATUS         "Status"
#define HDR_DESCRIPTION    "Description"

#define INITIAL_LIST_CAP 16

natsStatus
jsBatchFetchOptions_Init(jsBatchFetchOptions *opts)
{
    if (opts == NULL)
        return NATS_INVALID_ARG;
    memset(opts, 0, sizeof(*opts));
    return NATS_OK;
}

// Formats a nanosecond Unix time as RFC 3339 with nanosecond precision, the
// form the server expects for start_time and up_to_time.
static natsStatus
_formatTimeRFC3339(char *out, size_t outLen, uint64_t nsec)
{
    time_t secs = (time_t)(nsec / 1000000000ULL);
    uint32_t nanos = (uint32_t)(nsec % 1000000000ULL);
    struct tm tm;
    int n;

    if (!nats_gmtime(&secs, &tm))
        return NATS_ERR;

    n = snprintf(out, outLen, "%04d-%02d-%02dT%02d:%02d:%02d.%09" PRIu32 "Z",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                 tm.tm_hour, tm.tm_min, tm.tm_sec, nanos);
    if (n < 0 || n >= (int)outLen)
        return NATS_ERR;
    return NATS_OK;
}

// Serializes the request with utils' natsJSONWriter, so this and every other
// request encoder in orbit.c escape and format the same way. The options have
// already passed _validate, so MultiLastFor holds no NULL entry.
static natsStatus
_buildRequest(natsBuffer *out, const jsBatchFetchOptions *bopts)
{
    natsJSONWriter w;
    natsStatus s;
    char tmp[48];
    bool haveSeq = (bopts->Sequence > 0);
    bool haveT = (bopts->StartTime > 0);
    bool haveML = (bopts->MultiLastFor != NULL && bopts->MultiLastForLen > 0);

    natsJSONWriter_Init(&w, out);
    natsJSONWriter_StartObject(&w);

    // For non-multi-last requests with neither seq nor start_time, default
    // to seq=1; otherwise the server returns no messages.
    if (haveSeq || (!haveT && !haveML))
        natsJSONWriter_AddUInt(&w, "seq", haveSeq ? bopts->Sequence : 1);

    if (bopts->NextBySubject != NULL && bopts->NextBySubject[0] != '\0')
        natsJSONWriter_AddStr(&w, "next_by_subj", bopts->NextBySubject);

    if (bopts->Batch > 0)
        natsJSONWriter_AddInt(&w, "batch", bopts->Batch);

    if (bopts->MaxBytes > 0)
        natsJSONWriter_AddInt(&w, "max_bytes", bopts->MaxBytes);

    if (haveT)
    {
        s = _formatTimeRFC3339(tmp, sizeof(tmp), bopts->StartTime);
        if (s != NATS_OK)
            return s;
        natsJSONWriter_AddStr(&w, "start_time", tmp);
    }

    if (haveML)
        natsJSONWriter_AddStrArray(&w, "multi_last",
                                   (const char *const *)bopts->MultiLastFor,
                                   bopts->MultiLastForLen);

    if (bopts->UpToSeq > 0)
        natsJSONWriter_AddUInt(&w, "up_to_seq", bopts->UpToSeq);

    if (bopts->UpToTime > 0)
    {
        s = _formatTimeRFC3339(tmp, sizeof(tmp), bopts->UpToTime);
        if (s != NATS_OK)
            return s;
        natsJSONWriter_AddStr(&w, "up_to_time", tmp);
    }

    natsJSONWriter_EndObject(&w);
    return natsJSONWriter_Status(&w);
}

// Build the request subject "<prefix>DIRECT.GET.<stream>".
//
// Uses opts->Prefix when provided, otherwise "$JS.API.". Caller frees.

static natsStatus
_buildSubject(char **out, jsOptions *opts, const char *stream)
{
    const char *prefix = (opts != NULL && opts->Prefix != NULL && opts->Prefix[0] != '\0')
                             ? opts->Prefix
                             : DEFAULT_API_PREFIX;

    size_t plen = strlen(prefix);
    size_t slen = strlen(stream);
    bool addDot = (plen == 0) || (prefix[plen - 1] != '.');
    size_t total = plen + (addDot ? 1 : 0) + strlen(DIRECT_GET_INFIX) + slen + 1;
    char *buf = (char *)malloc(total);
    char *p;

    if (buf == NULL)
        return NATS_NO_MEMORY;

    p = buf;
    memcpy(p, prefix, plen);
    p += plen;
    if (addDot)
        *p++ = '.';

    memcpy(p, DIRECT_GET_INFIX, strlen(DIRECT_GET_INFIX));
    p += strlen(DIRECT_GET_INFIX);
    memcpy(p, stream, slen);
    p += slen;
    *p = '\0';

    *out = buf;
    return NATS_OK;
}

// Validation

static natsStatus
_validate(const char *stream, const jsBatchFetchOptions *bopts)
{
    if (stream == NULL || stream[0] == '\0')
        return NATS_INVALID_ARG;
    if (bopts == NULL)
        return NATS_INVALID_ARG;
    if (bopts->Batch < 0 || bopts->Batch > JS_BATCH_FETCH_MAX_BATCH)
        return NATS_INVALID_ARG;
    if (bopts->MaxBytes < 0)
        return NATS_INVALID_ARG;
    if (bopts->Sequence > 0 && bopts->StartTime > 0)
        return NATS_INVALID_ARG;
    if (bopts->UpToSeq > 0 && bopts->UpToTime > 0)
        return NATS_INVALID_ARG;
    if (bopts->MultiLastFor != NULL)
    {
        int i;

        if (bopts->MultiLastForLen <= 0)
            return NATS_INVALID_ARG;
        if (bopts->MultiLastForLen > JS_BATCH_FETCH_MAX_SUBJECTS)
            return NATS_INVALID_ARG;
        for (i = 0; i < bopts->MultiLastForLen; i++)
        {
            if (bopts->MultiLastFor[i] == NULL)
                return NATS_INVALID_ARG;
        }
    }
    else if (bopts->MultiLastForLen != 0)
    {
        return NATS_INVALID_ARG;
    }
    return NATS_OK;
}

// Response message classification.

typedef enum
{
    KIND_DATA = 0,   ///< A real message to deliver to the caller.
    KIND_TERM_OK,    ///< Clean end-of-batch sentinel.
    KIND_ERR_404,    ///< No matching messages.
    KIND_ERR_408,    ///< Bad request.
    KIND_ERR_413,    ///< Payload-too-large / too many subjects.
    KIND_UNSUPPORTED ///< Server lacks batch DIRECT.GET (no Nats-Num-Pending).

} _msgKind;

static const char *
_hdr(natsMsg *msg, const char *key)
{
    const char *v = NULL;
    if (natsMsgHeader_Get(msg, key, &v) != NATS_OK)
        return NULL;
    return v;
}

static _msgKind
_classify(natsMsg *msg, bool firstMessageCheck)
{
    int dataLen = natsMsg_GetDataLength(msg);
    const char *status;
    const char *desc;
    const char *seq;
    const char *numPending;
    const char *upToSeq;

    if (dataLen == 0)
    {
        status = _hdr(msg, HDR_STATUS);
        desc = _hdr(msg, HDR_DESCRIPTION);

        if (status != NULL)
        {
            if (strcmp(status, "404") == 0)
                return KIND_ERR_404;
            if (strcmp(status, "408") == 0)
                return KIND_ERR_408;
            if (strcmp(status, "413") == 0)
                return KIND_ERR_413;
            if (strcmp(status, "204") == 0 && desc != NULL && strcmp(desc, "EOB") == 0)
                return KIND_TERM_OK;
        }

        seq = _hdr(msg, JSSequence);
        numPending = _hdr(msg, NATS_NUM_PENDING);
        upToSeq = _hdr(msg, NATS_UPTO_SEQUENCE);

        if (status == NULL && seq == NULL && numPending == NULL && upToSeq == NULL)
            return KIND_TERM_OK;
    }

    // Non-empty payload: if this is the first message we're inspecting and it
    // lacks Nats-Num-Pending, the server doesn't support batch DIRECT.GET.
    if (firstMessageCheck && _hdr(msg, NATS_NUM_PENDING) == NULL)
        return KIND_UNSUPPORTED;

    return KIND_DATA;
}

static natsStatus
_kindToStatus(_msgKind k, jsErrCode *errCodeOut)
{
    if (errCodeOut != NULL)
        *errCodeOut = (jsErrCode)0;

    switch (k)
    {
        case KIND_TERM_OK:
            return NATS_OK;
        case KIND_ERR_404:
            return NATS_NOT_FOUND;
        case KIND_ERR_408:
            if (errCodeOut != NULL)
                *errCodeOut = JSBadRequestErr;
            return NATS_ERR;
        case KIND_ERR_413:
            return NATS_ERR;
        case KIND_UNSUPPORTED:
            return NATS_NO_SERVER_SUPPORT;
        case KIND_DATA:
        default:
            return NATS_OK;
    }
}

// Time helpers — millisecond monotonic clock.

static int64_t
_nowMs(void)
{
    // Public, cross-platform monotonic clock (QueryPerformanceCounter on
    // Windows, clock_gettime(CLOCK_MONOTONIC) on POSIX).
    return nats_NowMonotonicInNanoSeconds() / 1000000;
}

// Common request setup — build subject + body, create inbox.
// natsInbox is `typedef char` so it doubles as the reply subject string.

typedef struct
{
    char       *subj;
    char       *inbox; // owned natsInbox; freed with natsInbox_Destroy
    natsBuffer *body;

} _setupCtx;

static void
_setupFree(_setupCtx *c)
{
    free(c->subj);
    c->subj = NULL;
    natsInbox_Destroy(c->inbox);
    c->inbox = NULL;
    natsBuf_Destroy(c->body);
    c->body = NULL;
}

static natsStatus
_setup(const char *stream, jsOptions *opts, jsBatchFetchOptions *bopts, _setupCtx *out)
{
    natsStatus s;
    size_t bodyHint;

    memset(out, 0, sizeof(*out));

    s = _validate(stream, bopts);
    if (s != NATS_OK)
        return s;

    s = _buildSubject(&out->subj, opts, stream);
    if (s != NATS_OK)
        return s;

    // Pre-size the body buffer: small for non-multi-last; for multi-last,
    // estimate ~64 bytes per subject to avoid repeated reallocs.
    bodyHint = (bopts->MultiLastFor != NULL && bopts->MultiLastForLen > 0)
                   ? (size_t)64 + (size_t)bopts->MultiLastForLen * 64
                   : 128;

    s = natsBuf_Create(&out->body, (int)bodyHint);
    if (s != NATS_OK)
        goto err;

    s = _buildRequest(out->body, bopts);
    if (s != NATS_OK)
        goto err;

    s = natsInbox_Create(&out->inbox);
    if (s != NATS_OK)
        goto err;

    return NATS_OK;

err:
    _setupFree(out);
    return s;
}

// Sync API

static natsStatus
_growList(natsMsg ***arr, int *cap, int len)
{
    int newCap;
    natsMsg **p;

    if (len < *cap)
        return NATS_OK;

    newCap = (*cap == 0) ? INITIAL_LIST_CAP : *cap * 2;
    p = (natsMsg **)realloc(*arr, (size_t)newCap * sizeof(natsMsg *));
    if (p == NULL)
        return NATS_NO_MEMORY;

    *arr = p;
    *cap = newCap;
    return NATS_OK;
}

natsStatus
jsBatchFetch_Fetch(natsMsgList *list, natsConnection *nc, const char *stream, jsOptions *opts,
                   jsBatchFetchOptions *bopts, int64_t timeout, jsErrCode *errCode)
{
    natsStatus s;
    _setupCtx stp = { 0 };
    natsSubscription *sub = NULL;
    natsMsg **msgs = NULL;
    int msgCap = 0;
    int msgLen = 0;
    int64_t deadline;

    if (errCode != NULL)
        *errCode = (jsErrCode)0;

    if (list == NULL || nc == NULL || timeout <= 0)
        return NATS_INVALID_ARG;

    list->Msgs = NULL;
    list->Count = 0;

    s = _setup(stream, opts, bopts, &stp);
    if (s != NATS_OK)
        return s;

    // If the caller hinted a batch size, pre-size to avoid reallocs.
    if (bopts->Batch > 0)
    {
        msgs = (natsMsg **)malloc((size_t)bopts->Batch * sizeof(natsMsg *));
        if (msgs == NULL)
        {
            s = NATS_NO_MEMORY;
            goto cleanup;
        }
        msgCap = bopts->Batch;
    }

    s = natsConnection_SubscribeSync(&sub, nc, stp.inbox);
    if (s != NATS_OK)
        goto cleanup;

    s = natsConnection_PublishRequest(nc, stp.subj, stp.inbox,
                                      stp.body->data, stp.body->len);
    if (s != NATS_OK)
        goto cleanup;

    deadline = _nowMs() + timeout;

    while (s == NATS_OK)
    {
        natsMsg *m = NULL;
        int64_t leftMs = deadline - _nowMs();
        _msgKind kind;

        if (leftMs <= 0)
        {
            s = NATS_TIMEOUT;
            break;
        }

        s = natsSubscription_NextMsg(&m, sub, leftMs);
        if (s != NATS_OK)
            break;

        kind = _classify(m, msgLen == 0);

        if (kind == KIND_DATA)
        {
            s = _growList(&msgs, &msgCap, msgLen);
            if (s != NATS_OK)
            {
                natsMsg_Destroy(m);
                break;
            }
            msgs[msgLen++] = m;
            continue;
        }

        natsMsg_Destroy(m);
        s = _kindToStatus(kind, errCode);
        break;
    }

cleanup:
    if (sub != NULL)
    {
        natsSubscription_Unsubscribe(sub);
        natsSubscription_Destroy(sub);
    }
    _setupFree(&stp);

    list->Msgs = msgs;
    list->Count = msgLen;
    return s;
}

// Async API
typedef struct
{
    natsSubscription            *sub;
    natsMsgHandler              userMsgCB;
    jsBatchFetchCompleteHandler doneCB;
    void                        *closure;
    natsStatus                  finalStatus;
    jsErrCode                   finalErr;
    bool                        done;
    bool                        firstChecked;

} _asyncCtx;

static void
_asyncOnMsg(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    _asyncCtx *ctx = (_asyncCtx *)closure;
    _msgKind kind;

    if (ctx->done)
    {
        // Already terminated; the dispatcher is draining — drop late arrivals.
        natsMsg_Destroy(msg);
        return;
    }

    kind = _classify(msg, !ctx->firstChecked);
    ctx->firstChecked = true;

    if (kind == KIND_DATA)
    {
        ctx->userMsgCB(nc, sub, msg, ctx->closure);
        return;
    }

    // Any non-data classification terminates the fetch. We mark `done`,
    // record the final status, and Unsubscribe so the dispatcher drains.
    // The final cleanup (user's doneCB + ctx free + sub destroy) runs in
    // the OnComplete handler once the dispatcher has fully exited.
    ctx->finalStatus = _kindToStatus(kind, &ctx->finalErr);
    ctx->done = true;

    natsMsg_Destroy(msg);
    natsSubscription_Unsubscribe(sub);
}

static void
_asyncOnComplete(void *closure)
{
    _asyncCtx *ctx = (_asyncCtx *)closure;

    // doneCB may not have been set if Unsubscribe never fired (e.g. the
    // subscription was closed by connection shutdown). In that case we
    // still want to surface a status to the user.
    if (!ctx->done)
        ctx->finalStatus = NATS_CONNECTION_CLOSED;

    ctx->doneCB(ctx->finalStatus, ctx->finalErr, ctx->closure);
    natsSubscription_Destroy(ctx->sub);
    free(ctx);
}

natsStatus
jsBatchFetch_AsyncFetch(natsConnection *nc, const char *stream, jsOptions *opts,
                        jsBatchFetchOptions *bopts, natsMsgHandler msgCB,
                        jsBatchFetchCompleteHandler doneCB, void *closure)
{
    natsStatus s;
    _setupCtx stp = { 0 };
    natsSubscription *sub = NULL;
    _asyncCtx *ctx = NULL;

    if (nc == NULL || msgCB == NULL || doneCB == NULL)
        return NATS_INVALID_ARG;

    s = _setup(stream, opts, bopts, &stp);
    if (s != NATS_OK)
        return s;

    ctx = (_asyncCtx *)calloc(1, sizeof(*ctx));
    if (ctx == NULL)
    {
        s = NATS_NO_MEMORY;
        goto err;
    }

    ctx->userMsgCB = msgCB;
    ctx->doneCB = doneCB;
    ctx->closure = closure;

    s = natsConnection_Subscribe(&sub, nc, stp.inbox, _asyncOnMsg, ctx);
    if (s != NATS_OK)
        goto err;

    ctx->sub = sub;
    s = natsSubscription_SetOnCompleteCB(sub, _asyncOnComplete, ctx);
    if (s != NATS_OK)
        goto err;

    s = natsConnection_PublishRequest(nc, stp.subj, stp.inbox,
                                      stp.body->data, stp.body->len);
    if (s != NATS_OK)
    {
        natsSubscription_Unsubscribe(sub);
    }

    _setupFree(&stp);
    return s;

err:
    if (sub != NULL)
    {
        natsSubscription_Unsubscribe(sub);
        natsSubscription_Destroy(sub);
    }
    free(ctx);
    _setupFree(&stp);
    return s;
}
