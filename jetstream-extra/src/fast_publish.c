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

#include "fast_publish.h"

#include "os_shims.h"

#include <ctype.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define HDR_EXPECTED_STREAM             "Nats-Expected-Stream"
#define HDR_EXPECTED_LAST_SEQ           "Nats-Expected-Last-Sequence"
#define HDR_EXPECTED_LAST_SUBJ_SEQ      "Nats-Expected-Last-Subject-Sequence"
#define HDR_EXPECTED_LAST_SUBJ_SEQ_SUBJ "Nats-Expected-Last-Subject-Sequence-Subject"
#define HDR_MSG_TTL                     "Nats-TTL"

#define REPLY_SUFFIX "$FI"

#define GAP_FAIL "fail"
#define GAP_OK   "ok"

#define DEFAULT_FLOW                 100
#define DEFAULT_MAX_OUTSTANDING_ACKS 2
#define DEFAULT_ACK_TIMEOUT_MS       5000

// Operation tags encoded into the reply subject; the server uses these
// to classify the wire op.
enum
{
    OP_START      = 0,
    OP_ADD        = 1,
    OP_COMMIT     = 2,
    OP_COMMIT_EOB = 3,
};

struct __jsFastPublishCtx
{
    natsMutex      *mu;
    natsCondition  *cond;

    jsCtx          *js;
    natsConnection *nc;

    uint16_t                flow;
    uint16_t                maxOutstandingAcks;
    int64_t                 ackTimeoutMs;
    bool                    continueOnGap;
    jsFastPublishErrHandler errHandler;
    void                    *errHandlerClosure;

    char             *ackInboxPrefix;
    char             *replyPrefix;
    char             replySubj[256];
    natsSubscription *ackSub;

    uint64_t sequence;
    uint64_t ackSequence;
    char    *batchSubject;
    bool     closed;

    bool firstAckArrived;

    bool       commitArrived;
    jsPubAck   *commitAck;
    natsStatus commitErr;
};

// ----- internal helpers ------------------------------------------------------

static void
_reportErr(jsFastPublishCtx *ctx, natsStatus s, const char *desc)
{
    if (ctx == NULL || ctx->errHandler == NULL)
        return;
    ctx->errHandler(s, desc, ctx->errHandlerClosure);
}

static bool
_jsonTypeIs(const char *data, int dataLen, const char *typeVal)
{
    if (data == NULL || dataLen <= 0)
        return false;
    char needle[64];
    int n = snprintf(needle, sizeof(needle), "\"type\":\"%s\"", typeVal);
    if (n < 0 || n >= (int)sizeof(needle) || dataLen < n)
        return false;
    for (int i = 0; i + n <= dataLen; i++)
    {
        if (memcmp(data + i, needle, (size_t)n) == 0)
            return true;
    }
    return false;
}

static const char *
_jsonFindValue(const char *data, int dataLen, const char *key)
{
    char needle[96];
    int n = snprintf(needle, sizeof(needle), "\"%s\":", key);
    if (n < 0 || n >= (int)sizeof(needle))
        return NULL;
    for (int i = 0; i + n <= dataLen; i++)
    {
        if (memcmp(data + i, needle, (size_t)n) == 0)
        {
            const char *p   = data + i + n;
            const char *end = data + dataLen;
            while (p < end && (*p == ' ' || *p == '\t'))
                p++;
            return p < end ? p : NULL;
        }
    }
    return NULL;
}

static bool
_jsonExtractUint64(const char *data, int dataLen, const char *key, uint64_t *out)
{
    const char *p = _jsonFindValue(data, dataLen, key);
    if (p == NULL)
        return false;
    const char *end = data + dataLen;
    uint64_t v = 0;
    bool any = false;
    while (p < end && isdigit((unsigned char)*p))
    {
        v = v * 10 + (uint64_t)(*p - '0');
        p++;
        any = true;
    }
    if (!any)
        return false;
    *out = v;
    return true;
}

static bool
_jsonExtractBool(const char *data, int dataLen, const char *key, bool *out)
{
    const char *p = _jsonFindValue(data, dataLen, key);
    if (p == NULL)
        return false;
    const char *end = data + dataLen;
    if (p + 4 <= end && memcmp(p, "true", 4) == 0)
    {
        *out = true;
        return true;
    }
    if (p + 5 <= end && memcmp(p, "false", 5) == 0)
    {
        *out = false;
        return true;
    }
    return false;
}

static bool
_jsonExtractString(const char *data, int dataLen, const char *key, char **out)
{
    const char *p = _jsonFindValue(data, dataLen, key);
    if (p == NULL || *p != '"')
        return false;
    p++;
    const char *end   = data + dataLen;
    const char *start = p;
    while (p < end && *p != '"')
        p++;
    if (p >= end)
        return false;
    size_t len = (size_t)(p - start);
    char  *s   = (char *)malloc(len + 1);
    if (s == NULL)
        return false;
    memcpy(s, start, len);
    s[len] = '\0';
    *out   = s;
    return true;
}

// Build the per-message reply subject "<replyPrefix><seq>.<op>.$FI" into
// the context's scratch buffer. Runs once per published message, so it
// avoids per-message allocation; the buffer is only touched under ctx->mu.
static const char *
_buildReply(jsFastPublishCtx *ctx, uint64_t seq, int op)
{
    snprintf(ctx->replySubj, sizeof(ctx->replySubj),
             "%s%" PRIu64 ".%d." REPLY_SUFFIX, ctx->replyPrefix, seq, op);
    return ctx->replySubj;
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

// Builds the wire message for one batch entry. A caller-supplied message
// is cloned into a fresh natsMsg carrying the fast-publish reply inside
// the message's single allocation, leaving the caller's message
// untouched; otherwise a fresh natsMsg is built from the raw arguments.
// Caller holds ctx->mu.
static natsStatus
_prepareMsg(natsMsg **out, const char *reply, const char *subject,
            const void *data, int dataLen, natsMsg *userMsg)
{
    if (userMsg != NULL)
    {
        subject = natsMsg_GetSubject(userMsg);
        data    = natsMsg_GetData(userMsg);
        dataLen = natsMsg_GetDataLength(userMsg);
    }

    natsStatus s = natsMsg_Create(out, subject, reply, (const char *)data, dataLen);
    if (s == NATS_OK && userMsg != NULL)
    {
        s = _copyHeaders(*out, userMsg);
        if (s != NATS_OK)
        {
            natsMsg_Destroy(*out);
            *out = NULL;
        }
    }
    return s;
}

// Apply per-message options to an existing natsMsg as headers.
static natsStatus
_applyMsgOpts(natsMsg *msg, jsBatchMsgOpts *opts)
{
    if (opts == NULL)
        return NATS_OK;

    natsStatus s = NATS_OK;

    if (opts->TTL > 0)
    {
        char ttl[32];
        snprintf(ttl, sizeof(ttl), "%" PRId64 "ns", opts->TTL);
        s = natsMsgHeader_Set(msg, HDR_MSG_TTL, ttl);
    }
    if (s == NATS_OK && opts->ExpectedStream != NULL)
        s = natsMsgHeader_Set(msg, HDR_EXPECTED_STREAM, opts->ExpectedStream);
    if (s == NATS_OK && opts->ExpectedLastSubject != NULL)
        s = natsMsgHeader_Set(msg, HDR_EXPECTED_LAST_SUBJ_SEQ_SUBJ, opts->ExpectedLastSubject);
    if (s == NATS_OK && opts->HasExpectedLastSubjSeq)
    {
        char v[32];
        snprintf(v, sizeof(v), "%" PRIu64, opts->ExpectedLastSubjSeq);
        s = natsMsgHeader_Set(msg, HDR_EXPECTED_LAST_SUBJ_SEQ, v);
    }
    if (s == NATS_OK && opts->HasExpectedLastSeq)
    {
        char v[32];
        snprintf(v, sizeof(v), "%" PRIu64, opts->ExpectedLastSeq);
        s = natsMsgHeader_Set(msg, HDR_EXPECTED_LAST_SEQ, v);
    }
    return s;
}

// Parse a commit-ack JSON body into a fresh jsPubAck compatible with
// jsPubAck_Destroy
static jsPubAck *
_parseCommitAck(const char *data, int dataLen)
{
    jsPubAck *pa = (jsPubAck *)calloc(1, sizeof(*pa));
    if (pa == NULL)
        return NULL;
    _jsonExtractString(data, dataLen, "stream", &pa->Stream);
    _jsonExtractUint64(data, dataLen, "seq", &pa->Sequence);
    _jsonExtractString(data, dataLen, "domain", &pa->Domain);
    _jsonExtractBool(data, dataLen, "duplicate", &pa->Duplicate);
    _jsonExtractString(data, dataLen, "batch", &pa->Batch);
    _jsonExtractUint64(data, dataLen, "count", &pa->Count);
    _jsonExtractString(data, dataLen, "val", &pa->Value);
    return pa;
}

static void
_onAck(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    (void)nc;
    (void)sub;

    jsFastPublishCtx *ctx     = (jsFastPublishCtx *)closure;
    const char       *data    = natsMsg_GetData(msg);
    int               dataLen = natsMsg_GetDataLength(msg);
    char              desc[192];
    bool              report  = false; // emit desc via the error handler after unlock

    natsMutex_Lock(ctx->mu);

    if (_jsonTypeIs(data, dataLen, "gap"))
    {
        uint64_t lastSeq = 0, curSeq = 0;
        _jsonExtractUint64(data, dataLen, "last_seq", &lastSeq);
        _jsonExtractUint64(data, dataLen, "seq", &curSeq);
        snprintf(desc, sizeof(desc),
                 "fast publish gap: expected_last=%" PRIu64 " current=%" PRIu64,
                 lastSeq, curSeq);
        report = true;
        if (!ctx->continueOnGap)
        {
            ctx->closed = true;
            natsCondition_Broadcast(ctx->cond);
        }
    }
    else if (_jsonTypeIs(data, dataLen, "ack"))
    {
        uint64_t flowAckSeq = 0;
        uint64_t newFlow    = 0;
        _jsonExtractUint64(data, dataLen, "seq", &flowAckSeq);
        if (_jsonExtractUint64(data, dataLen, "msgs", &newFlow) && newFlow > 0)
            ctx->flow = (uint16_t)newFlow;
        ctx->ackSequence     = flowAckSeq;
        ctx->firstAckArrived = true;
        natsCondition_Broadcast(ctx->cond);
    }
    else if (_jsonTypeIs(data, dataLen, "err"))
    {
        uint64_t errSeq = 0;
        _jsonExtractUint64(data, dataLen, "seq", &errSeq);
        snprintf(desc, sizeof(desc),
                 "fast publish error at sequence %" PRIu64, errSeq);
        report = true;
    }
    else if (_jsonFindValue(data, dataLen, "error") != NULL)
    {
        // No type tag but an "error" object => terminal error response. It
        // carries an error object instead of a pub-ack and ends the batch
        // just as a commit does.
        uint64_t errCode  = 0;
        char     *errDesc = NULL;

        _jsonExtractUint64(data, dataLen, "err_code", &errCode);
        _jsonExtractString(data, dataLen, "description", &errDesc);
        if (errDesc != NULL)
            snprintf(desc, sizeof(desc),
                     "fast publish rejected (err_code %" PRIu64 "): %s",
                     errCode, errDesc);
        else
            snprintf(desc, sizeof(desc),
                     "fast publish rejected (err_code %" PRIu64 ")", errCode);
        free(errDesc);
        report = true;

        ctx->commitErr     = NATS_ERR;
        ctx->commitArrived = true;
        ctx->closed        = true;
        natsCondition_Broadcast(ctx->cond);
    }
    else
    {
        // No type tag and no error => commit ack (end of batch). A body
        // with no "stream" is not a valid pub-ack; treat it as a failed
        // commit.
        jsPubAck *pa   = _parseCommitAck(data, dataLen);
        ctx->commitAck = pa;
        if (pa == NULL)
            ctx->commitErr = NATS_NO_MEMORY;
        else if (pa->Stream == NULL)
            ctx->commitErr = NATS_ERR;
        else
            ctx->commitErr = NATS_OK;
        ctx->commitArrived = true;
        ctx->closed        = true;
        natsCondition_Broadcast(ctx->cond);
    }

    natsMutex_Unlock(ctx->mu);
    if (report)
        _reportErr(ctx, NATS_ERR, desc);
    natsMsg_Destroy(msg);
}

// ----- options ---------------------------------------------------------------

natsStatus
jsBatchMsgOpts_Init(jsBatchMsgOpts *opts)
{
    if (opts == NULL)
        return NATS_INVALID_ARG;
    memset(opts, 0, sizeof(*opts));
    return NATS_OK;
}

natsStatus
jsFastPublisherOptions_Init(jsFastPublisherOptions *opts)
{
    if (opts == NULL)
        return NATS_INVALID_ARG;
    memset(opts, 0, sizeof(*opts));
    return NATS_OK;
}

// ----- lifecycle -------------------------------------------------------------

natsStatus
jsFastPublishCtx_Create(jsFastPublishCtx **out, jsCtx *js, jsFastPublisherOptions *opts)
{
    if (out == NULL || js == NULL)
        return NATS_INVALID_ARG;

    jsFastPublishCtx *ctx = (jsFastPublishCtx *)calloc(1, sizeof(*ctx));
    if (ctx == NULL)
        return NATS_NO_MEMORY;

    ctx->js                 = js;
    ctx->flow               = DEFAULT_FLOW;
    ctx->maxOutstandingAcks = DEFAULT_MAX_OUTSTANDING_ACKS;
    ctx->ackTimeoutMs       = DEFAULT_ACK_TIMEOUT_MS;
    if (opts != NULL)
    {
        if (opts->Flow > 0)
            ctx->flow = opts->Flow;
        if (opts->MaxOutstandingAcks > 0)
            ctx->maxOutstandingAcks = opts->MaxOutstandingAcks;
        if (opts->AckTimeout > 0)
            ctx->ackTimeoutMs = opts->AckTimeout;
        ctx->continueOnGap      = opts->ContinueOnGap;
        ctx->errHandler         = opts->ErrHandler;
        ctx->errHandlerClosure  = opts->ErrHandlerClosure;
    }

    natsStatus s = natsMutex_Create(&ctx->mu);
    if (s == NATS_OK)
        s = natsCondition_Create(&ctx->cond);
    if (s != NATS_OK)
    {
        jsFastPublish_Destroy(ctx);
        return s;
    }

    *out = ctx;
    return NATS_OK;
}

static natsStatus
_ensureInbox(jsFastPublishCtx *ctx, natsConnection *nc)
{
    natsInbox        *inbox       = NULL;
    char             *inboxPrefix = NULL;
    char             *replyPrefix = NULL;
    char             *subFilter   = NULL;
    natsSubscription *sub         = NULL;
    const char       *gap;
    size_t            need;
    size_t            subLen;
    natsStatus        s;

    if (ctx->ackSub != NULL)
        return NATS_OK;

    // Everything is built into locals and only published into ctx once the
    // subscription succeeds. A failure leaves ctx untouched, so a later
    // retry re-enters cleanly instead of leaking a half-built inbox.
    s = natsInbox_Create(&inbox);
    if (s != NATS_OK)
        return s;

    inboxPrefix = strdup((const char *)inbox);
    natsInbox_Destroy(inbox);
    if (inboxPrefix == NULL)
        return NATS_NO_MEMORY;

    gap  = ctx->continueOnGap ? GAP_OK : GAP_FAIL;
    need = strlen(inboxPrefix) + 32;
    replyPrefix = (char *)malloc(need);
    if (replyPrefix == NULL)
    {
        free(inboxPrefix);
        return NATS_NO_MEMORY;
    }
    snprintf(replyPrefix, need, "%s.%u.%s.",
             inboxPrefix, (unsigned)ctx->flow, gap);

    subLen    = strlen(inboxPrefix) + 3;
    subFilter = (char *)malloc(subLen);
    if (subFilter == NULL)
    {
        free(inboxPrefix);
        free(replyPrefix);
        return NATS_NO_MEMORY;
    }
    snprintf(subFilter, subLen, "%s.>", inboxPrefix);

    s = natsConnection_Subscribe(&sub, nc, subFilter, _onAck, ctx);
    free(subFilter);
    if (s != NATS_OK)
    {
        free(inboxPrefix);
        free(replyPrefix);
        return s;
    }

    ctx->ackInboxPrefix = inboxPrefix;
    ctx->replyPrefix    = replyPrefix;
    ctx->ackSub         = sub;
    return NATS_OK;
}

// Common publish path for the Add family.
static natsStatus
_addPublish(jsFastPubAck *ackOut, jsFastPublishCtx *ctx, natsConnection *nc,
            const char *subject, const void *data, int dataLen,
            natsMsg *userMsg, jsBatchMsgOpts *opts)
{
    natsStatus s;
    natsMsg   *msg = NULL;

    natsMutex_Lock(ctx->mu);

    if (ctx->closed)
    {
        natsMutex_Unlock(ctx->mu);
        return NATS_ERR;
    }
    if (ctx->nc != NULL && ctx->nc != nc)
    {
        natsMutex_Unlock(ctx->mu);
        return NATS_INVALID_ARG;
    }
    ctx->nc = nc;

    s = _ensureInbox(ctx, nc);
    if (s != NATS_OK)
    {
        natsMutex_Unlock(ctx->mu);
        return s;
    }

    ctx->sequence++;
    uint64_t seq = ctx->sequence;
    int      op  = (seq == 1) ? OP_START : OP_ADD;

    s = _prepareMsg(&msg, _buildReply(ctx, seq, op),
                    subject, data, dataLen, userMsg);
    if (s != NATS_OK)
    {
        ctx->sequence--;
        natsMutex_Unlock(ctx->mu);
        return s;
    }

    s = _applyMsgOpts(msg, opts);
    if (s == NATS_OK && ctx->batchSubject == NULL)
    {
        ctx->batchSubject = strdup(natsMsg_GetSubject(msg));
        if (ctx->batchSubject == NULL)
            s = NATS_NO_MEMORY;
    }
    if (s == NATS_OK)
        s = natsConnection_PublishMsg(nc, msg);

    natsMsg_Destroy(msg);

    if (s != NATS_OK)
    {
        ctx->closed = true;
        natsMutex_Unlock(ctx->mu);
        return s;
    }

    // The first message blocks until the server confirms the batch was
    // accepted; later messages only block when the ack window is full.
    if (seq == 1)
    {
        int64_t deadline = nats_Now() + ctx->ackTimeoutMs;
        while (!ctx->firstAckArrived && !ctx->closed)
        {
            int64_t remaining = deadline - nats_Now();
            if (remaining <= 0)
            {
                ctx->closed = true;
                natsMutex_Unlock(ctx->mu);
                return NATS_TIMEOUT;
            }
            natsCondition_TimedWait(ctx->cond, ctx->mu, remaining);
        }
        if (ctx->closed && !ctx->firstAckArrived)
        {
            natsMutex_Unlock(ctx->mu);
            return NATS_ERR;
        }
    }
    else
    {
        uint64_t window   = (uint64_t)ctx->flow * (uint64_t)ctx->maxOutstandingAcks;
        int64_t  deadline = nats_Now() + ctx->ackTimeoutMs;
        while (!ctx->closed && ctx->ackSequence + window <= ctx->sequence)
        {
            int64_t remaining = deadline - nats_Now();
            if (remaining <= 0)
            {
                ctx->closed = true;
                natsMutex_Unlock(ctx->mu);
                return NATS_TIMEOUT;
            }
            natsCondition_TimedWait(ctx->cond, ctx->mu, remaining);
        }
        if (ctx->closed)
        {
            natsMutex_Unlock(ctx->mu);
            return NATS_ERR;
        }
    }

    if (ackOut != NULL)
    {
        ackOut->BatchSequence = seq;
        ackOut->AckSequence   = ctx->ackSequence;
    }

    natsMutex_Unlock(ctx->mu);
    return NATS_OK;
}

natsStatus
jsFastPublish_Add(jsFastPubAck *ack, jsFastPublishCtx *ctx, natsConnection *nc,
                  const char *subject, const void *data, int dataLen,
                  jsBatchMsgOpts *opts)
{
    if (ctx == NULL || nc == NULL || subject == NULL)
        return NATS_INVALID_ARG;
    return _addPublish(ack, ctx, nc, subject, data, dataLen, NULL, opts);
}

natsStatus
jsFastPublish_AddMsg(jsFastPubAck *ack, jsFastPublishCtx *ctx, natsMsg *msg,
                     jsBatchMsgOpts *opts)
{
    if (ctx == NULL || msg == NULL)
        return NATS_INVALID_ARG;
    // first call must be jsFastPublish_Add to register the connection
    if (ctx->nc == NULL)
        return NATS_INVALID_ARG;
    return _addPublish(ack, ctx, ctx->nc, NULL, NULL, 0, msg, opts);
}

// Common commit path. eob == true uses an empty body and OP_COMMIT_EOB
// reusing the batch's first subject; otherwise a user-supplied final
// message is sent under OP_COMMIT.
static natsStatus
_commit(jsPubAck **outAck, jsFastPublishCtx *ctx, const char *subject,
        const void *data, int dataLen, natsMsg *userMsg,
        jsBatchMsgOpts *opts, int64_t timeoutMs, bool eob)
{
    natsStatus      s;
    natsMsg        *msg = NULL;
    natsConnection *nc;

    if (timeoutMs <= 0)
        return NATS_INVALID_ARG;

    natsMutex_Lock(ctx->mu);

    if (ctx->closed)
    {
        natsMutex_Unlock(ctx->mu);
        return NATS_ERR;
    }
    nc = ctx->nc;
    if (nc == NULL || ctx->ackSub == NULL)
    {
        // Nothing has been added: nothing to commit / close.
        natsMutex_Unlock(ctx->mu);
        return NATS_ERR;
    }

    ctx->sequence++;
    uint64_t seq = ctx->sequence;
    int      op  = eob ? OP_COMMIT_EOB : OP_COMMIT;

    s = _prepareMsg(&msg, _buildReply(ctx, seq, op),
                    eob ? ctx->batchSubject : subject,
                    data, dataLen, userMsg);
    if (s != NATS_OK)
    {
        ctx->sequence--;
        natsMutex_Unlock(ctx->mu);
        return s;
    }

    s = _applyMsgOpts(msg, opts);
    if (s == NATS_OK)
        s = natsConnection_PublishMsg(nc, msg);

    natsMsg_Destroy(msg);

    if (s != NATS_OK)
    {
        ctx->closed = true;
        natsMutex_Unlock(ctx->mu);
        return s;
    }

    int64_t deadline = nats_Now() + timeoutMs;
    while (!ctx->commitArrived && !ctx->closed)
    {
        int64_t remaining = deadline - nats_Now();
        if (remaining <= 0)
        {
            ctx->closed = true;
            natsMutex_Unlock(ctx->mu);
            return NATS_TIMEOUT;
        }
        natsCondition_TimedWait(ctx->cond, ctx->mu, remaining);
    }

    // The batch can be closed out from under an in-flight commit without a
    // commit ack ever arriving — e.g. a fatal gap detected on the ack
    // inbox. Fail promptly rather than blocking until the deadline.
    if (!ctx->commitArrived)
    {
        natsMutex_Unlock(ctx->mu);
        return NATS_ERR;
    }

    jsPubAck   *pa        = ctx->commitAck;
    ctx->commitAck        = NULL;
    natsStatus  commitErr = ctx->commitErr;
    ctx->closed           = true;
    natsMutex_Unlock(ctx->mu);

    if (commitErr != NATS_OK)
    {
        if (pa != NULL)
            jsPubAck_Destroy(pa);
        return commitErr;
    }

    if (outAck != NULL)
        *outAck = pa;
    else if (pa != NULL)
        jsPubAck_Destroy(pa);

    return NATS_OK;
}

natsStatus
jsFastPublish_Commit(jsPubAck **pubAck, jsFastPublishCtx *ctx,
                     const char *subject, const void *data, int dataLen,
                     jsBatchMsgOpts *opts, int64_t timeout)
{
    if (ctx == NULL || subject == NULL)
        return NATS_INVALID_ARG;
    return _commit(pubAck, ctx, subject, data, dataLen, NULL, opts, timeout, false);
}

natsStatus
jsFastPublish_CommitMsg(jsPubAck **pubAck, jsFastPublishCtx *ctx, natsMsg *msg,
                        jsBatchMsgOpts *opts, int64_t timeout)
{
    if (ctx == NULL || msg == NULL)
        return NATS_INVALID_ARG;
    return _commit(pubAck, ctx, NULL, NULL, 0, msg, opts, timeout, false);
}

natsStatus
jsFastPublish_Close(jsPubAck **pubAck, jsFastPublishCtx *ctx, int64_t timeout)
{
    if (ctx == NULL)
        return NATS_INVALID_ARG;

    natsMutex_Lock(ctx->mu);
    if (ctx->closed || ctx->sequence == 0 || ctx->batchSubject == NULL)
    {
        natsMutex_Unlock(ctx->mu);
        return NATS_ERR;
    }
    natsMutex_Unlock(ctx->mu);

    return _commit(pubAck, ctx, NULL, NULL, 0, NULL, NULL, timeout, true);
}

bool
jsFastPublish_IsClosed(jsFastPublishCtx *ctx)
{
    if (ctx == NULL)
        return true;
    natsMutex_Lock(ctx->mu);
    bool closed = ctx->closed;
    natsMutex_Unlock(ctx->mu);
    return closed;
}

void
jsFastPublish_Destroy(jsFastPublishCtx *ctx)
{
    if (ctx == NULL)
        return;

    if (ctx->ackSub != NULL)
    {
        natsSubscription_Unsubscribe(ctx->ackSub);
        natsSubscription_Destroy(ctx->ackSub);
    }
    if (ctx->commitAck != NULL)
        jsPubAck_Destroy(ctx->commitAck);
    free(ctx->ackInboxPrefix);
    free(ctx->replyPrefix);
    free(ctx->batchSubject);
    if (ctx->cond != NULL)
        natsCondition_Destroy(ctx->cond);
    if (ctx->mu != NULL)
        natsMutex_Destroy(ctx->mu);
    free(ctx);
}
