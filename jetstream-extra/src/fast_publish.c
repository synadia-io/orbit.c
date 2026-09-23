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

#include "json.h"
#include "msg.h"
#include "os_shims.h"

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
    OP_PING       = 4,
};

struct __jsFastPublishCtx
{
    natsMutex      *mu;
    natsCondition  *cond;

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

    bool       commitPending;
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
        s = natsMsg_CopyHeaders(*out, userMsg);
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

// Moves a commit ack out of 'json' into a fresh jsPubAck compatible with
// jsPubAck_Destroy. An ack without a "stream" is NATS_ERR.
static natsStatus
_parseCommitAck(jsPubAck **newPa, natsJSON *json)
{
    natsStatus s  = NATS_OK;
    jsPubAck   *pa = (jsPubAck *)calloc(1, sizeof(*pa));

    if (pa == NULL)
        return NATS_NO_MEMORY;

    // Missing fields keep their zero value.
    natsJSON_TakeStr(json, "stream", &pa->Stream);
    natsJSON_TakeStr(json, "domain", &pa->Domain);
    natsJSON_TakeStr(json, "batch", &pa->Batch);
    natsJSON_TakeStr(json, "val", &pa->Value);
    natsJSON_GetUInt(json, "seq", &pa->Sequence);
    natsJSON_GetBool(json, "duplicate", &pa->Duplicate);
    natsJSON_GetUInt(json, "count", &pa->Count);
    if (pa->Stream == NULL)
        s = NATS_ERR;

    *newPa = pa;
    return s;
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
    natsJSON         *json    = NULL;
    natsJSON         *errObj  = NULL;
    const char       *type    = "";

    // A malformed body falls through to the commit-ack branch, which
    // rejects it for lacking a "stream".
    natsJSON_Parse(&json, data, dataLen);
    natsJSON_GetStrRef(json, "type", &type);

    natsMutex_Lock(ctx->mu);

    // Once the batch is closed the ack subscription is still live and may
    // deliver late or duplicate control messages. Ignore them: a second
    // commit ack would overwrite (and leak) ctx->commitAck, and no state
    // change is ever correct after close.
    if (ctx->closed)
    {
        natsMutex_Unlock(ctx->mu);
        natsJSON_Destroy(json);
        natsMsg_Destroy(msg);
        return;
    }

    if (strcmp(type, "gap") == 0)
    {
        uint64_t lastSeq = 0, curSeq = 0;
        natsJSON_GetUInt(json, "last_seq", &lastSeq);
        natsJSON_GetUInt(json, "seq", &curSeq);
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
    else if (strcmp(type, "ack") == 0)
    {
        uint64_t flowAckSeq = 0;
        uint64_t newFlow    = 0;
        natsJSON_GetUInt(json, "seq", &flowAckSeq);
        if (natsJSON_GetUInt(json, "msgs", &newFlow) == NATS_OK && newFlow > 0)
            ctx->flow = (uint16_t)newFlow;
        ctx->ackSequence     = flowAckSeq;
        ctx->firstAckArrived = true;
        natsCondition_Broadcast(ctx->cond);
    }
    else if (strcmp(type, "err") == 0)
    {
        uint64_t errSeq = 0;
        natsJSON_GetUInt(json, "seq", &errSeq);
        snprintf(desc, sizeof(desc),
                 "fast publish error at sequence %" PRIu64, errSeq);
        report = true;
    }
    else if (natsJSON_Lookup(json, "error", &errObj) == NATS_OK)
    {
        // No type tag but an "error" object => terminal error response. It
        // carries an error object instead of a pub-ack and ends the batch
        // just as a commit does. Report it exactly once: if a commit is
        // waiting, surface it there (the commit call returns the error);
        // otherwise hand it to the async error handler.
        uint64_t   errCode  = 0;
        const char *errDesc = NULL;

        natsJSON_GetStrRef(errObj, "description", &errDesc);

        natsJSON_GetUInt(errObj, "err_code", &errCode);

        ctx->commitErr     = NATS_ERR;
        ctx->commitArrived = true;
        ctx->closed        = true;

        if (!ctx->commitPending)
        {
            if (errDesc != NULL)
                snprintf(desc, sizeof(desc),
                         "fast publish rejected (err_code %" PRIu64 "): %s",
                         errCode, errDesc);
            else
                snprintf(desc, sizeof(desc),
                         "fast publish rejected (err_code %" PRIu64 ")", errCode);
            report = true;
        }
        natsCondition_Broadcast(ctx->cond);
    }
    else
    {
        // No type tag and no error => commit ack (end of batch). A body
        // with no "stream" is not a valid pub-ack; treat it as a failed
        // commit.
        ctx->commitErr     = _parseCommitAck(&ctx->commitAck, json);
        ctx->commitArrived = true;
        ctx->closed        = true;
        natsCondition_Broadcast(ctx->cond);
    }

    natsMutex_Unlock(ctx->mu);
    if (report)
        _reportErr(ctx, NATS_ERR, desc);
    natsJSON_Destroy(json);
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
jsFastPublishCtx_Create(jsFastPublishCtx **out, natsConnection *nc,
                        jsFastPublisherOptions *opts)
{
    if (out == NULL || nc == NULL)
        return NATS_INVALID_ARG;

    jsFastPublishCtx *ctx = (jsFastPublishCtx *)calloc(1, sizeof(*ctx));
    if (ctx == NULL)
        return NATS_NO_MEMORY;

    ctx->nc                 = nc;
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
_ensureInbox(jsFastPublishCtx *ctx)
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

    s = natsConnection_Subscribe(&sub, ctx->nc, subFilter, _onAck, ctx);
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

// Flow acks, gaps and commit acks are all best-effort and may be lost in
// transit. While stalled or awaiting a commit ack, the publisher pings the
// server (reusing the highest sequence sent so far) to prompt it to resend
// its latest ack, recovering rather than stalling out to the deadline.
// Caller holds ctx->mu.
static natsStatus
_publishPing(jsFastPublishCtx *ctx)
{
    if (ctx->batchSubject == NULL)
        return NATS_OK; // nothing published yet; nothing to ping about

    const char *reply = _buildReply(ctx, ctx->sequence, OP_PING);
    return natsConnection_PublishRequest(ctx->nc, ctx->batchSubject, reply,
                                         NULL, 0);
}

// Common publish path for the Add family.
static natsStatus
_addPublish(jsFastPubAck *ackOut, jsFastPublishCtx *ctx,
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

    s = _ensureInbox(ctx);
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

    bool published = false;

    s = _applyMsgOpts(msg, opts);
    if (s == NATS_OK && ctx->batchSubject == NULL)
    {
        ctx->batchSubject = strdup(natsMsg_GetSubject(msg));
        if (ctx->batchSubject == NULL)
            s = NATS_NO_MEMORY;
    }
    if (s == NATS_OK)
    {
        s         = natsConnection_PublishMsg(ctx->nc, msg);
        published = true;
    }

    natsMsg_Destroy(msg);

    if (s != NATS_OK)
    {
        if (published)
        {
            // The publish itself failed: the message may be partly on the
            // wire, leaving the batch in an indeterminate state. Close it.
            ctx->closed = true;
        }
        else
        {
            // Failed before anything reached the wire (option/header or a
            // bookkeeping allocation). The batch is untouched, so undo the
            // sequence bump and leave it open for retry — matching the
            // _prepareMsg failure path above.
            ctx->sequence--;
        }
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
        int64_t deadline     = nats_Now() + ctx->ackTimeoutMs;
        int64_t pingInterval = ctx->ackTimeoutMs / 3;
        if (pingInterval <= 0)
            pingInterval = ctx->ackTimeoutMs;
        int64_t nextPing = nats_Now() + pingInterval;

        // The window is recomputed each pass because the server may lower
        // ctx->flow via a flow ack mid-stall.
        while (!ctx->closed
               && ctx->ackSequence
                          + (uint64_t)ctx->flow * (uint64_t)ctx->maxOutstandingAcks
                      <= ctx->sequence)
        {
            int64_t now = nats_Now();
            if (now >= deadline)
            {
                ctx->closed = true;
                natsMutex_Unlock(ctx->mu);
                return NATS_TIMEOUT;
            }
            if (now >= nextPing)
            {
                s = _publishPing(ctx);
                if (s != NATS_OK)
                {
                    ctx->closed = true;
                    natsMutex_Unlock(ctx->mu);
                    return s;
                }
                nextPing = now + pingInterval;
            }
            int64_t wake = (nextPing < deadline ? nextPing : deadline) - now;
            if (wake <= 0)
                wake = 1;
            natsCondition_TimedWait(ctx->cond, ctx->mu, wake);
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
jsFastPublish_Add(jsFastPubAck *ack, jsFastPublishCtx *ctx,
                  const char *subject, const void *data, int dataLen,
                  jsBatchMsgOpts *opts)
{
    if (ctx == NULL || subject == NULL)
        return NATS_INVALID_ARG;
    return _addPublish(ack, ctx, subject, data, dataLen, NULL, opts);
}

natsStatus
jsFastPublish_AddMsg(jsFastPubAck *ack, jsFastPublishCtx *ctx, natsMsg *msg,
                     jsBatchMsgOpts *opts)
{
    if (ctx == NULL || msg == NULL)
        return NATS_INVALID_ARG;
    return _addPublish(ack, ctx, NULL, NULL, 0, msg, opts);
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
    if (ctx->ackSub == NULL)
    {
        // Nothing has been added: nothing to commit / close.
        natsMutex_Unlock(ctx->mu);
        return NATS_ERR;
    }
    nc = ctx->nc;

    ctx->sequence++;
    uint64_t seq = ctx->sequence;
    int      op  = eob ? OP_COMMIT_EOB : OP_COMMIT;

    s = _prepareMsg(&msg, _buildReply(ctx, seq, op),
                    eob ? ctx->batchSubject : subject,
                    data, dataLen, userMsg);
    if (s != NATS_OK)
    {
        // A commit is terminal: the documented contract is that the
        // context is closed on any return, so close here too rather than
        // leaving the batch open as the Add path does.
        ctx->closed = true;
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

    // A waiting commit takes ownership of a terminal error response so it is
    // not also dispatched to the async error handler (see _onAck).
    ctx->commitPending = true;

    int64_t deadline     = nats_Now() + timeoutMs;
    int64_t pingInterval = timeoutMs / 3;
    if (pingInterval <= 0)
        pingInterval = timeoutMs;
    int64_t nextPing = nats_Now() + pingInterval;

    while (!ctx->commitArrived && !ctx->closed)
    {
        int64_t now = nats_Now();
        if (now >= deadline)
        {
            ctx->closed = true;
            natsMutex_Unlock(ctx->mu);
            return NATS_TIMEOUT;
        }
        if (now >= nextPing)
        {
            // The commit ack may have been lost; prompt a resend.
            s = _publishPing(ctx);
            if (s != NATS_OK)
            {
                ctx->closed = true;
                natsMutex_Unlock(ctx->mu);
                return s;
            }
            nextPing = now + pingInterval;
        }
        int64_t wake = (nextPing < deadline ? nextPing : deadline) - now;
        if (wake <= 0)
            wake = 1;
        natsCondition_TimedWait(ctx->cond, ctx->mu, wake);
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
