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

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>
#include <inttypes.h>
#include <time.h>

#define DEFAULT_API_PREFIX  "$JS.API."
#define DIRECT_GET_INFIX    "DIRECT.GET."
#define NATS_NUM_PENDING    "Nats-Num-Pending"
#define NATS_UPTO_SEQUENCE  "Nats-UpTo-Sequence"
#define HDR_STATUS          "Status"
#define HDR_DESCRIPTION     "Description"

#define INITIAL_LIST_CAP    16

//-----------------------------------------------------------------------------
// Public: option struct init.
//-----------------------------------------------------------------------------

natsStatus
jsBatchFetchOptions_Init(jsBatchFetchOptions *opts)
{
    if (opts == NULL)
        return NATS_INVALID_ARG;
    memset(opts, 0, sizeof(*opts));
    return NATS_OK;
}

//-----------------------------------------------------------------------------
// Tiny growable byte buffer. natsBuffer is not in the cnats public API.
//-----------------------------------------------------------------------------

typedef struct
{
    char    *data;
    size_t   len;
    size_t   cap;
} jsxBuf;

static natsStatus
_bufInit(jsxBuf *b, size_t cap)
{
    b->data = (char *)malloc(cap);
    if (b->data == NULL)
        return NATS_NO_MEMORY;
    b->len = 0;
    b->cap = cap;
    return NATS_OK;
}

static void
_bufFree(jsxBuf *b)
{
    free(b->data);
    b->data = NULL;
    b->len = 0;
    b->cap = 0;
}

static natsStatus
_bufEnsure(jsxBuf *b, size_t extra)
{
    size_t need = b->len + extra;
    size_t newCap;
    char  *p;

    if (need <= b->cap)
        return NATS_OK;

    newCap = b->cap;
    if (newCap == 0)
        newCap = 64;
    while (newCap < need)
        newCap *= 2;

    p = (char *)realloc(b->data, newCap);
    if (p == NULL)
        return NATS_NO_MEMORY;

    b->data = p;
    b->cap  = newCap;
    return NATS_OK;
}

static natsStatus
_bufAppend(jsxBuf *b, const char *data, size_t len)
{
    natsStatus s = _bufEnsure(b, len);
    if (s != NATS_OK)
        return s;
    memcpy(b->data + b->len, data, len);
    b->len += len;
    return NATS_OK;
}

static natsStatus
_bufAppendC(jsxBuf *b, char c)
{
    natsStatus s = _bufEnsure(b, 1);
    if (s != NATS_OK)
        return s;
    b->data[b->len++] = c;
    return NATS_OK;
}

static natsStatus
_bufAppendStr(jsxBuf *b, const char *s)
{
    return _bufAppend(b, s, strlen(s));
}

//-----------------------------------------------------------------------------
// JSON helpers
//-----------------------------------------------------------------------------

static natsStatus
_jsonAppendStringQuoted(jsxBuf *b, const char *s)
{
    natsStatus  st = _bufAppendC(b, '"');
    const char *p;

    for (p = s; *p != '\0' && st == NATS_OK; p++)
    {
        unsigned char c = (unsigned char) *p;
        switch (c)
        {
            case '"':  st = _bufAppend(b, "\\\"", 2); break;
            case '\\': st = _bufAppend(b, "\\\\", 2); break;
            case '\b': st = _bufAppend(b, "\\b",  2); break;
            case '\f': st = _bufAppend(b, "\\f",  2); break;
            case '\n': st = _bufAppend(b, "\\n",  2); break;
            case '\r': st = _bufAppend(b, "\\r",  2); break;
            case '\t': st = _bufAppend(b, "\\t",  2); break;
            default:
                if (c < 0x20)
                {
                    char buf[8];
                    int  n = snprintf(buf, sizeof(buf), "\\u%04x", c);
                    st = _bufAppend(b, buf, (size_t)n);
                }
                else
                {
                    st = _bufAppendC(b, (char)c);
                }
                break;
        }
    }
    if (st == NATS_OK)
        st = _bufAppendC(b, '"');
    return st;
}

static natsStatus
_jsonAppendU64(jsxBuf *b, uint64_t v)
{
    char tmp[32];
    int  n = snprintf(tmp, sizeof(tmp), "%" PRIu64, v);
    if (n < 0 || n >= (int)sizeof(tmp))
        return NATS_ERR;
    return _bufAppend(b, tmp, (size_t)n);
}

static natsStatus
_jsonAppendInt(jsxBuf *b, int v)
{
    char tmp[16];
    int  n = snprintf(tmp, sizeof(tmp), "%d", v);
    if (n < 0 || n >= (int)sizeof(tmp))
        return NATS_ERR;
    return _bufAppend(b, tmp, (size_t)n);
}

//-----------------------------------------------------------------------------
// Build the DIRECT.GET request body (ADR-31 schema). Emits only the fields
// the caller has set.
//-----------------------------------------------------------------------------

static natsStatus
_buildRequest(jsxBuf *out, const jsBatchFetchOptions *bopts)
{
    natsStatus s        = NATS_OK;
    bool        comma   = false;
    bool        haveSeq = (bopts->Sequence > 0);
    bool        haveT   = (bopts->StartTime > 0);
    bool        haveML  = (bopts->MultiLastFor != NULL && bopts->MultiLastForLen > 0);
    bool        emitSeq;
    int         i;

#define COMMA_IF_NEEDED                                              \
    do {                                                             \
        if (s == NATS_OK && comma) s = _bufAppendC(out, ',');        \
        comma = true;                                                \
    } while (0)

    if ((s = _bufAppendC(out, '{')) != NATS_OK)
        return s;

    // For non-multi-last requests with neither seq nor start_time, default
    // to seq=1; otherwise the server returns no messages.
    emitSeq = haveSeq || (!haveT && !haveML);
    if (emitSeq)
    {
        uint64_t seq = haveSeq ? bopts->Sequence : 1;
        COMMA_IF_NEEDED;
        if (s == NATS_OK) s = _bufAppendStr(out, "\"seq\":");
        if (s == NATS_OK) s = _jsonAppendU64(out, seq);
    }

    if (s == NATS_OK && bopts->NextBySubject != NULL && bopts->NextBySubject[0] != '\0')
    {
        COMMA_IF_NEEDED;
        if (s == NATS_OK) s = _bufAppendStr(out, "\"next_by_subj\":");
        if (s == NATS_OK) s = _jsonAppendStringQuoted(out, bopts->NextBySubject);
    }

    if (s == NATS_OK && bopts->Batch > 0)
    {
        COMMA_IF_NEEDED;
        if (s == NATS_OK) s = _bufAppendStr(out, "\"batch\":");
        if (s == NATS_OK) s = _jsonAppendInt(out, bopts->Batch);
    }

    if (s == NATS_OK && bopts->MaxBytes > 0)
    {
        COMMA_IF_NEEDED;
        if (s == NATS_OK) s = _bufAppendStr(out, "\"max_bytes\":");
        if (s == NATS_OK) s = _jsonAppendInt(out, bopts->MaxBytes);
    }

    if (s == NATS_OK && haveT)
    {
        COMMA_IF_NEEDED;
        if (s == NATS_OK) s = _bufAppendStr(out, "\"start_time\":");
        if (s == NATS_OK) s = _jsonAppendU64(out, bopts->StartTime);
    }

    if (s == NATS_OK && haveML)
    {
        COMMA_IF_NEEDED;
        if (s == NATS_OK) s = _bufAppendStr(out, "\"multi_last\":[");
        for (i = 0; s == NATS_OK && i < bopts->MultiLastForLen; i++)
        {
            if (i > 0)
                s = _bufAppendC(out, ',');
            if (s == NATS_OK)
            {
                const char *subj = bopts->MultiLastFor[i];
                if (subj == NULL)
                    return NATS_INVALID_ARG;
                s = _jsonAppendStringQuoted(out, subj);
            }
        }
        if (s == NATS_OK) s = _bufAppendC(out, ']');
    }

    if (s == NATS_OK && bopts->UpToSeq > 0)
    {
        COMMA_IF_NEEDED;
        if (s == NATS_OK) s = _bufAppendStr(out, "\"up_to_seq\":");
        if (s == NATS_OK) s = _jsonAppendU64(out, bopts->UpToSeq);
    }

    if (s == NATS_OK && bopts->UpToTime > 0)
    {
        COMMA_IF_NEEDED;
        if (s == NATS_OK) s = _bufAppendStr(out, "\"up_to_time\":");
        if (s == NATS_OK) s = _jsonAppendU64(out, bopts->UpToTime);
    }

    if (s == NATS_OK) s = _bufAppendC(out, '}');

#undef COMMA_IF_NEEDED

    return s;
}

//-----------------------------------------------------------------------------
// Build the request subject "<prefix>DIRECT.GET.<stream>".
//
// Uses opts->Prefix when provided, otherwise "$JS.API.". Caller frees.
//-----------------------------------------------------------------------------

static natsStatus
_buildSubject(char **out, jsOptions *opts, const char *stream)
{
    const char *prefix = (opts != NULL && opts->Prefix != NULL && opts->Prefix[0] != '\0')
                            ? opts->Prefix
                            : DEFAULT_API_PREFIX;
    size_t      plen   = strlen(prefix);
    size_t      slen   = strlen(stream);
    bool        addDot = (plen == 0) || (prefix[plen - 1] != '.');
    size_t      total  = plen + (addDot ? 1 : 0) + strlen(DIRECT_GET_INFIX) + slen + 1;
    char       *buf    = (char *)malloc(total);
    char       *p;

    if (buf == NULL)
        return NATS_NO_MEMORY;

    p = buf;
    memcpy(p, prefix, plen);            p += plen;
    if (addDot)                         *p++ = '.';
    memcpy(p, DIRECT_GET_INFIX, strlen(DIRECT_GET_INFIX));
                                        p += strlen(DIRECT_GET_INFIX);
    memcpy(p, stream, slen);            p += slen;
    *p = '\0';

    *out = buf;
    return NATS_OK;
}

//-----------------------------------------------------------------------------
// Validation
//-----------------------------------------------------------------------------

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
        if (bopts->MultiLastForLen <= 0)
            return NATS_INVALID_ARG;
        if (bopts->MultiLastForLen > JS_BATCH_FETCH_MAX_SUBJECTS)
            return NATS_INVALID_ARG;
    }
    else if (bopts->MultiLastForLen != 0)
    {
        return NATS_INVALID_ARG;
    }
    return NATS_OK;
}

//-----------------------------------------------------------------------------
// Response message classification.
//-----------------------------------------------------------------------------

typedef enum
{
    KIND_DATA = 0,        ///< A real message to deliver to the caller.
    KIND_TERM_OK,         ///< Clean end-of-batch sentinel.
    KIND_ERR_404,         ///< No matching messages.
    KIND_ERR_408,         ///< Bad request.
    KIND_ERR_413,         ///< Payload-too-large / too many subjects.
    KIND_UNSUPPORTED      ///< Server lacks batch DIRECT.GET (no Nats-Num-Pending).
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
    int          dataLen = natsMsg_GetDataLength(msg);
    const char  *status;
    const char  *desc;
    const char  *seq;
    const char  *numPending;
    const char  *upToSeq;

    if (dataLen == 0)
    {
        status = _hdr(msg, HDR_STATUS);
        desc   = _hdr(msg, HDR_DESCRIPTION);

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

        // Empty payload with no useful headers: treat as terminator (current
        // server impl form predating the ADR-31 spec EOB marker).
        seq        = _hdr(msg, JSSequence);
        numPending = _hdr(msg, NATS_NUM_PENDING);
        upToSeq    = _hdr(msg, NATS_UPTO_SEQUENCE);

        if (status == NULL && seq == NULL && numPending == NULL && upToSeq == NULL)
            return KIND_TERM_OK;

        // Empty payload with hint headers: also a terminator.
        if (numPending != NULL || upToSeq != NULL || seq != NULL)
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
            if (errCodeOut != NULL) *errCodeOut = JSBadRequestErr;
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

//-----------------------------------------------------------------------------
// Time helpers — millisecond monotonic clock.
//-----------------------------------------------------------------------------

static int64_t
_nowMs(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((int64_t)ts.tv_sec * 1000) + (int64_t)(ts.tv_nsec / 1000000);
}

//-----------------------------------------------------------------------------
// Common request setup — build subject + body, create inbox.
// natsInbox is `typedef char` so it doubles as the reply subject string.
//-----------------------------------------------------------------------------

typedef struct
{
    char    *subj;
    char    *inbox;   // owned natsInbox; freed with natsInbox_Destroy
    jsxBuf   body;
} _setupCtx;

static void
_setupFree(_setupCtx *c)
{
    free(c->subj);              c->subj  = NULL;
    natsInbox_Destroy(c->inbox); c->inbox = NULL;
    _bufFree(&c->body);
}

static natsStatus
_setup(const char          *stream,
       jsOptions           *opts,
       jsBatchFetchOptions *bopts,
       _setupCtx           *out)
{
    natsStatus s;
    size_t     bodyHint;

    memset(out, 0, sizeof(*out));

    s = _validate(stream, bopts);
    if (s != NATS_OK) return s;

    s = _buildSubject(&out->subj, opts, stream);
    if (s != NATS_OK) return s;

    // Pre-size the body buffer: small for non-multi-last; for multi-last,
    // estimate ~64 bytes per subject to avoid repeated reallocs.
    bodyHint = (bopts->MultiLastFor != NULL && bopts->MultiLastForLen > 0)
                ? (size_t)64 + (size_t)bopts->MultiLastForLen * 64
                : 128;

    s = _bufInit(&out->body, bodyHint);
    if (s != NATS_OK) goto err;

    s = _buildRequest(&out->body, bopts);
    if (s != NATS_OK) goto err;

    s = natsInbox_Create(&out->inbox);
    if (s != NATS_OK) goto err;

    return NATS_OK;

err:
    _setupFree(out);
    return s;
}

//-----------------------------------------------------------------------------
// Sync API
//-----------------------------------------------------------------------------

static natsStatus
_growList(natsMsg ***arr, int *cap, int len)
{
    int       newCap;
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
jsBatchFetch(natsMsgList         *list,
             natsConnection      *nc,
             const char          *stream,
             jsOptions           *opts,
             jsBatchFetchOptions *bopts,
             int64_t              timeout,
             jsErrCode           *errCode)
{
    natsStatus         s;
    _setupCtx          stp    = {0};
    natsSubscription  *sub    = NULL;
    natsMsg          **msgs   = NULL;
    int                msgCap = 0;
    int                msgLen = 0;
    int64_t            deadline;

    if (errCode != NULL)
        *errCode = (jsErrCode)0;

    if (list == NULL || nc == NULL || timeout <= 0)
        return NATS_INVALID_ARG;

    list->Msgs  = NULL;
    list->Count = 0;

    s = _setup(stream, opts, bopts, &stp);
    if (s != NATS_OK) return s;

    // If the caller hinted a batch size, pre-size to avoid reallocs.
    if (bopts->Batch > 0)
    {
        msgs = (natsMsg **)malloc((size_t)bopts->Batch * sizeof(natsMsg *));
        if (msgs == NULL) { s = NATS_NO_MEMORY; goto cleanup; }
        msgCap = bopts->Batch;
    }

    s = natsConnection_SubscribeSync(&sub, nc, stp.inbox);
    if (s != NATS_OK) goto cleanup;

    s = natsConnection_PublishRequest(nc, stp.subj, stp.inbox,
                                      stp.body.data, (int)stp.body.len);
    if (s != NATS_OK) goto cleanup;

    deadline = _nowMs() + timeout;

    while (s == NATS_OK)
    {
        natsMsg  *m       = NULL;
        int64_t   leftMs  = deadline - _nowMs();
        _msgKind  kind;

        if (leftMs <= 0) { s = NATS_TIMEOUT; break; }

        s = natsSubscription_NextMsg(&m, sub, leftMs);
        if (s != NATS_OK) break;

        kind = _classify(m, msgLen == 0);

        if (kind == KIND_DATA)
        {
            s = _growList(&msgs, &msgCap, msgLen);
            if (s != NATS_OK) { natsMsg_Destroy(m); break; }
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

    list->Msgs  = msgs;
    list->Count = msgLen;
    return s;
}

//-----------------------------------------------------------------------------
// Async API
//-----------------------------------------------------------------------------

typedef struct
{
    natsSubscription           *sub;
    natsMsgHandler              userMsgCB;
    jsBatchFetchCompleteHandler doneCB;
    void                       *closure;
    natsStatus                  finalStatus;
    jsErrCode                   finalErr;
    bool                        done;
    bool                        firstChecked;
} _asyncCtx;

static void
_asyncOnMsg(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    _asyncCtx *ctx = (_asyncCtx *)closure;
    _msgKind   kind;

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
    ctx->done        = true;

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
jsBatchFetchAsync(natsConnection              *nc,
                  const char                  *stream,
                  jsOptions                   *opts,
                  jsBatchFetchOptions         *bopts,
                  natsMsgHandler               msgCB,
                  jsBatchFetchCompleteHandler  doneCB,
                  void                        *closure)
{
    natsStatus         s;
    _setupCtx          stp = {0};
    natsSubscription  *sub = NULL;
    _asyncCtx         *ctx = NULL;

    if (nc == NULL || msgCB == NULL || doneCB == NULL)
        return NATS_INVALID_ARG;

    s = _setup(stream, opts, bopts, &stp);
    if (s != NATS_OK) return s;

    ctx = (_asyncCtx *)calloc(1, sizeof(*ctx));
    if (ctx == NULL) { s = NATS_NO_MEMORY; goto err; }

    ctx->userMsgCB = msgCB;
    ctx->doneCB    = doneCB;
    ctx->closure   = closure;

    s = natsConnection_Subscribe(&sub, nc, stp.inbox, _asyncOnMsg, ctx);
    if (s != NATS_OK) goto err;

    ctx->sub = sub;
    s = natsSubscription_SetOnCompleteCB(sub, _asyncOnComplete, ctx);
    if (s != NATS_OK) goto err;

    s = natsConnection_PublishRequest(nc, stp.subj, stp.inbox,
                                      stp.body.data, (int)stp.body.len);
    if (s != NATS_OK) goto err;

    _setupFree(&stp);
    return NATS_OK;

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
