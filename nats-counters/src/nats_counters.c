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

#include "nats_counters.h"
#include "parser.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <limits.h>

struct __natsCounter
{
    jsCtx      *js;
    char       *stream; // owned copy of the stream name

};

// _counter_parseLL converts a decimal string produced by the server into a
// long long.  Returns NATS_ERR when the value is out of range or unparseable.
static natsStatus
_counter_parseLL(const char *str, long long *out)
{
    char *end = NULL;

    if (str == NULL || *str == '\0')
        return NATS_ERR;

    errno = 0;
    *out = strtoll(str, &end, 10);

    if (errno == ERANGE)
        return NATS_ERR;

    if (*end != '\0')
        return NATS_ERR;

    return NATS_OK;
}

natsStatus
natsCounter_GetFromStream(natsCounter **counter,
                           jsCtx        *js,
                           const char   *stream)
{
    natsStatus   s    = NATS_OK;
    jsStreamInfo *info = NULL;
    natsCounter  *c   = NULL;

    if (counter == NULL || js == NULL || stream == NULL)
        return NATS_INVALID_ARG;

    s = js_GetStreamInfo(&info, js, stream, NULL, NULL);
    if (s != NATS_OK)
        return s;

    if (!info->Config->AllowDirect || !info->Config->AllowMsgCounter)
    {
        jsStreamInfo_Destroy(info);
        return NATS_INVALID_CONFIG;
    }

    jsStreamInfo_Destroy(info);

    c = (natsCounter *)calloc(1, sizeof(*c));
    if (c == NULL)
        return NATS_NO_MEMORY;

    c->js     = js;
    c->stream = strdup(stream);
    if (c->stream == NULL)
    {
        free(c);
        return NATS_NO_MEMORY;
    }

    *counter = c;
    return NATS_OK;
}

void
natsCounter_Destroy(natsCounter *counter)
{
    if (counter == NULL)
        return;

    free(counter->stream);
    free(counter);
}

//-----------------------------------------------------------------------------
// Integer convenience operations
//-----------------------------------------------------------------------------

natsStatus
natsCounter_Add(natsCounter *counter,
                 const char  *subject,
                 long long    delta,
                 long long   *newValue)
{
    char        deltaStr[24];
    char       *valStr = NULL;
    natsStatus  s;

    snprintf(deltaStr, 24, "%lld", delta);
    s = natsCounter_AddStr(counter, subject, deltaStr, &valStr);
    if (s != NATS_OK)
        return s;

    s = _counter_parseLL(valStr, newValue);
    free(valStr);
    return s;
}

natsStatus
natsCounter_AddInt(natsCounter  *counter,
                    const char   *subject,
                    long long     delta,
                    char        **newValue)
{
    char deltaStr[24];

    snprintf(deltaStr, 24, "%lld", delta);
    return natsCounter_AddStr(counter, subject, deltaStr, newValue);
}

natsStatus
natsCounter_Increment(natsCounter *counter,
                       const char  *subject,
                       long long   *newValue)
{
    return natsCounter_Add(counter, subject, 1, newValue);
}

natsStatus
natsCounter_Decrement(natsCounter *counter,
                       const char  *subject,
                       long long   *newValue)
{
    return natsCounter_Add(counter, subject, -1, newValue);
}

natsStatus
natsCounter_Load(natsCounter *counter,
                  const char  *subject,
                  long long   *value)
{
    char       *valStr = NULL;
    natsStatus  s;

    s = natsCounter_LoadStr(counter, subject, &valStr);
    if (s != NATS_OK)
        return s;

    s = _counter_parseLL(valStr, value);
    free(valStr);
    return s;
}

//-----------------------------------------------------------------------------
// Arbitrary-precision (string) operations
//-----------------------------------------------------------------------------

natsStatus
natsCounter_AddStr(natsCounter  *counter,
                    const char   *subject,
                    const char   *delta,
                    char        **newValue)
{
    natsStatus   s   = NATS_OK;
    natsMsg     *msg = NULL;
    jsPubAck    *pa  = NULL;

    if (counter == NULL || subject == NULL || delta == NULL || newValue == NULL)
        return NATS_INVALID_ARG;

    // Build message with Nats-Incr header and empty body.
    s = natsMsg_Create(&msg, subject, NULL, NULL, 0);
    if (s != NATS_OK)
        return s;

    s = natsMsgHeader_Set(msg, NATS_COUNTER_INCREMENT_HDR, delta);
    if (s != NATS_OK)
    {
        natsMsg_Destroy(msg);
        return s;
    }

    // Publish via JetStream. The PubAck includes the counter value
    // post-increment in the "val" field (ADR-49).
    s = js_PublishMsg(&pa, counter->js, msg, NULL, NULL);
    natsMsg_Destroy(msg);
    if (s != NATS_OK)
        return s;

    s = natsCounterParser_ParsePubAckValue(pa->Val, newValue);
    jsPubAck_Destroy(pa);

    return s;
}

natsStatus
natsCounter_LoadStr(natsCounter  *counter,
                     const char   *subject,
                     char        **value)
{
    natsCounterEntry   *entry = NULL;
    natsStatus          s;

    s = natsCounter_Get(counter, subject, &entry);
    if (s != NATS_OK)
        return s;

    *value = strdup(entry->value);
    natsCounterEntry_Destroy(entry);

    return (*value != NULL) ? NATS_OK : NATS_NO_MEMORY;
}

//-----------------------------------------------------------------------------
// Entry operations
//-----------------------------------------------------------------------------

natsStatus
natsCounter_Get(natsCounter      *counter,
                 const char       *subject,
                 natsCounterEntry **entry)
{
    natsStatus               s   = NATS_OK;
    jsDirectGetMsgOptions    dgo;
    natsMsg                 *msg = NULL;
    natsCounterEntry        *e   = NULL;
    const char              *hdr = NULL;

    if (counter == NULL || subject == NULL || entry == NULL)
        return NATS_INVALID_ARG;

    jsDirectGetMsgOptions_Init(&dgo);
    dgo.LastBySubject = subject;

    s = js_DirectGetMsg(&msg, counter->js, counter->stream, NULL, &dgo);
    if (s != NATS_OK)
        return s;

    e = (natsCounterEntry *)calloc(1, sizeof(*e));
    if (e == NULL)
    {
        natsMsg_Destroy(msg);
        return NATS_NO_MEMORY;
    }

    // Subject — use the subject from the stored message.
    e->subject = strdup(natsMsg_GetSubject(msg));
    if (e->subject == NULL)
    {
        natsMsg_Destroy(msg);
        natsCounterEntry_Destroy(e);
        return NATS_NO_MEMORY;
    }

    // Parse body value.
    s = natsCounterParser_ParseValue(
            (const unsigned char *)natsMsg_GetData(msg),
            natsMsg_GetDataLength(msg),
            &e->value);

    // Parse Nats-Incr header (optional — present on live counter messages).
    if (s == NATS_OK)
    {
        if (natsMsgHeader_Get(msg, NATS_COUNTER_INCREMENT_HDR, &hdr) == NATS_OK
            && hdr != NULL)
        {
            natsCounterParser_ParseIncrement(hdr, &e->increment);
        }
    }

    // Parse Nats-Counter-Sources header (optional — present on sourced streams).
    if (s == NATS_OK)
    {
        hdr = NULL;
        if (natsMsgHeader_Get(msg, NATS_COUNTER_SOURCES_HDR, &hdr) == NATS_OK
            && hdr != NULL)
        {
            natsCounterParser_ParseSources(hdr, &e->sources);
        }
    }

    natsMsg_Destroy(msg);

    if (s == NATS_OK)
    {
        *entry = e;
        return NATS_OK;
    }

    natsCounterEntry_Destroy(e);
    return NATS_ERR;
}

natsStatus
natsCounter_GetMultiple(natsCounter       *counter,
                         const char       **subjects,
                         int                numSubjects,
                         natsCounterIterFn  handler,
                         void              *closure)
{
    if (counter == NULL || handler == NULL)
        return NATS_INVALID_ARG;

    if (subjects == NULL || numSubjects <= 0)
    {
        // Signal completion immediately.
        handler(counter, NULL, NATS_OK, closure);
        return NATS_OK;
    }

    // TODO: Use a single batch direct-get request instead of looping
    // with individual natsCounter_Get calls.
    for (int i = 0; i < numSubjects; i++)
    {
        natsCounterEntry *entry = NULL;
        natsStatus        s = natsCounter_Get(counter, subjects[i], &entry);

        if (s == NATS_NOT_FOUND)
            continue; // non-existent subjects are silently skipped

        if (s != NATS_OK)
        {
            handler(counter, NULL, s, closure);
            return s;
        }

        handler(counter, entry, NATS_OK, closure);
        natsCounterEntry_Destroy(entry);
    }

    // Signal completion.
    handler(counter, NULL, NATS_OK, closure);
    return NATS_OK;
}


bool
natsCounterEntry_HasIncrement(natsCounterEntry *entry)
{
    return (entry != NULL) && (entry->increment != NULL);
}

bool
natsCounterEntry_HasSources(natsCounterEntry *entry)
{
    return (entry != NULL) && (entry->sources != NULL);
}

natsStatus
natsCounterEntry_IterSources(natsCounterEntry        *entry,
                               natsCounterSourceIterFn  fn,
                               void                    *closure)
{
    natsCounterSource *src;

    if (entry == NULL || fn == NULL)
        return NATS_INVALID_ARG;

    for (src = entry->sources; src != NULL; src = src->next)
        fn(src->stream, src->subject, src->value, closure);

    return NATS_OK;
}

void
natsCounterEntry_Destroy(natsCounterEntry *entry)
{
    if (entry == NULL)
        return;

    free(entry->subject);
    free(entry->value);
    free(entry->increment);
    natsCounterParser_FreeSources(entry->sources);
    free(entry);
}
