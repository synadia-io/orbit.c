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

//-----------------------------------------------------------------------------
// Internal struct definitions
//-----------------------------------------------------------------------------

struct __natsCounter
{
    jsCtx      *js;
    char       *stream; // owned copy of the stream name

};

struct __natsCounterEntry
{
    char               *subject;
    char               *value;     // decimal string, arbitrary precision
    char               *increment; // decimal string; NULL if header absent
    natsCounterSource  *sources;   // linked list; NULL if header absent

};

//-----------------------------------------------------------------------------
// Status translation
//-----------------------------------------------------------------------------

// TODO: Surface the underlying nats error text to the caller, e.g. via
// a per-counter last-error string or a thread-local, so that
// ORBIT_COUNTERS_NATS_ERR is debuggable without nats_PrintLastErrorStack.
static orbitCountersStatus
_translateNatsStatus(natsStatus s)
{
    switch (s)
    {
    case NATS_OK:           return ORBIT_COUNTERS_OK;
    case NATS_INVALID_ARG:  return ORBIT_COUNTERS_INVALID_ARG;
    case NATS_NO_MEMORY:    return ORBIT_COUNTERS_NO_MEMORY;
    case NATS_NOT_FOUND:    return ORBIT_COUNTERS_NOT_FOUND;
    default:                return ORBIT_COUNTERS_NATS_ERR;
    }
}

const char *
orbitCountersStatus_GetText(orbitCountersStatus s)
{
    switch (s)
    {
    case ORBIT_COUNTERS_OK:             return "ok";
    case ORBIT_COUNTERS_ERR:            return "error";
    case ORBIT_COUNTERS_INVALID_ARG:    return "invalid argument";
    case ORBIT_COUNTERS_NO_MEMORY:      return "no memory";
    case ORBIT_COUNTERS_NOT_FOUND:      return "not found";
    case ORBIT_COUNTERS_INVALID_CONFIG: return "invalid stream configuration";
    case ORBIT_COUNTERS_OVERFLOW:       return "value overflow";
    case ORBIT_COUNTERS_PARSE_ERR:      return "parse error";
    case ORBIT_COUNTERS_NATS_ERR:       return "nats error";
    default:                            return "unknown error";
    }
}

//-----------------------------------------------------------------------------
// Helpers
//-----------------------------------------------------------------------------

// _counter_formatDelta formats a long long delta as a signed decimal string
// suitable for the Nats-Incr header (e.g., "+5" or "-3").
// dst must be at least 22 bytes.
static void
_counter_formatDelta(char *dst, long long delta)
{
    if (delta >= 0)
        snprintf(dst, 24, "+%lld", delta);
    else
        snprintf(dst, 24, "%lld", delta);
}

// _counter_parseLL converts a decimal string produced by the server into a
// long long.  Returns ORBIT_COUNTERS_OVERFLOW when the value is out of range.
static orbitCountersStatus
_counter_parseLL(const char *str, long long *out)
{
    char *end = NULL;

    if (str == NULL || *str == '\0')
        return ORBIT_COUNTERS_PARSE_ERR;

    errno = 0;
    *out = strtoll(str, &end, 10);

    if (errno == ERANGE)
        return ORBIT_COUNTERS_OVERFLOW;

    if (*end != '\0')
        return ORBIT_COUNTERS_PARSE_ERR;

    return ORBIT_COUNTERS_OK;
}

//-----------------------------------------------------------------------------
// Counter lifecycle
//-----------------------------------------------------------------------------

orbitCountersStatus
natsCounter_GetFromStream(natsCounter **counter,
                           jsCtx        *js,
                           const char   *stream)
{
    natsStatus   s    = NATS_OK;
    jsStreamInfo *info = NULL;
    natsCounter  *c   = NULL;

    if (counter == NULL || js == NULL || stream == NULL)
        return ORBIT_COUNTERS_INVALID_ARG;

    s = js_GetStreamInfo(&info, js, stream, NULL, NULL);
    if (s != NATS_OK)
        return _translateNatsStatus(s);

    if (!info->Config->AllowDirect || !info->Config->AllowMsgCounter)
    {
        jsStreamInfo_Destroy(info);
        return ORBIT_COUNTERS_INVALID_CONFIG;
    }

    jsStreamInfo_Destroy(info);

    c = (natsCounter *)calloc(1, sizeof(*c));
    if (c == NULL)
        return ORBIT_COUNTERS_NO_MEMORY;

    c->js     = js;
    c->stream = strdup(stream);
    if (c->stream == NULL)
    {
        free(c);
        return ORBIT_COUNTERS_NO_MEMORY;
    }

    *counter = c;
    return ORBIT_COUNTERS_OK;
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

orbitCountersStatus
natsCounter_Add(natsCounter *counter,
                 const char  *subject,
                 long long    delta,
                 long long   *newValue)
{
    char                deltaStr[24];
    char               *valStr = NULL;
    orbitCountersStatus  s;

    _counter_formatDelta(deltaStr, delta);
    s = natsCounter_AddStr(counter, subject, deltaStr, &valStr);
    if (s != ORBIT_COUNTERS_OK)
        return s;

    s = _counter_parseLL(valStr, newValue);
    free(valStr);
    return s;
}

orbitCountersStatus
natsCounter_AddInt(natsCounter  *counter,
                    const char   *subject,
                    long long     delta,
                    char        **newValue)
{
    char deltaStr[24];

    _counter_formatDelta(deltaStr, delta);
    return natsCounter_AddStr(counter, subject, deltaStr, newValue);
}

orbitCountersStatus
natsCounter_Increment(natsCounter *counter,
                       const char  *subject,
                       long long   *newValue)
{
    return natsCounter_Add(counter, subject, 1, newValue);
}

orbitCountersStatus
natsCounter_Decrement(natsCounter *counter,
                       const char  *subject,
                       long long   *newValue)
{
    return natsCounter_Add(counter, subject, -1, newValue);
}

orbitCountersStatus
natsCounter_Load(natsCounter *counter,
                  const char  *subject,
                  long long   *value)
{
    char               *valStr = NULL;
    orbitCountersStatus  s;

    s = natsCounter_LoadStr(counter, subject, &valStr);
    if (s != ORBIT_COUNTERS_OK)
        return s;

    s = _counter_parseLL(valStr, value);
    free(valStr);
    return s;
}

//-----------------------------------------------------------------------------
// Arbitrary-precision (string) operations
//-----------------------------------------------------------------------------

orbitCountersStatus
natsCounter_AddStr(natsCounter  *counter,
                    const char   *subject,
                    const char   *delta,
                    char        **newValue)
{
    natsStatus               ns  = NATS_OK;
    natsMsg                 *msg = NULL;
    jsPubAck                *pa  = NULL;
    natsMsg                 *resp = NULL;
    jsDirectGetMsgOptions    dgo;

    if (counter == NULL || subject == NULL || delta == NULL || newValue == NULL)
        return ORBIT_COUNTERS_INVALID_ARG;

    // Build message with Nats-Incr header and empty body.
    ns = natsMsg_Create(&msg, subject, NULL, NULL, 0);
    if (ns != NATS_OK)
        return _translateNatsStatus(ns);

    ns = natsMsgHeader_Set(msg, NATS_COUNTER_INCREMENT_HDR, delta);
    if (ns != NATS_OK)
    {
        natsMsg_Destroy(msg);
        return _translateNatsStatus(ns);
    }

    // Publish via JetStream.
    ns = js_PublishMsg(&pa, counter->js, msg, NULL, NULL);
    natsMsg_Destroy(msg);
    if (ns != NATS_OK)
        return _translateNatsStatus(ns);

    // TODO: Read the counter value directly from jsPubAck once the nats.c
    // client exposes the "val" field, eliminating this extra direct-get.
    //
    jsDirectGetMsgOptions_Init(&dgo);
    dgo.Sequence = pa->Sequence;
    jsPubAck_Destroy(pa);

    ns = js_DirectGetMsg(&resp, counter->js, counter->stream, NULL, &dgo);
    if (ns != NATS_OK)
        return _translateNatsStatus(ns);

    ns = natsCounterParser_ParseValue(
            (const unsigned char *)natsMsg_GetData(resp),
            natsMsg_GetDataLength(resp),
            newValue);
    natsMsg_Destroy(resp);

    if (ns != NATS_OK)
        return ORBIT_COUNTERS_PARSE_ERR;

    return ORBIT_COUNTERS_OK;
}

orbitCountersStatus
natsCounter_LoadStr(natsCounter  *counter,
                     const char   *subject,
                     char        **value)
{
    natsCounterEntry   *entry = NULL;
    orbitCountersStatus s;

    s = natsCounter_Get(counter, subject, &entry);
    if (s != ORBIT_COUNTERS_OK)
        return s;

    *value = strdup(natsCounterEntry_ValueStr(entry));
    natsCounterEntry_Destroy(entry);

    return (*value != NULL) ? ORBIT_COUNTERS_OK : ORBIT_COUNTERS_NO_MEMORY;
}

//-----------------------------------------------------------------------------
// Entry operations
//-----------------------------------------------------------------------------

orbitCountersStatus
natsCounter_Get(natsCounter      *counter,
                 const char       *subject,
                 natsCounterEntry **entry)
{
    natsStatus               ns  = NATS_OK;
    jsDirectGetMsgOptions    dgo;
    natsMsg                 *msg = NULL;
    natsCounterEntry        *e   = NULL;
    const char              *hdr = NULL;

    if (counter == NULL || subject == NULL || entry == NULL)
        return ORBIT_COUNTERS_INVALID_ARG;

    jsDirectGetMsgOptions_Init(&dgo);
    dgo.LastBySubject = subject;

    ns = js_DirectGetMsg(&msg, counter->js, counter->stream, NULL, &dgo);
    if (ns != NATS_OK)
        return _translateNatsStatus(ns);

    e = (natsCounterEntry *)calloc(1, sizeof(*e));
    if (e == NULL)
    {
        natsMsg_Destroy(msg);
        return ORBIT_COUNTERS_NO_MEMORY;
    }

    // Subject — use the subject from the stored message.
    e->subject = strdup(natsMsg_GetSubject(msg));
    if (e->subject == NULL)
    {
        natsMsg_Destroy(msg);
        natsCounterEntry_Destroy(e);
        return ORBIT_COUNTERS_NO_MEMORY;
    }

    // Parse body value.
    ns = natsCounterParser_ParseValue(
            (const unsigned char *)natsMsg_GetData(msg),
            natsMsg_GetDataLength(msg),
            &e->value);

    // Parse Nats-Incr header (optional — present on live counter messages).
    if (ns == NATS_OK)
    {
        if (natsMsgHeader_Get(msg, NATS_COUNTER_INCREMENT_HDR, &hdr) == NATS_OK
            && hdr != NULL)
        {
            natsCounterParser_ParseIncrement(hdr, &e->increment);
        }
    }

    // Parse Nats-Counter-Sources header (optional — present on sourced streams).
    if (ns == NATS_OK)
    {
        hdr = NULL;
        if (natsMsgHeader_Get(msg, NATS_COUNTER_SOURCES_HDR, &hdr) == NATS_OK
            && hdr != NULL)
        {
            natsCounterParser_ParseSources(hdr, &e->sources);
        }
    }

    natsMsg_Destroy(msg);

    if (ns == NATS_OK)
    {
        *entry = e;
        return ORBIT_COUNTERS_OK;
    }

    natsCounterEntry_Destroy(e);
    return ORBIT_COUNTERS_PARSE_ERR;
}

orbitCountersStatus
natsCounter_GetMultiple(natsCounter       *counter,
                         const char       **subjects,
                         int                numSubjects,
                         natsCounterIterFn  handler,
                         void              *closure)
{
    if (counter == NULL || handler == NULL)
        return ORBIT_COUNTERS_INVALID_ARG;

    if (subjects == NULL || numSubjects <= 0)
    {
        // Signal completion immediately.
        handler(counter, NULL, ORBIT_COUNTERS_OK, closure);
        return ORBIT_COUNTERS_OK;
    }

    // TODO: Use a single batch direct-get request instead of looping
    // with individual natsCounter_Get calls.
    for (int i = 0; i < numSubjects; i++)
    {
        natsCounterEntry    *entry = NULL;
        orbitCountersStatus  s = natsCounter_Get(counter, subjects[i], &entry);

        if (s == ORBIT_COUNTERS_NOT_FOUND)
            continue; // non-existent subjects are silently skipped

        if (s != ORBIT_COUNTERS_OK)
        {
            handler(counter, NULL, s, closure);
            return s;
        }

        handler(counter, entry, ORBIT_COUNTERS_OK, closure);
        natsCounterEntry_Destroy(entry);
    }

    // Signal completion.
    handler(counter, NULL, ORBIT_COUNTERS_OK, closure);
    return ORBIT_COUNTERS_OK;
}

//-----------------------------------------------------------------------------
// Entry accessors
//-----------------------------------------------------------------------------

const char *
natsCounterEntry_Subject(natsCounterEntry *entry)
{
    return (entry != NULL) ? entry->subject : NULL;
}

const char *
natsCounterEntry_ValueStr(natsCounterEntry *entry)
{
    return (entry != NULL) ? entry->value : NULL;
}

orbitCountersStatus
natsCounterEntry_Value(natsCounterEntry *entry, long long *value)
{
    if (entry == NULL || value == NULL)
        return ORBIT_COUNTERS_INVALID_ARG;

    return _counter_parseLL(entry->value, value);
}

bool
natsCounterEntry_HasIncrement(natsCounterEntry *entry)
{
    return (entry != NULL) && (entry->increment != NULL);
}

const char *
natsCounterEntry_IncrementStr(natsCounterEntry *entry)
{
    return (entry != NULL) ? entry->increment : NULL;
}

orbitCountersStatus
natsCounterEntry_Increment(natsCounterEntry *entry, long long *increment)
{
    if (entry == NULL || increment == NULL)
        return ORBIT_COUNTERS_INVALID_ARG;

    if (entry->increment == NULL)
        return ORBIT_COUNTERS_NOT_FOUND;

    return _counter_parseLL(entry->increment, increment);
}

bool
natsCounterEntry_HasSources(natsCounterEntry *entry)
{
    return (entry != NULL) && (entry->sources != NULL);
}

orbitCountersStatus
natsCounterEntry_IterSources(natsCounterEntry        *entry,
                               natsCounterSourceIterFn  fn,
                               void                    *closure)
{
    natsCounterSource *src;

    if (entry == NULL || fn == NULL)
        return ORBIT_COUNTERS_INVALID_ARG;

    for (src = entry->sources; src != NULL; src = src->next)
        fn(src->stream, src->subject, src->value, closure);

    return ORBIT_COUNTERS_OK;
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
