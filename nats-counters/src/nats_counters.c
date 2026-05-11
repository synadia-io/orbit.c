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
#include "batch_fetch.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <limits.h>

struct __natsCounter
{
    jsCtx           *js;
    natsConnection  *nc;
    char            *stream; // owned copy of the stream name

};

static void
_counter_formatDelta(char *dst, long long delta)
{
    if (delta >= 0)
        snprintf(dst, 24, "+%lld", delta);
    else
        snprintf(dst, 24, "%lld", delta);
}

// _counter_parseLL converts a decimal string produced by the server into a
// long long.  Returns NATS_ERR when the value is out of range or unparseable.
static natsStatus
_counter_parseLL(const char *str, long long *out)
{
    char *end = NULL;
    long long val;

    if (str == NULL || *str == '\0')
        return NATS_ERR;

    errno = 0;
    val = strtoll(str, &end, 10);

    if (errno == ERANGE)
        return NATS_ERR;

    if (*end != '\0')
        return NATS_ERR;

    *out = val;
    return NATS_OK;
}

natsStatus
natsCounter_GetFromStream(natsCounter **counter,
                          jsCtx *js,
                          natsConnection *nc,
                          const char *stream)
{
    natsStatus s = NATS_OK;
    jsStreamInfo *info = NULL;
    natsCounter *c = NULL;

    if (counter == NULL || js == NULL || nc == NULL || stream == NULL)
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

    c->js = js;
    c->nc = nc;
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

// Integer convenience operations

natsStatus
natsCounter_Add(natsCounter *counter,
                const char *subject,
                long long delta,
                long long *newValue)
{
    char deltaStr[24];
    char *valStr = NULL;
    natsStatus s;

    _counter_formatDelta(deltaStr, delta);
    s = natsCounter_AddStr(counter, subject, deltaStr, &valStr);
    if (s != NATS_OK)
        return s;
    s = _counter_parseLL(valStr, newValue);
    free(valStr);
    return s;
}

natsStatus
natsCounter_AddInt(natsCounter *counter,
                   const char *subject,
                   long long delta,
                   char **newValue)
{
    char deltaStr[24];

    _counter_formatDelta(deltaStr, delta);
    return natsCounter_AddStr(counter, subject, deltaStr, newValue);
}

natsStatus
natsCounter_Increment(natsCounter *counter,
                      const char *subject,
                      long long *newValue)
{
    return natsCounter_Add(counter, subject, 1, newValue);
}

natsStatus
natsCounter_Decrement(natsCounter *counter,
                      const char *subject,
                      long long *newValue)
{
    return natsCounter_Add(counter, subject, -1, newValue);
}

natsStatus
natsCounter_Load(natsCounter *counter,
                 const char *subject,
                 long long *value)
{
    char *valStr = NULL;
    natsStatus s;

    s = natsCounter_LoadStr(counter, subject, &valStr);
    if (s != NATS_OK)
        return s;

    s = _counter_parseLL(valStr, value);
    free(valStr);
    return s;
}

// Arbitrary-precision (string) operations

natsStatus
natsCounter_AddStr(natsCounter *counter,
                   const char *subject,
                   const char *delta,
                   char **newValue)
{
    natsStatus s = NATS_OK;
    natsMsg *msg = NULL;
    jsPubAck *pa = NULL;

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

    s = js_PublishMsg(&pa, counter->js, msg, NULL, NULL);
    natsMsg_Destroy(msg);
    if (s != NATS_OK)
        return s;

    s = natsCounterParser_ParsePubAckValue(pa->Value, newValue);
    jsPubAck_Destroy(pa);

    return s;
}

natsStatus
natsCounter_LoadStr(natsCounter *counter,
                    const char *subject,
                    char **value)
{
    natsCounterEntry *entry = NULL;
    natsStatus s;

    s = natsCounter_Get(counter, subject, &entry);
    if (s != NATS_OK)
        return s;

    *value = strdup(entry->value);
    natsCounterEntry_Destroy(entry);

    return (*value != NULL) ? NATS_OK : NATS_NO_MEMORY;
}

// Entry operations

// Convert a DIRECT.GET response message into a counter entry. The original
// stream subject is read from the `Nats-Subject` header when present and
// falls back to `natsMsg_GetSubject` (which cnats populates for the
// single-message js_DirectGetMsg path).
static natsStatus
_msgToEntry(natsMsg *msg, natsCounterEntry **entry)
{
    natsStatus s = NATS_OK;
    natsCounterEntry *e = NULL;
    const char *origSubj = NULL;
    const char *hdr = NULL;

    e = (natsCounterEntry *)calloc(1, sizeof(*e));
    if (e == NULL)
        return NATS_NO_MEMORY;

    if (natsMsgHeader_Get(msg, JSSubject, &origSubj) != NATS_OK || origSubj == NULL)
    {
        origSubj = natsMsg_GetSubject(msg);
    }
    if (origSubj == NULL)
    {
        natsCounterEntry_Destroy(e);
        return NATS_ERR;
    }

    e->subject = strdup(origSubj);
    if (e->subject == NULL)
    {
        natsCounterEntry_Destroy(e);
        return NATS_NO_MEMORY;
    }

    s = natsCounterParser_ParseValue(
        (const unsigned char *)natsMsg_GetData(msg),
        natsMsg_GetDataLength(msg),
        &e->value);

    if (s == NATS_OK && natsMsgHeader_Get(msg, NATS_COUNTER_INCREMENT_HDR, &hdr) == NATS_OK && hdr != NULL)
    {
        s = natsCounterParser_ParseIncrement(hdr, &e->increment);
    }

    if (s == NATS_OK)
    {
        hdr = NULL;
        if (natsMsgHeader_Get(msg, NATS_COUNTER_SOURCES_HDR, &hdr) == NATS_OK && hdr != NULL)
        {
            s = natsCounterParser_ParseSources(hdr, &e->sources);
        }
    }

    if (s != NATS_OK)
    {
        natsCounterEntry_Destroy(e);
        return s;
    }

    *entry = e;
    return NATS_OK;
}

natsStatus
natsCounter_Get(natsCounter *counter,
                const char *subject,
                natsCounterEntry **entry)
{
    natsStatus s;
    jsDirectGetMsgOptions dgo;
    natsMsg *msg = NULL;

    if (counter == NULL || subject == NULL || entry == NULL)
        return NATS_INVALID_ARG;

    jsDirectGetMsgOptions_Init(&dgo);
    dgo.LastBySubject = subject;

    s = js_DirectGetMsg(&msg, counter->js, counter->stream, NULL, &dgo);
    if (s != NATS_OK)
        return s;

    s = _msgToEntry(msg, entry);
    natsMsg_Destroy(msg);
    return s;
}

natsStatus
natsCounter_GetMultiple(natsCounterEntryList *outList,
                        natsCounter *counter,
                        const char **subjects,
                        int numSubjects,
                        int64_t timeout)
{
    natsStatus s;
    jsBatchFetchOptions bopts;
    natsMsgList list = { 0 };
    natsCounterEntryList entryList;
    int i;

    if (outList == NULL || counter == NULL)
        return NATS_INVALID_ARG;

    if (subjects == NULL || numSubjects <= 0)
    {
        outList->Entries = NULL;
        outList->Count = 0;
        return NATS_OK;
    }

    jsBatchFetchOptions_Init(&bopts);
    bopts.MultiLastFor = subjects;
    bopts.MultiLastForLen = numSubjects;

    s = jsBatchFetch_Fetch(&list, counter->nc, counter->stream, NULL, &bopts,
                           timeout, NULL);

    // The request is valid but the server has no data for any of the requested subjects
    if (s == NATS_NOT_FOUND)
    {
        outList->Entries = NULL;
        outList->Count = 0;
        return NATS_OK;
    }
    else if (s != NATS_OK)
    {
        natsMsgList_Destroy(&list);
        return s;
    }

    entryList.Entries = (natsCounterEntry **)calloc(list.Count, sizeof(natsCounterEntry *));
    if (entryList.Entries == NULL)
    {
        natsMsgList_Destroy(&list);
        return NATS_NO_MEMORY;
    }
    entryList.Count = 0;

    for (i = 0; i < list.Count; i++)
    {
        natsCounterEntry *entry = NULL;
        s = _msgToEntry(list.Msgs[i], &entry);
        if (s != NATS_OK)
        {
            natsMsgList_Destroy(&list);
            natsCounterEntryList_Destroy(&entryList);
            return s;
        }
        entryList.Entries[i] = entry;
        entryList.Count++;
    }

    natsMsgList_Destroy(&list);
    *outList = entryList;

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
natsCounterEntry_IterSources(natsCounterEntry *entry,
                             natsCounterSourceIterFn fn,
                             void *closure)
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

void
natsCounterEntryList_Destroy(natsCounterEntryList *list)
{
    if (list == NULL)
        return;

    for (int i = 0; i < list->Count; i++)
        natsCounterEntry_Destroy(list->Entries[i]);
    free(list->Entries);
}
