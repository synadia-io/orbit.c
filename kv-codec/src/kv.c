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

#include "kvcodecp.h"
#include "os_shims.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

natsStatus
kvCodec_New(kvCodec **newCodec, kvStore *kv, kvKeyCodec *keyCodec, kvValueCodec *valueCodec)
{
    kvCodec *c = NULL;

    if ((newCodec == NULL) || (kv == NULL))
        return NATS_INVALID_ARG;

    c = (kvCodec *)NATS_CALLOC(1, sizeof(kvCodec));
    if (c == NULL)
        return NATS_NO_MEMORY;

    c->kv         = kv;
    c->keyCodec   = keyCodec;
    c->valueCodec = valueCodec;

    *newCodec = c;
    return NATS_OK;
}

void
kvCodec_Destroy(kvCodec *c)
{
    NATS_FREE(c);
}

natsStatus
kvcodec_wrapEntry(kvCodecEntry **newEntry, kvEntry *entry,
                  const char *originalKey, kvKeyCodec *kc, kvValueCodec *vc)
{
    natsStatus    s        = NATS_OK;
    kvCodecEntry *e        = NULL;
    char         *key      = NULL;
    void         *value    = NULL;
    int           valueLen = 0;
    bool          rawValue = false;
    const void   *raw      = kvEntry_Value(entry);

    if (originalKey != NULL)
        s = kvcodec_decodeKey(&key, NULL, originalKey); // NULL codec = plain copy
    else
        s = kvcodec_decodeKey(&key, kc, kvEntry_Key(entry));

    // Delete/purge markers carry no real value (cnats exposes their empty
    // payload as a non-NULL, zero-length buffer): the value codec is never
    // invoked for them and the decoded value stays NULL. Zero-length Put
    // values also skip the codec (there is nothing to decode) and come back
    // as an empty, non-NULL value.
    if ((s == NATS_OK) && (raw != NULL) && (kvEntry_Operation(entry) == kvOp_Put))
    {
        int rawLen = kvEntry_ValueLen(entry);

        if (vc == NULL)
        {
            // No value codec: the accessors read straight from the wrapped
            // kvEntry (owned by this entry) instead of copying the payload.
            rawValue = true;
        }
        else
        {
            s = kvcodec_decodeValue(&value, &valueLen, (rawLen > 0 ? vc : NULL), raw, rawLen);
            if (s == NATS_OK)
            {
                // Guarantee the hidden trailing NUL for ValueString: custom
                // codecs only promise a buffer of exactly valueLen bytes.
                char *withNul = (char *)NATS_REALLOC(value, (size_t)valueLen + 1);

                if (withNul == NULL)
                    s = NATS_NO_MEMORY;
                else
                {
                    withNul[valueLen] = '\0';
                    value             = withNul;
                }
            }
        }
    }

    if (s == NATS_OK)
    {
        e = (kvCodecEntry *)NATS_CALLOC(1, sizeof(kvCodecEntry));
        if (e == NULL)
            s = NATS_NO_MEMORY;
    }
    if (s != NATS_OK)
    {
        NATS_FREE(key);
        NATS_FREE(value);
        return s;
    }

    e->entry    = entry;
    e->key      = key;
    e->value    = value;
    e->valueLen = valueLen;
    e->rawValue = rawValue;

    *newEntry = e;
    return NATS_OK;
}

static natsStatus
_write(uint64_t *rev, kvCodec *c, const char *key, const void *data, int len,
       uint64_t last, natsStatus (*op)(uint64_t *, kvStore *, const char *, const void *, int, uint64_t))
{
    natsStatus s          = NATS_OK;
    char      *encKey     = NULL;
    void      *encData    = NULL;
    int        encDataLen = 0;

    if ((c == NULL) || (key == NULL))
        return NATS_INVALID_ARG;

    if ((len < 0) || ((data == NULL) && (len > 0)))
        return NATS_INVALID_ARG;

    s = kvcodec_encodeKey(&encKey, c->keyCodec, key);
    if ((s == NATS_OK) && (c->valueCodec != NULL))
        s = kvcodec_encodeValue(&encData, &encDataLen, c->valueCodec, data, len);
    if (s == NATS_OK)
    {
        // No value codec: hand the caller's buffer straight to the store
        // (which copies it) instead of paying for an intermediate copy.
        if (c->valueCodec == NULL)
            s = op(rev, c->kv, encKey, data, len, last);
        else
            s = op(rev, c->kv, encKey, encData, encDataLen, last);
    }

    NATS_FREE(encKey);
    NATS_FREE(encData);
    return s;
}

static natsStatus
_putOp(uint64_t *rev, kvStore *kv, const char *key, const void *data, int len, uint64_t last)
{
    (void)last;
    return kvStore_Put(rev, kv, key, data, len);
}

static natsStatus
_createOp(uint64_t *rev, kvStore *kv, const char *key, const void *data, int len, uint64_t last)
{
    (void)last;
    return kvStore_Create(rev, kv, key, data, len);
}

static natsStatus
_updateOp(uint64_t *rev, kvStore *kv, const char *key, const void *data, int len, uint64_t last)
{
    return kvStore_Update(rev, kv, key, data, len, last);
}

natsStatus
kvCodec_Put(uint64_t *rev, kvCodec *c, const char *key, const void *data, int len)
{
    return _write(rev, c, key, data, len, 0, _putOp);
}

natsStatus
kvCodec_PutString(uint64_t *rev, kvCodec *c, const char *key, const char *data)
{
    if (data == NULL)
        return NATS_INVALID_ARG;
    return _write(rev, c, key, data, (int)strlen(data), 0, _putOp);
}

natsStatus
kvCodec_Create(uint64_t *rev, kvCodec *c, const char *key, const void *data, int len)
{
    return _write(rev, c, key, data, len, 0, _createOp);
}

natsStatus
kvCodec_CreateString(uint64_t *rev, kvCodec *c, const char *key, const char *data)
{
    if (data == NULL)
        return NATS_INVALID_ARG;
    return _write(rev, c, key, data, (int)strlen(data), 0, _createOp);
}

natsStatus
kvCodec_Update(uint64_t *rev, kvCodec *c, const char *key, const void *data, int len, uint64_t last)
{
    return _write(rev, c, key, data, len, last, _updateOp);
}

natsStatus
kvCodec_UpdateString(uint64_t *rev, kvCodec *c, const char *key, const char *data, uint64_t last)
{
    if (data == NULL)
        return NATS_INVALID_ARG;
    return _write(rev, c, key, data, (int)strlen(data), last, _updateOp);
}

static natsStatus
_get(kvCodecEntry **newEntry, kvCodec *c, const char *key, uint64_t revision, bool byRevision)
{
    natsStatus s      = NATS_OK;
    char      *encKey = NULL;
    kvEntry   *entry  = NULL;

    if ((newEntry == NULL) || (c == NULL) || (key == NULL))
        return NATS_INVALID_ARG;

    s = kvcodec_encodeKey(&encKey, c->keyCodec, key);
    if (s == NATS_OK)
    {
        if (byRevision)
            s = kvStore_GetRevision(&entry, c->kv, encKey, revision);
        else
            s = kvStore_Get(&entry, c->kv, encKey);
    }
    if (s == NATS_OK)
    {
        s = kvcodec_wrapEntry(newEntry, entry, key, c->keyCodec, c->valueCodec);
        if (s != NATS_OK)
            kvEntry_Destroy(entry);
    }

    NATS_FREE(encKey);
    return s;
}

natsStatus
kvCodec_Get(kvCodecEntry **newEntry, kvCodec *c, const char *key)
{
    return _get(newEntry, c, key, 0, false);
}

natsStatus
kvCodec_GetRevision(kvCodecEntry **newEntry, kvCodec *c, const char *key, uint64_t revision)
{
    return _get(newEntry, c, key, revision, true);
}

natsStatus
kvCodec_Delete(kvCodec *c, const char *key)
{
    natsStatus s      = NATS_OK;
    char      *encKey = NULL;

    if ((c == NULL) || (key == NULL))
        return NATS_INVALID_ARG;

    s = kvcodec_encodeKey(&encKey, c->keyCodec, key);
    if (s == NATS_OK)
        s = kvStore_Delete(c->kv, encKey);

    NATS_FREE(encKey);
    return s;
}

natsStatus
kvCodec_Purge(kvCodec *c, const char *key, const kvPurgeOptions *opts)
{
    natsStatus s      = NATS_OK;
    char      *encKey = NULL;

    if ((c == NULL) || (key == NULL))
        return NATS_INVALID_ARG;

    s = kvcodec_encodeKey(&encKey, c->keyCodec, key);
    if (s == NATS_OK)
        s = kvStore_Purge(c->kv, encKey, opts);

    NATS_FREE(encKey);
    return s;
}

// Encodes each watch/list pattern with the codec's filter rules; the result
// is freed with _freeFilters.
static natsStatus
_encodeFilters(char ***newFilters, kvCodec *c, const char **filters, int numFilters)
{
    natsStatus s   = NATS_OK;
    char     **enc = NULL;
    int        i;

    enc = (char **)NATS_CALLOC((size_t)numFilters, sizeof(char *));
    if (enc == NULL)
        return NATS_NO_MEMORY;

    for (i = 0; (s == NATS_OK) && (i < numFilters); i++)
    {
        if (filters[i] == NULL)
            s = NATS_INVALID_ARG;
        else
            s = kvcodec_encodeFilter(&(enc[i]), c->keyCodec, filters[i]);
    }
    if (s != NATS_OK)
    {
        for (i = 0; i < numFilters; i++)
            NATS_FREE(enc[i]);
        NATS_FREE(enc);
        return s;
    }

    *newFilters = enc;
    return NATS_OK;
}

static void
_freeFilters(char **filters, int numFilters)
{
    int i;

    if (filters == NULL)
        return;
    for (i = 0; i < numFilters; i++)
        NATS_FREE(filters[i]);
    NATS_FREE(filters);
}

static natsStatus
_wrapWatcher(kvCodecWatcher **newWatcher, kvCodec *c, kvWatcher *w)
{
    kvCodecWatcher *cw = (kvCodecWatcher *)NATS_CALLOC(1, sizeof(kvCodecWatcher));

    if (cw == NULL)
    {
        kvWatcher_Destroy(w);
        return NATS_NO_MEMORY;
    }
    cw->w          = w;
    cw->keyCodec   = c->keyCodec;
    cw->valueCodec = c->valueCodec;

    *newWatcher = cw;
    return NATS_OK;
}

natsStatus
kvCodec_Watch(kvCodecWatcher **newWatcher, kvCodec *c, const char *keys,
              const kvWatchOptions *opts)
{
    return kvCodec_WatchMulti(newWatcher, c, &keys, 1, opts);
}

natsStatus
kvCodec_WatchMulti(kvCodecWatcher **newWatcher, kvCodec *c,
                   const char **keys, int numKeys, const kvWatchOptions *opts)
{
    natsStatus s       = NATS_OK;
    char     **encKeys = NULL;
    kvWatcher *w       = NULL;

    if ((newWatcher == NULL) || (c == NULL) || (keys == NULL) || (numKeys < 1))
        return NATS_INVALID_ARG;

    s = _encodeFilters(&encKeys, c, keys, numKeys);
    if (s == NATS_OK)
    {
        s = kvStore_WatchMulti(&w, c->kv, (const char **)encKeys, numKeys, opts);
        _freeFilters(encKeys, numKeys);
    }
    if (s == NATS_OK)
        s = _wrapWatcher(newWatcher, c, w);
    return s;
}

natsStatus
kvCodec_WatchAll(kvCodecWatcher **newWatcher, kvCodec *c, const kvWatchOptions *opts)
{
    natsStatus s = NATS_OK;
    kvWatcher *w = NULL;

    if ((newWatcher == NULL) || (c == NULL))
        return NATS_INVALID_ARG;

    s = kvStore_WatchAll(&w, c->kv, opts);
    if (s == NATS_OK)
        s = _wrapWatcher(newWatcher, c, w);
    return s;
}

static natsStatus
_decodeKeysList(kvCodecKeysList *list, kvCodec *c, kvKeysList *raw)
{
    natsStatus s   = NATS_OK;
    char     **dec = NULL;
    int        i;

    if (raw->Count == 0)
        return NATS_OK;

    dec = (char **)NATS_CALLOC((size_t)raw->Count, sizeof(char *));
    if (dec == NULL)
        return NATS_NO_MEMORY;

    for (i = 0; (s == NATS_OK) && (i < raw->Count); i++)
        s = kvcodec_decodeKey(&(dec[i]), c->keyCodec, raw->Keys[i]);
    if (s != NATS_OK)
    {
        for (i = 0; i < raw->Count; i++)
            NATS_FREE(dec[i]);
        NATS_FREE(dec);
        return s;
    }

    list->Keys  = dec;
    list->Count = raw->Count;
    return NATS_OK;
}

natsStatus
kvCodec_Keys(kvCodecKeysList *list, kvCodec *c, const kvWatchOptions *opts)
{
    natsStatus s   = NATS_OK;
    kvKeysList raw = { NULL, 0 };

    if (list == NULL)
        return NATS_INVALID_ARG;
    // Initialized before any other check so the caller's (typically stack)
    // object is always safe to pass to kvCodecKeysList_Destroy.
    list->Keys  = NULL;
    list->Count = 0;
    if (c == NULL)
        return NATS_INVALID_ARG;

    s = kvStore_Keys(&raw, c->kv, opts);
    if (s == NATS_OK)
    {
        s = _decodeKeysList(list, c, &raw);
        kvKeysList_Destroy(&raw);
    }
    return s;
}

natsStatus
kvCodec_KeysWithFilters(kvCodecKeysList *list, kvCodec *c,
                        const char **filters, int numFilters,
                        const kvWatchOptions *opts)
{
    natsStatus s          = NATS_OK;
    char     **encFilters = NULL;
    kvKeysList raw        = { NULL, 0 };

    if (list == NULL)
        return NATS_INVALID_ARG;
    list->Keys  = NULL;
    list->Count = 0;
    if ((c == NULL) || (filters == NULL) || (numFilters < 1))
        return NATS_INVALID_ARG;

    s = _encodeFilters(&encFilters, c, filters, numFilters);
    if (s == NATS_OK)
    {
        s = kvStore_KeysWithFilters(&raw, c->kv, (const char **)encFilters,
                                    numFilters, opts);
        _freeFilters(encFilters, numFilters);
    }
    if (s == NATS_OK)
    {
        s = _decodeKeysList(list, c, &raw);
        kvKeysList_Destroy(&raw);
    }
    return s;
}

void
kvCodecKeysList_Destroy(kvCodecKeysList *list)
{
    int i;

    if ((list == NULL) || (list->Keys == NULL))
        return;
    for (i = 0; i < list->Count; i++)
        NATS_FREE(list->Keys[i]);
    NATS_FREE(list->Keys);
    list->Keys  = NULL;
    list->Count = 0;
}

natsStatus
kvCodec_History(kvCodecEntryList *list, kvCodec *c, const char *key,
                const kvWatchOptions *opts)
{
    natsStatus     s      = NATS_OK;
    char          *encKey = NULL;
    kvEntryList    raw    = { NULL, 0 };
    kvCodecEntry **dec    = NULL;
    int            i;

    if (list == NULL)
        return NATS_INVALID_ARG;
    list->Entries = NULL;
    list->Count   = 0;
    if ((c == NULL) || (key == NULL))
        return NATS_INVALID_ARG;

    s = kvcodec_encodeKey(&encKey, c->keyCodec, key);
    if (s == NATS_OK)
        s = kvStore_History(&raw, c->kv, encKey, opts);
    NATS_FREE(encKey);
    if (s != NATS_OK)
        return s;

    if (raw.Count > 0)
    {
        dec = (kvCodecEntry **)NATS_CALLOC((size_t)raw.Count, sizeof(kvCodecEntry *));
        if (dec == NULL)
            s = NATS_NO_MEMORY;

        for (i = 0; (s == NATS_OK) && (i < raw.Count); i++)
        {
            s = kvcodec_wrapEntry(&(dec[i]), raw.Entries[i], key,
                                  c->keyCodec, c->valueCodec);
            if (s == NATS_OK)
                raw.Entries[i] = NULL; // ownership moved to the wrapped entry
        }
        if (s != NATS_OK)
        {
            for (i = 0; (dec != NULL) && (i < raw.Count); i++)
                kvCodecEntry_Destroy(dec[i]);
            NATS_FREE(dec);
            // kvEntry_Destroy is NULL-safe, so the raw list can be destroyed
            // with the consumed slots NULLed out.
            kvEntryList_Destroy(&raw);
            return s;
        }
        list->Entries = dec;
        list->Count   = raw.Count;
    }
    kvEntryList_Destroy(&raw);
    return NATS_OK;
}

void
kvCodecEntryList_Destroy(kvCodecEntryList *list)
{
    int i;

    if ((list == NULL) || (list->Entries == NULL))
        return;
    for (i = 0; i < list->Count; i++)
        kvCodecEntry_Destroy(list->Entries[i]);
    NATS_FREE(list->Entries);
    list->Entries = NULL;
    list->Count   = 0;
}

natsStatus
kvCodec_PurgeDeletes(kvCodec *c, const kvPurgeOptions *opts)
{
    if (c == NULL)
        return NATS_INVALID_ARG;
    return kvStore_PurgeDeletes(c->kv, opts);
}

const char *
kvCodec_Bucket(const kvCodec *c)
{
    if (c == NULL)
        return NULL;
    return kvStore_Bucket(c->kv);
}

natsStatus
kvCodec_Status(kvStatus **newStatus, kvCodec *c)
{
    if (c == NULL)
        return NATS_INVALID_ARG;
    return kvStore_Status(newStatus, c->kv);
}

natsStatus
kvCodecWatcher_Next(kvCodecEntry **newEntry, kvCodecWatcher *w, int64_t timeout)
{
    natsStatus s     = NATS_OK;
    kvEntry   *entry = NULL;

    if ((newEntry == NULL) || (w == NULL))
        return NATS_INVALID_ARG;

    // Defined on every return, including NATS_TIMEOUT, as kvWatcher_Next does.
    *newEntry = NULL;

    s = kvWatcher_Next(&entry, w->w, timeout);
    if (s != NATS_OK)
        return s;

    // A NULL entry with NATS_OK signals that all initial values have been
    // delivered; it is passed through untouched.
    if (entry == NULL)
    {
        *newEntry = NULL;
        return NATS_OK;
    }

    s = kvcodec_wrapEntry(newEntry, entry, NULL, w->keyCodec, w->valueCodec);
    if (s != NATS_OK)
        kvEntry_Destroy(entry); // entry dropped; the watcher remains usable
    return s;
}

natsStatus
kvCodecWatcher_Stop(kvCodecWatcher *w)
{
    if (w == NULL)
        return NATS_INVALID_ARG;
    return kvWatcher_Stop(w->w);
}

void
kvCodecWatcher_Destroy(kvCodecWatcher *w)
{
    if (w == NULL)
        return;
    kvWatcher_Destroy(w->w);
    NATS_FREE(w);
}

const char *
kvCodecEntry_Bucket(const kvCodecEntry *e)
{
    if (e == NULL)
        return NULL;
    return kvEntry_Bucket(e->entry);
}

const char *
kvCodecEntry_Key(const kvCodecEntry *e)
{
    if (e == NULL)
        return NULL;
    return e->key;
}

const void *
kvCodecEntry_Value(const kvCodecEntry *e)
{
    if (e == NULL)
        return NULL;
    if (e->rawValue)
        return kvEntry_Value(e->entry);
    return e->value;
}

int
kvCodecEntry_ValueLen(const kvCodecEntry *e)
{
    if (e == NULL)
        return 0;
    if (e->rawValue)
        return kvEntry_ValueLen(e->entry);
    return e->valueLen;
}

const char *
kvCodecEntry_ValueString(const kvCodecEntry *e)
{
    if (e == NULL)
        return NULL;
    if (e->rawValue)
        return kvEntry_ValueString(e->entry);
    return (const char *)e->value;
}

uint64_t
kvCodecEntry_Revision(const kvCodecEntry *e)
{
    if (e == NULL)
        return 0;
    return kvEntry_Revision(e->entry);
}

int64_t
kvCodecEntry_Created(const kvCodecEntry *e)
{
    if (e == NULL)
        return 0;
    return kvEntry_Created(e->entry);
}

uint64_t
kvCodecEntry_Delta(const kvCodecEntry *e)
{
    if (e == NULL)
        return 0;
    return kvEntry_Delta(e->entry);
}

kvOperation
kvCodecEntry_Operation(const kvCodecEntry *e)
{
    if (e == NULL)
        return kvOp_Unknown;
    return kvEntry_Operation(e->entry);
}

void
kvCodecEntry_Destroy(kvCodecEntry *e)
{
    if (e == NULL)
        return;
    kvEntry_Destroy(e->entry);
    NATS_FREE(e->key);
    NATS_FREE(e->value);
    NATS_FREE(e);
}
