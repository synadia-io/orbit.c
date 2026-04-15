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

#include "parser.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

//-----------------------------------------------------------------------------
// Validation helpers
//-----------------------------------------------------------------------------

// Returns true when s is a valid decimal integer string.
// If requireSign is true, s must start with '+' or '-'.
static bool
_isValidNumber(const char *s, bool requireSign)
{
    if (s == NULL || *s == '\0')
        return false;

    if (*s == '+' || *s == '-')
        s++;
    else if (requireSign)
        return false;

    if (*s == '\0')
        return false;

    for (; *s != '\0'; s++)
    {
        if (*s < '0' || *s > '9')
            return false;
    }
    return true;
}

//-----------------------------------------------------------------------------
// Minimal JSON helpers — just enough for the formats used by counters.
//
// We avoid depending on nats.c internal JSON functions (not part of the
// public header) and instead hand-parse the small, well-defined formats.
//-----------------------------------------------------------------------------

static const char *
_skipws(const char *p, const char *end)
{
    while (p < end && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r'))
        p++;
    return p;
}

// Parse a JSON quoted string starting at *p (must point at '"').
// On success, sets *out to a heap-allocated copy of the string content
// and returns a pointer past the closing quote.  Returns NULL on error.
//
// TODO: This performs a raw copy and does not decode JSON escape sequences
// (e.g. \n, \uXXXX).  Counter values and stream/subject names produced by
// the server do not contain escapes, but a proper implementation should
// handle them.
static const char *
_parseStr(const char *p, const char *end, char **out)
{
    if (p >= end || *p != '"')
        return NULL;
    p++; // skip opening quote

    const char *start = p;
    while (p < end && *p != '"')
    {
        if (*p == '\\')
        {
            p++;
            if (p >= end) return NULL;
        }
        p++;
    }
    if (p >= end)
        return NULL;

    size_t len = (size_t)(p - start);
    *out = (char *)malloc(len + 1);
    if (*out == NULL)
        return NULL;
    memcpy(*out, start, len);
    (*out)[len] = '\0';
    return p + 1; // past closing quote
}

// Skip a single JSON value (string, number, object, array, bool, null).
// Returns pointer past the value, or NULL on error.
static const char *
_skipVal(const char *p, const char *end)
{
    p = _skipws(p, end);
    if (p >= end)
        return NULL;

    if (*p == '"')
    {
        p++;
        while (p < end && *p != '"')
        {
            if (*p == '\\') p++;
            p++;
        }
        return (p < end) ? p + 1 : NULL;
    }
    if (*p == '{')
    {
        int depth = 1;
        p++;
        while (p < end && depth > 0)
        {
            if (*p == '"')
            {
                p++;
                while (p < end && *p != '"') { if (*p == '\\') p++; p++; }
                if (p < end) p++;
                continue;
            }
            if (*p == '{') depth++;
            else if (*p == '}') depth--;
            p++;
        }
        return p;
    }
    if (*p == '[')
    {
        int depth = 1;
        p++;
        while (p < end && depth > 0)
        {
            if (*p == '"')
            {
                p++;
                while (p < end && *p != '"') { if (*p == '\\') p++; p++; }
                if (p < end) p++;
                continue;
            }
            if (*p == '[') depth++;
            else if (*p == ']') depth--;
            p++;
        }
        return p;
    }
    // number, true, false, null — skip until delimiter
    while (p < end && *p != ',' && *p != '}' && *p != ']'
           && *p != ' ' && *p != '\t' && *p != '\n' && *p != '\r')
        p++;
    return p;
}

// Find a string-valued field named key in a flat JSON object.
// Sets *value to a heap-allocated string on success.
static natsStatus
_jsonGetStr(const char *json, int jsonLen, const char *key, char **value)
{
    const char *p   = json;
    const char *end = json + jsonLen;

    p = _skipws(p, end);
    if (p >= end || *p != '{')
        return NATS_ERR;
    p++;

    while (p < end)
    {
        p = _skipws(p, end);
        if (p >= end) break;
        if (*p == '}') return NATS_NOT_FOUND;
        if (*p == ',') { p++; continue; }

        // Parse field key
        char *k = NULL;
        p = _parseStr(p, end, &k);
        if (p == NULL) return NATS_ERR;

        p = _skipws(p, end);
        if (p >= end || *p != ':') { free(k); return NATS_ERR; }
        p++;
        p = _skipws(p, end);

        bool match = (strcmp(k, key) == 0);
        free(k);

        if (match)
        {
            if (p >= end || *p != '"')
                return NATS_ERR;
            p = _parseStr(p, end, value);
            return (p != NULL) ? NATS_OK : NATS_ERR;
        }

        // Skip the value
        p = _skipVal(p, end);
        if (p == NULL) return NATS_ERR;
    }
    return NATS_NOT_FOUND;
}

// Parse a two-level JSON object: {"outerKey":{"innerKey":"val"}, ...}
// For each (outerKey, innerKey, val) triple, prepend a node to *head.
static natsStatus
_parseNestedObj(const char *json, int jsonLen, natsCounterSource **head)
{
    const char *p   = json;
    const char *end = json + jsonLen;

    p = _skipws(p, end);
    if (p >= end || *p != '{')
        return NATS_ERR;
    p++;

    while (p < end)
    {
        p = _skipws(p, end);
        if (p >= end) break;
        if (*p == '}') return NATS_OK;
        if (*p == ',') { p++; continue; }

        // Outer key (stream name)
        char *stream = NULL;
        p = _parseStr(p, end, &stream);
        if (p == NULL) return NATS_ERR;

        p = _skipws(p, end);
        if (p >= end || *p != ':') { free(stream); return NATS_ERR; }
        p++;
        p = _skipws(p, end);

        // Inner object
        if (p >= end || *p != '{') { free(stream); return NATS_ERR; }
        p++;

        while (p < end)
        {
            p = _skipws(p, end);
            if (p >= end) { free(stream); return NATS_ERR; }
            if (*p == '}') { p++; break; }
            if (*p == ',') { p++; continue; }

            // Inner key (subject)
            char *subject = NULL;
            p = _parseStr(p, end, &subject);
            if (p == NULL) { free(stream); return NATS_ERR; }

            p = _skipws(p, end);
            if (p >= end || *p != ':') { free(stream); free(subject); return NATS_ERR; }
            p++;
            p = _skipws(p, end);

            // Inner value (counter value string)
            char *val = NULL;
            p = _parseStr(p, end, &val);
            if (p == NULL) { free(stream); free(subject); return NATS_ERR; }

            natsCounterSource *node = (natsCounterSource *)calloc(1, sizeof(*node));
            if (node == NULL)
            {
                free(stream); free(subject); free(val);
                return NATS_NO_MEMORY;
            }
            node->stream  = strdup(stream);
            node->subject = subject;
            node->value   = val;
            node->next    = *head;
            *head = node;

            if (node->stream == NULL)
            {
                free(stream);
                return NATS_NO_MEMORY;
            }
        }
        free(stream);
    }
    return NATS_OK;
}

// ---------------------------------------------------------------------------
// natsCounterParser_ParseValue
//
// Expected body format: {"val":"<decimal>"}
// ---------------------------------------------------------------------------
natsStatus
natsCounterParser_ParseValue(const unsigned char *data,
                              int                  dataLen,
                              char               **value)
{
    natsStatus s;
    char      *val = NULL;

    if (data == NULL || dataLen <= 0 || value == NULL)
        return NATS_ERR;

    s = _jsonGetStr((const char *)data, dataLen, "val", &val);
    if (s != NATS_OK)
        return NATS_ERR;

    if (!_isValidNumber(val, false))
    {
        free(val);
        return NATS_ERR;
    }

    *value = val;
    return NATS_OK;
}

// ---------------------------------------------------------------------------
// natsCounterParser_ParsePubAckValue
// ---------------------------------------------------------------------------
natsStatus
natsCounterParser_ParsePubAckValue(const char  *ackVal,
                                    char       **value)
{
    if (ackVal == NULL)
        return NATS_NOT_FOUND;

    if (!_isValidNumber(ackVal, false))
        return NATS_ERR;

    *value = strdup(ackVal);
    return (*value != NULL) ? NATS_OK : NATS_NO_MEMORY;
}

// ---------------------------------------------------------------------------
// natsCounterParser_ParseSources
//
// Expected header format (JSON):
//   {"STREAM_A":{"subject.a":"100"},"STREAM_B":{"subject.b":"200"}}
// ---------------------------------------------------------------------------
natsStatus
natsCounterParser_ParseSources(const char         *headerValue,
                                natsCounterSource **sources)
{
    *sources = NULL;

    if (headerValue == NULL)
        return NATS_OK;

    return _parseNestedObj(headerValue, (int)strlen(headerValue), sources);
}

// ---------------------------------------------------------------------------
// natsCounterParser_FreeSources
// ---------------------------------------------------------------------------
void
natsCounterParser_FreeSources(natsCounterSource *sources)
{
    natsCounterSource *next;

    while (sources != NULL)
    {
        next = sources->next;
        free(sources->stream);
        free(sources->subject);
        free(sources->value);
        free(sources);
        sources = next;
    }
}

// ---------------------------------------------------------------------------
// natsCounterParser_ParseIncrement
// ---------------------------------------------------------------------------
natsStatus
natsCounterParser_ParseIncrement(const char  *headerValue,
                                  char       **increment)
{
    if (headerValue == NULL)
        return NATS_NOT_FOUND;

    // ADR-49 requires a sign prefix on Nats-Incr values: ^[+-]\d+$
    if (!_isValidNumber(headerValue, true))
        return NATS_ERR;

    *increment = strdup(headerValue);
    return (*increment != NULL) ? NATS_OK : NATS_NO_MEMORY;
}
