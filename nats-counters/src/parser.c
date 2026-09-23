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

#include "json.h"

#include <stdlib.h>
#include <string.h>

// Returns true when s is a valid decimal integer string,
// with an optional leading '+' or '-'.
static bool
_isValidNumber(const char *s)
{
    if (s == NULL || *s == '\0')
        return false;

    if (*s == '+' || *s == '-')
        s++;

    if (*s == '\0')
        return false;

    for (; *s != '\0'; s++)
    {
        if (*s < '0' || *s > '9')
            return false;
    }
    return true;
}

// Parse a two-level JSON object: {"outerKey":{"innerKey":"val"}, ...}
// For each (outerKey, innerKey, val) triple, prepend a node to *head.
static natsStatus
_parseNestedObj(natsJSON *root, natsCounterSource **head)
{
    if (natsJSON_Type(root) != NATS_JSON_OBJECT)
        return NATS_ERR;

    for (int i = 0; i < natsJSON_FieldCount(root); i++)
    {
        const char *stream = NULL;
        natsJSON   *inner  = NULL;

        natsJSON_FieldAt(root, i, &stream, &inner);
        if (natsJSON_Type(inner) != NATS_JSON_OBJECT)
            return NATS_ERR;

        for (int j = 0; j < natsJSON_FieldCount(inner); j++)
        {
            const char *subject = NULL;
            const char *val     = NULL;
            natsJSON   *valNode = NULL;

            natsJSON_FieldAt(inner, j, &subject, &valNode);
            if (natsJSON_AsStr(valNode, &val) != NATS_OK)
                return NATS_ERR;

            natsCounterSource *node = (natsCounterSource *)calloc(1, sizeof(*node));
            if (node == NULL)
                return NATS_NO_MEMORY;
            node->stream  = strdup(stream);
            node->subject = strdup(subject);
            node->value   = strdup(val);
            if (node->stream == NULL || node->subject == NULL || node->value == NULL)
            {
                natsCounterParser_FreeSources(node);
                return NATS_NO_MEMORY;
            }
            node->next = *head;
            *head      = node;
        }
    }
    return NATS_OK;
}


// Expected body format: {"val":"<decimal>"}
natsStatus
natsCounterParser_ParseValue(const unsigned char *data,
                             int dataLen, char **value)
{
    natsStatus s;
    natsJSON  *json = NULL;
    char      *val  = NULL;

    if (data == NULL || dataLen <= 0 || value == NULL)
        return NATS_ERR;

    s = natsJSON_Parse(&json, (const char *)data, dataLen);
    if (s == NATS_OK)
        s = natsJSON_TakeStr(json, "val", &val);
    natsJSON_Destroy(json);
    if (s != NATS_OK)
        return NATS_ERR;

    if (!_isValidNumber(val))
    {
        free(val);
        return NATS_ERR;
    }

    *value = val;
    return NATS_OK;
}

natsStatus
natsCounterParser_ParsePubAckValue(const char *ackVal,
                                   char **value)
{
    if (ackVal == NULL)
        return NATS_NOT_FOUND;

    if (!_isValidNumber(ackVal))
        return NATS_ERR;

    *value = strdup(ackVal);
    return (*value != NULL) ? NATS_OK : NATS_NO_MEMORY;
}


// Expected header format (JSON):
//   {"STREAM_A":{"subject.a":"100"},"STREAM_B":{"subject.b":"200"}}
natsStatus
natsCounterParser_ParseSources(const char *headerValue,
                               natsCounterSource **sources)
{
    natsStatus s;
    natsJSON  *json = NULL;

    *sources = NULL;

    if (headerValue == NULL)
        return NATS_OK;

    s = natsJSON_Parse(&json, headerValue, (int)strlen(headerValue));
    if (s == NATS_OK)
        s = _parseNestedObj(json, sources);
    natsJSON_Destroy(json);

    if (s != NATS_OK)
    {
        natsCounterParser_FreeSources(*sources);
        *sources = NULL;
    }
    return s;
}

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

natsStatus
natsCounterParser_ParseIncrement(const char *headerValue,
                                 char **increment)
{
    if (headerValue == NULL)
        return NATS_NOT_FOUND;

    if (!_isValidNumber(headerValue))
        return NATS_ERR;

    *increment = strdup(headerValue);
    return (*increment != NULL) ? NATS_OK : NATS_NO_MEMORY;
}
