// Copyright 2026 The NATS Authors
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

#include "json.h"
#include "os_shims.h"

#include <inttypes.h>
#include <stdio.h>
#include <errno.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

// Cap on nesting depth for the recursive-descent parser, so deeply nested
// input cannot exhaust the C stack.
#define JSON_MAX_DEPTH 512

struct __natsJSON
{
    natsJSONType type;

    // Input span this value was parsed from; see natsJSON_Raw().
    int         rawLen;
    const char *raw;

    union
    {
        bool  boolean; // NATS_JSON_BOOL
        char *str;     // NATS_JSON_STRING (decoded) / NATS_JSON_NUMBER (literal)
        struct
        {
            natsJSON **items;
            int        count;
            int        cap;
        } array; // NATS_JSON_ARRAY
        struct
        {
            char     **names;
            natsJSON **values;
            int        count;
            int        cap;
        } object; // NATS_JSON_OBJECT
    } v;
};

// Cursor over the input document, plus the current nesting depth.
typedef struct
{
    const char *cur;
    const char *end;
    int         depth;

} _jsonParser;

static natsStatus
_parseValue(_jsonParser *ps, natsJSON **out);

static natsJSON *
_newNode(natsJSONType type)
{
    natsJSON *node = (natsJSON *) NATS_CALLOC(1, sizeof(natsJSON));
    if (node != NULL)
        node->type = type;
    return node;
}

static void
_skipWS(_jsonParser *ps)
{
    while (ps->cur < ps->end)
    {
        char c = *ps->cur;
        if ((c != ' ') && (c != '\t') && (c != '\n') && (c != '\r'))
            break;
        ps->cur++;
    }
}

static bool
_isDigit(char c)
{
    return ((c >= '0') && (c <= '9'));
}

// Parses exactly four hex digits at 'p' into *out (0..0xFFFF). Returns false
// on a non-hex digit.
static bool
_hex4(const char *p, unsigned int *out)
{
    unsigned int val = 0;
    int          i;

    for (i = 0; i < 4; i++)
    {
        char         c = p[i];
        unsigned int d;

        if ((c >= '0') && (c <= '9'))
            d = (unsigned int) (c - '0');
        else if ((c >= 'a') && (c <= 'f'))
            d = (unsigned int) (c - 'a' + 10);
        else if ((c >= 'A') && (c <= 'F'))
            d = (unsigned int) (c - 'A' + 10);
        else
            return false;

        val = (val << 4) | d;
    }
    *out = val;
    return true;
}

// Encodes Unicode code point 'cp' as UTF-8 into 'dst' and returns the number
// of bytes written (1..4). 'cp' is assumed to be a valid scalar value.
static int
_utf8Encode(unsigned int cp, char *dst)
{
    if (cp < 0x80)
    {
        dst[0] = (char) cp;
        return 1;
    }
    if (cp < 0x800)
    {
        dst[0] = (char) (0xC0 | (cp >> 6));
        dst[1] = (char) (0x80 | (cp & 0x3F));
        return 2;
    }
    if (cp < 0x10000)
    {
        dst[0] = (char) (0xE0 | (cp >> 12));
        dst[1] = (char) (0x80 | ((cp >> 6) & 0x3F));
        dst[2] = (char) (0x80 | (cp & 0x3F));
        return 3;
    }
    dst[0] = (char) (0xF0 | (cp >> 18));
    dst[1] = (char) (0x80 | ((cp >> 12) & 0x3F));
    dst[2] = (char) (0x80 | ((cp >> 6) & 0x3F));
    dst[3] = (char) (0x80 | (cp & 0x3F));
    return 4;
}

// Parses a JSON string literal starting at ps->cur (which must point at the
// opening quote) and returns the decoded, NUL-terminated content in *out.
// Advances ps->cur past the closing quote.
static natsStatus
_parseString(_jsonParser *ps, char **out)
{
    const char *start;
    const char *q;
    const char *p;
    char       *buf;
    size_t      o = 0;

    // ps->cur is the opening quote.
    start = ps->cur + 1;

    // Locate the closing quote, honouring backslash escapes. Reject raw
    // control characters, which JSON requires to be escaped.
    q = start;
    while (q < ps->end)
    {
        unsigned char c = (unsigned char) *q;
        if (c == '\\')
        {
            q++;
            if (q >= ps->end)
                return NATS_ERR;
            q++;
            continue;
        }
        if (c == '"')
            break;
        if (c < 0x20)
            return NATS_ERR;
        q++;
    }
    if (q >= ps->end)
        return NATS_ERR; // unterminated string

    // Decoding never grows the content: (q - start) + 1 is a safe upper bound.
    buf = (char *) NATS_MALLOC((size_t) (q - start) + 1);
    if (buf == NULL)
        return NATS_NO_MEMORY;

    p = start;
    while (p < q)
    {
        if (*p != '\\')
        {
            buf[o++] = *p++;
            continue;
        }

        p++; // step onto the escape selector
        switch (*p)
        {
            case '"': buf[o++] = '"'; p++; break;
            case '\\': buf[o++] = '\\'; p++; break;
            case '/': buf[o++] = '/'; p++; break;
            case 'b': buf[o++] = '\b'; p++; break;
            case 'f': buf[o++] = '\f'; p++; break;
            case 'n': buf[o++] = '\n'; p++; break;
            case 'r': buf[o++] = '\r'; p++; break;
            case 't': buf[o++] = '\t'; p++; break;
            case 'u':
            {
                unsigned int cp;

                p++; // step onto the first hex digit
                if ((q - p < 4) || !_hex4(p, &cp))
                {
                    NATS_FREE(buf);
                    return NATS_ERR;
                }
                p += 4;

                if ((cp >= 0xD800) && (cp <= 0xDBFF))
                {
                    // High surrogate: must be followed by a \uXXXX low
                    // surrogate to form a valid code point.
                    unsigned int lo;
                    if ((q - p < 6) || (p[0] != '\\') || (p[1] != 'u') ||
                        !_hex4(p + 2, &lo) || (lo < 0xDC00) || (lo > 0xDFFF))
                    {
                        NATS_FREE(buf);
                        return NATS_ERR;
                    }
                    p += 6;
                    cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                }
                else if ((cp >= 0xDC00) && (cp <= 0xDFFF))
                {
                    // Unpaired low surrogate.
                    NATS_FREE(buf);
                    return NATS_ERR;
                }

                o += (size_t) _utf8Encode(cp, buf + o);
                break;
            }
            default:
                NATS_FREE(buf);
                return NATS_ERR;
        }
    }

    buf[o] = '\0';
    ps->cur = q + 1; // past the closing quote
    *out = buf;
    return NATS_OK;
}

static natsStatus
_parseStringNode(_jsonParser *ps, natsJSON **out)
{
    natsJSON  *node;
    char      *str = NULL;
    natsStatus s;

    s = _parseString(ps, &str);
    if (s != NATS_OK)
        return s;

    node = _newNode(NATS_JSON_STRING);
    if (node == NULL)
    {
        NATS_FREE(str);
        return NATS_NO_MEMORY;
    }
    node->v.str = str;
    *out = node;
    return NATS_OK;
}

static natsStatus
_parseNumber(_jsonParser *ps, natsJSON **out)
{
    const char *s = ps->cur;
    const char *p = s;
    natsJSON   *node;
    char       *lit;
    size_t      len;

    if ((p < ps->end) && (*p == '-'))
        p++;

    // Integer part: a lone '0' or a non-zero digit run.
    if (p >= ps->end)
        return NATS_ERR;
    if (*p == '0')
    {
        p++;
    }
    else if (_isDigit(*p))
    {
        while ((p < ps->end) && _isDigit(*p))
            p++;
    }
    else
    {
        return NATS_ERR;
    }

    // Fractional part.
    if ((p < ps->end) && (*p == '.'))
    {
        p++;
        if ((p >= ps->end) || !_isDigit(*p))
            return NATS_ERR;
        while ((p < ps->end) && _isDigit(*p))
            p++;
    }

    // Exponent part.
    if ((p < ps->end) && ((*p == 'e') || (*p == 'E')))
    {
        p++;
        if ((p < ps->end) && ((*p == '+') || (*p == '-')))
            p++;
        if ((p >= ps->end) || !_isDigit(*p))
            return NATS_ERR;
        while ((p < ps->end) && _isDigit(*p))
            p++;
    }

    len = (size_t) (p - s);
    lit = (char *) NATS_MALLOC(len + 1);
    if (lit == NULL)
        return NATS_NO_MEMORY;
    memcpy(lit, s, len);
    lit[len] = '\0';

    node = _newNode(NATS_JSON_NUMBER);
    if (node == NULL)
    {
        NATS_FREE(lit);
        return NATS_NO_MEMORY;
    }
    node->v.str = lit;
    ps->cur = p;
    *out = node;
    return NATS_OK;
}

// Matches a bare literal ("true", "false" or "null") at ps->cur.
static natsStatus
_parseLiteral(_jsonParser *ps, natsJSON **out)
{
    size_t avail = (size_t) (ps->end - ps->cur);

    if ((avail >= 4) && (memcmp(ps->cur, "true", 4) == 0))
    {
        natsJSON *node = _newNode(NATS_JSON_BOOL);
        if (node == NULL)
            return NATS_NO_MEMORY;
        node->v.boolean = true;
        ps->cur += 4;
        *out = node;
        return NATS_OK;
    }
    if ((avail >= 5) && (memcmp(ps->cur, "false", 5) == 0))
    {
        natsJSON *node = _newNode(NATS_JSON_BOOL);
        if (node == NULL)
            return NATS_NO_MEMORY;
        node->v.boolean = false;
        ps->cur += 5;
        *out = node;
        return NATS_OK;
    }
    if ((avail >= 4) && (memcmp(ps->cur, "null", 4) == 0))
    {
        natsJSON *node = _newNode(NATS_JSON_NULL);
        if (node == NULL)
            return NATS_NO_MEMORY;
        ps->cur += 4;
        *out = node;
        return NATS_OK;
    }
    return NATS_ERR;
}

static natsStatus
_arrayAppend(natsJSON *arr, natsJSON *item)
{
    if (arr->v.array.count == arr->v.array.cap)
    {
        int        newCap = (arr->v.array.cap == 0) ? 4 : arr->v.array.cap * 2;
        natsJSON **grown =
            (natsJSON **) NATS_REALLOC(arr->v.array.items, (size_t) newCap * sizeof(natsJSON *));
        if (grown == NULL)
            return NATS_NO_MEMORY;
        arr->v.array.items = grown;
        arr->v.array.cap = newCap;
    }
    arr->v.array.items[arr->v.array.count++] = item;
    return NATS_OK;
}

static natsStatus
_objectAppend(natsJSON *obj, char *name, natsJSON *value)
{
    if (obj->v.object.count == obj->v.object.cap)
    {
        int        newCap = (obj->v.object.cap == 0) ? 4 : obj->v.object.cap * 2;
        char     **gn = (char **) NATS_REALLOC(obj->v.object.names, (size_t) newCap * sizeof(char *));
        natsJSON **gv;

        if (gn == NULL)
            return NATS_NO_MEMORY;
        obj->v.object.names = gn;

        gv = (natsJSON **) NATS_REALLOC(obj->v.object.values, (size_t) newCap * sizeof(natsJSON *));
        if (gv == NULL)
            return NATS_NO_MEMORY;
        obj->v.object.values = gv;

        obj->v.object.cap = newCap;
    }
    obj->v.object.names[obj->v.object.count] = name;
    obj->v.object.values[obj->v.object.count] = value;
    obj->v.object.count++;
    return NATS_OK;
}

static natsStatus
_parseArray(_jsonParser *ps, natsJSON **out)
{
    natsJSON  *node;
    natsStatus s = NATS_OK;

    if (ps->depth >= JSON_MAX_DEPTH)
        return NATS_ERR;

    node = _newNode(NATS_JSON_ARRAY);
    if (node == NULL)
        return NATS_NO_MEMORY;

    ps->cur++; // past '['
    _skipWS(ps);
    if ((ps->cur < ps->end) && (*ps->cur == ']'))
    {
        ps->cur++;
        *out = node;
        return NATS_OK;
    }

    ps->depth++;
    for (;;)
    {
        natsJSON *item = NULL;

        s = _parseValue(ps, &item);
        if (s != NATS_OK)
            break;

        s = _arrayAppend(node, item);
        if (s != NATS_OK)
        {
            natsJSON_Destroy(item);
            break;
        }

        _skipWS(ps);
        if (ps->cur >= ps->end)
        {
            s = NATS_ERR;
            break;
        }
        if (*ps->cur == ',')
        {
            ps->cur++;
            continue;
        }
        if (*ps->cur == ']')
        {
            ps->cur++;
            break;
        }
        s = NATS_ERR;
        break;
    }
    ps->depth--;

    if (s != NATS_OK)
    {
        natsJSON_Destroy(node);
        return s;
    }
    *out = node;
    return NATS_OK;
}

static natsStatus
_parseObject(_jsonParser *ps, natsJSON **out)
{
    natsJSON  *node;
    natsStatus s = NATS_OK;

    if (ps->depth >= JSON_MAX_DEPTH)
        return NATS_ERR;

    node = _newNode(NATS_JSON_OBJECT);
    if (node == NULL)
        return NATS_NO_MEMORY;

    ps->cur++; // past '{'
    _skipWS(ps);
    if ((ps->cur < ps->end) && (*ps->cur == '}'))
    {
        ps->cur++;
        *out = node;
        return NATS_OK;
    }

    ps->depth++;
    for (;;)
    {
        char     *key = NULL;
        natsJSON *val = NULL;

        _skipWS(ps);
        if ((ps->cur >= ps->end) || (*ps->cur != '"'))
        {
            s = NATS_ERR;
            break;
        }

        s = _parseString(ps, &key);
        if (s != NATS_OK)
            break;

        _skipWS(ps);
        if ((ps->cur >= ps->end) || (*ps->cur != ':'))
        {
            NATS_FREE(key);
            s = NATS_ERR;
            break;
        }
        ps->cur++;

        s = _parseValue(ps, &val);
        if (s != NATS_OK)
        {
            NATS_FREE(key);
            break;
        }

        s = _objectAppend(node, key, val);
        if (s != NATS_OK)
        {
            NATS_FREE(key);
            natsJSON_Destroy(val);
            break;
        }

        _skipWS(ps);
        if (ps->cur >= ps->end)
        {
            s = NATS_ERR;
            break;
        }
        if (*ps->cur == ',')
        {
            ps->cur++;
            continue;
        }
        if (*ps->cur == '}')
        {
            ps->cur++;
            break;
        }
        s = NATS_ERR;
        break;
    }
    ps->depth--;

    if (s != NATS_OK)
    {
        natsJSON_Destroy(node);
        return s;
    }
    *out = node;
    return NATS_OK;
}

static natsStatus
_parseValue(_jsonParser *ps, natsJSON **out)
{
    const char *start;
    natsStatus  s;
    char        c;

    _skipWS(ps);
    if (ps->cur >= ps->end)
        return NATS_ERR;

    start = ps->cur;
    c     = *start;
    switch (c)
    {
        case '{': s = _parseObject(ps, out); break;
        case '[': s = _parseArray(ps, out); break;
        case '"': s = _parseStringNode(ps, out); break;
        case 't':
        case 'f':
        case 'n': s = _parseLiteral(ps, out); break;
        default:
            if (!(c == '-') && !_isDigit(c))
                return NATS_ERR;
            s = _parseNumber(ps, out);
            break;
    }
    if (s != NATS_OK)
        return s;

    (*out)->raw    = start;
    (*out)->rawLen = (int) (ps->cur - start);
    return NATS_OK;
}

natsStatus
natsJSON_Parse(natsJSON **newJSON, const char *data, int len)
{
    _jsonParser ps;
    natsJSON   *root = NULL;
    natsStatus  s;

    if ((newJSON == NULL) || ((data == NULL) && (len != 0)) || (len < 0))
        return NATS_INVALID_ARG;

    *newJSON = NULL;

    // Empty input; also keeps NULL data out of the pointer arithmetic below.
    if (len == 0)
        return NATS_ERR;

    ps.cur   = data;
    ps.end   = data + len;
    ps.depth = 0;

    s = _parseValue(&ps, &root);
    if (s != NATS_OK)
        return s;

    // Reject trailing content after the top-level value.
    _skipWS(&ps);
    if (ps.cur != ps.end)
    {
        natsJSON_Destroy(root);
        return NATS_ERR;
    }

    *newJSON = root;
    return NATS_OK;
}

void
natsJSON_Destroy(natsJSON *json)
{
    int i;

    if (json == NULL)
        return;

    switch (json->type)
    {
        case NATS_JSON_STRING:
        case NATS_JSON_NUMBER:
            NATS_FREE(json->v.str);
            break;
        case NATS_JSON_ARRAY:
            for (i = 0; i < json->v.array.count; i++)
                natsJSON_Destroy(json->v.array.items[i]);
            NATS_FREE(json->v.array.items);
            break;
        case NATS_JSON_OBJECT:
            for (i = 0; i < json->v.object.count; i++)
            {
                NATS_FREE(json->v.object.names[i]);
                natsJSON_Destroy(json->v.object.values[i]);
            }
            NATS_FREE(json->v.object.names);
            NATS_FREE(json->v.object.values);
            break;
        default:
            break;
    }
    NATS_FREE(json);
}

natsJSONType
natsJSON_Type(const natsJSON *json)
{
    return (json == NULL) ? NATS_JSON_NULL : json->type;
}

natsStatus
natsJSON_AsBool(const natsJSON *json, bool *out)
{
    if ((json == NULL) || (out == NULL) || (json->type != NATS_JSON_BOOL))
        return NATS_INVALID_ARG;
    *out = json->v.boolean;
    return NATS_OK;
}

natsStatus
natsJSON_AsNumber(const natsJSON *json, double *out)
{
    if ((json == NULL) || (out == NULL) || (json->type != NATS_JSON_NUMBER))
        return NATS_INVALID_ARG;
    *out = strtod(json->v.str, NULL);

    if (isinf(*out))
        return NATS_INVALID_ARG;
    return NATS_OK;
}

// Anything strtoll leaves unparsed is a fraction or exponent.
natsStatus
natsJSON_AsInt(const natsJSON *json, int64_t *out)
{
    int64_t val;
    char   *end = NULL;

    if ((json == NULL) || (out == NULL) || (json->type != NATS_JSON_NUMBER))
        return NATS_INVALID_ARG;

    errno = 0;
    val   = (int64_t) strtoll(json->v.str, &end, 10);

    if ((errno == ERANGE) || (*end != '\0'))
        return NATS_INVALID_ARG;
    *out = val;
    return NATS_OK;
}

natsStatus
natsJSON_AsUInt(const natsJSON *json, uint64_t *out)
{
    uint64_t val;
    char    *end = NULL;

    if ((json == NULL) || (out == NULL) || (json->type != NATS_JSON_NUMBER))
        return NATS_INVALID_ARG;

    if (json->v.str[0] == '-')
        return NATS_INVALID_ARG;

    errno = 0;
    val   = (uint64_t) strtoull(json->v.str, &end, 10);

    if ((errno == ERANGE) || (*end != '\0'))
        return NATS_INVALID_ARG;
    *out = val;
    return NATS_OK;
}

natsStatus
natsJSON_AsStr(const natsJSON *json, const char **out)
{
    if ((json == NULL) || (out == NULL) || (json->type != NATS_JSON_STRING))
        return NATS_INVALID_ARG;
    *out = json->v.str;
    return NATS_OK;
}

natsStatus
natsJSON_Raw(const natsJSON *json, const char **text, int *len)
{
    if ((json == NULL) || (text == NULL) || (len == NULL))
        return NATS_INVALID_ARG;
    *text = json->raw;
    *len  = json->rawLen;
    return NATS_OK;
}

// Moves a string node's content out and leaves the node a JSON null.
static char *
_takeStrNode(natsJSON *node)
{
    char *str = node->v.str;

    node->v.str = NULL;
    node->type  = NATS_JSON_NULL;
    return str;
}

natsStatus
natsJSON_Field(const natsJSON *json, const char *key, natsJSON **out)
{
    int i;

    if ((json == NULL) || (key == NULL) || (out == NULL))
        return NATS_INVALID_ARG;
    if (json->type != NATS_JSON_OBJECT)
        return NATS_INVALID_ARG;

    for (i = 0; i < json->v.object.count; i++)
    {
        if (strcmp(json->v.object.names[i], key) == 0)
        {
            *out = json->v.object.values[i];
            return NATS_OK;
        }
    }
    return NATS_NOT_FOUND;
}

int
natsJSON_FieldCount(const natsJSON *json)
{
    if ((json == NULL) || (json->type != NATS_JSON_OBJECT))
        return 0;
    return json->v.object.count;
}

natsStatus
natsJSON_FieldAt(const natsJSON *json, int idx, const char **key, natsJSON **value)
{
    if ((json == NULL) || (json->type != NATS_JSON_OBJECT))
        return NATS_INVALID_ARG;
    if ((idx < 0) || (idx >= json->v.object.count))
        return NATS_INVALID_ARG;

    if (key != NULL)
        *key = json->v.object.names[idx];
    if (value != NULL)
        *value = json->v.object.values[idx];
    return NATS_OK;
}

natsStatus
natsJSON_Lookup(const natsJSON *json, const char *key, natsJSON **out)
{
    natsStatus s = natsJSON_Field(json, key, out);

    if (s != NATS_OK)
        return s;
    if ((*out)->type == NATS_JSON_NULL)
        return NATS_NOT_FOUND;
    return NATS_OK;
}

natsStatus
natsJSON_GetStr(const natsJSON *json, const char *key, char **out)
{
    natsJSON  *field = NULL;
    natsStatus s;

    if (out == NULL)
        return NATS_INVALID_ARG;

    s = natsJSON_Lookup(json, key, &field);
    if (s != NATS_OK)
        return s;
    if (field->type != NATS_JSON_STRING)
        return NATS_INVALID_ARG;

    *out = NATS_STRDUP(field->v.str);
    return (*out == NULL) ? NATS_NO_MEMORY : NATS_OK;
}

natsStatus
natsJSON_GetStrRef(const natsJSON *json, const char *key, const char **out)
{
    natsJSON  *field = NULL;
    natsStatus s;

    if (out == NULL)
        return NATS_INVALID_ARG;

    s = natsJSON_Lookup(json, key, &field);
    if (s != NATS_OK)
        return s;
    if (field->type != NATS_JSON_STRING)
        return NATS_INVALID_ARG;

    *out = field->v.str;
    return NATS_OK;
}

natsStatus
natsJSON_TakeStr(natsJSON *json, const char *key, char **out)
{
    natsJSON  *field = NULL;
    natsStatus s;

    if (out == NULL)
        return NATS_INVALID_ARG;

    s = natsJSON_Lookup(json, key, &field);
    if (s != NATS_OK)
        return s;
    if (field->type != NATS_JSON_STRING)
        return NATS_INVALID_ARG;

    *out = _takeStrNode(field);
    return NATS_OK;
}

natsStatus
natsJSON_GetBool(const natsJSON *json, const char *key, bool *out)
{
    natsJSON  *field = NULL;
    natsStatus s;

    if (out == NULL)
        return NATS_INVALID_ARG;

    s = natsJSON_Lookup(json, key, &field);
    if (s != NATS_OK)
        return s;

    return natsJSON_AsBool(field, out);
}

natsStatus
natsJSON_GetNumber(const natsJSON *json, const char *key, double *out)
{
    natsJSON  *field = NULL;
    natsStatus s;

    if (out == NULL)
        return NATS_INVALID_ARG;

    s = natsJSON_Lookup(json, key, &field);
    if (s != NATS_OK)
        return s;

    return natsJSON_AsNumber(field, out);
}

natsStatus
natsJSON_GetInt(const natsJSON *json, const char *key, int64_t *out)
{
    natsJSON  *field = NULL;
    natsStatus s;

    if (out == NULL)
        return NATS_INVALID_ARG;

    s = natsJSON_Lookup(json, key, &field);
    if (s != NATS_OK)
        return s;

    return natsJSON_AsInt(field, out);
}

natsStatus
natsJSON_GetUInt(const natsJSON *json, const char *key, uint64_t *out)
{
    natsJSON  *field = NULL;
    natsStatus s;

    if (out == NULL)
        return NATS_INVALID_ARG;

    s = natsJSON_Lookup(json, key, &field);
    if (s != NATS_OK)
        return s;

    return natsJSON_AsUInt(field, out);
}

// Every element is type-checked before anything is allocated or moved.
static natsStatus
_strArray(natsJSON *json, const char *key, char ***out, int *count, bool take)
{
    natsJSON  *field = NULL;
    char     **arr;
    int        n;
    int        i;
    natsStatus s;

    if ((out == NULL) || (count == NULL))
        return NATS_INVALID_ARG;

    s = natsJSON_Lookup(json, key, &field);
    if (s != NATS_OK)
        return s;
    if (field->type != NATS_JSON_ARRAY)
        return NATS_INVALID_ARG;

    n = field->v.array.count;
    for (i = 0; i < n; i++)
    {
        if (field->v.array.items[i]->type != NATS_JSON_STRING)
            return NATS_INVALID_ARG;
    }

    if (n <= 0)
    {
        *out   = NULL;
        *count = 0;
        return NATS_OK;
    }

    arr = (char **) NATS_CALLOC((size_t) n, sizeof(char *));
    if (arr == NULL)
        return NATS_NO_MEMORY;

    for (i = 0; i < n; i++)
    {
        natsJSON *elem = field->v.array.items[i];

        arr[i] = take ? _takeStrNode(elem) : NATS_STRDUP(elem->v.str);
        if (arr[i] == NULL)
        {
            for (i = 0; i < n; i++)
                NATS_FREE(arr[i]);
            NATS_FREE(arr);
            return NATS_NO_MEMORY;
        }
    }

    *out   = arr;
    *count = n;
    return NATS_OK;
}

natsStatus
natsJSON_GetStrArray(const natsJSON *json, const char *key, char ***out, int *count)
{
    return _strArray((natsJSON *) json, key, out, count, false);
}

natsStatus
natsJSON_TakeStrArray(natsJSON *json, const char *key, char ***out, int *count)
{
    return _strArray(json, key, out, count, true);
}

int
natsJSON_ArraySize(const natsJSON *json)
{
    if ((json == NULL) || (json->type != NATS_JSON_ARRAY))
        return 0;
    return json->v.array.count;
}

natsStatus
natsJSON_ArrayGet(const natsJSON *json, int idx, natsJSON **out)
{
    if ((json == NULL) || (out == NULL) || (json->type != NATS_JSON_ARRAY))
        return NATS_INVALID_ARG;
    if ((idx < 0) || (idx >= json->v.array.count))
        return NATS_INVALID_ARG;

    *out = json->v.array.items[idx];
    return NATS_OK;
}

//
// Serialization.
//

// Appends 's' as a quoted JSON string; bytes >= 0x20 other than quote and
// backslash pass through untouched.
static natsStatus
_writeQuoted(natsBuffer *b, const char *s)
{
    natsStatus  st  = NATS_OK;
    const char *run = s; // start of the current escape-free span
    const char *p   = s;

    if ((b == NULL) || (s == NULL))
        return NATS_INVALID_ARG;

    st = natsBuf_AppendByte(b, '"');
    for (; (*p != '\0') && (st == NATS_OK); p++)
    {
        unsigned char c   = (unsigned char) *p;
        const char   *esc = NULL;
        char          tmp[8];

        switch (c)
        {
            case '"':  esc = "\\\""; break;
            case '\\': esc = "\\\\"; break;
            case '\b': esc = "\\b"; break;
            case '\f': esc = "\\f"; break;
            case '\n': esc = "\\n"; break;
            case '\r': esc = "\\r"; break;
            case '\t': esc = "\\t"; break;
            default:
                if (c < 0x20)
                {
                    snprintf(tmp, sizeof(tmp), "\\u%04x", c);
                    esc = tmp;
                }
                break;
        }

        if (esc == NULL)
            continue;

        if (p > run)
            st = natsBuf_Append(b, run, (int) (p - run));
        if (st == NATS_OK)
            st = natsBuf_Append(b, esc, (int) strlen(esc));
        run = p + 1;
    }

    if ((st == NATS_OK) && (p > run))
        st = natsBuf_Append(b, run, (int) (p - run));
    if (st == NATS_OK)
        st = natsBuf_AppendByte(b, '"');
    return st;
}

static natsStatus
_writeLit(natsBuffer *b, const char *s)
{
    return natsBuf_Append(b, s, (int) strlen(s));
}

//
// Writer.
//

natsStatus
natsJSONWriter_Init(natsJSONWriter *w, natsBuffer *buf)
{
    if (w == NULL)
        return NATS_INVALID_ARG;

    memset(w, 0, sizeof(*w));
    if (buf == NULL)
    {
        w->st = NATS_INVALID_ARG;
        return NATS_INVALID_ARG;
    }

    w->buf = buf;
    return NATS_OK;
}

natsStatus
natsJSONWriter_Status(const natsJSONWriter *w)
{
    if (w == NULL)
        return NATS_INVALID_ARG;
    return w->st;
}

natsStatus
natsJSONWriter_Fail(natsJSONWriter *w, natsStatus st)
{
    if (w == NULL)
        return NATS_INVALID_ARG;
    if ((w->st == NATS_OK) && (st != NATS_OK))
        w->st = st;
    return w->st;
}

// Emits the separator, the quoted key and its colon.
static natsStatus
_writeKey(natsJSONWriter *w, const char *key)
{
    if (key == NULL)
    {
        w->st = NATS_INVALID_ARG;
        return w->st;
    }

    if (w->needComma && (w->st == NATS_OK))
        w->st = natsBuf_AppendByte(w->buf, ',');
    if (w->st == NATS_OK)
        w->st = _writeQuoted(w->buf, key);
    if (w->st == NATS_OK)
        w->st = natsBuf_AppendByte(w->buf, ':');

    return w->st;
}

// Rejects a NULL writer and makes the call a no-op after a failure.
#define WRITER_READY(w)          \
    if ((w) == NULL)             \
        return NATS_INVALID_ARG; \
    if ((w)->st != NATS_OK)      \
        return (w)->st

// Emits the opening brace and enters the new object.
static natsStatus
_openObject(natsJSONWriter *w)
{
    w->st = natsBuf_AppendByte(w->buf, '{');
    if (w->st == NATS_OK)
    {
        w->open      = true;
        w->needComma = false;
    }
    return w->st;
}

natsStatus
natsJSONWriter_StartObject(natsJSONWriter *w)
{
    WRITER_READY(w);

    // A writer produces exactly one object.
    if (w->open || w->needComma)
    {
        w->st = NATS_ERR;
        return w->st;
    }

    return _openObject(w);
}

natsStatus
natsJSONWriter_EndObject(natsJSONWriter *w)
{
    WRITER_READY(w);

    if (!w->open)
    {
        w->st = NATS_ERR;
        return w->st;
    }

    w->st = natsBuf_AppendByte(w->buf, '}');
    if (w->st == NATS_OK)
    {
        w->open      = false;
        w->needComma = true;
    }
    return w->st;
}

natsStatus
natsJSONWriter_AddStr(natsJSONWriter *w, const char *key, const char *val)
{
    WRITER_READY(w);

    if (_writeKey(w, key) != NATS_OK)
        return w->st;

    w->st = _writeQuoted(w->buf, (val != NULL ? val : ""));
    if (w->st == NATS_OK)
        w->needComma = true;
    return w->st;
}

natsStatus
natsJSONWriter_AddBool(natsJSONWriter *w, const char *key, bool val)
{
    WRITER_READY(w);

    if (_writeKey(w, key) != NATS_OK)
        return w->st;

    w->st = _writeLit(w->buf, val ? "true" : "false");
    if (w->st == NATS_OK)
        w->needComma = true;
    return w->st;
}

// Appends a number member from its formatted text; 'n' is snprintf's return.
static natsStatus
_addNumber(natsJSONWriter *w, const char *key, const char *text, int n, int cap)
{
    WRITER_READY(w);

    if (_writeKey(w, key) != NATS_OK)
        return w->st;

    if ((n < 0) || (n >= cap))
    {
        w->st = NATS_ERR;
        return w->st;
    }

    w->st = natsBuf_Append(w->buf, text, n);
    if (w->st == NATS_OK)
        w->needComma = true;
    return w->st;
}

natsStatus
natsJSONWriter_AddInt(natsJSONWriter *w, const char *key, int64_t val)
{
    char tmp[32];
    int  n = snprintf(tmp, sizeof(tmp), "%" PRId64, val);

    return _addNumber(w, key, tmp, n, (int) sizeof(tmp));
}

natsStatus
natsJSONWriter_AddUInt(natsJSONWriter *w, const char *key, uint64_t val)
{
    char tmp[32];
    int  n = snprintf(tmp, sizeof(tmp), "%" PRIu64, val);

    return _addNumber(w, key, tmp, n, (int) sizeof(tmp));
}

natsStatus
natsJSONWriter_AddStrArray(natsJSONWriter *w, const char *key, const char *const *vals, int count)
{
    int i;

    WRITER_READY(w);

    if (_writeKey(w, key) != NATS_OK)
        return w->st;

    w->st = natsBuf_AppendByte(w->buf, '[');
    for (i = 0; (vals != NULL) && (i < count) && (w->st == NATS_OK); i++)
    {
        if (i > 0)
            w->st = natsBuf_AppendByte(w->buf, ',');
        if (w->st == NATS_OK)
            w->st = _writeQuoted(w->buf, (vals[i] != NULL ? vals[i] : ""));
    }
    if (w->st == NATS_OK)
        w->st = natsBuf_AppendByte(w->buf, ']');
    if (w->st == NATS_OK)
        w->needComma = true;
    return w->st;
}

#undef WRITER_READY
