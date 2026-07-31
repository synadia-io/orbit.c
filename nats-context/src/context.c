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

#include "context.h"
#include "socks.h"
#include "os_shims.h"
#include "json.h"
#include "buf.h"

#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

#define IFOK(s, c)      if (s == NATS_OK) { s = (c); }

typedef struct __natsContext
{
    char *name;
    natsContextSettings *settings;
    char *path;

    char *nscCreds;
    char *nscUrl;

} natsContext;

static natsStatus
_natsContextSettings_Create(natsContextSettings **settings)
{
    *settings = (natsContextSettings *) NATS_CALLOC(1, sizeof(natsContextSettings));
    if (*settings == NULL)
        return NATS_NO_MEMORY;

    return NATS_OK;
}

void
natsContextSettings_Destroy(natsContextSettings *settings)
{
    if (settings == NULL)
        return;

    NATS_FREE(settings->Name);
    NATS_FREE(settings->Description);
    NATS_FREE(settings->URL);
    NATS_FREE(settings->SocksProxy);
    NATS_FREE(settings->Token);
    NATS_FREE(settings->User);
    NATS_FREE(settings->Password);
    NATS_FREE(settings->Creds);
    NATS_FREE(settings->Cert);
    NATS_FREE(settings->Key);
    NATS_FREE(settings->CA);
    NATS_FREE(settings->NSCLookup);
    NATS_FREE(settings->JSDomain);
    NATS_FREE(settings->JSAPIPrefix);
    NATS_FREE(settings->JSEventPrefix);
    NATS_FREE(settings->InboxPrefix);
    NATS_FREE(settings->UserJwt);
    NATS_FREE(settings->ColorScheme);
    NATS_FREE(settings->WinCertStoreType);
    NATS_FREE(settings->WinCertStoreMatchBy);
    NATS_FREE(settings->WinCertStoreMatch);
    for (int i = 0; i < settings->WinCertStoreCaMatchLen; i++)
    {
        NATS_FREE(settings->WinCertStoreCaMatch[i]);
    }
    NATS_FREE(settings->WinCertStoreCaMatch);

    NATS_FREE(settings);
}

static natsStatus
_optStr(natsJSON *j, const char *key, char **dst)
{
    natsStatus s = natsJSON_GetStr(j, key, dst);
    return (s == NATS_NOT_FOUND) ? NATS_OK : s;
}

static natsStatus
_optBool(natsJSON *j, const char *key, bool *dst)
{
    natsStatus s = natsJSON_GetBool(j, key, dst);
    return (s == NATS_NOT_FOUND) ? NATS_OK : s;
}

static natsStatus
_optStrArray(natsJSON *j, const char *key, char ***dst, int *len)
{
    natsStatus s = natsJSON_GetStrArray(j, key, dst, len);
    return (s == NATS_NOT_FOUND) ? NATS_OK : s;
}

static natsStatus
_unmarshalContextSettings(natsContextSettings **settings, natsJSON *j)
{
    natsStatus s = NATS_OK;

    IFOK(s, _optStr(j, "name", &(*settings)->Name));
    IFOK(s, _optStr(j, "description", &(*settings)->Description));
    IFOK(s, _optStr(j, "url", &(*settings)->URL));
    IFOK(s, _optStr(j, "socks_proxy", &(*settings)->SocksProxy));
    IFOK(s, _optStr(j, "token", &(*settings)->Token));
    IFOK(s, _optStr(j, "user", &(*settings)->User));
    IFOK(s, _optStr(j, "password", &(*settings)->Password));
    IFOK(s, _optStr(j, "creds", &(*settings)->Creds));
    IFOK(s, _optStr(j, "cert", &(*settings)->Cert));
    IFOK(s, _optStr(j, "key", &(*settings)->Key));
    IFOK(s, _optStr(j, "ca", &(*settings)->CA));
    IFOK(s, _optStr(j, "nsc", &(*settings)->NSCLookup));
    IFOK(s, _optStr(j, "jetstream_domain", &(*settings)->JSDomain));
    IFOK(s, _optStr(j, "jetstream_api_prefix", &(*settings)->JSAPIPrefix));
    IFOK(s, _optStr(j, "jetstream_event_prefix", &(*settings)->JSEventPrefix));
    IFOK(s, _optStr(j, "inbox_prefix", &(*settings)->InboxPrefix));
    IFOK(s, _optStr(j, "user_jwt", &(*settings)->UserJwt));
    IFOK(s, _optStr(j, "color_scheme", &(*settings)->ColorScheme));
    IFOK(s, _optBool(j, "tls_first", &(*settings)->TLSFirst));
    IFOK(s, _optStr(j, "windows_cert_store", &(*settings)->WinCertStoreType));
    IFOK(s, _optStr(j, "windows_cert_match_by", &(*settings)->WinCertStoreMatchBy));
    IFOK(s, _optStr(j, "windows_cert_match", &(*settings)->WinCertStoreMatch));
    IFOK(s, _optStrArray(j, "windows_ca_certs_match", &(*settings)->WinCertStoreCaMatch,
                         &(*settings)->WinCertStoreCaMatchLen));

    // A document that is not an object, or a key holding a value of the wrong
    // type, surfaces as NATS_INVALID_ARG from the accessors above. Report it as
    // the malformed-file error the API documents; NATS_INVALID_ARG is reserved
    // for a bad argument to natsContext_Connect itself.
    if ((s != NATS_OK) && (s != NATS_NO_MEMORY))
        s = NATS_ERR;

    return s;
}

// XDG_CONFIG_HOME if set, otherwise <home>/.config — on every platform,
// including Windows, to stay compatible with where the `nats` CLI stores its
// contexts.
static natsStatus
_context_parentDir(char **dir)
{
    const char *parent = getenv("XDG_CONFIG_HOME");
    char *home = NULL;
    natsStatus s = NATS_OK;
    size_t len = 0;

    *dir = NULL;

    if (!nats_IsStringEmpty(parent))
    {
        *dir = NATS_STRDUP(parent);
        return (*dir == NULL) ? NATS_NO_MEMORY : NATS_OK;
    }

    s = natsSys_GetHomeDir(&home);
    if (s != NATS_OK)
        return s;

    len = strlen(home) + sizeof(".config") + 1;
    *dir = (char *) NATS_MALLOC(len);
    if (*dir == NULL)
        s = NATS_NO_MEMORY;
    else
        snprintf(*dir, len, "%s%c.config", home, NATS_PATH_SEP);

    NATS_FREE(home);
    return s;
}

static natsStatus
_readFileContent(const char *path, char **content, long *size);

// Reads <config>/nats/<file> and returns its content with surrounding
// whitespace trimmed. A missing or unreadable file (or config dir) yields
// *content == NULL with NATS_OK, not an error; only allocation failures are
// reported.
static natsStatus
_context_readCtxFromFile(char **content, const char *file)
{
    natsStatus  s;
    char       *parent = NULL;
    char       *path   = NULL;
    char       *raw    = NULL;
    long        size   = 0;
    size_t      len    = 0;
    const char *start;
    const char *end;

    *content = NULL;

    s = _context_parentDir(&parent);
    if (s == NATS_NO_MEMORY)
        return s;
    if (s != NATS_OK)
        return NATS_OK;

    len  = strlen(parent) + strlen(file) + sizeof("nats") + 2;
    path = (char *) NATS_MALLOC(len);
    if (path == NULL)
    {
        NATS_FREE(parent);
        return NATS_NO_MEMORY;
    }
    snprintf(path, len, "%s%cnats%c%s", parent, NATS_PATH_SEP, NATS_PATH_SEP, file);
    NATS_FREE(parent);

    s = _readFileContent(path, &raw, &size);
    NATS_FREE(path);
    if (s == NATS_NO_MEMORY)
        return s;
    if (s != NATS_OK)
        return NATS_OK; // a missing/unreadable file is not an error here

    // Trim surrounding whitespace into the returned copy.
    start = raw;
    end   = raw + size;
    while ((start < end) && isspace((unsigned char) start[0]))
        start++;
    while ((end > start) && isspace((unsigned char) end[-1]))
        end--;

    len      = (size_t) (end - start);
    *content = (char *) NATS_MALLOC(len + 1);
    if (*content != NULL)
    {
        memcpy(*content, start, len);
        (*content)[len] = '\0';
    }

    NATS_FREE(raw);
    return (*content == NULL) ? NATS_NO_MEMORY : NATS_OK;
}

static bool
_isNameValid(const char *name)
{
    return (!nats_IsStringEmpty(name) && strchr(name, '/') == NULL &&
            strchr(name, '\\') == NULL && strstr(name, "..") == NULL);
}

static natsStatus
_createContextPath(char **path, const char *name)
{
    char *parent = NULL;
    size_t len = 0;
    natsStatus s = NATS_OK;

    if (!_isNameValid(name))
        return NATS_INVALID_ARG;

    s = _context_parentDir(&parent);
    if (s != NATS_OK)
        return s;

    len = strlen(parent) + strlen(name) + sizeof("nats") + sizeof("context") +
          sizeof(".json") + 2;
    *path = (char *) NATS_MALLOC(len);
    if (*path == NULL)
        s = NATS_NO_MEMORY;
    else
        snprintf(*path, len, "%s%cnats%ccontext%c%s.json", parent,
                 NATS_PATH_SEP, NATS_PATH_SEP, NATS_PATH_SEP, name);

    NATS_FREE(parent);
    return s;
}

// Reads the entire file at 'path' into a NUL-terminated, heap-allocated buffer
// and reports its byte length in *size.
static natsStatus
_readFileContent(const char *path, char **content, long *size)
{
    natsStatus s   = NATS_OK;
    FILE      *f   = NULL;
    char      *buf = NULL;
    long       sz  = 0;

    *content = NULL;
    *size    = 0;

    f = fopen(path, "rb");
    if (f == NULL)
        return NATS_ERR;

    if ((fseek(f, 0, SEEK_END) != 0) || ((sz = ftell(f)) < 0))
    {
        s = NATS_ERR;
        goto done;
    }
    rewind(f);

    buf = (char *) NATS_MALLOC((size_t) sz + 1);
    if (buf == NULL)
    {
        s = NATS_NO_MEMORY;
        goto done;
    }
    if (fread(buf, 1, (size_t) sz, f) != (size_t) sz)
    {
        s = NATS_ERR;
        goto done;
    }
    buf[sz] = '\0';

    *content = buf;
    *size    = sz;
    buf      = NULL;

done:
    fclose(f);
    NATS_FREE(buf);
    return s;
}

// The one-character variable names a shell recognises, and the characters that
// may appear in a bare $name reference.
static bool
_isShellSpecialVar(char c)
{
    switch (c)
    {
        case '*':
        case '#':
        case '$':
        case '@':
        case '!':
        case '?':
        case '-':
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
            return true;
        default:
            return false;
    }
}

static bool
_isAlphaNum(char c)
{
    return (c == '_') || ((c >= '0') && (c <= '9')) ||
           ((c >= 'a') && (c <= 'z')) || ((c >= 'A') && (c <= 'Z'));
}

// Given the text right after a '$', reports the referenced variable name via
// *name/*nameLen and returns the number of bytes consumed from 's'.
// nameLen == 0 with a positive return means invalid syntax
// to be dropped; nameLen == 0 with a zero return means the '$' was not followed
// by a name and should be kept verbatim.
static size_t
_getShellName(const char *s, size_t slen, const char **name, size_t *nameLen)
{
    size_t i = 0;

    *name    = s;
    *nameLen = 0;

    if (slen == 0)
        return 0;

    if (s[0] == '{')
    {
        // ${#} and similar single-character special vars.
        if (slen > 2 && _isShellSpecialVar(s[1]) && s[2] == '}')
        {
            *name    = s + 1;
            *nameLen = 1;
            return 3;
        }
        // Scan to the closing brace.
        for (i = 1; i < slen; i++)
        {
            if (s[i] == '}')
            {
                if (i == 1)
                    return 2; // "${}" — bad syntax, eat it
                *name    = s + 1;
                *nameLen = i - 1;
                return i + 1;
            }
        }
        return 1; // "${" with no closing brace — bad syntax, eat the brace
    }

    if (_isShellSpecialVar(s[0]))
    {
        *nameLen = 1;
        return 1;
    }

    // A bare $name: a run of alphanumerics/underscore.
    while (i < slen && _isAlphaNum(s[i]))
        i++;
    *nameLen = i;
    return i;
}

// Expands $VAR and ${VAR} references in 'src' against the process environment.
// Undefined variables expand to the empty string. On success *out is a newly
// allocated, NUL-terminated string owned by the caller.
static natsStatus
_expandEnv(const char *src, char **out)
{
    natsBuffer buf = NATS_EMPTY_BUFFER;
    natsStatus s;
    size_t     len = strlen(src);
    size_t     i   = 0;

    s = natsBuf_Init(&buf, (int) len + 1);
    if (s != NATS_OK)
        return s;

    while ((s == NATS_OK) && (i < len))
    {
        const char *name;
        size_t      nameLen;
        size_t      w;

        if ((src[i] != '$') || ((i + 1) >= len))
        {
            s = natsBuf_AppendByte(&buf, src[i]);
            i++;
            continue;
        }

        w = _getShellName(src + i + 1, len - i - 1, &name, &nameLen);
        if (nameLen == 0)
        {
            // A valid '$' not followed by a name is kept; invalid syntax
            // (w > 0) is dropped.
            if (w == 0)
                s = natsBuf_AppendByte(&buf, '$');
        }
        else
        {
            char *key = (char *) NATS_MALLOC(nameLen + 1);
            if (key == NULL)
            {
                s = NATS_NO_MEMORY;
                break;
            }
            memcpy(key, name, nameLen);
            key[nameLen] = '\0';

            const char *val = getenv(key);
            NATS_FREE(key);

            if (val != NULL)
                s = natsBuf_Append(&buf, val, (int) strlen(val));
        }

        i += w + 1; // consume the '$' and the reference
    }

    IFOK(s, natsBuf_AppendByte(&buf, '\0'));
    if (s == NATS_OK)
    {
        *out = NATS_STRDUP(natsBuf_Data(&buf));
        if (*out == NULL)
            s = NATS_NO_MEMORY;
    }

    natsBuf_Destroy(&buf);
    return s;
}

// Joins 'count' strings with commas into a newly allocated string. The inverse
// of _splitServers below.
static natsStatus
_joinServers(char **servers, int count, char **out)
{
    natsBuffer buf = NATS_EMPTY_BUFFER;
    natsStatus s;
    int        i;

    s = natsBuf_Init(&buf, 64);

    for (i = 0; (s == NATS_OK) && (i < count); i++)
    {
        if (i > 0)
            s = natsBuf_AppendByte(&buf, ',');
        IFOK(s, natsBuf_Append(&buf, servers[i], (int) strlen(servers[i])));
    }

    IFOK(s, natsBuf_AppendByte(&buf, '\0'));
    if (s == NATS_OK)
    {
        *out = NATS_STRDUP(natsBuf_Data(&buf));
        if (*out == NULL)
            s = NATS_NO_MEMORY;
    }

    natsBuf_Destroy(&buf);
    return s;
}

// Runs `nsc generate profile <nsc>` and records the user credentials and
// operator service URLs it reports. The program is run directly rather
// than through a shell, so the context's `nsc` value is passed
// as one argument instead of being interpolated into a command line.
static natsStatus
_context_resolveNscLookup(natsContext *ctx)
{
    natsStatus  s        = NATS_OK;
    natsJSON   *j        = NULL;
    natsJSON   *op       = NULL;
    char       *out      = NULL;
    char       *creds    = NULL;
    char      **services = NULL;
    size_t      len      = 0;
    int         count    = 0;
    int         code     = 0;
    int         i;
    const char *args[]   = { "nsc", "generate", "profile", NULL, NULL };

    if ((ctx == NULL) || (ctx->settings == NULL))
        return NATS_INVALID_ARG;
    if (nats_IsStringEmpty(ctx->settings->NSCLookup))
        return NATS_OK;

    args[3] = ctx->settings->NSCLookup;

    // NATS_NOT_FOUND here means 'nsc' is not on the PATH.
    s = natsSys_RunCommand(args, &out, &code);
    if (s != NATS_OK)
        return s;

    // nsc ran and failed. Its combined output is the diagnostic, but there is no
    // way to hand it back: nats_setError is private to nats.c.
    len = strlen(out);
    if ((code != 0) || (len > INT_MAX))
    {
        NATS_FREE(out);
        return NATS_ERR;
    }

    s = natsJSON_Parse(&j, out, (int) len);
    NATS_FREE(out);
    if (s != NATS_OK)
        return (s == NATS_NO_MEMORY) ? s : NATS_ERR; // unparsable nsc output

    // Absent keys are not an error; they leave their field NULL.
    s = _optStr(j, "user_creds", &creds);
    if (s == NATS_OK)
    {
        s = natsJSON_Field(j, "operator", &op);
        if (s == NATS_NOT_FOUND)
            s = NATS_OK;
        else if ((s == NATS_OK) && (natsJSON_Type(op) != NATS_JSON_NULL))
        {
            s = natsJSON_GetStrArray(op, "service", &services, &count);
            if (s == NATS_NOT_FOUND)
                s = NATS_OK;
        }
    }

    if ((s == NATS_OK) && !nats_IsStringEmpty(creds))
    {
        NATS_FREE(ctx->nscCreds);
        ctx->nscCreds = creds;
        creds         = NULL;
    }

    if ((s == NATS_OK) && (count > 0))
    {
        char *url = NULL;

        s = _joinServers(services, count, &url);
        if (s == NATS_OK)
        {
            NATS_FREE(ctx->nscUrl);
            ctx->nscUrl = url;
        }
    }

    for (i = 0; i < count; i++)
        NATS_FREE(services[i]);
    NATS_FREE(services);
    NATS_FREE(creds);
    natsJSON_Destroy(j);

    if ((s != NATS_OK) && (s != NATS_NO_MEMORY))
        s = NATS_ERR;

    return s;
}

static natsStatus
_context_loadActiveContext(natsContext *ctx)
{
    natsStatus s       = NATS_OK;
    natsJSON  *j       = NULL;
    char      *content = NULL;
    long       size    = 0;

    if (nats_IsStringEmpty(ctx->path))
    {
        if (nats_IsStringEmpty(ctx->name))
        {
            // ctx->name may be an allocated empty string; free it before
            // _context_readCtxFromFile overwrites the pointer.
            NATS_FREE(ctx->name);
            ctx->name = NULL;
            s = _context_readCtxFromFile(&ctx->name, "context.txt");
            if (s != NATS_OK)
                return s;

            // No name given and no selected context is not an error: the
            // settings stay at their zero values and the connection falls back
            // to nats.c's default server with no auth.
            if (nats_IsStringEmpty(ctx->name))
                return NATS_OK;
        }

        s = _createContextPath(&ctx->path, ctx->name);
        if (s != NATS_OK)
            return s;

        // Only a genuinely absent file is an unknown context; anything else
        // (an unreadable file, say) is reported by the read below.
        if (!natsSys_PathExists(ctx->path))
            return NATS_NOT_FOUND;
    }

    s = _readFileContent(ctx->path, &content, &size);
    if (s != NATS_OK)
        return s;

    if (size > INT_MAX)
    {
        NATS_FREE(content);
        return NATS_ERR;
    }

    s = natsJSON_Parse(&j, content, (int) size);
    NATS_FREE(content);
    if (s != NATS_OK)
        return s;

    s = _unmarshalContextSettings(&ctx->settings, j);
    natsJSON_Destroy(j);
    if (s != NATS_OK)
        return s;

    // Perform environment variable expansion for the path of the creds.
    if (!nats_IsStringEmpty(ctx->settings->Creds))
    {
        char *expanded = NULL;

        s = _expandEnv(ctx->settings->Creds, &expanded);
        if (s != NATS_OK)
            return s;

        NATS_FREE(ctx->settings->Creds);
        ctx->settings->Creds = expanded;
    }

    if (!nats_IsStringEmpty(ctx->settings->NSCLookup))
    {
        s = _context_resolveNscLookup(ctx);
        if (s != NATS_OK)
            return s;
    }

    return s;
}

// Replaces a leading '~' with the user's home directory. If 'path' does not
// start with '~', or the home directory cannot be resolved, a plain copy is
// returned. Only allocation failure is an error.
static natsStatus
_expandHomedir(const char *path, char **out)
{
    char  *home = NULL;
    char  *res  = NULL;
    size_t len  = 0;

    if (nats_IsStringEmpty(path) || path[0] != '~')
    {
        *out = NATS_STRDUP((path == NULL) ? "" : path);
        return (*out == NULL) ? NATS_NO_MEMORY : NATS_OK;
    }

    if (natsSys_GetHomeDir(&home) != NATS_OK)
    {
        *out = NATS_STRDUP(path);
        return (*out == NULL) ? NATS_NO_MEMORY : NATS_OK;
    }

    // <home> followed by 'path' with its leading '~' removed.
    len = strlen(home) + strlen(path + 1) + 1;
    res = (char *) NATS_MALLOC(len);
    if (res != NULL)
        snprintf(res, len, "%s%s", home, path + 1);

    NATS_FREE(home);
    if (res == NULL)
        return NATS_NO_MEMORY;

    *out = res;
    return NATS_OK;
}

// Splits a comma-separated server list into a heap array of heap strings, with
// surrounding whitespace trimmed and empty entries skipped. An empty list yields
// *out == NULL and *count == 0. Caller frees each entry and then the array.
static natsStatus
_splitServers(const char *url, char ***out, int *count)
{
    char      **arr = NULL;
    int         n   = 0;
    int         cap = 0;
    const char *p   = url;
    int         i;

    *out   = NULL;
    *count = 0;

    if (nats_IsStringEmpty(url))
        return NATS_OK;

    while (*p != '\0')
    {
        const char *start = p;
        const char *end;

        while ((*p != '\0') && (*p != ','))
            p++;
        end = p;

        while ((start < end) && isspace((unsigned char) start[0]))
            start++;
        while ((end > start) && isspace((unsigned char) end[-1]))
            end--;

        if (end > start)
        {
            char *entry;

            if (n == cap)
            {
                int    newCap = (cap == 0) ? 4 : cap * 2;
                char **grown  = (char **) NATS_REALLOC(arr, (size_t) newCap * sizeof(char *));
                if (grown == NULL)
                    goto fail;
                arr = grown;
                cap = newCap;
            }

            entry = (char *) NATS_MALLOC((size_t) (end - start) + 1);
            if (entry == NULL)
                goto fail;
            memcpy(entry, start, (size_t) (end - start));
            entry[end - start] = '\0';
            arr[n++] = entry;
        }

        if (*p == ',')
            p++;
    }

    *out   = arr;
    *count = n;
    return NATS_OK;

fail:
    for (i = 0; i < n; i++)
        NATS_FREE(arr[i]);
    NATS_FREE(arr);
    return NATS_NO_MEMORY;
}

// Translates the resolved context settings onto a nats.c natsOptions. Windows
// cert stores have no nats.c equivalent, so a context asking for one is rejected
// rather than silently connected without. An 'nkey' is ignored: nats.c needs the
// public key to put in the CONNECT, the context file holds only a path to the
// seed, and nats.c exposes no way to derive one from the other.
static natsStatus
_context_setOptions(natsOptions *opts, natsContextSettings *cfg)
{
    natsStatus s = NATS_OK;

    // An empty URL leaves nats.c to use its default server, and leaves any
    // servers the caller had set on 'opts' in place.
    if (!nats_IsStringEmpty(cfg->URL))
    {
        char **servers = NULL;
        int    count   = 0;
        int    i;

        s = _splitServers(cfg->URL, &servers, &count);
        if (s != NATS_OK)
            return s;

        if (count > 0)
            s = natsOptions_SetServers(opts, (const char **) servers, count);

        for (i = 0; i < count; i++)
            NATS_FREE(servers[i]);
        NATS_FREE(servers);

        if (s != NATS_OK)
            return s;
    }

    // Authentication is mutually exclusive: user/password, then creds.
    if (!nats_IsStringEmpty(cfg->User))
    {
        IFOK(s, natsOptions_SetUserInfo(opts, cfg->User, cfg->Password));
    }
    else if (!nats_IsStringEmpty(cfg->Creds))
    {
        char *creds = NULL;

        s = _expandHomedir(cfg->Creds, &creds);
        IFOK(s, natsOptions_SetUserCredentialsFromFiles(opts, creds, NULL));
        NATS_FREE(creds);
    }

    if ((s == NATS_OK) && !nats_IsStringEmpty(cfg->Token))
    {
        char *token = NULL;

        s = _expandHomedir(cfg->Token, &token);
        IFOK(s, natsOptions_SetToken(opts, token));
        NATS_FREE(token);
    }

    if ((s == NATS_OK) && !nats_IsStringEmpty(cfg->Cert) && !nats_IsStringEmpty(cfg->Key))
    {
        char *cert = NULL;
        char *key  = NULL;

        s = _expandHomedir(cfg->Cert, &cert);
        IFOK(s, _expandHomedir(cfg->Key, &key));
        IFOK(s, natsOptions_LoadCertificatesChain(opts, cert, key));
        // A context carrying TLS material means to require TLS, but the nats.c
        // loader only builds the SSL context: without this the connection stays
        // in plain text unless the server's INFO happens to demand TLS.
        IFOK(s, natsOptions_SetSecure(opts, true));
        NATS_FREE(cert);
        NATS_FREE(key);
    }

    if ((s == NATS_OK) && !nats_IsStringEmpty(cfg->CA))
    {
        char *ca = NULL;

        s = _expandHomedir(cfg->CA, &ca);
        IFOK(s, natsOptions_LoadCATrustedCertificates(opts, ca));
        IFOK(s, natsOptions_SetSecure(opts, true)); // as above
        NATS_FREE(ca);
    }

    // Every server in the context is then reached through the proxy: nats.c
    // hands the whole dial over to the handler, for the first connect and for
    // every reconnect after it.
    if ((s == NATS_OK) && !nats_IsStringEmpty(cfg->SocksProxy))
    {
        natsProxyConnHandler handler = NULL;
        void                *closure = NULL;

        s = natsSocks_ProxyHandler(cfg->SocksProxy, &handler, &closure);

        // A proxy URL the context file got wrong is malformed content like any
        // other, not a bad argument to natsContext_Connect.
        if (s == NATS_INVALID_ARG)
            return NATS_ERR;

        IFOK(s, natsOptions_SetProxyConnHandler(opts, handler, closure));
    }

    if ((s == NATS_OK) && !nats_IsStringEmpty(cfg->InboxPrefix))
    {
        IFOK(s, natsOptions_SetCustomInboxPrefix(opts, cfg->InboxPrefix));
    }

    if ((s == NATS_OK) && cfg->TLSFirst)
    {
        IFOK(s, natsOptions_TLSHandshakeFirst(opts));
    }

    if ((s == NATS_OK) && !nats_IsStringEmpty(cfg->WinCertStoreType))
        return NATS_ILLEGAL_STATE; // nats.c cannot read certificates from a Windows store

    return s;
}

natsStatus
natsContext_Connect(natsConnection **nc, natsContextSettings **settings,
                    const char *name, natsOptions *opts)
{
    natsContext ctx;
    natsConnection *conn = NULL;
    natsContextSettings *ctxSettings = NULL;
    natsOptions *effectiveOpts = NULL;
    bool ownOpts = false;
    natsStatus s = NATS_OK;

    if (nc == NULL)
    {
        if (settings != NULL)
            *settings = NULL;
        return NATS_INVALID_ARG;
    }

    // NULL and "" both mean "connect to the selected context".
    if (name == NULL)
        name = "";

    memset(&ctx, 0, sizeof(natsContext));

    if (natsSys_IsAbsPath(name))
    {
        // An absolute path names the context file directly, and the context
        // name is then never consulted (settings->Name comes from the file's
        // "name" key).
        ctx.path = NATS_STRDUP(name);
        if (ctx.path == NULL)
        {
            s = NATS_NO_MEMORY;
            goto done;
        }
    }
    else
    {
        ctx.name = NATS_STRDUP(name);
        if (ctx.name == NULL)
        {
            s = NATS_NO_MEMORY;
            goto done;
        }
    }

    s = _natsContextSettings_Create(&ctxSettings);
    if (s != NATS_OK)
        goto done;
    ctx.settings = ctxSettings;

    s = _context_loadActiveContext(&ctx);
    if (s != NATS_OK)
        goto done;

    // Build the connection options. A caller-supplied 'opts' is borrowed and
    // configured in place (context values are layered on top), and remains the
    // caller's to destroy; otherwise a private options object is created here
    // and destroyed below.
    effectiveOpts = opts;
    if (effectiveOpts == NULL)
    {
        s = natsOptions_Create(&effectiveOpts);
        if (s != NATS_OK)
            goto done;
        ownOpts = true;
    }

    s = _context_setOptions(effectiveOpts, ctx.settings);
    if (s != NATS_OK)
        goto done;

    // NATS_NOT_YET_CONNECTED is not a failure: 'opts' asked for
    // retry-on-failed-connect and nats.c hands back a live connection that keeps
    // trying in the background, so it is returned to the caller like NATS_OK.
    s = natsConnection_Connect(&conn, effectiveOpts);

done:
    if (ownOpts)
        natsOptions_Destroy(effectiveOpts);
    NATS_FREE(ctx.name);
    NATS_FREE(ctx.path);
    NATS_FREE(ctx.nscCreds);
    NATS_FREE(ctx.nscUrl);
    if ((s != NATS_OK) && (s != NATS_NOT_YET_CONNECTED))
    {
        natsContextSettings_Destroy(ctxSettings);
        natsConnection_Destroy(conn);
        ctxSettings = NULL;
        conn = NULL;
    }

    if (settings != NULL)
        *settings = ctxSettings;
    else
        natsContextSettings_Destroy(ctxSettings);

    *nc = conn;
    return s;
}
