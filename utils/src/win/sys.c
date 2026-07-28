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

#include "../os_shims.h"
#include "../buf.h"

#include <stdio.h>
#include <stdlib.h>

natsStatus
natsSys_GetHomeDir(char **homeDir)
{
    const char *home = getenv("USERPROFILE");

    *homeDir = NULL;

    if (nats_IsStringEmpty(home))
    {
        const char *drive = getenv("HOMEDRIVE");
        const char *path = getenv("HOMEPATH");
        size_t len = 0;

        if (nats_IsStringEmpty(drive) || nats_IsStringEmpty(path))
            return NATS_ERR;

        len = strlen(drive) + strlen(path) + 1;
        *homeDir = (char *) NATS_MALLOC(len);
        if (*homeDir == NULL)
            return NATS_NO_MEMORY;

        snprintf(*homeDir, len, "%s%s", drive, path);
        return NATS_OK;
    }

    *homeDir = NATS_STRDUP(home);
    return (*homeDir == NULL) ? NATS_NO_MEMORY : NATS_OK;
}

bool
natsSys_IsPathSep(char c)
{
    return ((c == '/') || (c == '\\'));
}

// Mirrors Go's filepath.IsAbs on Windows: drive-relative ("C:foo") and
// rooted ("\foo") paths are not considered absolute.
bool
natsSys_IsAbsPath(const char *path)
{
    if (nats_IsStringEmpty(path))
        return false;

    // UNC or device path, e.g. \\host\share or \\?\C:\...
    if ((path[0] == '\\' || path[0] == '/') && (path[1] == '\\' || path[1] == '/'))
        return true;

    // Drive letter followed by a separator, e.g. C:\ or C:/
    return (((path[0] >= 'A' && path[0] <= 'Z') || (path[0] >= 'a' && path[0] <= 'z'))
            && (path[1] == ':')
            && (path[2] == '\\' || path[2] == '/'));
}

// Appends 'arg' to a command line, quoted the way CommandLineToArgvW (and so
// the child's CRT) splits it back apart: backslashes are literal except in the
// run that precedes a quote, where each must be doubled.
static natsStatus
_appendQuotedArg(natsBuffer *buf, const char *arg)
{
    natsStatus  s = NATS_OK;
    const char *p = arg;

    if ((arg[0] != '\0') && (strpbrk(arg, " \t\n\v\"") == NULL))
        return natsBuf_Append(buf, arg, (int) strlen(arg));

    s = natsBuf_AppendByte(buf, '"');

    while ((s == NATS_OK) && (*p != '\0'))
    {
        size_t slashes = 0;
        size_t i;

        while (*p == '\\')
        {
            slashes++;
            p++;
        }

        // The run runs into a quote — either one from 'arg' or the closing one
        // appended below — so it has to be doubled.
        if ((*p == '"') || (*p == '\0'))
            slashes *= 2;

        for (i = 0; (s == NATS_OK) && (i < slashes); i++)
            s = natsBuf_AppendByte(buf, '\\');

        if ((s == NATS_OK) && (*p == '"'))
            s = natsBuf_AppendByte(buf, '\\');

        if ((s == NATS_OK) && (*p != '\0'))
        {
            s = natsBuf_AppendByte(buf, *p);
            p++;
        }
    }

    if (s == NATS_OK)
        s = natsBuf_AppendByte(buf, '"');

    return s;
}

natsStatus
natsSys_RunCommand(const char *const *args, char **out, int *exitCode)
{
    natsStatus          s       = NATS_OK;
    natsBuffer          cmdLine = NATS_EMPTY_BUFFER;
    natsBuffer          buf     = NATS_EMPTY_BUFFER;
    SECURITY_ATTRIBUTES sa;
    STARTUPINFOA        si;
    PROCESS_INFORMATION pi;
    HANDLE              rd      = NULL;
    HANDLE              wr      = NULL;
    DWORD               code    = 0;
    int                 i;

    if ((args == NULL) || (args[0] == NULL) || (out == NULL) || (exitCode == NULL))
        return NATS_INVALID_ARG;

    *out      = NULL;
    *exitCode = 0;

    // CreateProcess takes a single command line, so the vector is joined back
    // into one; with no lpApplicationName it also searches the PATH for us.
    s = natsBuf_Init(&cmdLine, 256);
    for (i = 0; (s == NATS_OK) && (args[i] != NULL); i++)
    {
        if (i > 0)
            s = natsBuf_AppendByte(&cmdLine, ' ');
        if (s == NATS_OK)
            s = _appendQuotedArg(&cmdLine, args[i]);
    }
    if (s == NATS_OK)
        s = natsBuf_AppendByte(&cmdLine, '\0');
    if (s == NATS_OK)
        s = natsBuf_Init(&buf, 512);
    if (s != NATS_OK)
        goto done;

    // stdout and stderr share the pipe's write end, which the child inherits;
    // the read end stays private to this process.
    sa.nLength              = sizeof(sa);
    sa.lpSecurityDescriptor = NULL;
    sa.bInheritHandle       = TRUE;

    if (!CreatePipe(&rd, &wr, &sa, 0))
    {
        s = NATS_ERR;
        goto done;
    }
    SetHandleInformation(rd, HANDLE_FLAG_INHERIT, 0);

    memset(&si, 0, sizeof(si));
    si.cb         = sizeof(si);
    si.dwFlags    = STARTF_USESTDHANDLES;
    si.hStdInput  = GetStdHandle(STD_INPUT_HANDLE);
    si.hStdOutput = wr;
    si.hStdError  = wr;
    memset(&pi, 0, sizeof(pi));

    if (!CreateProcessA(NULL, natsBuf_Data(&cmdLine), NULL, NULL, TRUE, 0, NULL, NULL, &si, &pi))
    {
        DWORD err = GetLastError();

        s = ((err == ERROR_FILE_NOT_FOUND) || (err == ERROR_PATH_NOT_FOUND))
                ? NATS_NOT_FOUND
                : NATS_ERR;
        goto done;
    }

    // Drop this process' copy of the write end, otherwise the read below never
    // sees the pipe break.
    CloseHandle(wr);
    wr = NULL;

    while (s == NATS_OK)
    {
        char  chunk[512];
        DWORD n = 0;

        if (!ReadFile(rd, chunk, sizeof(chunk), &n, NULL))
        {
            if (GetLastError() != ERROR_BROKEN_PIPE)
                s = NATS_ERR;
            break;
        }
        if (n == 0)
            break;

        s = natsBuf_Append(&buf, chunk, (int) n);
    }

    WaitForSingleObject(pi.hProcess, INFINITE);
    if ((s == NATS_OK) && !GetExitCodeProcess(pi.hProcess, &code))
        s = NATS_ERR;

    CloseHandle(pi.hThread);
    CloseHandle(pi.hProcess);

    if (s == NATS_OK)
        s = natsBuf_AppendByte(&buf, '\0');

    if (s == NATS_OK)
    {
        *out = NATS_STRDUP(natsBuf_Data(&buf));
        if (*out == NULL)
            s = NATS_NO_MEMORY;
        else
            *exitCode = (int) code;
    }

done:
    if (rd != NULL)
        CloseHandle(rd);
    if (wr != NULL)
        CloseHandle(wr);
    natsBuf_Destroy(&cmdLine);
    natsBuf_Destroy(&buf);

    return s;
}
