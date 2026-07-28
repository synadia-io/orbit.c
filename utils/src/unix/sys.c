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

#include <fcntl.h>
#include <pwd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

natsStatus
natsSys_GetHomeDir(char **homeDir)
{
    const char *home = getenv("HOME");

    *homeDir = NULL;

    if (nats_IsStringEmpty(home))
    {
        struct passwd *pw = getpwuid(getuid());

        if (pw != NULL)
            home = pw->pw_dir;
    }

    if (nats_IsStringEmpty(home))
        return NATS_ERR;

    *homeDir = NATS_STRDUP(home);
    return (*homeDir == NULL) ? NATS_NO_MEMORY : NATS_OK;
}

bool
natsSys_IsPathSep(char c)
{
    return (c == '/');
}

bool
natsSys_IsAbsPath(const char *path)
{
    return (!nats_IsStringEmpty(path)) && (path[0] == '/');
}

natsStatus
natsSys_RunCommand(const char *const *args, char **out, int *exitCode)
{
    natsStatus s         = NATS_OK;
    natsBuffer buf       = NATS_EMPTY_BUFFER;
    int        outFds[2] = {-1, -1};
    int        errFds[2] = {-1, -1};
    pid_t      pid       = -1;
    int        execErrno = 0;
    int        status    = 0;

    if ((args == NULL) || (args[0] == NULL) || (out == NULL) || (exitCode == NULL))
        return NATS_INVALID_ARG;

    *out      = NULL;
    *exitCode = 0;

    if (pipe(outFds) != 0)
        return NATS_ERR;

    // A failed exec has no other way to reach the parent: the child reports
    // errno over this second pipe, which a successful exec closes instead,
    // leaving the parent's read empty.
    if ((pipe(errFds) != 0) ||
        (fcntl(errFds[0], F_SETFD, FD_CLOEXEC) != 0) ||
        (fcntl(errFds[1], F_SETFD, FD_CLOEXEC) != 0))
    {
        s = NATS_ERR;
        goto done;
    }

    s = natsBuf_Init(&buf, 512);
    if (s != NATS_OK)
        goto done;

    pid = fork();
    if (pid < 0)
    {
        s = NATS_ERR;
        goto done;
    }

    if (pid == 0)
    {
        // Child: join stdout and stderr onto the pipe, then exec. Only
        // async-signal-safe calls are made between the fork and the exec.
        close(outFds[0]);
        if ((dup2(outFds[1], STDOUT_FILENO) >= 0) && (dup2(outFds[1], STDERR_FILENO) >= 0))
        {
            close(outFds[1]);
            execvp(args[0], (char *const *) args);
        }
        execErrno = errno;
        (void) write(errFds[1], &execErrno, sizeof(execErrno));
        _exit(127);
    }

    close(outFds[1]);
    outFds[1] = -1;
    close(errFds[1]);
    errFds[1] = -1;

    // Drain the output before waiting on anything else: a child that filled
    // the pipe would otherwise block writing while we blocked reading.
    for (;;)
    {
        char    chunk[512];
        ssize_t n = read(outFds[0], chunk, sizeof(chunk));

        if (n > 0)
        {
            s = natsBuf_Append(&buf, chunk, (int) n);
            if (s != NATS_OK)
                break;
            continue;
        }
        if ((n < 0) && (errno == EINTR))
            continue;
        if (n < 0)
            s = NATS_ERR;
        break;
    }

    // The child is done writing by now, so this cannot block.
    if (read(errFds[0], &execErrno, sizeof(execErrno)) != (ssize_t) sizeof(execErrno))
        execErrno = 0;

    while ((waitpid(pid, &status, 0) < 0) && (errno == EINTR))
        ;

    if (s == NATS_OK)
    {
        if (execErrno != 0)
            s = (execErrno == ENOENT) ? NATS_NOT_FOUND : NATS_ERR;
        else if (!WIFEXITED(status))
            s = NATS_ERR; // killed by a signal
    }

    if (s == NATS_OK)
        s = natsBuf_AppendByte(&buf, '\0');

    if (s == NATS_OK)
    {
        *out = NATS_STRDUP(natsBuf_Data(&buf));
        if (*out == NULL)
            s = NATS_NO_MEMORY;
        else
            *exitCode = WEXITSTATUS(status);
    }

done:
    natsBuf_Destroy(&buf);
    if (outFds[0] != -1)
        close(outFds[0]);
    if (outFds[1] != -1)
        close(outFds[1]);
    if (errFds[0] != -1)
        close(errFds[0]);
    if (errFds[1] != -1)
        close(errFds[1]);

    return s;
}
