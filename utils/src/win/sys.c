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
