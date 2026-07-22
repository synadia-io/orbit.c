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

#include <pwd.h>
#include <stdlib.h>
#include <sys/types.h>
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
