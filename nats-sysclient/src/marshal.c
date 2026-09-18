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

#include "marshal.h"

#include "os_shims.h"

natsStatus
sysclient_optStr(natsJSONWriter *w, const char *key, const char *val)
{
    if (nats_IsStringEmpty(val))
        return natsJSONWriter_Status(w);
    return natsJSONWriter_AddStr(w, key, val);
}

natsStatus
sysclient_optBool(natsJSONWriter *w, const char *key, bool val)
{
    if (!val)
        return natsJSONWriter_Status(w);
    return natsJSONWriter_AddBool(w, key, val);
}

natsStatus
sysclient_optInt(natsJSONWriter *w, const char *key, int64_t val)
{
    if (val == 0)
        return natsJSONWriter_Status(w);
    return natsJSONWriter_AddInt(w, key, val);
}

natsStatus
sysclient_optStrArray(natsJSONWriter *w, const char *key, const char *const *vals, int count)
{
    int i;

    if ((vals == NULL) || (count <= 0))
        return natsJSONWriter_Status(w);

    // A NULL entry is a caller bug (a partly filled array, say), not an empty
    // value: the writer would emit "" for it, which no server matches, and
    // the request would silently select nothing. Poison the writer so the
    // call fails with NATS_INVALID_ARG before anything is sent.
    for (i = 0; i < count; i++)
    {
        if (vals[i] == NULL)
            return natsJSONWriter_Fail(w, NATS_INVALID_ARG);
    }
    return natsJSONWriter_AddStrArray(w, key, vals, count);
}

natsStatus
sysclient_writeEventFilter(natsJSONWriter *w, const natsSysEventFilterOptions *filter)
{
    if (filter == NULL)
        return natsJSONWriter_Status(w);

    sysclient_optStr(w, "server_name", filter->Name);
    sysclient_optStr(w, "cluster", filter->Cluster);
    sysclient_optStr(w, "host", filter->Host);
    sysclient_optStrArray(w, "tags", (const char *const *) filter->Tags, filter->TagsCount);
    sysclient_optStr(w, "domain", filter->Domain);

    return natsJSONWriter_Status(w);
}

natsStatus
sysclient_marshalFilterOnly(natsBuffer *buf, const natsSysEventFilterOptions *filter)
{
    natsJSONWriter w;

    natsJSONWriter_Init(&w, buf);
    natsJSONWriter_StartObject(&w);

    sysclient_writeEventFilter(&w, filter);

    natsJSONWriter_EndObject(&w);
    return natsJSONWriter_Status(&w);
}
