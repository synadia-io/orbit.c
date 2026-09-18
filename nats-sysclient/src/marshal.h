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

// Internal to nats-sysclient. Not installed.
//
// Request options are marshalled with utils' natsJSONWriter. The helpers here
// omit a field whose value is empty ("" / NULL / false / 0 / an empty array)
// rather than emitting it.
//
// Which helper a field uses is the whole encoding contract, so it is worth
// being deliberate: five of the six option structs omit their unset fields and
// use sysclient_opt*(), while CONNZ always sends every one of its own fields
// and so uses the plain natsJSONWriter_Add*() calls, emitting `"sort":""` and
// `"state":0` on an otherwise empty request. That difference is visible to the
// server, so it is preserved rather than tidied away.

#ifndef NATS_SYSCLIENT_MARSHAL_H_
#define NATS_SYSCLIENT_MARSHAL_H_

#include "sysclient.h"

#include "json.h"

#include <nats/nats.h>
#include <stdbool.h>
#include <stdint.h>

natsStatus
sysclient_optStr(natsJSONWriter *w, const char *key, const char *val);

natsStatus
sysclient_optBool(natsJSONWriter *w, const char *key, bool val);

natsStatus
sysclient_optInt(natsJSONWriter *w, const char *key, int64_t val);

// Rejects a NULL entry with NATS_INVALID_ARG, poisoning the writer.
natsStatus
sysclient_optStrArray(natsJSONWriter *w, const char *key, const char *const *vals, int count);

// Appends the server-filter members to the object already open on 'w'.
//
// The filter keys are flattened into the request object rather than nested.
// Passing NULL appends nothing.
natsStatus
sysclient_writeEventFilter(natsJSONWriter *w, const natsSysEventFilterOptions *filter);

// Marshals a whole request whose only content is the server filter.
natsStatus
sysclient_marshalFilterOnly(natsBuffer *buf, const natsSysEventFilterOptions *filter);

#endif /* NATS_SYSCLIENT_MARSHAL_H_ */
