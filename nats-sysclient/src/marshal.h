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
// Request options are marshalled with utils' natsJSONWriter. The sysclient_opt*
// helpers omit a field whose value is empty; CONNZ sends every one of its own
// fields and SUBSZ sends offset, limit and subscriptions unconditionally, so
// those use the plain natsJSONWriter_Add* calls. The difference is
// server-visible.

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

// A NULL entry poisons the writer with NATS_INVALID_ARG.
natsStatus
sysclient_optStrArray(natsJSONWriter *w, const char *key, const char *const *vals, int count);

// Appends the server-filter keys, flattened, to the open object on 'w'.
natsStatus
sysclient_writeEventFilter(natsJSONWriter *w, const natsSysEventFilterOptions *filter);

// Marshals a request whose only content is the server filter.
natsStatus
sysclient_marshalFilterOnly(natsBuffer *buf, const natsSysEventFilterOptions *filter);

#endif /* NATS_SYSCLIENT_MARSHAL_H_ */
