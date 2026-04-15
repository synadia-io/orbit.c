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

// Internal parsing utilities for counter message bodies and headers.
//
// These functions are NOT part of the public API.

#ifndef NATS_COUNTERS_PARSER_H_
#define NATS_COUNTERS_PARSER_H_

#include <nats/nats.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// natsCounterSource is a node in the linked list produced by
// natsCounterParser_ParseSources().  It holds one (stream, subject, value)
// triple from the Nats-Counter-Sources JSON object.
//
// The stream, subject, and value fields are heap-allocated; the entire list
// is freed with natsCounterParser_FreeSources().
typedef struct natsCounterSource {
    char                      *stream;
    char                      *subject;
    char                      *value; // decimal string, arbitrary precision
    struct natsCounterSource  *next;

} natsCounterSource;

// natsCounterParser_ParseValue parses the counter message body.
//
// The body is a JSON object of the form: {"val":"<decimal>"}
//
// On success, *value is set to a heap-allocated null-terminated decimal
// string.  Caller must free() it.
natsStatus natsCounterParser_ParseValue(const unsigned char *data,
                                         int                  dataLen,
                                         char               **value);

// natsCounterParser_ParsePubAckValue extracts the counter total returned
// in the PubAck "val" field.
//
// ackVal is the raw string from the PubAck JSON (may be NULL when the
// stream does not have allow_msg_counter enabled).
// Returns NATS_NOT_FOUND when ackVal is NULL.
// On success, *value is heap-allocated; caller must free() it.
natsStatus natsCounterParser_ParsePubAckValue(const char  *ackVal,
                                               char       **value);

// natsCounterParser_ParseSources parses the Nats-Counter-Sources header.
//
// headerValue is the raw header string (may be NULL, producing an empty list).
// On success, *sources is set to the head of a linked list (possibly NULL if
// the object is empty).  Free with natsCounterParser_FreeSources().
natsStatus natsCounterParser_ParseSources(const char         *headerValue,
                                           natsCounterSource **sources);

// natsCounterParser_FreeSources releases the list returned by
// natsCounterParser_ParseSources().  Passing NULL is a no-op.
void natsCounterParser_FreeSources(natsCounterSource *sources);

// natsCounterParser_ParseIncrement parses the Nats-Incr header value.
//
// headerValue is the raw header string (may be NULL).
// Returns NATS_NOT_FOUND when headerValue is NULL.
// On success, *increment is heap-allocated; caller must free() it.
natsStatus natsCounterParser_ParseIncrement(const char  *headerValue,
                                             char       **increment);

#ifdef __cplusplus
}
#endif

#endif /* NATS_COUNTERS_PARSER_H_ */
