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

// Internal to nats-sysclient. Not installed. Cross-file internals use the
// `sysclient_` prefix (`natsSys_` belongs to utils/src/os_shims.h).

#ifndef NATS_SYSCLIENT_P_H_
#define NATS_SYSCLIENT_P_H_

#include "sysclient.h"

#include "json.h"
#include "unmarshal.h"

#include <nats/nats.h>
#include <stddef.h>
#include <string.h>

// Request subjects; %s is a server ID or "PING".
#define SYS_SUBJ_VARZ    "$SYS.REQ.SERVER.%s.VARZ"
#define SYS_SUBJ_STATSZ  "$SYS.REQ.SERVER.%s.STATSZ"
#define SYS_SUBJ_CONNZ   "$SYS.REQ.SERVER.%s.CONNZ"
#define SYS_SUBJ_SUBSZ   "$SYS.REQ.SERVER.%s.SUBSZ"
#define SYS_SUBJ_HEALTHZ "$SYS.REQ.SERVER.%s.HEALTHZ"
#define SYS_SUBJ_JSZ     "$SYS.REQ.SERVER.%s.JSZ"

#define SYS_PING_TARGET "PING"

#define IFOK(s, c) \
    if (s == NATS_OK) { s = (c); }

struct __natsSysClient
{
    natsConnection *nc; // borrowed
    int             serverCount;
    int64_t         stallInterval;
};

// Every endpoint is a request subject, an options marshaller, and a response
// laid out as {Server; payload; Error} with a parser and releaser for the
// payload. sysclient.c does the envelope work for all six through this
// descriptor.

// Serializes an endpoint's options (NULL meaning defaults) into 'buf'.
typedef natsStatus (*sysMarshalFn)(natsBuffer *buf, const void *opts);

typedef struct
{
    const char  *Subject;
    const char  *PayloadKey; // "data", or "statsz" for STATSZ
    size_t       RespSize;
    size_t       ServerOff;
    size_t       ErrorOff;
    size_t       PayloadOff;
    sysMarshalFn Marshal;
    sysParseFn   ParsePayload;
    sysFreeFn    FreePayload;

} sysEndpoint;

#define SYS_ENDPOINT(respType, payloadMember, subj, key, marshal, parse, freep) \
    {                                                                          \
        (subj), (key), sizeof(respType), offsetof(respType, Server),           \
        offsetof(respType, Error), offsetof(respType, payloadMember),          \
        (marshal), (parse), (freep)                                            \
    }

// Requests one server and decodes the reply into a new response; *newResp is
// NULL on error. NATS_NO_RESPONDERS is reported as NATS_NOT_FOUND.
natsStatus
sysclient_request(void **newResp, natsSysClient *client, const char *serverID,
                  const void *opts, int64_t timeout, const sysEndpoint *ep);

// As sysclient_request, but scatters to every server. Timing out is normal
// termination (NATS_OK, possibly with *count == 0); NATS_NO_RESPONDERS is
// an error. One undecodable reply fails the whole batch.
natsStatus
sysclient_ping(void ***resps, int *count, natsSysClient *client, const void *opts,
               int64_t timeout, const sysEndpoint *ep);

// Releases a response; untyped so it doubles as a sysDestroyFn.
void
sysclient_destroyResp(void *resp, const void *ep);

typedef void (*sysDestroyFn)(void *elem, const void *ctx);

// Releases an array and its elements and zeroes both out-params.
void
sysclient_freeList(void ***items, int *count, sysDestroyFn destroy, const void *ctx);

// Page walks. The loop that drives one (deadline, offset arithmetic, and the
// empty-page guard orbit.go's per-server iterators lack) lives in
// sysclient.c; an endpoint describes its pages by offset and supplies code
// only for fetching a page and copying its options.

typedef bool (*sysPageHandler)(void *page, void *closure);

// offsetof() a member of exactly 'type's width; anything else fails to compile.
#define SYS_INT_OFF(st, fld) \
    (offsetof(st, fld) + 0 * sizeof(char[(sizeof(((st *) 0)->fld) == sizeof(int)) ? 1 : -1]))

typedef struct
{
    const sysEndpoint *Endpoint;
    size_t             OptsSize;
    size_t             OffsetOff; // SYS_INT_OFF of the options' Offset
    size_t             CountOff;  // SYS_INT_OFF of a page's element count
    size_t             TotalOff;  // SYS_INT_OFF of the total a page reports

    // Deep-copies 'src' (NULL meaning defaults) into the zeroed 'dst'; on
    // failure 'dst' must still be safe to hand to FreeOptions.
    natsStatus (*CopyOptions)(void *dst, const void *src);
    void (*FreeOptions)(void *opts);

    // Requests one page with 'opts' (NULL meaning defaults) at 'offset'.
    natsStatus (*Fetch)(void **page, natsSysClient *client, const char *serverID,
                        const void *opts, int offset, int64_t timeout);

    // Whether 'opts' (NULL meaning defaults) pages; NULL means always.
    bool (*Paged)(const void *opts);

} sysWalkOps;

// Defines `_sink` and `_deliver`, the sysPageHandler that re-types the
// caller's page handler. One per endpoint file.
#define SYS_WALK_SINK(handlerType, respType)     \
    typedef struct                               \
    {                                            \
        handlerType handler;                     \
        void       *closure;                     \
                                                 \
    } _sink;                                     \
                                                 \
    static bool                                  \
    _deliver(void *page, void *closure)          \
    {                                            \
        _sink *sink = (_sink *) closure;         \
                                                 \
        return sink->handler((respType *) page, sink->closure); \
    }

// One walk over one server's pages; the public natsSys*Walk types are tags
// for this.
typedef struct
{
    const sysWalkOps *ops;
    natsSysClient    *client;   // borrowed
    char             *serverID;
    void             *opts;     // deep copy
    void             *first;    // the ping's page, until delivered
    int               total;    // from the first page, never refreshed
    int               offset;   // of the next page
    bool              done;

} sysWalk;

// Walks every page on one server; one timeout covers the whole walk.
natsStatus
sysclient_walkEach(natsSysClient *client, const char *serverID, const void *opts,
                   int64_t timeout, sysPageHandler handler, void *closure,
                   const sysWalkOps *ops);

// Pings every server and turns each reply into a walk; a reply without a
// server ID is rejected with NATS_ERR. On failure *walks is NULL.
natsStatus
sysclient_pingEach(void ***walks, int *count, natsSysClient *client, const void *opts,
                   int64_t timeout, const sysWalkOps *ops);

// Delivers the ping's page, then every remaining page by ID.
natsStatus
sysclient_walkRun(sysWalk *walk, int64_t timeout, sysPageHandler handler, void *closure);

const char *
sysclient_walkID(const sysWalk *walk);

// A sysDestroyFn; 'ctx' is unused.
void
sysclient_walkDestroy(void *walk, const void *ctx);

// Parsers for the shared types in sysclient.h. Each takes the object whose
// keys hold the members, so the flattened JSZ forms use the same parsers.

natsStatus
sysclient_parseJetStreamStats(void *dst, natsJSON *node);

natsStatus
sysclient_parseJetStreamConfig(void *dst, natsJSON *node);

void
sysclient_freeJetStreamConfig(void *dst);

natsStatus
sysclient_parseJetStreamVarz(void *dst, natsJSON *node);

void
sysclient_freeJetStreamVarz(void *dst);

natsStatus
sysclient_parseMetaClusterInfo(void *dst, natsJSON *node);

void
sysclient_freeMetaClusterInfo(void *dst);

natsStatus
sysclient_parseSlowConsumersStats(void *dst, natsJSON *node);

natsStatus
sysclient_parseSubDetail(void *dst, natsJSON *node);

void
sysclient_freeSubDetail(void *dst);

// Option copying, for walks that outlive the caller's option strings. The
// public option structs declare their strings const; a walk's copy owns them.
natsStatus
sysclient_dupOptStr(const char **dst, const char *src);

void
sysclient_freeOptStr(const char **str);

natsStatus
sysclient_copyEventFilter(natsSysEventFilterOptions *dst,
                          const natsSysEventFilterOptions *src);

void
sysclient_freeEventFilter(natsSysEventFilterOptions *filter);

// The body of every natsSys*Options_Init.
static inline natsStatus
sysclient_initOpts(void *opts, size_t size)
{
    if (opts == NULL)
        return NATS_INVALID_ARG;

    memset(opts, 0, size);
    return NATS_OK;
}

#endif /* NATS_SYSCLIENT_P_H_ */
