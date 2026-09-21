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
// Cross-translation-unit internals use the `sysclient_` prefix, following
// kv-codec's `kvcodec_` convention. Note that `natsSys_<Verb>` is NOT
// available as a naming shape: utils/src/os_shims.h already owns it
// (natsSys_GetHomeDir, natsSys_RunCommand, ...) and orbit_utils links into
// the same binary.

#ifndef NATS_SYSCLIENT_P_H_
#define NATS_SYSCLIENT_P_H_

#include "sysclient.h"

#include "json.h"
#include "unmarshal.h"

#include <nats/nats.h>
#include <stddef.h>
#include <string.h>

// Request subjects. The single %s is replaced either with a server ID or with
// the literal "PING" to scatter to the whole cluster.
#define SYS_SUBJ_VARZ    "$SYS.REQ.SERVER.%s.VARZ"
#define SYS_SUBJ_STATSZ  "$SYS.REQ.SERVER.%s.STATSZ"
#define SYS_SUBJ_CONNZ   "$SYS.REQ.SERVER.%s.CONNZ"
#define SYS_SUBJ_SUBSZ   "$SYS.REQ.SERVER.%s.SUBSZ"
#define SYS_SUBJ_HEALTHZ "$SYS.REQ.SERVER.%s.HEALTHZ"
#define SYS_SUBJ_JSZ     "$SYS.REQ.SERVER.%s.JSZ"

// The wildcard substituted into a subject to reach every server.
#define SYS_PING_TARGET "PING"

#define IFOK(s, c) \
    if (s == NATS_OK) { s = (c); }

struct __natsSysClient
{
    natsConnection *nc; // borrowed
    int             serverCount;
    int64_t         stallInterval;
};

//
// Endpoints.
//
// Every endpoint is the same shape: a request subject, an options marshaller,
// and a response laid out as {Server; <payload>; Error;} with a parser and a
// releaser for the payload. One descriptor per endpoint carries that shape,
// and sysclient.c does the envelope work — allocate, decode, release — for
// all six, so a change to the envelope contract is made in one place.
//

/** Serializes an endpoint's request options into 'buf'.
 *
 * 'opts' is the endpoint's own options type and may be NULL, meaning defaults;
 * each implementation re-types it on its first line.
 */
typedef natsStatus (*sysMarshalFn)(natsBuffer *buf, const void *opts);

typedef struct
{
    const char  *Subject;    ///< One of the SYS_SUBJ_* templates.
    const char  *PayloadKey; ///< "data" for every endpoint but STATSZ ("statsz").
    size_t       RespSize;   ///< sizeof the endpoint's response struct.
    size_t       ServerOff;  ///< offsetof its natsSysServerInfo member.
    size_t       ErrorOff;   ///< offsetof its natsSysAPIError member.
    size_t       PayloadOff; ///< offsetof its payload member.
    sysMarshalFn Marshal;
    sysParseFn   ParsePayload; ///< Decodes the payload object into the zeroed member.
    sysFreeFn    FreePayload;  ///< Releases the payload member's contents.

} sysEndpoint;

// Fills a descriptor for a response type laid out as {Server; payload; Error;}.
#define SYS_ENDPOINT(respType, payloadMember, subj, key, marshal, parse, freep) \
    {                                                                          \
        (subj), (key), sizeof(respType), offsetof(respType, Server),           \
        offsetof(respType, Error), offsetof(respType, payloadMember),          \
        (marshal), (parse), (freep)                                            \
    }

// Marshals the options, sends the request to one server and decodes the reply
// into a freshly allocated response; *newResp is NULL on error.
//
// Returns NATS_INVALID_ARG for a NULL or empty serverID or a negative timeout,
// NATS_NOT_FOUND when nobody answered (see the remap comment at the request
// call in sysclient.c), or the underlying transport status.
natsStatus
sysclient_request(void **newResp, natsSysClient *client, const char *serverID,
                  const void *opts, int64_t timeout, const sysEndpoint *ep);

// As sysclient_request, but scatters to every server and decodes every reply
// into a freshly allocated array, one entry per server that answered.
//
// The gather ends on the first of: the configured server count, the stall
// interval elapsing with no new reply, or the timeout. Timeout expiry is
// normal termination, not an error, so a gather that times out having heard
// nothing returns NATS_OK with *count == 0. NATS_NO_RESPONDERS — nobody
// subscribed to the subject at all — is a genuine error and propagates. If any
// reply fails to decode the whole batch is released and the error returned.
natsStatus
sysclient_ping(void ***resps, int *count, natsSysClient *client, const void *opts,
               int64_t timeout, const sysEndpoint *ep);

// Releases one response, including the response object itself. 'ep' is the
// endpoint's descriptor; the argument is untyped so this doubles as a
// sysDestroyFn. NULL is a no-op.
void
sysclient_destroyResp(void *resp, const void *ep);

/** Releases one element of a list, including the element itself. */
typedef void (*sysDestroyFn)(void *elem, const void *ctx);

// Releases an array of elements and zeroes both out-params. A NULL 'destroy'
// releases the array alone, for elements that need no cleanup of their own.
void
sysclient_freeList(void ***items, int *count, sysDestroyFn destroy, const void *ctx);

//
// Page walks.
//
// CONNZ, SUBSZ and JSZ page their results. The loop that drives a walk — the
// deadline, the offset arithmetic, and the empty-page guard that deliberately
// deviates from orbit.go (whose per-server iterators can spin forever on a
// shrinking result set) — lives once, in sysclient.c. An endpoint describes
// its pages as data where an offset will do, and supplies a typed function
// only where real code differs: fetching a page and copying its options.
//

/** Delivers one page to the caller; returns false to stop the walk. */
typedef bool (*sysPageHandler)(void *page, void *closure);

// offsetof() a member of exactly the given type's width, rejected at compile
// time for any other, so a walk table cannot silently read four bytes of an
// int64_t.
#define SYS_OFF_OF(st, fld, type) \
    (offsetof(st, fld) + 0 * sizeof(char[(sizeof(((st *) 0)->fld) == sizeof(type)) ? 1 : -1]))
#define SYS_INT_OFF(st, fld)  SYS_OFF_OF(st, fld, int)
#define SYS_BOOL_OFF(st, fld) SYS_OFF_OF(st, fld, bool)

// For PagedOff: the endpoint always pages.
#define SYS_ALWAYS_PAGED ((size_t) -1)

typedef struct
{
    const sysEndpoint *Endpoint;
    size_t             OptsSize;  ///< sizeof the endpoint's options struct.
    size_t             OffsetOff; ///< SYS_INT_OFF of the options' Offset member.
    size_t             CountOff;  ///< SYS_INT_OFF of a page's element count.
    size_t             TotalOff;  ///< SYS_INT_OFF of the total a page reports.

    /** Deep-copies 'src' (NULL meaning defaults) into the zeroed 'dst'. On
     * failure 'dst' holds only owned or NULL strings, so FreeOptions is safe. */
    natsStatus (*CopyOptions)(void *dst, const void *src);

    /** Releases the members of an options copy, not the struct itself. */
    void (*FreeOptions)(void *opts);

    /** Requests one page: a shallow copy of 'opts' (NULL meaning defaults)
     * with its offset replaced. */
    natsStatus (*Fetch)(void **page, natsSysClient *client, const char *serverID,
                        const void *opts, int offset, int64_t timeout);

    /** SYS_BOOL_OFF of the option that asks for a paged result, or
     * SYS_ALWAYS_PAGED. JSZ pages only over account details, so without
     * Accounts there is exactly one page whatever it reports. */
    size_t PagedOff;

} sysWalkOps;

// Defines the trampoline that re-types an endpoint's page handler for the
// driver: a `_sink` holding the caller's handler and closure, and `_deliver`,
// the sysPageHandler that unpacks it. One per endpoint file.
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

// One walk over one server's pages. The public natsSys*Walk types are never
// completed: a walk *is* one of these, and the endpoint's pointer type is only
// a tag telling the caller which _Run to hand it to.
typedef struct
{
    const sysWalkOps *ops;
    natsSysClient    *client;   // borrowed; must outlive the walk
    char             *serverID; // owned
    void             *opts;     // owned deep copy, so the caller may drop its strings
    void             *first;    // owned until delivered by the first _Run
    int               total;    // taken from the first page, never refreshed
    int               offset;   // offset of the next page to request
    bool              done;

} sysWalk;

// Walks every page on one server, delivering each to 'handler'. The options
// are borrowed for the duration of the call. One timeout budget covers the
// whole walk.
natsStatus
sysclient_walkEach(natsSysClient *client, const char *serverID, const void *opts,
                   int64_t timeout, sysPageHandler handler, void *closure,
                   const sysWalkOps *ops);

// Scatters the first page to every server and turns each reply into a walk.
// On failure no walk survives and *walks is NULL. An empty gather is not an
// error: it yields an empty list. A reply that did not name its sender is
// rejected with NATS_ERR, since every later page is fetched by ID.
natsStatus
sysclient_pingEach(void ***walks, int *count, natsSysClient *client, const void *opts,
                   int64_t timeout, const sysWalkOps *ops);

// Continues a walk: delivers the page the ping fetched, then every remaining
// page by ID. Returns NATS_OK at once for a finished walk.
natsStatus
sysclient_walkRun(sysWalk *walk, int64_t timeout, sysPageHandler handler, void *closure);

const char *
sysclient_walkID(const sysWalk *walk);

// A sysDestroyFn for walks; 'ctx' is unused.
void
sysclient_walkDestroy(void *walk, const void *ctx);

//
// Parsers for the shared types declared in sysclient.h. The rule is simply
// that whatever sysclient.h declares, sysclient.c decodes — endpoint files own
// only their own types.
//
// Each takes the object whose keys hold the members, so the JetStream stats
// parser serves both the nested VARZ form and the flattened JSZ one.
//

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

//
// Option copying.
//
// A *PingEach walk outlives the call that created it, so it cannot keep
// borrowing the caller's option strings — the caller is free to drop them as
// soon as the call returns. These deep-copy the parts of an options struct
// that are not plain scalars.
//

// Duplicates a string into a member the public option structs declare const.
// To a caller those strings are inputs, which is why they are const there; a
// walk's copy owns its strings, and this pair is the only place that
// ownership is expressed. A NULL 'src' yields NULL; on failure *dst is NULL.
// (The string-array equivalent stays local to sysclient.c: only the event
// filter has an array member, and only sysclient.c copies it.)
natsStatus
sysclient_dupOptStr(const char **dst, const char *src);

void
sysclient_freeOptStr(const char **str);

natsStatus
sysclient_copyEventFilter(natsSysEventFilterOptions *dst,
                          const natsSysEventFilterOptions *src);

void
sysclient_freeEventFilter(natsSysEventFilterOptions *filter);

// The body of every natsSys*Options_Init: reject a NULL out-param, otherwise
// zero the struct. Each public function stays a real symbol rather than a macro
// expansion so it remains greppable and steppable; only the shared policy --
// which status a NULL gets, and that "default" means "zeroed" -- lives here.
static inline natsStatus
sysclient_initOpts(void *opts, size_t size)
{
    if (opts == NULL)
        return NATS_INVALID_ARG;

    memset(opts, 0, size);
    return NATS_OK;
}

#endif /* NATS_SYSCLIENT_P_H_ */
