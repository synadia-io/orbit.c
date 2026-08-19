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

// Sends a request to one server and waits for its reply.
//
// 'subjFmt' is one of the SYS_SUBJ_* templates. A 'timeout' of 0 selects
// NATS_SYS_DEFAULT_REQUEST_TIMEOUT. On success *replyMsg is the reply, which
// the caller destroys.
//
// Returns NATS_INVALID_ARG for a NULL or empty serverID, NATS_NOT_FOUND when
// nobody answered (see the remap comment at the call site), or the underlying
// transport status.
natsStatus
sysclient_requestByID(natsMsg **replyMsg, natsSysClient *client, const char *serverID,
                      const char *subjFmt, const char *payload, int payloadLen,
                      int64_t timeout);

// Scatters a request to every server and gathers the replies.
//
// The gather ends on the first of: the configured server count, the stall
// interval elapsing with no new reply, or the timeout. Timeout expiry is
// normal termination, not an error, so a gather that times out having heard
// nothing returns NATS_OK with an empty list. NATS_NO_RESPONDERS — nobody
// subscribed to the subject at all — is a genuine error and propagates.
// Always destroy *list with natsMsgList_Destroy unless the return was
// NATS_INVALID_ARG.
natsStatus
sysclient_pingServers(natsMsgList *list, natsSysClient *client, const char *subjFmt,
                      const char *payload, int payloadLen, int64_t timeout);

// Splits a response envelope into its parts. 'payloadKey' is "data" for every
// endpoint except STATSZ, which uses "statsz".
//
// *payload borrows a node owned by 'root' and is set to NULL when the key is
// absent. 'server' and 'apiErr' are filled in place; the caller releases them
// with sysclient_freeServerInfo / sysclient_freeAPIError.
natsStatus
sysclient_parseEnvelope(natsSysServerInfo *server, natsSysAPIError *apiErr,
                        natsJSON **payload, natsJSON *root, const char *payloadKey);

// Decodes a whole reply into an already-allocated, zeroed response.
//
// The caller passes pointers to its own members rather than offsets, so this
// stays type-safe at the call site while the envelope handling lives in one
// place. Returns the status already mapped through sysclient_responseStatus().
natsStatus
sysclient_decodeResp(natsMsg *msg, natsSysServerInfo *server, natsSysAPIError *apiErr,
                     void *payloadDst, const char *payloadKey, sysParseFn parsePayload);

/** Builds one fully-owned response from a reply; *newResp is NULL on error. */
typedef natsStatus (*sysRespFromMsgFn)(void **newResp, natsMsg *msg);

/** Destroys a response, including the response object itself. */
typedef void (*sysRespDestroyFn)(void *resp);

/** Serializes an endpoint's request options into 'buf'.
 *
 * 'opts' is the endpoint's own options type and may be NULL, meaning defaults;
 * each implementation re-types it on its first line. This mirrors sysParseFn
 * and sysRespFromMsgFn, which erase the type for the same reason.
 */
typedef natsStatus (*sysMarshalFn)(natsBuffer *buf, const void *opts);

// Marshals the options, sends the request to one server and decodes the reply.
//
// 'bufHint' is the initial payload buffer size — big enough that the common
// request never has to grow it.
natsStatus
sysclient_request(void **newResp, natsSysClient *client, const char *serverID,
                  const char *subjFmt, sysMarshalFn marshal, const void *opts,
                  int bufHint, int64_t timeout, sysRespFromMsgFn fromMsg);

// As sysclient_request, but scatters to every server; see sysclient_pingList
// for how the gather terminates.
natsStatus
sysclient_ping(void ***resps, int *count, natsSysClient *client, const char *subjFmt,
               sysMarshalFn marshal, const void *opts, int bufHint, int64_t timeout,
               sysRespFromMsgFn fromMsg, sysRespDestroyFn destroyResp);

// Releases an array of responses and zeroes both out-params.
void
sysclient_freeRespList(void ***resps, int *count, sysRespDestroyFn destroyResp);

// Scatters a request and decodes every reply into a freshly allocated array.
//
// Shared by all six *Ping calls: only the response type differs, and that is
// carried by the two callbacks. If any reply fails to decode the whole batch
// is released and the error returned.
natsStatus
sysclient_pingList(void ***resps, int *count, natsSysClient *client, const char *subjFmt,
                   const char *payload, int payloadLen, int64_t timeout,
                   sysRespFromMsgFn fromMsg, sysRespDestroyFn destroyResp);

void
sysclient_freeServerInfo(natsSysServerInfo *server);

void
sysclient_freeAPIError(natsSysAPIError *apiErr);

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

natsStatus
sysclient_dupStr(char **dst, const char *src);

// Copies the server ID a walk must be pinned to, rejecting a reply that did not
// name its sender. Only the walk builders need this: a VARZ or HEALTHZ reply
// without an ID is still perfectly usable, so the check does not belong in the
// shared ping plumbing.
natsStatus
sysclient_walkServerID(char **dst, const char *id);

// The same, for members the public option structs declare const. To a caller
// those strings are inputs, which is why they are const there; a walk's copy
// owns its strings, and these two are the only places that ownership is
// expressed. (The string-array equivalents stay local to sysclient.c: only the
// event filter has an array member, and only sysclient.c copies it.)

natsStatus
sysclient_dupOptStr(const char **dst, const char *src);

void
sysclient_freeOptStr(const char **str);

natsStatus
sysclient_copyEventFilter(natsSysEventFilterOptions *dst,
                          const natsSysEventFilterOptions *src);

void
sysclient_freeEventFilter(natsSysEventFilterOptions *filter);

// Milliseconds from a monotonic clock, for whole-walk deadlines. Matches how
// nats-extra/src/requestmany.c measures its own budget.
int64_t
sysclient_nowMs(void);

// The longest timeout honoured, in milliseconds. Seven days is far beyond any
// real monitoring request, and bounding it keeps every millisecond value this
// library hands downwards small enough to survive being turned into a deadline:
// both cnats and nats-extra compute `now + timeout` into an int64, which
// overflows for values near INT64_MAX.
#define SYSCLIENT_MAX_TIMEOUT_MS ((int64_t) 7 * 24 * 60 * 60 * 1000)

// Applied at every point a caller's timeout crosses into cnats or nats-extra —
// the two request funnels and the walk deadline below — so INT64_MAX means
// "no practical limit" everywhere rather than only inside a page loop.
int64_t
sysclient_capTimeout(int64_t timeout);

// A whole-walk deadline 'timeout' milliseconds from now, capped as above.
int64_t
sysclient_deadline(int64_t timeout);

// Maps an accessor-level failure onto the status the public API documents.
//
// A payload that is not an object, or a key holding a value of the wrong type,
// surfaces as NATS_INVALID_ARG from the json.h accessors. Report that as
// NATS_ERR (malformed response); NATS_INVALID_ARG stays reserved for a bad
// argument to the natsSysClient_* call itself. Mirrors
// nats-context/src/context.c:135-141.
static inline natsStatus
sysclient_responseStatus(natsStatus s)
{
    if ((s != NATS_OK) && (s != NATS_NO_MEMORY))
        return NATS_ERR;
    return s;
}

#endif /* NATS_SYSCLIENT_P_H_ */
