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

#ifndef NATS_SYSCLIENT_JSZ_H_
#define NATS_SYSCLIENT_JSZ_H_

#include "sysclient.h"

#include <nats/nats.h>
#include <stdbool.h>
#include <stdint.h>

#if defined(__cplusplus)
extern "C" {
#endif

/** \defgroup natsSysJszGroup JSZ
 *
 * JetStream state, from `$SYS.REQ.SERVER.<id>.JSZ`. The result is paginated
 * over accounts, and a walk pages only when #natsSysJszOptions.Accounts is
 * set and #natsSysJszOptions.Account is not; otherwise it is a single page.
 * @{
 */

/** \brief The Raft group backing one consumer. */
typedef struct __natsSysRaftGroupDetail
{
    char *Name;
    char *RaftGroup;

} natsSysRaftGroupDetail;

/** \brief One stream.
 *
 * The `*JSON` members carry the server's JSON verbatim: they describe
 * JetStream assets this library has no model for, and nothing is dropped.
 */
typedef struct __natsSysStreamDetail
{
    char *Name;
    char *Created; ///< RFC 3339.

    char *ClusterJSON; ///< `NULL` when absent.
    char *ConfigJSON;  ///< `NULL` when absent.
    char *StateJSON;   ///< `NULL` when absent.
    char *MirrorJSON;  ///< `NULL` when absent.

    char **ConsumerJSON; ///< Elements of `consumer_detail`.
    int    ConsumerJSONCount;
    char **SourcesJSON;  ///< Elements of `sources`.
    int    SourcesJSONCount;

    char *RaftGroup; ///< Wire key `stream_raft_group`.

    natsSysRaftGroupDetail **ConsumerRaftGroups;
    int                      ConsumerRaftGroupsCount;

} natsSysStreamDetail;

/** \brief JetStream details for one account. */
typedef struct __natsSysAccountDetail
{
    char                  *Name;
    char                  *Id;
    natsSysJetStreamStats  JetStreamStats; ///< Flattened into the object on the wire.
    natsSysStreamDetail   *Streams;        ///< Wire key `stream_detail`.
    int                    StreamsCount;

} natsSysAccountDetail;

/** \brief One page of a server's JetStream state. */
typedef struct __natsSysJSInfo
{
    char *ID;  ///< Wire key `server_id`.
    char *Now; ///< RFC 3339.

    bool                   Disabled;
    natsSysJetStreamConfig Config;
    natsSysJetStreamStats  JetStreamStats; ///< Flattened; `Accounts` is the pagination total.

    int      Streams;
    int      Consumers;
    uint64_t Messages;
    uint64_t Bytes;

    natsSysMetaClusterInfo *Meta; ///< Wire key `meta_cluster`; `NULL` when absent.

    natsSysAccountDetail **AccountDetails;
    int                    AccountDetailsCount;

} natsSysJSInfo;

/** \brief A JSZ response. Check `Error.Code` before reading #JSInfo. */
typedef struct __natsSysJszResp
{
    natsSysServerInfo Server;
    natsSysJSInfo     JSInfo; ///< Wire key `data`.
    natsSysAPIError   Error;

} natsSysJszResp;

/** \brief Responses gathered by #natsSysClient_JszPing; see
 * #natsSysHealthzRespList. */
typedef struct __natsSysJszRespList
{
    natsSysJszResp **Resps;
    int              Count;

} natsSysJszRespList;

/** \brief JSZ request options; a zero-valued field is left out. */
typedef struct __natsSysJszOptions
{
    const char *Account;   ///< Report only this account; the server then ignores paging.

    bool Accounts;         ///< Include account details, and enable pagination.
    bool Streams;          ///< Include stream details; implies #Accounts server-side.
    bool Consumer;         ///< Include consumer details; implies #Streams server-side.
    bool Config;           ///< Include stream and consumer configuration.
    bool LeaderOnly;       ///< Only the meta leader answers.
    int  Offset;
    int  Limit;
    bool RaftGroups;       ///< Wire key `raft`.
    bool StreamLeaderOnly; ///< Only report streams this server leads.

    natsSysEventFilterOptions Filter;

} natsSysJszOptions;

/** \brief Called once per page of a walk; see #natsSysConnzPageHandler. */
typedef bool (*natsSysJszPageHandler)(const natsSysJszResp *page, void *closure);

/** \brief A resumable pagination over one server. */
typedef struct __natsSysJszWalk natsSysJszWalk;

/** \brief One walk per server that answered a ping. */
typedef struct __natsSysJszWalkList
{
    natsSysJszWalk **Walks;
    int              Count;

} natsSysJszWalkList;

/** \brief Initializes options to their defaults (all unset). */
NATS_EXTERN natsStatus
natsSysJszOptions_Init(natsSysJszOptions *opts);

/** \brief Requests JetStream state from one server; see
 * #natsSysClient_Healthz. */
NATS_EXTERN natsStatus
natsSysClient_Jsz(natsSysJszResp **newResp, natsSysClient *client,
                  const char *serverID, const natsSysJszOptions *opts, int64_t timeout);

/** \brief Requests JetStream state from every server; see
 * #natsSysClient_HealthzPing. */
NATS_EXTERN natsStatus
natsSysClient_JszPing(natsSysJszRespList *list, natsSysClient *client,
                      const natsSysJszOptions *opts, int64_t timeout);

/** \brief Walks every page of account details on one server; see
 * #natsSysClient_ConnzEach. Without #natsSysJszOptions.Accounts this is a
 * single request, as it is with #natsSysJszOptions.Account. */
NATS_EXTERN natsStatus
natsSysClient_JszEach(natsSysClient *client, const char *serverID,
                      const natsSysJszOptions *opts, int64_t timeout,
                      natsSysJszPageHandler handler, void *closure);

/** \brief Pings every server and returns one walk per reply; see
 * #natsSysClient_ConnzPingEach. */
NATS_EXTERN natsStatus
natsSysClient_JszPingEach(natsSysJszWalkList *list, natsSysClient *client,
                          const natsSysJszOptions *opts, int64_t timeout);

/** \brief Returns the ID of the server a walk covers. */
NATS_EXTERN const char *
natsSysJszWalk_ServerID(const natsSysJszWalk *walk);

/** \brief Runs a walk to completion; see #natsSysConnzWalk_Run. */
NATS_EXTERN natsStatus
natsSysJszWalk_Run(natsSysJszWalk *walk, int64_t timeout,
                   natsSysJszPageHandler handler, void *closure);

/** \brief Destroys the contents of a walk list, not the list itself. */
NATS_EXTERN void
natsSysJszWalkList_Destroy(natsSysJszWalkList *list);

/** \brief Destroys a response; `NULL` is a no-op. */
NATS_EXTERN void
natsSysJszResp_Destroy(natsSysJszResp *resp);

/** \brief Destroys the contents of a list, not the list itself. */
NATS_EXTERN void
natsSysJszRespList_Destroy(natsSysJszRespList *list);

/** @} */ // end natsSysJszGroup

#if defined(__cplusplus)
}
#endif

#endif /* NATS_SYSCLIENT_JSZ_H_ */
