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

#include "connz.h"

#include "marshal.h"
#include "sysclientp.h"
#include "unmarshal.h"

#include "buf.h"
#include "os_shims.h"

#include <string.h>

struct __natsSysConnzWalk
{
    natsSysClient      *client;   // borrowed; must outlive the walk
    char               *serverID; // owned
    natsSysConnzOptions opts;     // deep copy, so the caller may drop its strings
    natsSysConnzResp   *first;    // owned until delivered by the first _Run
    int                 total;    // taken from the first page, never refreshed
    int                 offset;   // offset of the next page to request
    bool                done;
};

static const sysField _tlsPeerCertFields[] = {
    SYS_F(SYS_FLD_STR, natsSysTLSPeerCert, Subject, "subject"),
    SYS_F(SYS_FLD_STR, natsSysTLSPeerCert, SubjectPKISha256, "spki_sha256"),
    SYS_F(SYS_FLD_STR, natsSysTLSPeerCert, CertSha256, "cert_sha256"),
};

static const sysField _connInfoFields[] = {
    SYS_F(SYS_FLD_U64, natsSysConnInfo, Cid, "cid"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, Kind, "kind"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, Type, "type"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, IP, "ip"),
    SYS_F(SYS_FLD_INT, natsSysConnInfo, Port, "port"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, Start, "start"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, LastActivity, "last_activity"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, Stop, "stop"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, Reason, "reason"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, RTT, "rtt"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, Uptime, "uptime"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, Idle, "idle"),
    SYS_F(SYS_FLD_INT, natsSysConnInfo, Pending, "pending_bytes"),
    SYS_F(SYS_FLD_I64, natsSysConnInfo, InMsgs, "in_msgs"),
    SYS_F(SYS_FLD_I64, natsSysConnInfo, OutMsgs, "out_msgs"),
    SYS_F(SYS_FLD_I64, natsSysConnInfo, InBytes, "in_bytes"),
    SYS_F(SYS_FLD_I64, natsSysConnInfo, OutBytes, "out_bytes"),
    SYS_F(SYS_FLD_U32, natsSysConnInfo, NumSubs, "subscriptions"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, Name, "name"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, Lang, "lang"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, Version, "version"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, TLSVersion, "tls_version"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, TLSCipher, "tls_cipher_suite"),
    SYS_F(SYS_FLD_BOOL, natsSysConnInfo, TLSFirst, "tls_first"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, AuthorizedUser, "authorized_user"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, Account, "account"),
    SYS_FA(natsSysConnInfo, Subs, SubsCount, "subscriptions_list"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, JWT, "jwt"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, IssuerKey, "issuer_key"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, NameTag, "name_tag"),
    SYS_FA(natsSysConnInfo, Tags, TagsCount, "tags"),
    SYS_F(SYS_FLD_STR, natsSysConnInfo, MQTTClient, "mqtt_client"),
};

static const sysField _connzFields[] = {
    SYS_F(SYS_FLD_STR, natsSysConnz, ID, "server_id"),
    SYS_F(SYS_FLD_STR, natsSysConnz, Now, "now"),
    SYS_F(SYS_FLD_INT, natsSysConnz, NumConns, "num_connections"),
    SYS_F(SYS_FLD_INT, natsSysConnz, Total, "total"),
    SYS_F(SYS_FLD_INT, natsSysConnz, Offset, "offset"),
    SYS_F(SYS_FLD_INT, natsSysConnz, Limit, "limit"),
};

natsStatus
natsSysConnzOptions_Init(natsSysConnzOptions *opts)
{
    if (opts == NULL)
        return NATS_INVALID_ARG;

    memset(opts, 0, sizeof(*opts));
    return NATS_OK;
}

// Every CONNZ field is emitted whatever its value — an "empty" request still
// carries `"sort":""` and `"state":0`. The shared server filter does omit its
// unset keys, hence the switch to sysclient_opt* partway down. This split is
// server-visible, so it is preserved rather than tidied away.
static natsStatus
_marshalOptions(natsBuffer *buf, const void *optsv)
{
    const natsSysConnzOptions *opts = (const natsSysConnzOptions *) optsv;
    natsJSONWriter      w;
    natsSysConnzOptions defaults;

    if (opts == NULL)
    {
        natsSysConnzOptions_Init(&defaults);
        opts = &defaults;
    }

    natsJSONWriter_Init(&w, buf);
    natsJSONWriter_StartObject(&w);

    natsJSONWriter_AddStr(&w, "sort", opts->Sort);
    natsJSONWriter_AddBool(&w, "auth", opts->Username);
    natsJSONWriter_AddBool(&w, "subscriptions", opts->Subscriptions);
    natsJSONWriter_AddBool(&w, "subscriptions_detail", opts->SubscriptionsDetail);
    natsJSONWriter_AddInt(&w, "offset", opts->Offset);
    natsJSONWriter_AddInt(&w, "limit", opts->Limit);
    natsJSONWriter_AddUInt(&w, "cid", opts->CID);
    natsJSONWriter_AddStr(&w, "mqtt_client", opts->MQTTClient);
    natsJSONWriter_AddInt(&w, "state", (int64_t) opts->State);
    natsJSONWriter_AddStr(&w, "user", opts->User);
    natsJSONWriter_AddStr(&w, "acc", opts->Account);
    natsJSONWriter_AddStr(&w, "filter_subject", opts->FilterSubject);

    sysclient_writeEventFilter(&w, &opts->Filter);

    natsJSONWriter_EndObject(&w);
    return natsJSONWriter_Status(&w);
}

static natsStatus
_copyOptions(natsSysConnzOptions *dst, const natsSysConnzOptions *src)
{
    natsStatus s = NATS_OK;

    natsSysConnzOptions_Init(dst);
    if (src == NULL)
        return NATS_OK;

    // Scalars first, then the strings that need their own storage.
    dst->Username            = src->Username;
    dst->Subscriptions       = src->Subscriptions;
    dst->SubscriptionsDetail = src->SubscriptionsDetail;
    dst->Offset              = src->Offset;
    dst->Limit               = src->Limit;
    dst->CID                 = src->CID;
    dst->State               = src->State;

    IFOK(s, sysclient_dupOptStr(&dst->Sort, src->Sort));
    IFOK(s, sysclient_dupOptStr(&dst->MQTTClient, src->MQTTClient));
    IFOK(s, sysclient_dupOptStr(&dst->User, src->User));
    IFOK(s, sysclient_dupOptStr(&dst->Account, src->Account));
    IFOK(s, sysclient_dupOptStr(&dst->FilterSubject, src->FilterSubject));
    IFOK(s, sysclient_copyEventFilter(&dst->Filter, &src->Filter));

    return s;
}

static void
_freeOptionsCopy(natsSysConnzOptions *opts)
{
    if (opts == NULL)
        return;

    sysclient_freeOptStr(&opts->Sort);
    sysclient_freeOptStr(&opts->MQTTClient);
    sysclient_freeOptStr(&opts->User);
    sysclient_freeOptStr(&opts->Account);
    sysclient_freeOptStr(&opts->FilterSubject);
    sysclient_freeEventFilter(&opts->Filter);
    memset(opts, 0, sizeof(*opts));
}

static natsStatus
_parseTLSPeerCert(void *dst, natsJSON *node)
{
    return sysclient_scanFields(dst, node, _tlsPeerCertFields,
                                SYS_NFIELDS(_tlsPeerCertFields));
}

static void
_freeTLSPeerCert(void *dst)
{
    sysclient_freeFields(dst, _tlsPeerCertFields, SYS_NFIELDS(_tlsPeerCertFields));
}

static natsStatus
_parseConnInfo(void *dst, natsJSON *node)
{
    natsSysConnInfo *conn = (natsSysConnInfo *) dst;
    natsStatus       s;

    s = sysclient_scanFields(conn, node, _connInfoFields, SYS_NFIELDS(_connInfoFields));
    IFOK(s, sysclient_ptrArray((void ***) &conn->TLSPeerCerts, &conn->TLSPeerCertsCount,
                               node, "tls_peer_certs", sizeof(natsSysTLSPeerCert),
                               _parseTLSPeerCert, _freeTLSPeerCert));
    IFOK(s, sysclient_valueArray((void **) &conn->SubsDetail, &conn->SubsDetailCount,
                                 node, "subscriptions_list_detail",
                                 sizeof(natsSysSubDetail), sysclient_parseSubDetail,
                                 sysclient_freeSubDetail));
    return s;
}

static void
_freeConnInfo(void *dst)
{
    natsSysConnInfo *conn = (natsSysConnInfo *) dst;

    sysclient_freeFields(conn, _connInfoFields, SYS_NFIELDS(_connInfoFields));
    sysclient_freePtrArray((void ***) &conn->TLSPeerCerts, &conn->TLSPeerCertsCount,
                           _freeTLSPeerCert);
    sysclient_freeValueArray((void **) &conn->SubsDetail, &conn->SubsDetailCount,
                             sizeof(natsSysSubDetail), sysclient_freeSubDetail);
}

static natsStatus
_parseConnz(void *dst, natsJSON *node)
{
    natsSysConnz *connz = (natsSysConnz *) dst;
    natsStatus    s;

    s = sysclient_scanFields(connz, node, _connzFields, SYS_NFIELDS(_connzFields));
    IFOK(s, sysclient_ptrArray((void ***) &connz->Conns, &connz->ConnsCount, node,
                               "connections", sizeof(natsSysConnInfo), _parseConnInfo,
                               _freeConnInfo));
    return s;
}

static void
_freeConnz(natsSysConnz *connz)
{
    sysclient_freeFields(connz, _connzFields, SYS_NFIELDS(_connzFields));
    sysclient_freePtrArray((void ***) &connz->Conns, &connz->ConnsCount, _freeConnInfo);
}

static natsStatus
_respFromMsg(void **newResp, natsMsg *msg)
{
    natsSysConnzResp *resp;
    natsStatus        s;

    *newResp = NULL;

    resp = (natsSysConnzResp *) NATS_CALLOC(1, sizeof(natsSysConnzResp));
    if (resp == NULL)
        return NATS_NO_MEMORY;

    s = sysclient_decodeResp(msg, &resp->Server, &resp->Error, &resp->Connz, "data",
                             _parseConnz);
    if (s != NATS_OK)
    {
        natsSysConnzResp_Destroy(resp);
        return s;
    }

    *newResp = resp;
    return NATS_OK;
}

static void
_destroyResp(void *resp)
{
    natsSysConnzResp_Destroy((natsSysConnzResp *) resp);
}

natsStatus
natsSysClient_Connz(natsSysConnzResp **newResp, natsSysClient *client,
                    const char *serverID, const natsSysConnzOptions *opts,
                    int64_t timeout)
{
    return sysclient_request((void **) newResp, client, serverID, SYS_SUBJ_CONNZ,
                             _marshalOptions, opts, 256, timeout,
                             _respFromMsg);
}

natsStatus
natsSysClient_ConnzPing(natsSysConnzRespList *list, natsSysClient *client,
                        const natsSysConnzOptions *opts, int64_t timeout)
{
    if (list == NULL)
        return NATS_INVALID_ARG;

    return sysclient_ping((void ***) &list->Resps, &list->Count, client, SYS_SUBJ_CONNZ,
                          _marshalOptions, opts, 256, timeout, _respFromMsg,
                          _destroyResp);
}

natsStatus
natsSysClient_ConnzEach(natsSysClient *client, const char *serverID,
                        const natsSysConnzOptions *opts, int64_t timeout,
                        natsSysConnzPageHandler handler, void *closure)
{
    natsSysConnzOptions page;
    natsSysConnzOptions defaults;
    int64_t             deadline;
    int                 offset;

    if ((client == NULL) || (handler == NULL))
        return NATS_INVALID_ARG;
    if (timeout < 0)
        return NATS_INVALID_ARG;

    if (opts == NULL)
    {
        natsSysConnzOptions_Init(&defaults);
        opts = &defaults;
    }

    if (timeout == 0)
        timeout = NATS_SYS_DEFAULT_REQUEST_TIMEOUT;

    // One budget covers the whole walk, not each page.
    deadline = sysclient_deadline(timeout);
    offset   = opts->Offset;

    for (;;)
    {
        natsSysConnzResp *resp = NULL;
        natsStatus        s;
        int64_t           left;
        int               received;
        int               n;
        int               total;
        bool              wantMore;

        left = deadline - sysclient_nowMs();
        if (left <= 0)
            return NATS_TIMEOUT;

        // The options are borrowed, so the offset is advanced on a copy. Only
        // scalars differ between pages, so a shallow copy is safe here: it
        // lives no longer than this call.
        page        = *opts;
        page.Offset = offset;

        s = natsSysClient_Connz(&resp, client, serverID, &page, left);
        if (s != NATS_OK)
            return s;

        n        = resp->Connz.ConnsCount;
        total    = resp->Connz.Total;
        wantMore = handler(resp, closure);
        natsSysConnzResp_Destroy(resp);

        if (!wantMore)
            return NATS_OK;

        // Stop once the reported total is covered, and also on an empty page
        // so a shrinking result set cannot loop forever.
        received = offset + n;
        if ((received >= total) || (n == 0))
            return NATS_OK;

        offset = received;
    }
}

static void
_walkDestroy(void *w)
{
    natsSysConnzWalk *walk = (natsSysConnzWalk *) w;

    if (walk == NULL)
        return;

    natsSysConnzResp_Destroy(walk->first);
    _freeOptionsCopy(&walk->opts);
    NATS_FREE(walk->serverID);
    NATS_FREE(walk);
}

natsStatus
natsSysClient_ConnzPingEach(natsSysConnzWalkList *list, natsSysClient *client,
                            const natsSysConnzOptions *opts, int64_t timeout)
{
    natsStatus           s;
    natsSysConnzRespList pages = {NULL, 0};
    natsSysConnzWalk   **walks;
    int                  count;
    int                  i;

    if ((list == NULL) || (client == NULL))
        return NATS_INVALID_ARG;

    list->Walks = NULL;
    list->Count = 0;

    s = natsSysClient_ConnzPing(&pages, client, opts, timeout);
    if (s != NATS_OK)
    {
        natsSysConnzRespList_Destroy(&pages);
        return s;
    }
    if (pages.Count == 0)
    {
        natsSysConnzRespList_Destroy(&pages);
        return NATS_OK;
    }

    // Captured before the list is destroyed below, which zeroes its Count.
    count = pages.Count;

    walks = (natsSysConnzWalk **) NATS_CALLOC((size_t) count, sizeof(natsSysConnzWalk *));
    if (walks == NULL)
    {
        natsSysConnzRespList_Destroy(&pages);
        return NATS_NO_MEMORY;
    }

    for (i = 0; (i < count) && (s == NATS_OK); i++)
    {
        natsSysConnzWalk *walk;

        walk = (natsSysConnzWalk *) NATS_CALLOC(1, sizeof(natsSysConnzWalk));
        if (walk == NULL)
        {
            s = NATS_NO_MEMORY;
            break;
        }
        walks[i] = walk;

        walk->client = client;

        s = sysclient_walkServerID(&walk->serverID, pages.Resps[i]->Server.ID);
        IFOK(s, _copyOptions(&walk->opts, opts));

        if (s == NATS_OK)
        {
            // Adopt the page the ping already produced. The slot is cleared so
            // destroying the list below cannot free it a second time.
            walk->first    = pages.Resps[i];
            pages.Resps[i] = NULL;
            walk->total    = walk->first->Connz.Total;
            walk->offset   = walk->opts.Offset;
        }
    }

    natsSysConnzRespList_Destroy(&pages);

    if (s != NATS_OK)
    {
        sysclient_freeRespList((void ***) &walks, &count, _walkDestroy);
        return s;
    }

    list->Walks = walks;
    list->Count = count;
    return NATS_OK;
}

const char *
natsSysConnzWalk_ServerID(const natsSysConnzWalk *walk)
{
    return (walk != NULL) ? walk->serverID : NULL;
}

natsStatus
natsSysConnzWalk_Run(natsSysConnzWalk *walk, int64_t timeout,
                     natsSysConnzPageHandler handler, void *closure)
{
    int64_t deadline;

    if ((walk == NULL) || (handler == NULL))
        return NATS_INVALID_ARG;
    if (timeout < 0)
        return NATS_INVALID_ARG;
    if (walk->done)
        return NATS_OK;

    if (timeout == 0)
        timeout = NATS_SYS_DEFAULT_REQUEST_TIMEOUT;

    deadline = sysclient_deadline(timeout);

    // Deliver the page the ping already fetched before asking for more.
    if (walk->first != NULL)
    {
        natsSysConnzResp *resp = walk->first;
        int               n    = resp->Connz.ConnsCount;
        bool              wantMore;

        walk->first = NULL;
        wantMore    = handler(resp, closure);
        natsSysConnzResp_Destroy(resp);

        walk->offset += n;

        if (!wantMore || (n == 0))
        {
            walk->done = true;
            return NATS_OK;
        }
    }

    while (walk->offset < walk->total)
    {
        natsSysConnzOptions page;
        natsSysConnzResp   *resp = NULL;
        natsStatus          s;
        int64_t             left;
        int                 n;
        bool                wantMore;

        left = deadline - sysclient_nowMs();
        if (left <= 0)
            return NATS_TIMEOUT;

        page        = walk->opts;
        page.Offset = walk->offset;

        // By ID, not by ping. A server that left the cluster meanwhile
        // yields NATS_NOT_FOUND.
        s = natsSysClient_Connz(&resp, walk->client, walk->serverID, &page, left);
        if (s != NATS_OK)
            return s;

        n        = resp->Connz.ConnsCount;
        wantMore = handler(resp, closure);
        natsSysConnzResp_Destroy(resp);

        if (!wantMore || (n == 0))
        {
            walk->done = true;
            return NATS_OK;
        }

        walk->offset += n;
    }

    walk->done = true;
    return NATS_OK;
}

void
natsSysConnzWalkList_Destroy(natsSysConnzWalkList *list)
{
    if (list == NULL)
        return;

    sysclient_freeRespList((void ***) &list->Walks, &list->Count, _walkDestroy);
}

void
natsSysConnzResp_Destroy(natsSysConnzResp *resp)
{
    if (resp == NULL)
        return;

    sysclient_freeServerInfo(&resp->Server);
    sysclient_freeAPIError(&resp->Error);
    _freeConnz(&resp->Connz);
    NATS_FREE(resp);
}

void
natsSysConnzRespList_Destroy(natsSysConnzRespList *list)
{
    if (list == NULL)
        return;

    sysclient_freeRespList((void ***) &list->Resps, &list->Count, _destroyResp);
}
