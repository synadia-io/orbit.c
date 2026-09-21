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
    SYS_F(SYS_FLD_I64, natsSysConnInfo, Pending, "pending_bytes"),
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
    return sysclient_initOpts(opts, sizeof(*opts));
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

// Deep-copies 'src' (NULL meaning defaults) into the zeroed 'dst'; a
// sysWalkOps.CopyOptions.
static natsStatus
_copyOptions(void *dstv, const void *srcv)
{
    natsSysConnzOptions       *dst = (natsSysConnzOptions *) dstv;
    const natsSysConnzOptions *src = (const natsSysConnzOptions *) srcv;
    natsStatus s   = NATS_OK;

    if (src == NULL)
        return natsSysConnzOptions_Init(dst);

    // The struct copy carries every scalar, so a new one cannot be forgotten
    // here. The members that need their own storage are then cleared before
    // being copied, so a failure part-way leaves _freeOptionsCopy nothing but
    // owned strings or NULL.
    *dst = *src;
    dst->Sort          = NULL;
    dst->MQTTClient    = NULL;
    dst->User          = NULL;
    dst->Account       = NULL;
    dst->FilterSubject = NULL;
    memset(&dst->Filter, 0, sizeof(dst->Filter));

    IFOK(s, sysclient_dupOptStr(&dst->Sort, src->Sort));
    IFOK(s, sysclient_dupOptStr(&dst->MQTTClient, src->MQTTClient));
    IFOK(s, sysclient_dupOptStr(&dst->User, src->User));
    IFOK(s, sysclient_dupOptStr(&dst->Account, src->Account));
    IFOK(s, sysclient_dupOptStr(&dst->FilterSubject, src->FilterSubject));
    IFOK(s, sysclient_copyEventFilter(&dst->Filter, &src->Filter));

    return s;
}

// Releases the members of an options copy; a sysWalkOps.FreeOptions.
static void
_freeOptionsCopy(void *optsv)
{
    natsSysConnzOptions *opts = (natsSysConnzOptions *) optsv;

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
_freeConnz(void *dst)
{
    natsSysConnz *connz = (natsSysConnz *) dst;

    sysclient_freeFields(connz, _connzFields, SYS_NFIELDS(_connzFields));
    sysclient_freePtrArray((void ***) &connz->Conns, &connz->ConnsCount, _freeConnInfo);
}

static const sysEndpoint _endpoint = SYS_ENDPOINT(natsSysConnzResp, Connz, SYS_SUBJ_CONNZ, "data",
                                                  _marshalOptions, _parseConnz, _freeConnz);

natsStatus
natsSysClient_Connz(natsSysConnzResp **newResp, natsSysClient *client,
                    const char *serverID, const natsSysConnzOptions *opts,
                    int64_t timeout)
{
    return sysclient_request((void **) newResp, client, serverID, opts, timeout, &_endpoint);
}

natsStatus
natsSysClient_ConnzPing(natsSysConnzRespList *list, natsSysClient *client,
                        const natsSysConnzOptions *opts, int64_t timeout)
{
    if (list == NULL)
        return NATS_INVALID_ARG;

    return sysclient_ping((void ***) &list->Resps, &list->Count, client, opts, timeout,
                          &_endpoint);
}

void
natsSysConnzResp_Destroy(natsSysConnzResp *resp)
{
    sysclient_destroyResp(resp, &_endpoint);
}

void
natsSysConnzRespList_Destroy(natsSysConnzRespList *list)
{
    if (list == NULL)
        return;

    sysclient_freeList((void ***) &list->Resps, &list->Count, sysclient_destroyResp,
                       &_endpoint);
}

//
// Walk adapters: the typed reads the shared driver in sysclient.c cannot do.
//

// The options are borrowed, so the offset is advanced on a shallow copy. Only
// scalars differ between pages, and the copy lives no longer than this call.
static natsStatus
_fetch(void **page, natsSysClient *client, const char *serverID, const void *optsv,
       int offset, int64_t timeout)
{
    natsSysConnzOptions pageOpts;

    if (optsv != NULL)
        pageOpts = *(const natsSysConnzOptions *) optsv;
    else
        natsSysConnzOptions_Init(&pageOpts);
    pageOpts.Offset = offset;

    return natsSysClient_Connz((natsSysConnzResp **) page, client, serverID, &pageOpts, timeout);
}

static const sysWalkOps _walkOps = {
    &_endpoint,
    sizeof(natsSysConnzOptions),
    SYS_INT_OFF(natsSysConnzOptions, Offset),
    SYS_INT_OFF(natsSysConnzResp, Connz.ConnsCount),
    SYS_INT_OFF(natsSysConnzResp, Connz.Total),
    _copyOptions,
    _freeOptionsCopy,
    _fetch,
    SYS_ALWAYS_PAGED,
};

SYS_WALK_SINK(natsSysConnzPageHandler, natsSysConnzResp)

natsStatus
natsSysClient_ConnzEach(natsSysClient *client, const char *serverID,
                        const natsSysConnzOptions *opts, int64_t timeout,
                        natsSysConnzPageHandler handler, void *closure)
{
    _sink sink = {handler, closure};

    if (handler == NULL)
        return NATS_INVALID_ARG;

    return sysclient_walkEach(client, serverID, opts, timeout, _deliver, &sink, &_walkOps);
}

natsStatus
natsSysClient_ConnzPingEach(natsSysConnzWalkList *list, natsSysClient *client,
                            const natsSysConnzOptions *opts, int64_t timeout)
{
    if (list == NULL)
        return NATS_INVALID_ARG;

    return sysclient_pingEach((void ***) &list->Walks, &list->Count, client, opts, timeout,
                              &_walkOps);
}

const char *
natsSysConnzWalk_ServerID(const natsSysConnzWalk *walk)
{
    return sysclient_walkID((const sysWalk *) walk);
}

natsStatus
natsSysConnzWalk_Run(natsSysConnzWalk *walk, int64_t timeout,
                     natsSysConnzPageHandler handler, void *closure)
{
    _sink sink = {handler, closure};

    if (handler == NULL)
        return NATS_INVALID_ARG;

    return sysclient_walkRun((sysWalk *) walk, timeout, _deliver, &sink);
}

void
natsSysConnzWalkList_Destroy(natsSysConnzWalkList *list)
{
    if (list == NULL)
        return;

    sysclient_freeList((void ***) &list->Walks, &list->Count, sysclient_walkDestroy, NULL);
}
