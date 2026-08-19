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

// One table row per wire field, so the whole set can be read off against the Go
// source rather than by following the parsing logic in this file.

#include "varz.h"

#include "marshal.h"
#include "sysclientp.h"
#include "unmarshal.h"

#include "buf.h"
#include "os_shims.h"

#include <string.h>

static const sysField _clusterOptsFields[] = {
    SYS_F(SYS_FLD_STR, natsSysClusterOptsVarz, Name, "name"),
    SYS_F(SYS_FLD_STR, natsSysClusterOptsVarz, Host, "addr"),
    SYS_F(SYS_FLD_INT, natsSysClusterOptsVarz, Port, "cluster_port"),
    SYS_F(SYS_FLD_DOUBLE, natsSysClusterOptsVarz, AuthTimeout, "auth_timeout"),
    SYS_FA(natsSysClusterOptsVarz, URLs, URLsCount, "urls"),
    SYS_F(SYS_FLD_DOUBLE, natsSysClusterOptsVarz, TLSTimeout, "tls_timeout"),
    SYS_F(SYS_FLD_BOOL, natsSysClusterOptsVarz, TLSRequired, "tls_required"),
    SYS_F(SYS_FLD_BOOL, natsSysClusterOptsVarz, TLSVerify, "tls_verify"),
    SYS_F(SYS_FLD_INT, natsSysClusterOptsVarz, PoolSize, "pool_size"),
};

static const sysField _remoteGatewayFields[] = {
    SYS_F(SYS_FLD_STR, natsSysRemoteGatewayOptsVarz, Name, "name"),
    SYS_F(SYS_FLD_DOUBLE, natsSysRemoteGatewayOptsVarz, TLSTimeout, "tls_timeout"),
    SYS_FA(natsSysRemoteGatewayOptsVarz, URLs, URLsCount, "urls"),
};

static const sysField _gatewayOptsFields[] = {
    SYS_F(SYS_FLD_STR, natsSysGatewayOptsVarz, Name, "name"),
    SYS_F(SYS_FLD_STR, natsSysGatewayOptsVarz, Host, "host"),
    SYS_F(SYS_FLD_INT, natsSysGatewayOptsVarz, Port, "port"),
    SYS_F(SYS_FLD_DOUBLE, natsSysGatewayOptsVarz, AuthTimeout, "auth_timeout"),
    SYS_F(SYS_FLD_DOUBLE, natsSysGatewayOptsVarz, TLSTimeout, "tls_timeout"),
    SYS_F(SYS_FLD_BOOL, natsSysGatewayOptsVarz, TLSRequired, "tls_required"),
    SYS_F(SYS_FLD_BOOL, natsSysGatewayOptsVarz, TLSVerify, "tls_verify"),
    SYS_F(SYS_FLD_STR, natsSysGatewayOptsVarz, Advertise, "advertise"),
    SYS_F(SYS_FLD_INT, natsSysGatewayOptsVarz, ConnectRetries, "connect_retries"),
    SYS_F(SYS_FLD_BOOL, natsSysGatewayOptsVarz, RejectUnknown, "reject_unknown"),
};

static const sysField _denyRulesFields[] = {
    SYS_FA(natsSysDenyRules, Exports, ExportsCount, "exports"),
    SYS_FA(natsSysDenyRules, Imports, ImportsCount, "imports"),
};

static const sysField _remoteLeafFields[] = {
    SYS_F(SYS_FLD_STR, natsSysRemoteLeafOptsVarz, LocalAccount, "local_account"),
    SYS_F(SYS_FLD_DOUBLE, natsSysRemoteLeafOptsVarz, TLSTimeout, "tls_timeout"),
    SYS_FA(natsSysRemoteLeafOptsVarz, URLs, URLsCount, "urls"),
    SYS_F(SYS_FLD_BOOL, natsSysRemoteLeafOptsVarz, TLSOCSPPeerVerify, "tls_ocsp_peer_verify"),
};

static const sysField _leafNodeOptsFields[] = {
    SYS_F(SYS_FLD_STR, natsSysLeafNodeOptsVarz, Host, "host"),
    SYS_F(SYS_FLD_INT, natsSysLeafNodeOptsVarz, Port, "port"),
    SYS_F(SYS_FLD_DOUBLE, natsSysLeafNodeOptsVarz, AuthTimeout, "auth_timeout"),
    SYS_F(SYS_FLD_DOUBLE, natsSysLeafNodeOptsVarz, TLSTimeout, "tls_timeout"),
    SYS_F(SYS_FLD_BOOL, natsSysLeafNodeOptsVarz, TLSRequired, "tls_required"),
    SYS_F(SYS_FLD_BOOL, natsSysLeafNodeOptsVarz, TLSVerify, "tls_verify"),
    SYS_F(SYS_FLD_BOOL, natsSysLeafNodeOptsVarz, TLSOCSPPeerVerify, "tls_ocsp_peer_verify"),
};

static const sysField _mqttOptsFields[] = {
    SYS_F(SYS_FLD_STR, natsSysMQTTOptsVarz, Host, "host"),
    SYS_F(SYS_FLD_INT, natsSysMQTTOptsVarz, Port, "port"),
    SYS_F(SYS_FLD_STR, natsSysMQTTOptsVarz, NoAuthUser, "no_auth_user"),
    SYS_F(SYS_FLD_DOUBLE, natsSysMQTTOptsVarz, AuthTimeout, "auth_timeout"),
    SYS_F(SYS_FLD_BOOL, natsSysMQTTOptsVarz, TLSMap, "tls_map"),
    SYS_F(SYS_FLD_DOUBLE, natsSysMQTTOptsVarz, TLSTimeout, "tls_timeout"),
    SYS_FA(natsSysMQTTOptsVarz, TLSPinnedCerts, TLSPinnedCertsCount, "tls_pinned_certs"),
    SYS_F(SYS_FLD_STR, natsSysMQTTOptsVarz, JsDomain, "js_domain"),
    SYS_F(SYS_FLD_I64, natsSysMQTTOptsVarz, AckWait, "ack_wait"),
    SYS_F(SYS_FLD_U16, natsSysMQTTOptsVarz, MaxAckPending, "max_ack_pending"),
    SYS_F(SYS_FLD_BOOL, natsSysMQTTOptsVarz, TLSOCSPPeerVerify, "tls_ocsp_peer_verify"),
};

static const sysField _websocketOptsFields[] = {
    SYS_F(SYS_FLD_STR, natsSysWebsocketOptsVarz, Host, "host"),
    SYS_F(SYS_FLD_INT, natsSysWebsocketOptsVarz, Port, "port"),
    SYS_F(SYS_FLD_STR, natsSysWebsocketOptsVarz, Advertise, "advertise"),
    SYS_F(SYS_FLD_STR, natsSysWebsocketOptsVarz, NoAuthUser, "no_auth_user"),
    SYS_F(SYS_FLD_STR, natsSysWebsocketOptsVarz, JWTCookie, "jwt_cookie"),
    SYS_F(SYS_FLD_I64, natsSysWebsocketOptsVarz, HandshakeTimeout, "handshake_timeout"),
    SYS_F(SYS_FLD_DOUBLE, natsSysWebsocketOptsVarz, AuthTimeout, "auth_timeout"),
    SYS_F(SYS_FLD_BOOL, natsSysWebsocketOptsVarz, NoTLS, "no_tls"),
    SYS_F(SYS_FLD_BOOL, natsSysWebsocketOptsVarz, TLSMap, "tls_map"),
    SYS_FA(natsSysWebsocketOptsVarz, TLSPinnedCerts, TLSPinnedCertsCount, "tls_pinned_certs"),
    SYS_F(SYS_FLD_BOOL, natsSysWebsocketOptsVarz, SameOrigin, "same_origin"),
    SYS_FA(natsSysWebsocketOptsVarz, AllowedOrigins, AllowedOriginsCount, "allowed_origins"),
    SYS_F(SYS_FLD_BOOL, natsSysWebsocketOptsVarz, Compression, "compression"),
    SYS_F(SYS_FLD_BOOL, natsSysWebsocketOptsVarz, TLSOCSPPeerVerify, "tls_ocsp_peer_verify"),
};

static const sysField _ocspCacheFields[] = {
    SYS_F(SYS_FLD_STR, natsSysOCSPResponseCacheVarz, Type, "cache_type"),
    SYS_F(SYS_FLD_I64, natsSysOCSPResponseCacheVarz, Hits, "cache_hits"),
    SYS_F(SYS_FLD_I64, natsSysOCSPResponseCacheVarz, Misses, "cache_misses"),
    SYS_F(SYS_FLD_I64, natsSysOCSPResponseCacheVarz, Responses, "cached_responses"),
    SYS_F(SYS_FLD_I64, natsSysOCSPResponseCacheVarz, Revokes, "cached_revoked_responses"),
    SYS_F(SYS_FLD_I64, natsSysOCSPResponseCacheVarz, Goods, "cached_good_responses"),
    SYS_F(SYS_FLD_I64, natsSysOCSPResponseCacheVarz, Unknowns, "cached_unknown_responses"),
};

static const sysField _varzFields[] = {
    SYS_F(SYS_FLD_STR, natsSysVarz, ID, "server_id"),
    SYS_F(SYS_FLD_STR, natsSysVarz, Name, "server_name"),
    SYS_F(SYS_FLD_STR, natsSysVarz, Version, "version"),
    SYS_F(SYS_FLD_INT, natsSysVarz, Proto, "proto"),
    SYS_F(SYS_FLD_STR, natsSysVarz, GitCommit, "git_commit"),
    SYS_F(SYS_FLD_STR, natsSysVarz, GoVersion, "go"),
    SYS_F(SYS_FLD_STR, natsSysVarz, Host, "host"),
    SYS_F(SYS_FLD_INT, natsSysVarz, Port, "port"),
    SYS_F(SYS_FLD_BOOL, natsSysVarz, AuthRequired, "auth_required"),
    SYS_F(SYS_FLD_BOOL, natsSysVarz, TLSRequired, "tls_required"),
    SYS_F(SYS_FLD_BOOL, natsSysVarz, TLSVerify, "tls_verify"),
    SYS_F(SYS_FLD_BOOL, natsSysVarz, TLSOCSPPeerVerify, "tls_ocsp_peer_verify"),
    SYS_F(SYS_FLD_STR, natsSysVarz, IP, "ip"),
    SYS_FA(natsSysVarz, ClientConnectURLs, ClientConnectURLsCount, "connect_urls"),
    SYS_FA(natsSysVarz, WSConnectURLs, WSConnectURLsCount, "ws_connect_urls"),
    SYS_F(SYS_FLD_INT, natsSysVarz, MaxConn, "max_connections"),
    SYS_F(SYS_FLD_INT, natsSysVarz, MaxSubs, "max_subscriptions"),
    SYS_F(SYS_FLD_I64, natsSysVarz, PingInterval, "ping_interval"),
    SYS_F(SYS_FLD_INT, natsSysVarz, MaxPingsOut, "ping_max"),
    SYS_F(SYS_FLD_STR, natsSysVarz, HTTPHost, "http_host"),
    SYS_F(SYS_FLD_INT, natsSysVarz, HTTPPort, "http_port"),
    SYS_F(SYS_FLD_STR, natsSysVarz, HTTPBasePath, "http_base_path"),
    SYS_F(SYS_FLD_INT, natsSysVarz, HTTPSPort, "https_port"),
    SYS_F(SYS_FLD_DOUBLE, natsSysVarz, AuthTimeout, "auth_timeout"),
    SYS_F(SYS_FLD_I32, natsSysVarz, MaxControlLine, "max_control_line"),
    SYS_F(SYS_FLD_INT, natsSysVarz, MaxPayload, "max_payload"),
    SYS_F(SYS_FLD_I64, natsSysVarz, MaxPending, "max_pending"),
    SYS_F(SYS_FLD_DOUBLE, natsSysVarz, TLSTimeout, "tls_timeout"),
    SYS_F(SYS_FLD_I64, natsSysVarz, WriteDeadline, "write_deadline"),
    SYS_F(SYS_FLD_STR, natsSysVarz, Start, "start"),
    SYS_F(SYS_FLD_STR, natsSysVarz, Now, "now"),
    SYS_F(SYS_FLD_STR, natsSysVarz, Uptime, "uptime"),
    SYS_F(SYS_FLD_I64, natsSysVarz, Mem, "mem"),
    SYS_F(SYS_FLD_INT, natsSysVarz, Cores, "cores"),
    SYS_F(SYS_FLD_INT, natsSysVarz, MaxProcs, "gomaxprocs"),
    SYS_F(SYS_FLD_DOUBLE, natsSysVarz, CPU, "cpu"),
    SYS_F(SYS_FLD_INT, natsSysVarz, Connections, "connections"),
    SYS_F(SYS_FLD_U64, natsSysVarz, TotalConnections, "total_connections"),
    SYS_F(SYS_FLD_INT, natsSysVarz, Routes, "routes"),
    SYS_F(SYS_FLD_INT, natsSysVarz, Remotes, "remotes"),
    SYS_F(SYS_FLD_INT, natsSysVarz, Leafs, "leafnodes"),
    SYS_F(SYS_FLD_I64, natsSysVarz, InMsgs, "in_msgs"),
    SYS_F(SYS_FLD_I64, natsSysVarz, OutMsgs, "out_msgs"),
    SYS_F(SYS_FLD_I64, natsSysVarz, InBytes, "in_bytes"),
    SYS_F(SYS_FLD_I64, natsSysVarz, OutBytes, "out_bytes"),
    SYS_F(SYS_FLD_I64, natsSysVarz, SlowConsumers, "slow_consumers"),
    SYS_F(SYS_FLD_U32, natsSysVarz, Subscriptions, "subscriptions"),
    SYS_F(SYS_FLD_STR, natsSysVarz, ConfigLoadTime, "config_load_time"),
    SYS_FA(natsSysVarz, Tags, TagsCount, "tags"),
    SYS_FA(natsSysVarz, TrustedOperatorsJwt, TrustedOperatorsJwtCount,
           "trusted_operators_jwt"),
    SYS_F(SYS_FLD_STR, natsSysVarz, SystemAccount, "system_account"),
    SYS_F(SYS_FLD_U64, natsSysVarz, PinnedAccountFail, "pinned_account_fails"),
};

natsStatus
natsSysVarzOptions_Init(natsSysVarzOptions *opts)
{
    return sysclient_initOpts(opts, sizeof(*opts));
}

// VARZ has no options of its own, so the request is the five optional server
// filter keys and nothing else.
static natsStatus
_marshalOptions(natsBuffer *buf, const void *optsv)
{
    const natsSysVarzOptions *opts = (const natsSysVarzOptions *) optsv;

    return sysclient_marshalFilterOnly(buf, (opts != NULL) ? &opts->Filter : NULL);
}

//
// Nested option structures.
//

static natsStatus
_parseClusterOpts(void *dst, natsJSON *node)
{
    return sysclient_scanFields(dst, node, _clusterOptsFields,
                                SYS_NFIELDS(_clusterOptsFields));
}

static void
_freeClusterOpts(void *dst)
{
    sysclient_freeFields(dst, _clusterOptsFields, SYS_NFIELDS(_clusterOptsFields));
}

static natsStatus
_parseRemoteGateway(void *dst, natsJSON *node)
{
    return sysclient_scanFields(dst, node, _remoteGatewayFields,
                                SYS_NFIELDS(_remoteGatewayFields));
}

static void
_freeRemoteGateway(void *dst)
{
    sysclient_freeFields(dst, _remoteGatewayFields, SYS_NFIELDS(_remoteGatewayFields));
}

static natsStatus
_parseGatewayOpts(void *dst, natsJSON *node)
{
    natsSysGatewayOptsVarz *gw = (natsSysGatewayOptsVarz *) dst;
    natsStatus              s;

    s = sysclient_scanFields(gw, node, _gatewayOptsFields, SYS_NFIELDS(_gatewayOptsFields));
    IFOK(s, sysclient_valueArray((void **) &gw->Gateways, &gw->GatewaysCount, node,
                                 "gateways", sizeof(natsSysRemoteGatewayOptsVarz),
                                 _parseRemoteGateway, _freeRemoteGateway));
    return s;
}

static void
_freeGatewayOpts(void *dst)
{
    natsSysGatewayOptsVarz *gw = (natsSysGatewayOptsVarz *) dst;

    sysclient_freeFields(gw, _gatewayOptsFields, SYS_NFIELDS(_gatewayOptsFields));
    sysclient_freeValueArray((void **) &gw->Gateways, &gw->GatewaysCount,
                             sizeof(natsSysRemoteGatewayOptsVarz), _freeRemoteGateway);
}

static natsStatus
_parseDenyRules(void *dst, natsJSON *node)
{
    return sysclient_scanFields(dst, node, _denyRulesFields, SYS_NFIELDS(_denyRulesFields));
}

static void
_freeDenyRules(void *dst)
{
    sysclient_freeFields(dst, _denyRulesFields, SYS_NFIELDS(_denyRulesFields));
}

static natsStatus
_parseRemoteLeaf(void *dst, natsJSON *node)
{
    natsSysRemoteLeafOptsVarz *leaf = (natsSysRemoteLeafOptsVarz *) dst;
    natsStatus                 s;

    s = sysclient_scanFields(leaf, node, _remoteLeafFields, SYS_NFIELDS(_remoteLeafFields));
    IFOK(s, sysclient_objectPtr((void **) &leaf->Deny, node, "deny",
                                sizeof(natsSysDenyRules), _parseDenyRules, _freeDenyRules));
    return s;
}

static void
_freeRemoteLeaf(void *dst)
{
    natsSysRemoteLeafOptsVarz *leaf = (natsSysRemoteLeafOptsVarz *) dst;

    sysclient_freeFields(leaf, _remoteLeafFields, SYS_NFIELDS(_remoteLeafFields));
    sysclient_freeObjectPtr((void **) &leaf->Deny, _freeDenyRules);
}

static natsStatus
_parseLeafNodeOpts(void *dst, natsJSON *node)
{
    natsSysLeafNodeOptsVarz *leaf = (natsSysLeafNodeOptsVarz *) dst;
    natsStatus               s;

    s = sysclient_scanFields(leaf, node, _leafNodeOptsFields,
                             SYS_NFIELDS(_leafNodeOptsFields));
    IFOK(s, sysclient_valueArray((void **) &leaf->Remotes, &leaf->RemotesCount, node,
                                 "remotes", sizeof(natsSysRemoteLeafOptsVarz),
                                 _parseRemoteLeaf, _freeRemoteLeaf));
    return s;
}

static void
_freeLeafNodeOpts(void *dst)
{
    natsSysLeafNodeOptsVarz *leaf = (natsSysLeafNodeOptsVarz *) dst;

    sysclient_freeFields(leaf, _leafNodeOptsFields, SYS_NFIELDS(_leafNodeOptsFields));
    sysclient_freeValueArray((void **) &leaf->Remotes, &leaf->RemotesCount,
                             sizeof(natsSysRemoteLeafOptsVarz), _freeRemoteLeaf);
}

static natsStatus
_parseMQTTOpts(void *dst, natsJSON *node)
{
    return sysclient_scanFields(dst, node, _mqttOptsFields, SYS_NFIELDS(_mqttOptsFields));
}

static void
_freeMQTTOpts(void *dst)
{
    sysclient_freeFields(dst, _mqttOptsFields, SYS_NFIELDS(_mqttOptsFields));
}

static natsStatus
_parseWebsocketOpts(void *dst, natsJSON *node)
{
    return sysclient_scanFields(dst, node, _websocketOptsFields,
                                SYS_NFIELDS(_websocketOptsFields));
}

static void
_freeWebsocketOpts(void *dst)
{
    sysclient_freeFields(dst, _websocketOptsFields, SYS_NFIELDS(_websocketOptsFields));
}

static natsStatus
_parseOCSPCache(void *dst, natsJSON *node)
{
    return sysclient_scanFields(dst, node, _ocspCacheFields, SYS_NFIELDS(_ocspCacheFields));
}

static void
_freeOCSPCache(void *dst)
{
    sysclient_freeFields(dst, _ocspCacheFields, SYS_NFIELDS(_ocspCacheFields));
}

// `http_req_stats` is a JSON object keyed by path. C has no map, so its
// members are walked in document order into a parallel array.
static natsStatus
_parseHTTPReqStats(natsSysVarz *varz, natsJSON *node)
{
    natsStatus          s = NATS_OK;
    natsJSON           *obj = NULL;
    natsSysHTTPReqStat *arr;
    int                 n;
    int                 i;

    s = natsJSON_Field(node, "http_req_stats", &obj);
    if (s == NATS_NOT_FOUND)
        return NATS_OK;
    if (s != NATS_OK)
        return s;
    if (natsJSON_Type(obj) == NATS_JSON_NULL)
        return NATS_OK;
    if (natsJSON_Type(obj) != NATS_JSON_OBJECT)
        return NATS_INVALID_ARG;

    n = natsJSON_FieldCount(obj);
    if (n == 0)
        return NATS_OK;

    arr = (natsSysHTTPReqStat *) NATS_CALLOC((size_t) n, sizeof(natsSysHTTPReqStat));
    if (arr == NULL)
        return NATS_NO_MEMORY;

    for (i = 0; i < n; i++)
    {
        const char *key   = NULL;
        natsJSON   *value = NULL;

        s = natsJSON_FieldAt(obj, i, &key, &value);
        if (s == NATS_OK)
        {
            arr[i].Path = NATS_STRDUP(key);
            if (arr[i].Path == NULL)
                s = NATS_NO_MEMORY;
        }
        if ((s == NATS_OK) && (natsJSON_Type(value) != NATS_JSON_NULL))
            s = natsJSON_AsUInt(value, &arr[i].Count);

        if (s != NATS_OK)
        {
            int j;

            for (j = 0; j <= i; j++)
                NATS_FREE(arr[j].Path);
            NATS_FREE(arr);
            return s;
        }
    }

    varz->HTTPReqStats      = arr;
    varz->HTTPReqStatsCount = n;
    return NATS_OK;
}

static void
_freeHTTPReqStats(natsSysVarz *varz)
{
    int i;

    for (i = 0; (varz->HTTPReqStats != NULL) && (i < varz->HTTPReqStatsCount); i++)
        NATS_FREE(varz->HTTPReqStats[i].Path);

    NATS_FREE(varz->HTTPReqStats);
    varz->HTTPReqStats      = NULL;
    varz->HTTPReqStatsCount = 0;
}

static natsStatus
_parseVarz(void *dst, natsJSON *node)
{
    natsSysVarz *varz = (natsSysVarz *) dst;
    natsStatus   s;

    s = sysclient_scanFields(varz, node, _varzFields, SYS_NFIELDS(_varzFields));

    // Nested objects held by value: absent means the zero value.
    IFOK(s, sysclient_objectInline(&varz->Cluster, node, "cluster", _parseClusterOpts));
    IFOK(s, sysclient_objectInline(&varz->Gateway, node, "gateway", _parseGatewayOpts));
    IFOK(s, sysclient_objectInline(&varz->LeafNode, node, "leaf", _parseLeafNodeOpts));
    IFOK(s, sysclient_objectInline(&varz->MQTT, node, "mqtt", _parseMQTTOpts));
    IFOK(s, sysclient_objectInline(&varz->Websocket, node, "websocket", _parseWebsocketOpts));
    IFOK(s, sysclient_objectInline(&varz->JetStream, node, "jetstream",
                                   sysclient_parseJetStreamVarz));

    // Held by pointer, so NULL distinguishes absent from all-zero.
    IFOK(s, sysclient_objectPtr((void **) &varz->OCSPResponseCache, node,
                                "ocsp_peer_cache", sizeof(natsSysOCSPResponseCacheVarz),
                                _parseOCSPCache, _freeOCSPCache));
    IFOK(s, sysclient_objectPtr((void **) &varz->SlowConsumersStats, node,
                                "slow_consumer_stats", sizeof(natsSysSlowConsumersStats),
                                sysclient_parseSlowConsumersStats, NULL));

    IFOK(s, _parseHTTPReqStats(varz, node));
    IFOK(s, sysclient_rawJSONArray(&varz->TrustedOperatorsClaimJSON,
                                   &varz->TrustedOperatorsClaimJSONCount, node,
                                   "trusted_operators_claim"));
    return s;
}

static void
_freeVarz(natsSysVarz *varz)
{
    sysclient_freeFields(varz, _varzFields, SYS_NFIELDS(_varzFields));

    _freeClusterOpts(&varz->Cluster);
    _freeGatewayOpts(&varz->Gateway);
    _freeLeafNodeOpts(&varz->LeafNode);
    _freeMQTTOpts(&varz->MQTT);
    _freeWebsocketOpts(&varz->Websocket);
    sysclient_freeJetStreamVarz(&varz->JetStream);

    sysclient_freeObjectPtr((void **) &varz->OCSPResponseCache, _freeOCSPCache);
    sysclient_freeObjectPtr((void **) &varz->SlowConsumersStats, NULL);

    _freeHTTPReqStats(varz);
    sysclient_freeStrArray(&varz->TrustedOperatorsClaimJSON,
                           &varz->TrustedOperatorsClaimJSONCount);
}

static natsStatus
_respFromMsg(void **newResp, natsMsg *msg)
{
    natsSysVarzResp *resp;
    natsStatus       s;

    *newResp = NULL;

    resp = (natsSysVarzResp *) NATS_CALLOC(1, sizeof(natsSysVarzResp));
    if (resp == NULL)
        return NATS_NO_MEMORY;

    s = sysclient_decodeResp(msg, &resp->Server, &resp->Error, &resp->Varz, "data",
                             _parseVarz);
    if (s != NATS_OK)
    {
        natsSysVarzResp_Destroy(resp);
        return s;
    }

    *newResp = resp;
    return NATS_OK;
}

static void
_destroyResp(void *resp)
{
    natsSysVarzResp_Destroy((natsSysVarzResp *) resp);
}

natsStatus
natsSysClient_Varz(natsSysVarzResp **newResp, natsSysClient *client,
                   const char *serverID, const natsSysVarzOptions *opts, int64_t timeout)
{
    return sysclient_request((void **) newResp, client, serverID, SYS_SUBJ_VARZ,
                             _marshalOptions, opts, 64, timeout,
                             _respFromMsg);
}

natsStatus
natsSysClient_VarzPing(natsSysVarzRespList *list, natsSysClient *client,
                       const natsSysVarzOptions *opts, int64_t timeout)
{
    if (list == NULL)
        return NATS_INVALID_ARG;

    return sysclient_ping((void ***) &list->Resps, &list->Count, client, SYS_SUBJ_VARZ,
                          _marshalOptions, opts, 64, timeout, _respFromMsg,
                          _destroyResp);
}

void
natsSysVarzResp_Destroy(natsSysVarzResp *resp)
{
    if (resp == NULL)
        return;

    sysclient_freeServerInfo(&resp->Server);
    sysclient_freeAPIError(&resp->Error);
    _freeVarz(&resp->Varz);
    NATS_FREE(resp);
}

void
natsSysVarzRespList_Destroy(natsSysVarzRespList *list)
{
    if (list == NULL)
        return;

    sysclient_freeRespList((void ***) &list->Resps, &list->Count, _destroyResp);
}
