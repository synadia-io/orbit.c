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

#include "healthz.h"

#include "marshal.h"
#include "sysclientp.h"
#include "unmarshal.h"

#include "buf.h"
#include "os_shims.h"

#include <string.h>

static const sysField _healthzErrorFields[] = {
    SYS_F(SYS_FLD_INT, natsSysHealthzError, Type, "type"),
    SYS_F(SYS_FLD_STR, natsSysHealthzError, Account, "account"),
    SYS_F(SYS_FLD_STR, natsSysHealthzError, Stream, "stream"),
    SYS_F(SYS_FLD_STR, natsSysHealthzError, Consumer, "consumer"),
    SYS_F(SYS_FLD_STR, natsSysHealthzError, Error, "error"),
};

static const sysField _healthzFields[] = {
    SYS_F(SYS_FLD_STR, natsSysHealthz, Status, "status"),
    SYS_F(SYS_FLD_INT, natsSysHealthz, StatusCode, "status_code"),
    SYS_F(SYS_FLD_STR, natsSysHealthz, Error, "error"),
};

natsStatus
natsSysHealthzOptions_Init(natsSysHealthzOptions *opts)
{
    return sysclient_initOpts(opts, sizeof(*opts));
}

// Every field is optional, so a zeroed options struct marshals to "{}".
static natsStatus
_marshalOptions(natsBuffer *buf, const void *optsv)
{
    const natsSysHealthzOptions *opts = (const natsSysHealthzOptions *) optsv;
    natsJSONWriter w;

    natsJSONWriter_Init(&w, buf);
    natsJSONWriter_StartObject(&w);

    if (opts != NULL)
    {
        sysclient_optBool(&w, "js-enabled-only", opts->JSEnabledOnly);
        sysclient_optBool(&w, "js-server-only", opts->JSServerOnly);
        sysclient_optStr(&w, "account", opts->Account);
        sysclient_optStr(&w, "stream", opts->Stream);
        sysclient_optStr(&w, "consumer", opts->Consumer);
        sysclient_optBool(&w, "details", opts->Details);
    }

    natsJSONWriter_EndObject(&w);
    return natsJSONWriter_Status(&w);
}

static natsStatus
_parseHealthzError(void *elem, natsJSON *node)
{
    return sysclient_scanFields(elem, node, _healthzErrorFields,
                                SYS_NFIELDS(_healthzErrorFields));
}

static void
_freeHealthzError(void *elem)
{
    sysclient_freeFields(elem, _healthzErrorFields, SYS_NFIELDS(_healthzErrorFields));
}

static natsStatus
_parseHealthz(void *dst, natsJSON *node)
{
    natsSysHealthz *healthz = (natsSysHealthz *) dst;
    natsStatus      s;

    s = sysclient_scanFields(healthz, node, _healthzFields, SYS_NFIELDS(_healthzFields));
    IFOK(s, sysclient_valueArray((void **) &healthz->Errors, &healthz->ErrorsCount,
                                 node, "errors", sizeof(natsSysHealthzError),
                                 _parseHealthzError, _freeHealthzError));
    return s;
}

static void
_freeHealthz(void *dst)
{
    natsSysHealthz *healthz = (natsSysHealthz *) dst;

    sysclient_freeFields(healthz, _healthzFields, SYS_NFIELDS(_healthzFields));
    sysclient_freeValueArray((void **) &healthz->Errors, &healthz->ErrorsCount,
                             sizeof(natsSysHealthzError), _freeHealthzError);
}

static const sysEndpoint _endpoint = SYS_ENDPOINT(natsSysHealthzResp, Healthz, SYS_SUBJ_HEALTHZ, "data",
                                                  _marshalOptions, _parseHealthz, _freeHealthz);

natsStatus
natsSysClient_Healthz(natsSysHealthzResp **newResp, natsSysClient *client,
                      const char *serverID, const natsSysHealthzOptions *opts,
                      int64_t timeout)
{
    return sysclient_request((void **) newResp, client, serverID, opts, timeout, &_endpoint);
}

natsStatus
natsSysClient_HealthzPing(natsSysHealthzRespList *list, natsSysClient *client,
                          const natsSysHealthzOptions *opts, int64_t timeout)
{
    if (list == NULL)
        return NATS_INVALID_ARG;

    return sysclient_ping((void ***) &list->Resps, &list->Count, client, opts, timeout,
                          &_endpoint);
}

void
natsSysHealthzResp_Destroy(natsSysHealthzResp *resp)
{
    sysclient_destroyResp(resp, &_endpoint);
}

void
natsSysHealthzRespList_Destroy(natsSysHealthzRespList *list)
{
    if (list == NULL)
        return;

    sysclient_freeList((void ***) &list->Resps, &list->Count, sysclient_destroyResp,
                       &_endpoint);
}
