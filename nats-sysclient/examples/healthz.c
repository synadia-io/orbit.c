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

// Reports the health of every server in the cluster.
//
// Prerequisites:
//   A nats-server with a system account, e.g.
//
//     accounts {
//       $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
//     }
//
// Usage:
//   nats-sysclient-healthz [url]
//
//   url defaults to nats://admin:s3cr3t!@127.0.0.1:4222

#include "healthz.h"
#include "sysclient.h"

#include <nats/nats.h>

#include <stdio.h>

#define DEFAULT_URL "nats://admin:s3cr3t!@127.0.0.1:4222"

static const char *
_orDash(const char *s)
{
    return (s != NULL ? s : "-");
}

int
main(int argc, char **argv)
{
    const char             *url  = (argc > 1 ? argv[1] : DEFAULT_URL);
    natsConnection         *nc   = NULL;
    natsSysClient          *sys  = NULL;
    natsSysHealthzRespList  list = {NULL, 0};
    natsSysHealthzOptions   opts;
    natsStatus              s;
    int                     rc = 1;
    int                     i;

    s = natsConnection_ConnectTo(&nc, url);
    if (s != NATS_OK)
    {
        fprintf(stderr, "Unable to connect to %s: %s\n", url, natsStatus_GetText(s));
        return 1;
    }

    s = natsSysClient_Create(&sys, nc, NULL);
    if (s != NATS_OK)
    {
        fprintf(stderr, "Unable to create the system client: %s\n", natsStatus_GetText(s));
        natsConnection_Destroy(nc);
        return 1;
    }

    // Ask for the detailed error list, so an unhealthy server explains itself.
    natsSysHealthzOptions_Init(&opts);
    opts.Details = true;

    s = natsSysClient_HealthzPing(&list, sys, &opts, 0);
    if (s != NATS_OK)
    {
        fprintf(stderr, "HEALTHZ ping failed: %s\n", natsStatus_GetText(s));
        if (s == NATS_NO_RESPONDERS)
            fprintf(stderr, "Is this connection on the system account?\n");
        goto done;
    }

    printf("%d server(s) responded\n", list.Count);

    for (i = 0; i < list.Count; i++)
    {
        natsSysHealthzResp *resp = list.Resps[i];
        int                 j;

        // The client decodes the envelope's error but never acts on it, so
        // check it before trusting the payload.
        if (resp->Error.Code != 0)
        {
            printf("  %-16s error %d: %s\n", _orDash(resp->Server.Name), resp->Error.Code,
                   _orDash(resp->Error.Description));
            continue;
        }

        printf("  %-16s %s\n", _orDash(resp->Server.Name), _orDash(resp->Healthz.Status));

        for (j = 0; j < resp->Healthz.ErrorsCount; j++)
        {
            natsSysHealthzError *e = &resp->Healthz.Errors[j];

            printf("      type=%d account=%s stream=%s consumer=%s: %s\n",
                   (int) e->Type, _orDash(e->Account), _orDash(e->Stream),
                   _orDash(e->Consumer), _orDash(e->Error));
        }
    }

    rc = 0;

done:
    natsSysHealthzRespList_Destroy(&list);
    natsSysClient_Destroy(sys);
    natsConnection_Destroy(nc);

    return rc;
}
