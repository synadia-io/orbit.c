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

// Summarises traffic across the cluster, and shows how to bound a gather.
//
// A ping has no way to know how many servers should answer, so by default it
// waits for the stall interval to elapse with no new reply. When you know the
// cluster size, ServerCount lets the gather finish as soon as everyone has
// answered instead of waiting out the stall.
//
// Prerequisites:
//   A nats-server with a system account, e.g.
//
//     accounts {
//       $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
//     }
//
// Usage:
//   nats-sysclient-statsz [url] [expected-server-count]
//
//   url defaults to nats://admin:s3cr3t!@127.0.0.1:4222

#include "statsz.h"
#include "sysclient.h"

#include <inttypes.h>
#include <nats/nats.h>

#include <stdio.h>
#include <stdlib.h>

#define DEFAULT_URL "nats://admin:s3cr3t!@127.0.0.1:4222"

static const char *
_orDash(const char *s)
{
    return (s != NULL ? s : "-");
}

int
main(int argc, char **argv)
{
    const char           *url      = (argc > 1 ? argv[1] : DEFAULT_URL);
    int                   expected = (argc > 2 ? atoi(argv[2]) : -1);
    natsConnection       *nc       = NULL;
    natsSysClient        *sys      = NULL;
    natsSysStatszRespList list     = {NULL, 0};
    natsSysClientOpts     opts;
    natsStatus            s;
    int64_t               totalIn  = 0;
    int64_t               totalOut = 0;
    int                   rc       = 1;
    int                   i;

    s = natsConnection_ConnectTo(&nc, url);
    if (s != NATS_OK)
    {
        fprintf(stderr, "Unable to connect to %s: %s\n", url, natsStatus_GetText(s));
        return 1;
    }

    natsSysClientOpts_Init(&opts);
    if (expected > 0)
    {
        // Stop as soon as this many servers have answered. Left at the default
        // of -1 the gather always waits out the stall interval.
        opts.ServerCount = expected;
    }

    s = natsSysClient_Create(&sys, nc, &opts);
    if (s != NATS_OK)
    {
        fprintf(stderr, "Unable to create the system client: %s\n", natsStatus_GetText(s));
        natsConnection_Destroy(nc);
        return 1;
    }

    s = natsSysClient_StatszPing(&list, sys, NULL, 0);
    if (s != NATS_OK)
    {
        fprintf(stderr, "STATSZ ping failed: %s\n", natsStatus_GetText(s));
        if (s == NATS_NO_RESPONDERS)
            fprintf(stderr, "Is this connection on the system account?\n");
        goto done;
    }

    // Running out of time is normal termination for a gather, not an error, so
    // a short count means some servers did not answer rather than a failure.
    printf("%d server(s) responded", list.Count);
    if ((expected > 0) && (list.Count < expected))
        printf(" (expected %d)", expected);
    printf("\n\n");

    printf("%-16s %5s %6s %12s %12s %6s\n", "SERVER", "CONNS", "SUBS", "IN MSGS", "OUT MSGS",
           "ROUTES");

    for (i = 0; i < list.Count; i++)
    {
        natsSysStatszResp  *resp   = list.Resps[i];
        natsSysServerStats *statsz = &resp->Statsz;

        // The client decodes the envelope's error but never acts on it, so
        // check it before trusting the payload.
        if (resp->Error.Code != 0)
        {
            printf("%-16s error %d: %s\n", _orDash(resp->Server.Name), resp->Error.Code,
                   _orDash(resp->Error.Description));
            continue;
        }

        printf("%-16s %5d %6u %12" PRId64 " %12" PRId64 " %6d\n", _orDash(resp->Server.Name),
               statsz->Connections, statsz->NumSubs, statsz->Received.Msgs, statsz->Sent.Msgs,
               statsz->RoutesCount);

        totalIn += statsz->Received.Msgs;
        totalOut += statsz->Sent.Msgs;
    }

    printf("\nCluster totals: %" PRId64 " in, %" PRId64 " out\n", totalIn, totalOut);

    // Every server reports how many it can see, so a server whose count is
    // below the rest has not converged with the others.
    for (i = 0; i < list.Count; i++)
    {
        if (list.Resps[i]->Error.Code == 0)
        {
            printf("%s sees %d active server(s)\n", _orDash(list.Resps[i]->Server.Name),
                   list.Resps[i]->Statsz.ActiveServers);
        }
    }

    rc = 0;

done:
    natsSysStatszRespList_Destroy(&list);
    natsSysClient_Destroy(sys);
    natsConnection_Destroy(nc);

    return rc;
}
