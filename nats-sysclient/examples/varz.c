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

// Prints general information about every server in the cluster.
//
// Also shows the three places where a VARZ field is not the plain C type its
// name suggests: timestamps arrive as RFC 3339 text, durations as nanoseconds,
// and the monitoring request counters as an array rather than a map.
//
// Prerequisites:
//   A nats-server with a system account, e.g.
//
//     accounts {
//       $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
//     }
//
// Usage:
//   nats-sysclient-varz [url]
//
//   url defaults to nats://admin:s3cr3t!@127.0.0.1:4222

#include "sysclient.h"
#include "varz.h"

#include <nats/nats.h>

#include <inttypes.h>
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
    const char         *url  = (argc > 1 ? argv[1] : DEFAULT_URL);
    natsConnection     *nc   = NULL;
    natsSysClient      *sys  = NULL;
    natsSysVarzRespList list = {NULL, 0};
    natsStatus          s;
    int                 rc = 1;
    int                 i;

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

    s = natsSysClient_VarzPing(&list, sys, NULL, 0);
    if (s != NATS_OK)
    {
        fprintf(stderr, "VARZ ping failed: %s\n", natsStatus_GetText(s));
        if (s == NATS_NO_RESPONDERS)
            fprintf(stderr, "Is this connection on the system account?\n");
        goto done;
    }

    printf("%d server(s) responded\n", list.Count);

    for (i = 0; i < list.Count; i++)
    {
        natsSysVarzResp *resp = list.Resps[i];
        natsSysVarz     *varz = &resp->Varz;
        int64_t          startNanos;
        int              j;

        // The client decodes the envelope's error but never acts on it, so
        // check it before trusting the payload.
        if (resp->Error.Code != 0)
        {
            printf("\n%s: error %d: %s\n", _orDash(resp->Server.Name), resp->Error.Code,
                   _orDash(resp->Error.Description));
            continue;
        }

        printf("\n%s (%s)\n", _orDash(varz->Name), _orDash(varz->ID));
        printf("  version      %s on %s:%d\n", _orDash(varz->Version), _orDash(varz->Host),
               varz->Port);
        printf("  uptime       %s\n", _orDash(varz->Uptime));
        printf("  connections  %d current, %" PRIu64 " total\n", varz->Connections,
               varz->TotalConnections);
        printf("  traffic      in %" PRId64 " msgs / out %" PRId64 " msgs\n", varz->InMsgs,
               varz->OutMsgs);

        // Timestamps are the RFC 3339 text the server sent. natsSysTime_Parse
        // turns one into Unix nanoseconds when you need to compare or subtract.
        if (natsSysTime_Parse(&startNanos, varz->Start) == NATS_OK)
            printf("  started      %s (unix %" PRId64 "s)\n", varz->Start,
                   startNanos / 1000000000);

        // Durations are nanoseconds, which is what the server sends. Every
        // other timeout in this library is milliseconds.
        printf("  ping every   %" PRId64 " ms\n", varz->PingInterval / 1000000);

        if (varz->TagsCount > 0)
        {
            printf("  tags        ");
            for (j = 0; j < varz->TagsCount; j++)
                printf(" %s", varz->Tags[j]);
            printf("\n");
        }

        // The server sends these as a JSON object keyed by path. C has no map,
        // so they arrive as an array in the order the server sent them.
        for (j = 0; j < varz->HTTPReqStatsCount; j++)
            printf("  http %-10s %" PRIu64 "\n", varz->HTTPReqStats[j].Path,
                   varz->HTTPReqStats[j].Count);

        // Members the server may omit entirely are pointers, so NULL means the
        // server did not report them rather than "zero".
        if (varz->JetStream.Stats != NULL)
            printf("  jetstream    %d account(s), %" PRIu64 " bytes stored\n",
                   varz->JetStream.Stats->Accounts, varz->JetStream.Stats->Store);
        if (varz->JetStream.Meta != NULL)
            printf("  meta group   %s, leader %s, %d peer(s)\n",
                   _orDash(varz->JetStream.Meta->Name), _orDash(varz->JetStream.Meta->Leader),
                   varz->JetStream.Meta->Size);
    }

    rc = 0;

done:
    natsSysVarzRespList_Destroy(&list);
    natsSysClient_Destroy(sys);
    natsConnection_Destroy(nc);

    return rc;
}
