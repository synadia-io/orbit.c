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

// Lists every connection on one server, a page at a time.
//
// Prerequisites:
//   A nats-server with a system account, e.g.
//
//     accounts {
//       $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
//     }
//
// Usage:
//   nats-sysclient-connz [url] [server-id]
//
//   url       defaults to nats://admin:s3cr3t!@127.0.0.1:4222
//   server-id defaults to whichever server answers a ping first

#include "connz.h"
#include "sysclient.h"

#include <inttypes.h>
#include <nats/nats.h>

#include <stdio.h>
#include <string.h>

#define DEFAULT_URL "nats://admin:s3cr3t!@127.0.0.1:4222"
#define PAGE_SIZE   (8)
#define MAX_CONNS   (500)

static const char *
_orDash(const char *s)
{
    return (s != NULL ? s : "-");
}

typedef struct
{
    int pages;
    int conns;
} walkState;

// Called once per page; the page is destroyed when this returns.
static bool
_onPage(const natsSysConnzResp *page, void *closure)
{
    walkState *st = (walkState *) closure;
    int        i;

    st->pages++;

    printf("-- page %d: connections %d-%d of %d\n", st->pages, page->Connz.Offset + 1,
           page->Connz.Offset + page->Connz.ConnsCount, page->Connz.Total);

    for (i = 0; i < page->Connz.ConnsCount; i++)
    {
        natsSysConnInfo *conn = page->Connz.Conns[i];

        printf("   cid %-6" PRIu64 " %-8s %s:%-5d %-12s subs=%-3u in=%" PRId64 " out=%" PRId64
               "\n",
               conn->Cid, _orDash(conn->Kind), _orDash(conn->IP), conn->Port,
               _orDash(conn->Name), conn->NumSubs, conn->InMsgs, conn->OutMsgs);
    }

    st->conns += page->Connz.ConnsCount;

    // Stop early on a very busy server.
    return (st->conns < MAX_CONNS);
}

int
main(int argc, char **argv)
{
    const char         *url      = (argc > 1 ? argv[1] : DEFAULT_URL);
    const char         *serverID = (argc > 2 ? argv[2] : NULL);
    natsConnection     *nc       = NULL;
    natsSysClient      *sys      = NULL;
    natsSysConnzResp   *probe    = NULL;
    natsSysConnzOptions opts;
    walkState           st = {0, 0};
    char                id[128];
    natsStatus          s;
    int                 rc = 1;

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

    // "PING" as a by-ID target reaches every server and the first reply wins,
    // which is enough to discover an ID; the walk itself must use a real one.
    if (serverID == NULL)
    {
        s = natsSysClient_Connz(&probe, sys, "PING", NULL, 0);
        if (s != NATS_OK)
        {
            fprintf(stderr, "Unable to discover a server: %s\n", natsStatus_GetText(s));
            if (s == NATS_NOT_FOUND)
                fprintf(stderr, "Is this connection on the system account?\n");
            goto done;
        }
        // From the envelope, not the payload: an error response zeroes the
        // payload, so Connz.ID could be NULL while Server.ID is not.
        if (probe->Error.Code != 0)
        {
            fprintf(stderr, "CONNZ reported error %d: %s\n", probe->Error.Code,
                    _orDash(probe->Error.Description));
            goto done;
        }
        snprintf(id, sizeof(id), "%s", _orDash(probe->Server.ID));
        serverID = id;
    }

    natsSysConnzOptions_Init(&opts);
    opts.Limit         = PAGE_SIZE;
    opts.Subscriptions = false;
    opts.Sort          = NATS_SYS_SORT_CID;

    printf("Walking CONNZ on %s, %d per page\n\n", serverID, PAGE_SIZE);

    s = natsSysClient_ConnzEach(sys, serverID, &opts, 30000, _onPage, &st);
    if (s != NATS_OK)
    {
        fprintf(stderr, "\nCONNZ walk failed: %s\n", natsStatus_GetText(s));
        if (s == NATS_NOT_FOUND)
            fprintf(stderr, "No server answered to the ID %s\n", serverID);
        goto done;
    }

    printf("\n%d connection(s) over %d page(s)\n", st.conns, st.pages);
    rc = 0;

done:
    natsSysConnzResp_Destroy(probe);
    natsSysClient_Destroy(sys);
    natsConnection_Destroy(nc);

    return rc;
}
