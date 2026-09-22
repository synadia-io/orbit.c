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

// Lists the subscriptions on every server in the cluster.
//
// Prerequisites:
//   A nats-server with a system account, e.g.
//
//     accounts {
//       $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
//     }
//
// Usage:
//   nats-sysclient-subsz [url] [subject]
//
//   url     defaults to nats://admin:s3cr3t!@127.0.0.1:4222
//   subject reports only subscriptions that would match it; must be a literal
//           publish subject, not a wildcard pattern

#include "subsz.h"
#include "sysclient.h"

#include <inttypes.h>
#include <nats/nats.h>

#include <stdio.h>

#define DEFAULT_URL "nats://admin:s3cr3t!@127.0.0.1:4222"

// Large enough for one page per server: paging SUBSZ is unreliable
// (nats-server#7009).
#define PAGE_SIZE (4096)

static const char *
_orDash(const char *s)
{
    return (s != NULL ? s : "-");
}

typedef struct
{
    int pages;
    int subs;
} walkState;

// Called once per page; the page is destroyed when this returns.
static bool
_onPage(const natsSysSubszResp *page, void *closure)
{
    walkState *st = (walkState *) closure;
    int        i;

    st->pages++;

    for (i = 0; i < page->Subsz.SubsCount; i++)
    {
        natsSysSubDetail *sub = &page->Subsz.Subs[i];

        printf("   %-30s account=%-12s queue=%-10s cid=%" PRIu64 " msgs=%" PRId64 "\n",
               _orDash(sub->Subject), _orDash(sub->Account), _orDash(sub->Queue), sub->Cid,
               sub->Msgs);
    }

    st->subs += page->Subsz.SubsCount;
    return true;
}

int
main(int argc, char **argv)
{
    const char          *url     = (argc > 1 ? argv[1] : DEFAULT_URL);
    natsConnection      *nc      = NULL;
    natsSysClient       *sys     = NULL;
    natsSysSubszWalkList walks   = {NULL, 0};
    natsSysSubszOptions  opts;
    natsStatus           s;
    int                  rc = 1;
    int                  i;

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

    natsSysSubszOptions_Init(&opts);
    opts.Subscriptions = true;
    opts.Limit         = PAGE_SIZE;
    if (argc > 2)
        opts.Test = argv[2];

    // The options are copied into each walk, so they may live on the stack and
    // go out of scope before the walks run.
    s = natsSysClient_SubszPingEach(&walks, sys, &opts, 0);
    if (s != NATS_OK)
    {
        fprintf(stderr, "SUBSZ ping failed: %s\n", natsStatus_GetText(s));
        if (s == NATS_NO_RESPONDERS)
            fprintf(stderr, "Is this connection on the system account?\n");
        goto done;
    }

    printf("%d server(s) responded\n", walks.Count);

    for (i = 0; i < walks.Count; i++)
    {
        walkState st = {0, 0};

        printf("\n-- %s\n", _orDash(natsSysSubszWalk_ServerID(walks.Walks[i])));

        // The timeout is the budget for the whole walk, not for each page.
        s = natsSysSubszWalk_Run(walks.Walks[i], 30000, _onPage, &st);
        if (s != NATS_OK)
        {
            fprintf(stderr, "   walk failed: %s\n", natsStatus_GetText(s));
            // A server can leave the cluster between the ping and the walk, in
            // which case only that walk fails. Carry on with the others.
            continue;
        }

        printf("   %d subscription(s) over %d page(s)\n", st.subs, st.pages);
    }

    printf("\nNote: a Limit of %d was used so each server answers in one page;\n"
           "      see the SUBSZ pagination caveat in the README.\n",
           PAGE_SIZE);
    rc = 0;

done:
    natsSysSubszWalkList_Destroy(&walks);
    natsSysClient_Destroy(sys);
    natsConnection_Destroy(nc);

    return rc;
}
