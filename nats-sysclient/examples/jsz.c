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

// Reports JetStream state for one server, an account at a time.
//
// Prerequisites:
//   A nats-server with JetStream and a system account, e.g.
//
//     accounts {
//       $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
//       APP  { jetstream: enabled, users: [ { user: app, pass: app } ] }
//     }
//     jetstream { store_dir: "/tmp/jsz-example" }
//
// Usage:
//   nats-sysclient-jsz [url] [server-id]
//
//   url       defaults to nats://admin:s3cr3t!@127.0.0.1:4222
//   server-id defaults to whichever server answers a ping first

#include "jsz.h"
#include "sysclient.h"

#include <inttypes.h>
#include <nats/nats.h>

#include <stdio.h>
#include <string.h>

#define DEFAULT_URL "nats://admin:s3cr3t!@127.0.0.1:4222"

// One account per page, so the walk is visible even on a small server.
#define PAGE_SIZE (1)

typedef struct
{
    int pages;
    int accounts;
} walkState;

static const char *
_orDash(const char *s)
{
    return (s != NULL ? s : "-");
}

// Truncates a raw subtree to keep the output readable.
static void
_printRaw(const char *label, const char *json)
{
    if (json == NULL)
        return;

    // Not strnlen: this library is built as C99, where it does not exist.
    printf("        %-9s %.100s%s\n", label, json, (strlen(json) > 100 ? " ..." : ""));
}

// Called once per page; the page is destroyed when this returns.
static bool
_onPage(const natsSysJszResp *page, void *closure)
{
    walkState *st = (walkState *) closure;
    int        i;
    int        j;

    st->pages++;

    for (i = 0; i < page->JSInfo.AccountDetailsCount; i++)
    {
        natsSysAccountDetail *acct = page->JSInfo.AccountDetails[i];

        printf("\n   account %s (%s)\n", _orDash(acct->Name), _orDash(acct->Id));
        printf("     memory %" PRIu64 " / store %" PRIu64 " / %d HA asset(s)\n",
               acct->JetStreamStats.Memory, acct->JetStreamStats.Store,
               acct->JetStreamStats.HAAssets);

        for (j = 0; j < acct->StreamsCount; j++)
        {
            natsSysStreamDetail *stream = &acct->Streams[j];

            printf("     stream %s\n", _orDash(stream->Name));

            // These subtrees are typed by nats.go in the original, but cnats
            // exposes no public JSON unmarshaller for its equivalents, so they
            // are carried as the JSON text the server sent. Nothing is
            // dropped: numbers round-trip exactly and strings are re-escaped.
            _printRaw("config", stream->ConfigJSON);
            _printRaw("state", stream->StateJSON);
            _printRaw("cluster", stream->ClusterJSON);
        }

        st->accounts++;
    }

    return true;
}

int
main(int argc, char **argv)
{
    const char       *url      = (argc > 1 ? argv[1] : DEFAULT_URL);
    const char       *serverID = (argc > 2 ? argv[2] : NULL);
    natsConnection   *nc       = NULL;
    natsSysClient    *sys      = NULL;
    natsSysJszResp   *summary  = NULL;
    natsSysJszOptions opts;
    walkState         st = {0, 0};
    char              id[128];
    natsStatus        s;
    int               rc = 1;

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

    // Without Accounts the response is the server-wide summary. "PING" as a
    // by-ID target reaches every server and the first reply wins, which is
    // enough to discover an ID; the walk itself must use a real one.
    natsSysJszOptions_Init(&opts);
    s = natsSysClient_Jsz(&summary, sys, (serverID != NULL ? serverID : "PING"), &opts, 0);
    if (s != NATS_OK)
    {
        fprintf(stderr, "JSZ request failed: %s\n", natsStatus_GetText(s));
        if (s == NATS_NOT_FOUND)
            fprintf(stderr, "No server answered%s%s\n", (serverID != NULL ? " to the ID " : ""),
                    (serverID != NULL ? serverID : ""));
        goto done;
    }

    // From the envelope: an error response carries a zeroed payload.
    if (serverID == NULL)
    {
        if (summary->Error.Code != 0)
        {
            fprintf(stderr, "JSZ reported error %d: %s\n", summary->Error.Code,
                    _orDash(summary->Error.Description));
            goto done;
        }
        snprintf(id, sizeof(id), "%s", _orDash(summary->Server.ID));
        serverID = id;
    }

    printf("JSZ on %s\n", serverID);

    if (summary->JSInfo.Disabled)
    {
        printf("  JetStream is disabled on this server\n");
        rc = 0;
        goto done;
    }

    printf("  %d stream(s), %d consumer(s), %" PRIu64 " message(s), %" PRIu64 " byte(s)\n",
           summary->JSInfo.Streams, summary->JSInfo.Consumers, summary->JSInfo.Messages,
           summary->JSInfo.Bytes);
    printf("  %d account(s) using JetStream\n", summary->JSInfo.JetStreamStats.Accounts);

    if (summary->JSInfo.Meta != NULL)
        printf("  meta group %s, leader %s, %d peer(s)\n", _orDash(summary->JSInfo.Meta->Name),
               _orDash(summary->JSInfo.Meta->Leader), summary->JSInfo.Meta->Size);

    natsSysJszOptions_Init(&opts);
    opts.Accounts = true;
    opts.Streams  = true;
    opts.Config   = true;
    opts.Limit    = PAGE_SIZE;

    s = natsSysClient_JszEach(sys, serverID, &opts, 30000, _onPage, &st);
    if (s != NATS_OK)
    {
        fprintf(stderr, "\nJSZ walk failed: %s\n", natsStatus_GetText(s));
        goto done;
    }

    printf("\n%d account(s) over %d page(s)\n", st.accounts, st.pages);
    rc = 0;

done:
    natsSysJszResp_Destroy(summary);
    natsSysClient_Destroy(sys);
    natsConnection_Destroy(nc);

    return rc;
}
