# NATS System Client

A client for the [NATS server monitoring endpoints](https://docs.nats.io/running-a-nats-service/configuration/sys_accounts)
published over the `$SYS` account, on top of
[cnats](https://github.com/nats-io/nats.c). A C port of orbit.go's
[`natssysclient`](https://github.com/synadia-io/orbit.go/tree/main/natssysclient).

Each of `VARZ`, `STATSZ`, `CONNZ`, `SUBSZ`, `HEALTHZ` and `JSZ` can be queried
from one server by ID, or from every server by a `PING` scatter-gather that
ends on the first of a configured server count, a stall interval with no new
reply, or the request timeout. The connection must be on the system account.
Response structures follow the nats-server v2.10.23 wire format.

# Quick Start

The headers install under `include/nats-sysclient/`; the in-tree examples and
tests put `src/` on the include path and include `"sysclient.h"` directly.

```c
#include <nats-sysclient/sysclient.h>
#include <nats-sysclient/healthz.h>

natsConnection         *nc   = NULL;
natsSysClient          *sys  = NULL;
natsSysHealthzRespList  list = {NULL, 0};
natsStatus              s;
int                     i;

s = natsConnection_ConnectTo(&nc, "nats://admin:s3cr3t!@127.0.0.1:4222");
if (s == NATS_OK)
    s = natsSysClient_Create(&sys, nc, NULL);

// Every server; a timeout of 0 means the default 10s.
if (s == NATS_OK)
    s = natsSysClient_HealthzPing(&list, sys, NULL, 0);

for (i = 0; (s == NATS_OK) && (i < list.Count); i++)
{
    natsSysHealthzResp *resp = list.Resps[i];

    // A server-reported error is decoded but not acted on; the payload is
    // zeroed. Strings the server omitted are NULL.
    if (resp->Error.Code != 0)
        printf("%s: error %d\n", resp->Server.Name, resp->Error.Code);
    else
        printf("%s: %s\n", resp->Server.Name,
               (resp->Healthz.Status != NULL ? resp->Healthz.Status : "?"));
}

natsSysHealthzRespList_Destroy(&list);
natsSysClient_Destroy(sys);
natsConnection_Destroy(nc);
```

For one server, pass a `Server.ID` from a previous response to
`natsSysClient_Healthz`.

## Pagination

`CONNZ`, `SUBSZ` and `JSZ` return one page at a time. `natsSysClient_ConnzEach`
walks every page on one server, calling a handler per page:

```c
static bool
onPage(const natsSysConnzResp *page, void *closure)
{
    int i;

    for (i = 0; i < page->Connz.ConnsCount; i++)
        printf("cid %" PRIu64 "\n", page->Connz.Conns[i]->Cid);

    return true; // false to stop early
}

natsSysConnzOptions opts;

natsSysConnzOptions_Init(&opts);
opts.Limit = 100; // page size

s = natsSysClient_ConnzEach(sys, serverID, &opts, 0, onPage, NULL);
```

The page is destroyed when the handler returns. The `timeout` covers the whole
walk, not each page.

`natsSysClient_ConnzPingEach` pings once and returns one independent walk per
responding server; walks may run on separate threads:

```c
natsSysConnzWalkList walks = {NULL, 0};
int                  i;

s = natsSysClient_ConnzPingEach(&walks, sys, &opts, 0);

for (i = 0; i < walks.Count; i++)
    natsSysConnzWalk_Run(walks.Walks[i], 0, onPage, NULL);

natsSysConnzWalkList_Destroy(&walks);
```

`JSZ` paginates over accounts and only when `Accounts` is set; its total is
`JSInfo.JetStreamStats.Accounts`.

> **Note**: nats-server pages `SUBSZ` by offset over a sublist with no stable
> ordering ([nats-server#7009](https://github.com/nats-io/nats-server/pull/7009)),
> so a walk can miss subscriptions and repeat others. For a complete list,
> request a `Limit` that covers the reported `Total` and take the single page.

## Configuration

`natsSysClientOpts` controls when a `*Ping` gather stops:

- `StallInterval`: stop when no reply arrives within this many milliseconds
  after the first. Defaults to 300; must be positive.
- `ServerCount`: stop after this many replies. Defaults to -1 (no limit).

## Timeouts

Every request takes a trailing `timeout` in milliseconds; 0 selects
`NATS_SYS_DEFAULT_REQUEST_TIMEOUT` (10s). It is capped at seven days, so
`INT64_MAX` means "unbounded".

# Error handling

| Status | Meaning |
| --- | --- |
| `NATS_INVALID_ARG` | A bad argument: a `NULL` out-param, an empty server ID, a negative timeout. |
| `NATS_NOT_FOUND` | A by-ID request nobody answered. |
| `NATS_NO_RESPONDERS` | A ping with nothing subscribed to the subject; usually the connection is not on the system account. |
| `NATS_TIMEOUT` | A by-ID request not answered in time. |
| `NATS_ERR` | A malformed response. |

A ping that hears nothing before the timeout succeeds with `Count == 0`. A
response carrying a server-reported error returns `NATS_OK` with a zeroed
payload; check `Error.Code`. If any reply in a ping fails to decode, the whole
batch is discarded.

## Timestamps

Timestamps are RFC 3339 text. `natsSysTime_Parse` converts one to Unix
nanoseconds and rejects anything outside 1677-09-21 … 2262-04-11, the `int64`
range; a JetStream stream that has never been written reports
`0001-01-01T00:00:00Z`.

# Deviations from orbit.go

- `ServerStatsz`/`ServerSubsz` become `natsSysClient_Statsz`/`_Subsz`; the
  `Server` prefix only avoided a Go name clash.
- `natsSysClient` is an opaque handle rather than an exported struct.
- Timestamps are RFC 3339 text, not a numeric type; see `natsSysTime_Parse`.
- Durations the server sends as a Go `time.Duration` (`PingInterval`,
  `WriteDeadline`, `SyncInterval`, `AckWait`, `HandshakeTimeout`,
  `PeerInfo.Active`) are `int64_t` nanoseconds. Every other timeout in this
  library is milliseconds.
- `Varz.HTTPReqStats` is an array of `{Path, Count}` in server order, not a
  map.
- The per-server ping iterators stop on an empty page. orbit.go's
  (`AllConnzPing`, `AllServerSubszPing`, `AllJszPing`) do not, and spin
  forever when connections close between pages; its single-server iterators
  have the guard.
- `All*` becomes `*Each`, and `[]iter.Seq2` becomes a walk list.
- `Varz.TrustedOperatorsClaim` and `JSZ`'s stream subtrees (`cluster`,
  `config`, `state`, `consumer_detail`, `mirror`, `sources`) are carried as
  the JSON text the server sent: orbit.c has no JWT or JetStream model for
  them and cnats exposes no unmarshaller.
- Shared types (`SlowConsumersStats`, `JetStreamVarz`, ...) live in
  `sysclient.h` rather than split across the endpoint headers, which would
  make those mutually dependent.

# Examples

Build with `-DORBIT_BUILD_EXAMPLES=ON`. One per endpoint in
[examples/](examples/); each takes an optional URL and needs a server with a
system account:

```
accounts {
  $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
}
```

`jsz.c` also needs JetStream and an account using it:

```
accounts {
  $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
  APP  { jetstream: enabled, users: [ { user: app, pass: app } ] }
}
jetstream { store_dir: "/tmp/jsz-example" }
```

# License

Unless otherwise noted, the NATS source files are distributed under the Apache
Version 2.0 license found in the LICENSE file.
