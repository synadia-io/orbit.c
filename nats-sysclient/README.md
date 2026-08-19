# NATS System Client

A client for the [NATS server monitoring endpoints](https://docs.nats.io/running-a-nats-service/configuration/sys_accounts)
published over the `$SYS` account, on top of
[cnats](https://github.com/nats-io/nats.c).

This is a C port of orbit.go's
[`natssysclient`](https://github.com/synadia-io/orbit.go/tree/main/natssysclient).

Each endpoint can be queried two ways: from one server by its ID, or from every
server in the cluster by scattering the request to a `PING` subject and
gathering the replies. The gather ends on whichever comes first of a configured
server count, a stall interval elapsing with no new reply, or the request
timeout.

The connection you hand the client must be authenticated to the system account,
otherwise no server will answer.

> **Note**: response structures follow the NATS server v2.10.23 wire format.

All six endpoints are implemented.

## Pagination

`CONNZ`, `SUBSZ` and `JSZ` return one page at a time. Rather than managing
offsets yourself, hand a callback to `natsSysClient_ConnzEach`:

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

The page is borrowed and destroyed once the callback returns, so copy anything
you need to keep. The `timeout` covers the **whole walk**, not each page.

For the cluster, `natsSysClient_ConnzPingEach` pings once and hands back one
independent walk per responding server:

```c
natsSysConnzWalkList walks = {NULL, 0};
int                  i;

s = natsSysClient_ConnzPingEach(&walks, sys, &opts, 0);

for (i = 0; i < walks.Count; i++)
    natsSysConnzWalk_Run(walks.Walks[i], 0, onPage, NULL);

natsSysConnzWalkList_Destroy(&walks);
```

Each walk owns its first page and shares nothing but the connection, so they
may be run concurrently from separate threads.

> **Note**: `SUBSZ` pagination is unreliable, in the server rather than here.
> nats-server pages by offset over a sublist that has no stable ordering
> ([nats-server#7009](https://github.com/nats-io/nats-server/pull/7009)), so a
> walk can miss some subscriptions and repeat others while the per-page counts
> and the reported total stay consistent — measured on v2.14.0-RC.1, seven
> subscriptions walked at `Limit: 5` returned one twice and three not at all.
> When you need a complete list, ask for a `Limit` large enough to cover the
> reported `Total` and take the single page.

# Quick Start

```c
#include <nats-sysclient/sysclient.h>
#include <nats-sysclient/healthz.h>

natsConnection         *nc   = NULL;
natsSysClient          *sys  = NULL;
natsSysHealthzRespList  list = {NULL, 0};
natsStatus              s;
int                     i;

// The connection must be on the system account.
s = natsConnection_ConnectTo(&nc, "nats://admin:s3cr3t!@127.0.0.1:4222");
if (s == NATS_OK)
    s = natsSysClient_Create(&sys, nc, NULL);

// Health of every server in the cluster. A timeout of 0 means the default 10s.
if (s == NATS_OK)
    s = natsSysClient_HealthzPing(&list, sys, NULL, 0);

for (i = 0; (s == NATS_OK) && (i < list.Count); i++)
{
    natsSysHealthzResp *resp = list.Resps[i];

    // The envelope's error is decoded but never acted on, so check it before
    // trusting the payload — it is zeroed when the server reported one. Every
    // string is NULL when the server omitted it.
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

For a single server, pass the ID from a previous response's
`Server.ID` to `natsSysClient_Healthz`.

## Configuration

`natsSysClientOpts` controls when a scatter-gather stops. Both settings apply
only to the `*Ping` calls.

- `StallInterval` — once the first reply has arrived, stop if no further reply
  turns up within this many milliseconds. Defaults to 300.
- `ServerCount` — stop after this many replies. Set it to the number of servers
  in the cluster as seen by the connected server. Defaults to -1 (no limit).

They are not exclusive; set both to wait for a specific number of servers
within a specific time frame.

## Timeouts

Every request takes a trailing `timeout` in milliseconds, where 0 selects
`NATS_SYS_DEFAULT_REQUEST_TIMEOUT` (10s). For the paginated `*Each` and
`*Walk_Run` calls it is the budget for the whole walk, not for each page.

# Error handling

Everything returns a `natsStatus`.

| Status | Meaning |
| --- | --- |
| `NATS_INVALID_ARG` | A bad argument to the call — a `NULL` out-param, an empty server ID, a negative timeout. |
| `NATS_NOT_FOUND` | A by-ID request that nobody answered. |
| `NATS_NO_RESPONDERS` | A ping where nothing at all is subscribed to the system subject — usually the connection is not on the system account. |
| `NATS_TIMEOUT` | A by-ID request that was not answered in time. |
| `NATS_ERR` | A malformed response. `NATS_INVALID_ARG` is reserved for bad arguments to the call itself. |

Two behaviours are worth calling out because they surprise C callers:

**A ping that hears nothing before the timeout succeeds with `Count == 0`.**
There is no way to know how many servers should answer, so a gather returns
whatever arrived. That is distinct from `NATS_NO_RESPONDERS` above.

**The envelope's `error` field is decoded but never acted on.** A response
carrying a server-reported error still returns `NATS_OK`, with a zeroed
payload. Check `resp->Error.Code != 0` before trusting the payload.

If any reply in a ping fails to decode, the whole batch is discarded rather
than yielding a partial result.

## Timestamps

`natsSysTime_Parse` converts the RFC 3339 text the server sends into Unix
nanoseconds. It rejects anything outside **1677-09-21 … 2262-04-11**, the range
`int64` nanoseconds can represent — the same window Go documents for
`time.Time.UnixNano()`. That boundary is reachable from real data: a JetStream
stream that has never been written reports `0001-01-01T00:00:00Z` for
`first_ts` and `last_ts`.

A walk's `timeout` is capped at seven days. `INT64_MAX` is accepted and means
"effectively unbounded" rather than overflowing into an instant timeout.

# Deviations from orbit.go

Go quirks are reproduced faithfully; where the C API departs, here is why.

- **`ServerStatsz`/`ServerSubsz` become `natsSysClient_Statsz`/`_Subsz`.** The
  `Server` prefix exists in Go only to avoid colliding with the `ServerStats`
  and `Subsz` type names inside one package. `natsSysClient_` already
  disambiguates.
- **`natsSysClient` is an opaque handle** where Go's `System` is an exported
  struct with unexported fields. It holds no mutable state after creation, so
  the client is safe to use from multiple threads.
- **Timestamps are RFC 3339 text, not a numeric type.** orbit.c has no date
  parser and cnats does not export one. `natsSysTime_Parse` converts to Unix
  nanoseconds when you need it.
- **Durations are nanoseconds.** Fields the server sends as a Go
  `time.Duration` — `PingInterval`, `WriteDeadline`, `SyncInterval`, `AckWait`,
  `HandshakeTimeout`, `PeerInfo.Active` — are `int64_t` nanoseconds, which is
  the wire format. Every *other* timeout in this library is milliseconds, so
  the units are called out on each field.
- **`Varz.HTTPReqStats` is an array, not a map.** orbit.go types it
  `map[string]uint64`; C has no map, so the entries are carried in the order
  the server sent them as `natsSysHTTPReqStat { Path, Count }`.
- **The empty-page infinite loop is fixed, not reproduced.** orbit.go's
  per-server ping iterators (`AllConnzPing`, `AllServerSubszPing`,
  `AllJszPing`) advance the offset by the page size with no empty-page guard,
  so a server returning a zero-length page while `offset < total` spins forever
  hammering the system account. That happens naturally when connections close
  between pages. The single-server iterators immediately above them in the Go
  source *do* have the guard, so the omission reads as an oversight, and there
  is no observable behaviour to preserve in a loop that never returns.
- **`All*` becomes `*Each`, and `[]iter.Seq2` becomes a walk list.** C has no
  lazy iterators. See the Pagination section above.
- **`Varz.TrustedOperatorsClaim` is raw JSON.** orbit.go types it
  `[]*jwt.OperatorClaims`, a full nats-io/jwt claims tree. orbit.c ships no JWT
  claims model and cnats exposes none, so rather than drop the data or decode
  only part of it, each element arrives as the JSON text the server sent, in
  `TrustedOperatorsClaimJSON`.
- **`JSZ`'s stream subtrees are raw JSON.** `StreamDetail`'s `cluster`,
  `config`, `state`, `consumer_detail`, `mirror` and `sources` are typed in
  orbit.go with nats.go's JetStream structures. cnats declares equivalents
  (`jsStreamConfig` and friends) but exposes no public JSON unmarshaller for
  them, so they arrive as `ClusterJSON`, `ConfigJSON`, `StateJSON`,
  `ConsumerJSON`, `MirrorJSON` and `SourcesJSON`. Nothing is dropped; numbers
  round-trip exactly and strings are re-escaped.
- **Only `natsSysJszOptions.Accounts` enables JSZ pagination.** The server
  treats `Streams` and `Consumer` as implying it, so a request carrying only
  those still returns account details — but a walk fetches one page of them and
  stops, exactly as orbit.go's `AllJsz`/`AllJszPing` do. Set `Accounts`
  explicitly whenever you walk.
- **`JSZ` has no `Total`.** Its pagination total is the account count in the
  flattened stats, `JSInfo.JetStreamStats.Accounts`, and pagination applies
  only when `Accounts` is set — without it a walk yields exactly one page.
- **Types are grouped differently across headers.** Go declares
  `SlowConsumersStats` in its statsz file but uses it from varz, and
  `JetStreamVarz` in varz but uses it from statsz. Mirroring that split would
  make the C headers mutually dependent, so shared types live in
  `sysclient.h`.

# Examples

Build with `-DORBIT_BUILD_EXAMPLES=ON`. One per endpoint, in
[examples/](examples/), each picking up whichever part of the API that endpoint
is the best place to show:

| Example | Endpoint | Shows |
| --- | --- | --- |
| [`healthz.c`](examples/healthz.c) | HEALTHZ | The simplest gather, and reading the optional error detail |
| [`varz.c`](examples/varz.c) | VARZ | The fields that are not the plain C type their name suggests: RFC 3339 timestamps, nanosecond durations, the request-counter array |
| [`statsz.c`](examples/statsz.c) | STATSZ | Bounding a gather with `ServerCount` instead of waiting out the stall |
| [`connz.c`](examples/connz.c) | CONNZ | Callback pagination over one server, and stopping a walk early |
| [`subsz.c`](examples/subsz.c) | SUBSZ | The walk list: one independent, resumable walk per server |
| [`jsz.c`](examples/jsz.c) | JSZ | Pagination keyed on `Accounts`, and the raw-JSON stream subtrees |

Each takes an optional URL argument and needs a server with a system account:

```
accounts {
  $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
}
```

`jsz.c` additionally needs JetStream and an account using it:

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
