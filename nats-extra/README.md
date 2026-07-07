# Core NATS Extensions

Higher-level core NATS utilities on top of [cnats](https://github.com/nats-io/nats.c).

**Request-many**: scatter a single request and gather multiple replies. Core NATS
request/reply returns only the first response; this library publishes to a private
inbox and collects every reply until a caller-defined stop condition is met. The
gather ends on the first of these to occur:

- **timeout** — an overall deadline for the whole gather (defaults to a built-in
  value so a gather never blocks indefinitely);
- **stall** — a quiet period with no new reply, measured from the last one received;
- **count** — a fixed number of replies;
- **sentinel** — a caller-supplied predicate that recognises a terminating reply.

Requires only a core NATS server; JetStream is not needed.

# Quick Start: request-many

Synchronous gather — block until a stop condition is met or `timeout` elapses:

```c
#include <requestmany.h>

natsConnection      *nc;
natsRequestManyOpts  opts;
natsMsgList          list = {0};

natsConnection_ConnectTo(&nc, "nats://localhost:4222");

natsRequestManyOpts_Init(&opts);
opts.stall   = 100;  // stop 100 ms after the last reply arrives
opts.count   = 10;   // ...or once 10 replies are gathered
opts.timeout = 5000; // ...or after 5 s overall, whichever comes first

natsStatus s = natsRequestMany_Request(&list, nc, "help", NULL, 0, &opts);
// s == NATS_OK when a stop condition ended the gather; NATS_TIMEOUT when only
// the overall deadline was reached (the partial result is still in `list`).

for (int i = 0; i < list.Count; i++)
    /* ... use list.Msgs[i] ... */;

natsMsgList_Destroy(&list);
```

Leave a field at its zero value to disable that stop condition. `timeout` is the
exception: 0 selects a built-in default rather than "no deadline".

A **sentinel** ends the gather on a reply the caller recognises. The inspected
message is destroyed and is not appended to the list:

```c
static bool onReply(natsMsg *msg, void *closure) {
    // e.g. stop on an empty-payload "no more responders" marker.
    return natsMsg_GetDataLength(msg) == 0;
}

natsRequestManyOpts_Init(&opts);
opts.sentinel = onReply;
// Optionally carry caller state into the predicate:
// opts.sentinelClosure = &myState;
natsRequestMany_Request(&list, nc, "help", NULL, 0, &opts);
```

To attach custom headers to the request, build the message yourself and use the
`Msg` form. The library publishes a copy whose reply subject is its private
inbox, leaving your message untouched:

```c
natsMsg *req = NULL;
natsMsg_Create(&req, "help", NULL, "ping", 4);
natsMsgHeader_Set(req, "X-Req", "orbit");

natsRequestMany_RequestMsg(&list, nc, req, &opts);

natsMsg_Destroy(req);
natsMsgList_Destroy(&list);
```

Unless a call returns `NATS_INVALID_ARG`, always release the results with
`natsMsgList_Destroy`, even on `NATS_TIMEOUT` or a transport error — a partial
result may still be present.

# License

Unless otherwise noted, the NATS source files are distributed
under the Apache Version 2.0 license found in the LICENSE file.
