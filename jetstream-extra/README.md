# JetStream Extra

Higher-level JetStream utilities on top of [cnats](https://github.com/nats-io/nats.c).

The first feature is **batched DIRECT.GET** ([ADR-31](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-31.md)): retrieve many messages from a stream in a single round-trip, including the "last message per subject" (`multi_last`) form. cnats's `js_DirectGetMsg` accepts the batch options but only returns one message; this library streams the full server response and returns the whole batch.

Requires NATS Server v2.10 or newer and a stream created with `AllowDirect = true`.

# Quick Start

Synchronous fetch — block until the batch arrives or `timeout` elapses:

```c
#include <batch_fetch.h>

natsConnection      *nc;
jsBatchFetchOptions  opts;
natsMsgList          list = {0};

natsConnection_ConnectTo(&nc, "nats://localhost:4222");

jsBatchFetchOptions_Init(&opts);
opts.Batch = 100;

jsBatchFetch(&list, nc, "EVENTS", NULL, &opts, 5000, NULL);

for (int i = 0; i < list.Count; i++)
    /* ... use list.Msgs[i] ... */;

natsMsgList_Destroy(&list);
```

Asynchronous fetch — messages are dispatched to a callback as they arrive:

```c
static void onMsg(natsConnection *nc, natsSubscription *sub,
                  natsMsg *msg, void *closure) {
    /* ... use msg ... */
    natsMsg_Destroy(msg);
}

static void onDone(natsStatus s, jsErrCode je, void *closure) {
    /* fires exactly once when the batch terminates */
}

jsBatchFetchAsync(nc, "EVENTS", NULL, &opts, onMsg, onDone, NULL);
```

# License

Unless otherwise noted, the NATS source files are distributed
under the Apache Version 2.0 license found in the LICENSE file.
