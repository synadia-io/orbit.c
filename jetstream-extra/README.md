# JetStream Extra

Higher-level JetStream utilities on top of [cnats](https://github.com/nats-io/nats.c).

**Batched DIRECT.GET** ([ADR-31](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-31.md)): retrieve many messages from a stream in a single round-trip, including the "last message per subject" (`multi_last`) form. cnats's `js_DirectGetMsg` accepts the batch options but only returns one message; this library streams the full server response and returns the whole batch.

Requires NATS Server v2.10 or newer and a stream created with `AllowDirect = true`.

**Fast publish**: stream a batch of messages into a stream using server-side flow control instead of awaiting an ack per message. The server emits a coalesced flow ack every `Flow` messages, and the publisher only stalls when too many flow-ack windows are outstanding. The batch is sealed with a single commit.

Requires a stream created with `AllowBatched = true`.

# Quick Start: batched DIRECT.GET

Synchronous fetch — block until the batch arrives or `timeout` elapses:

```c
#include <batch_fetch.h>

natsConnection      *nc;
jsBatchFetchOptions  opts;
natsMsgList          list = {0};

natsConnection_ConnectTo(&nc, "nats://localhost:4222");

jsBatchFetchOptions_Init(&opts);
opts.Batch = 100;

jsBatchFetch_Fetch(&list, nc, "EVENTS", NULL, &opts, 5000, NULL);

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

jsBatchFetch_AsyncFetch(nc, "EVENTS", NULL, &opts, onMsg, onDone, NULL);
```

# Quick Start: fast publish

Stream a batch of messages, then seal it with a single commit:

```c
#include <fast_publish.h>

natsConnection   *nc;
jsCtx            *js;
jsFastPublishCtx *fp = NULL;
jsPubAck         *pubAck = NULL;

natsConnection_ConnectTo(&nc, "nats://localhost:4222");
natsConnection_JetStream(&js, nc, NULL);

// Stream must be created with AllowBatched = true.
// NULL options selects the defaults: Flow = 100, MaxOutstandingAcks = 2,
// AckTimeout = 5000 ms.
jsFastPublishCtx_Create(&fp, js, NULL);

for (int i = 1; i < 1000; i++)
{
    char subj[32], body[48];
    snprintf(subj, sizeof(subj), "orders.new.%d", i);
    snprintf(body, sizeof(body), "{\"order\":%d}", i);
    // Add publishes immediately and only stalls when the outstanding
    // flow-ack window is full.
    jsFastPublish_Add(NULL, fp, nc, subj, body, (int)strlen(body), NULL);
}

// Commit publishes the final message and seals the batch, blocking until
// the server confirms. The context is closed afterwards.
jsFastPublish_Commit(&pubAck, fp, "orders.new.0", "{\"order\":0}", 11, NULL, 5000);

jsPubAck_Destroy(pubAck);
jsFastPublish_Destroy(fp);
```

A #jsFastPublishCtx holds one in-progress batch and is not safe for
concurrent use: all calls on a context must come from a single thread.
Destroying a context whose batch is still open abandons it without
committing — call `jsFastPublish_Commit`, `jsFastPublish_CommitMsg`, or
`jsFastPublish_Close` first to seal it.

# License

Unless otherwise noted, the NATS source files are distributed
under the Apache Version 2.0 license found in the LICENSE file.
