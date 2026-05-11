# NATS Counters

Distributed counters for NATS JetStream, providing high-performance counter operations with arbitrary precision integers.

Wraps a JetStream stream configured with `allow_msg_counter` and `allow_direct`. The server atomically increments and stores each counter.

Requires NATS Server v2.12 or newer and a stream created with both `AllowDirect = true` and `AllowMsgCounter = true`.

# Quick Start

Increment, decrement, read:

```c
#include <nats_counters.h>

natsConnection *nc;
jsCtx          *js;
natsCounter    *c;
long long       total;

natsConnection_ConnectTo(&nc, "nats://localhost:4222");
natsConnection_JetStream(&js, nc, NULL);

natsCounter_GetFromStream(&c, js, nc, "EVENTS_COUNTER");

natsCounter_Add(c, "events_counter.clicks", 5, &total);    /* total = 5 */
natsCounter_Increment(c, "events_counter.clicks", &total); /* total = 6 */
natsCounter_Load(c, "events_counter.clicks", &total);      /* total = 6 */

natsCounter_Destroy(c);
```

For values that may exceed `long long`, use the `Str` variants — they accept and return heap-allocated decimal strings:

```c
char *value = NULL;
natsCounter_AddStr(c, "big.counter", "+99999999999999999999999999", &value);
/* ... use value ... */
free(value);
```

Fetch multiple counters in a single round-trip:

```c
const char *subjects[] = { "events.a", "events.b", "events.c" };
natsCounterEntryList list = {0};

natsCounter_GetMultiple(&list, c, subjects, 3, 5000);
for (int i = 0; i < list.Count; i++) {
    natsCounterEntry *e = list.Entries[i];
    /* e->subject, e->value, e->increment, ... */
}
natsCounterEntryList_Destroy(&list);
```

See `examples/distributed_counter.c` for source-tracking across sourced streams.

# License

Unless otherwise noted, the NATS source files are distributed
under the Apache Version 2.0 license found in the LICENSE file.
