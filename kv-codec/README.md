# KV Codec

Transparent key and value encoding for JetStream KeyValue buckets, on top of
[cnats](https://github.com/nats-io/nats.c)

A `kvCodec` wraps an existing `kvStore` and runs every key and/or value through
a pluggable codec on the way in and out. Use it to:

- store keys that are invalid NATS subjects (spaces, unicode, arbitrary bytes)
  with the **Base64 codec** — each dot-delimited token is encoded separately
  (URL-safe, unpadded), so the subject structure and wildcards keep working;
- address a bucket with filesystem-style keys via the **path codec** —
  `/config/app/database` is stored as `_root_.config.app.database`;
- plug in your own transformation (encryption, compression, ...) as a
  **custom codec** built from plain callbacks;
- compose codecs with **chains** (encode first-to-last, decode last-to-first).

The full `kvStore` surface is wrapped: put/get/create/update/delete/purge,
watchers, key listing (with filters), history, and status.

# Quick Start

```c
#include <kvcodec.h>

natsConnection *nc      = NULL;
jsCtx          *js      = NULL;
kvStore        *kv      = NULL;
kvKeyCodec     *keyC    = NULL;
kvValueCodec   *valC    = NULL;
kvCodec        *codecKV = NULL;
kvCodecEntry   *entry   = NULL;

natsConnection_ConnectTo(&nc, "nats://localhost:4222");
natsConnection_JetStream(&js, nc, NULL);

kvConfig cfg;
kvConfig_Init(&cfg);
cfg.Bucket = "EXAMPLE";
js_CreateKeyValue(&kv, js, &cfg);

// Base64-encode both keys and values (pass NULL for a dimension to leave it raw).
kvKeyCodec_Base64(&keyC);
kvValueCodec_Base64(&valC);
kvCodec_New(&codecKV, kv, keyC, valC);

// "Acme Inc." contains a space — invalid in a raw KV key, fine through the codec.
kvCodec_PutString(NULL, codecKV, "Acme Inc.contact", "info@acme.com");

kvCodec_Get(&entry, codecKV, "Acme Inc.contact");
printf("%s = %s\n", kvCodecEntry_Key(entry), kvCodecEntry_ValueString(entry));
kvCodecEntry_Destroy(entry);

// The wrapper borrows the store and the codecs: destroy it first, then them.
kvCodec_Destroy(codecKV);
kvKeyCodec_Destroy(keyC);
kvValueCodec_Destroy(valC);
kvStore_Destroy(kv);
```

## Watching with wildcards

A key codec is *filterable* when it can encode a pattern while preserving `*`
and `>` wildcards. The built-in codecs all are; watch and filtered-list calls
encode the pattern and decode delivered entries:

```c
kvCodecWatcher *w = NULL;
kvCodecEntry   *e = NULL;

kvCodec_Watch(&w, codecKV, "orders.>", NULL);
while (kvCodecWatcher_Next(&e, w, 5000) == NATS_OK)
{
    if (e == NULL) // initial values delivered
        continue;
    printf("%s = %s\n", kvCodecEntry_Key(e), kvCodecEntry_ValueString(e));
    kvCodecEntry_Destroy(e);
}
kvCodecWatcher_Destroy(w);
```

A custom codec without a filter encoder rejects wildcard patterns with
`NATS_INVALID_SUBJECT`; watch exact keys or use `kvCodec_WatchAll` and filter
client-side.

## Path-style keys

```c
kvKeyCodec *pathC = NULL;

kvKeyCodec_Path(&pathC);
kvCodec_New(&codecKV, kv, pathC, NULL); // keys only, values stored raw

kvCodec_PutString(NULL, codecKV, "/config/app/database", "postgres://localhost");
// stored as the subject "_root_.config.app.database"
```

Note: a trailing `/` is trimmed during encoding, so `foo/bar/` decodes back as
`foo/bar`.

## Chaining codecs

```c
kvKeyCodec *members[2] = { pathC, b64C }; // path first, then base64
kvKeyCodec *chainC     = NULL;

kvKeyCodec_Chain(&chainC, members, 2);
// encode runs path -> base64; decode runs base64 -> path
```

A chain borrows its members (destroy them after the chain) and is filterable
only if every member is. Unlike orbit.go — where a chain with a non-filterable
member rejects every watch/list pattern — only wildcard patterns are rejected;
wildcard-free patterns still work, encoded as plain keys.

## Custom codecs

Provide encode/decode callbacks (and optionally a filter encoder and a closure
destructor). Callbacks allocate their result with `kvCodec_AllocBuf()`; the
library takes ownership and releases it:

```c
static natsStatus
myEncodeKey(char **out, const char *in, void *closure) { /* ... */ }

static natsStatus
myDecodeKey(char **out, const char *in, void *closure) { /* ... */ }

kvKeyCodec *customC = NULL;
kvKeyCodec_New(&customC, myEncodeKey, myDecodeKey,
               NULL /* no filter support */, NULL, NULL);
```

# Error handling

Codec failures surface as the `natsStatus` of the triggering call: built-in
codecs return `NATS_ERR` on malformed input, custom codec statuses propagate
verbatim, and wildcard patterns with a non-filterable codec return
`NATS_INVALID_SUBJECT`. Underlying store statuses (such as `NATS_NOT_FOUND`)
pass through untouched.

Entries returned by get/history/watch are decoded eagerly, so a corrupt stored
key or value fails the fetching call instead of silently returning encoded
data. A failed watcher entry is dropped and the watcher stays usable.

Since the wrapper only borrows the `kvStore`, the raw handle remains the
escape hatch for undecodable data: `kvStore_Get` on the encoded key returns
the raw entry with all its metadata (revision, operation, ...) for inspection
or repair.

# Examples

See [examples/](examples/): `base64.c`, `path_keys.c`, `chain.c`,
`custom_codec.c`. Build with `-DORBIT_BUILD_EXAMPLES=ON`; each needs a
JetStream-enabled server (`nats-server -js`).

# License

Unless otherwise noted, the NATS source files are distributed under the
Apache Version 2.0 license found in the LICENSE file.
