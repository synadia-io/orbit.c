# NATS Context

Connect to NATS using a context created by the [`nats` Command Line
Tool](https://github.com/nats-io/natscli), on top of
[nats.c](https://github.com/nats-io/nats.c).

A context bundles everything needed to reach a NATS deployment — server
URLs, credentials, TLS files, JetStream domain, inbox prefix — in a JSON
file under `<config>/nats/context/<name>.json`, where `<config>` is
`$XDG_CONFIG_HOME` or `<home>/.config`.

# Quick Start

Create a context with the `nats` CLI:

```bash
nats context add staging --creds /home/user/staging.creds --js-domain STAGING
```

Then connect to it from C:

```c
#include <context.h>

natsConnection      *nc       = NULL;
natsContextSettings *settings = NULL;
jsCtx               *js       = NULL;
jsOptions            jsOpts;

natsContext_Connect(&nc, &settings, "staging", NULL);

// Use the returned settings for anything not applied to the connection
// itself, like the JetStream domain:
jsOptions_Init(&jsOpts);
jsOpts.Domain = settings->JSDomain;
natsConnection_JetStream(&js, nc, &jsOpts);

// ...

jsCtx_Destroy(js);
natsContextSettings_Destroy(settings);
natsConnection_Destroy(nc);
```

The `name` argument may be:

- a context name (`"staging"`) — loads `<config>/nats/context/staging.json`;
- an absolute path to a context `.json` file — loads that file directly;
- `NULL` or `""` — loads the context selected with `nats context select`
  (from `<config>/nats/context.txt`); if none is selected either, connects
  to the default server with no context applied.

Pass `NULL` for `settings` if you do not need them.

## Adding your own options

The last argument is a `natsOptions` for anything the context does not cover
(callbacks, timeouts, connection name, ...):

```c
natsOptions *opts = NULL;

natsOptions_Create(&opts);
natsOptions_SetName(opts, "my-app");

natsContext_Connect(&nc, &settings, "staging", opts);

// The options are borrowed, not adopted: destroying them does not affect the
// connection, which took its own copy.
natsOptions_Destroy(opts);
```

The object is configured **in place** and stays modified after the call —
possibly partially, if the call failed. Context values are layered on top, so
they overwrite anything the two both set. Note this is the opposite of
orbit.go, where caller options are appended last and therefore win; nats.c
exposes no accessors for reading a `natsOptions` back, so they cannot be
merged the other way round.

# Supported settings

All context settings are applied to the connection except `nkey`, which is
ignored: nats.c needs the public key to put in the `CONNECT`, a context file
stores only the path to the seed, and nats.c exposes no way to derive one from
the other. A context carrying one connects without it, and there is no `NKey`
in `natsContextSettings`.

`windows_cert_store` (with `windows_cert_match_by`, `windows_cert_match` and
`windows_ca_certs_match`) has no nats.c equivalent either, and a context asking
for one fails with `NATS_ILLEGAL_STATE` rather than silently connecting without
it.

A context carrying `ca`, or `cert` and `key`, requires TLS: the connection
fails rather than falling back to plain text against a server that does not
insist on TLS.

`socks_proxy` is written as `[socks5://][user[:password]@]host[:port]`, with
the port defaulting to 1080 and the credentials, when given, used for
username/password authentication ([RFC
1929](https://datatracker.ietf.org/doc/html/rfc1929)). Every server in the
context is then reached through that proxy, on the first connect and on every
reconnect, with host names left for the proxy to resolve. A proxy that cannot
be reached, refuses the credentials, or refuses the target fails the connect —
it is never bypassed for a direct connection. Reaching the proxy and getting
through the handshake has 5 seconds to happen, per server tried; the connect
timeout in `natsOptions` does not cover it, as nats.c applies that only to the
connections it makes itself.

`nsc` is resolved before connecting by running `nsc generate profile <value>`,
which must be on the `PATH`; a lookup that cannot be run, fails, or returns
something other than a profile fails the connect. The credentials and server
URLs the profile reports are *not* applied to the connection — the lookup only
has to succeed.

`jetstream_domain`, `jetstream_api_prefix`, `jetstream_event_prefix`,
`user_jwt` and `color_scheme` are returned in `natsContextSettings` for the
caller to consume, and are not applied to the connection.

# Examples

Buildable examples live in [examples](examples); configure the build with
`-DORBIT_BUILD_EXAMPLES=ON` to compile them.

# License

Unless otherwise noted, the NATS source files are distributed under the
Apache Version 2.0 license found in the LICENSE file.
