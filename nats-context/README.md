# NATS Context

Connect to NATS using a context created by the [`nats` Command Line
Tool](https://github.com/nats-io/natscli), on top of
[cnats](https://github.com/nats-io/nats.c).

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

Pass `NULL` for `settings` if you do not need them, and a `natsOptions`
object as the last argument to add options the context does not cover
(callbacks, timeouts, connection name, ...).

# Supported settings

All context settings are applied to the connection except:

- `nkey` — cnats cannot derive the public key from a seed file;
- `socks_proxy` — cnats has no SOCKS dialer;
- `windows_cert_store` (and related) — also unsupported by orbit.go.

A context requesting any of these fails with an error rather than silently
connecting without it.

`nsc` is resolved before connecting by running `nsc generate profile <value>`,
which must be on the `PATH`; a lookup that cannot be run, fails, or returns
something other than a profile fails the connect. The credentials and server
URLs the profile reports are *not* applied to the connection, matching
orbit.go, where they are stored in unexported fields that nothing reads.

`jetstream_domain`, `jetstream_api_prefix`, `jetstream_event_prefix`,
`user_jwt` and `color_scheme` are returned in `natsContextSettings` for the
caller to consume, and are not applied to the connection.

# Examples

Buildable examples live in [examples](examples); configure the build with
`-DORBIT_BUILD_EXAMPLES=ON` to compile them.

# License

Unless otherwise noted, the NATS source files are distributed under the
Apache Version 2.0 license found in the LICENSE file.
