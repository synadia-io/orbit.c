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

#ifndef NATS_CONTEXT_H_
#define NATS_CONTEXT_H_

#include <nats/nats.h>
#include <stdbool.h>

#if defined(__cplusplus)
extern "C" {
#endif

/** \defgroup natsContextGroup NATS Context
 *
 * Connect to NATS using a context created by the `nats` Command Line Tool
 * @{
 */

/** \brief Settings of a NATS Context, as stored by the `nats` CLI in
 * `<config>/nats/context/<name>.json`.
 *
 *
 * A string field is `NULL` when its key is absent from (or `null` in) the
 * JSON file, and an empty string when present but empty — the `nats` CLI
 * writes every key, so expect the latter. Treat `NULL` and `""` the same.
 *
 * The JetStream fields, #UserJwt and #ColorScheme are informational only:
 * they are returned for the caller to consume (e.g. set `jsOptions.Domain`
 * from #JSDomain) and are not applied to the connection.
 *
 * A context's `nkey` has no field here and is not applied: nats.c needs the
 * public key to put in the CONNECT, the context file stores only a path to
 * the seed, and nats.c offers no way to derive one from the other.
 */
typedef struct __natsContextSettings
{
    char *Name;
    char *Description;
    char *URL;
    char *SocksProxy;
    char *Token;
    char *User;
    char *Password;
    char *Creds;
    char *Cert;
    char *Key;
    char *CA;
    char *NSCLookup;
    char *JSDomain;
    char *JSAPIPrefix;
    char *JSEventPrefix;
    char *InboxPrefix;
    char *UserJwt;
    char *ColorScheme;
    bool  TLSFirst;
    char *WinCertStoreType;
    char *WinCertStoreMatchBy;
    char *WinCertStoreMatch;
    char **WinCertStoreCaMatch;
    int   WinCertStoreCaMatchLen;

} natsContextSettings;

/** \brief Destroys settings returned by #natsContext_Connect.
 *
 * @param settings the settings to destroy; `NULL` is a no-op.
 */
NATS_EXTERN void
natsContextSettings_Destroy(natsContextSettings *settings);

/** \brief Connects to the NATS server configured by the named context.
 *
 * The context with:
 * - an absolute path to a `.json` file loads that file directly;
 * - any other non-empty `name` loads
 *   `<config>/nats/context/<name>.json`;
 * - `NULL` or `""` loads the context selected in
 *   `<config>/nats/context.txt`; with no selection either, it connects to
 *   the default server with no context applied.
 *
 * `<config>` is `$XDG_CONFIG_HOME`, or `<home>/.config` when unset — on
 * every platform, matching where the `nats` CLI stores its contexts.
 *
 * \warning A caller-supplied `opts` is configured **in place** and remains
 * the caller's to destroy. Context values are layered on top and overwrite
 * overlapping caller settings, and the object stays modified after the call,
 * possibly partially on error. Note this is the opposite of orbit.go, where
 * caller options are appended last and therefore win; nats.c exposes no
 * accessors for reading back a `natsOptions`, so the two cannot be merged
 * the other way round. Pass options that the context does not set
 * (callbacks, timeouts, connection name, ...), or pass `NULL` to use a fresh
 * internal object.
 *
 * @param nc out-param: the connection; destroy with
 * #natsConnection_Destroy.
 * @param settings optional out-param: the resolved context settings, e.g.
 * to read #natsContextSettings.JSDomain; destroy with
 * #natsContextSettings_Destroy. Set to `NULL` on error. May be `NULL` when
 * the settings are not needed.
 * @param name context name, absolute path to a context file, or
 * `NULL`/`""` for the selected context.
 * @param opts options applied to the connection, or `NULL` (see warning).
 * @return #NATS_OK, or #NATS_NOT_YET_CONNECTED when `opts` enabled
 * #natsOptions_SetRetryOnFailedConnect() and the connection is still being
 * established — in both cases `nc` and `settings` are set and owned by the
 * caller. Otherwise: #NATS_INVALID_ARG if `nc` is `NULL` or the name is not
 * a valid context name, #NATS_NOT_FOUND if no context exists under the name
 * or `nsc` is not on the `PATH`, #NATS_ILLEGAL_STATE for a setting this
 * library cannot apply (`windows_cert_store`), #NATS_ERR for an unreadable or
 * malformed context file (including a `socks_proxy` that is not a SOCKS5 URL)
 * or a failed `nsc` lookup, #NATS_NO_MEMORY, or any status from the connection
 * attempt itself.
 */
NATS_EXTERN natsStatus
natsContext_Connect(natsConnection **nc, natsContextSettings **settings,
                    const char *name, natsOptions *opts);

/** @} */ // end of natsContextGroup

#if defined(__cplusplus)
}
#endif
#endif // NATS_CONTEXT_H_
