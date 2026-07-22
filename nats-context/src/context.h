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

/** \brief Settings of a NATS Context, as stored by the `nats` CLI in
 * `<config>/nats/context/<name>.json`.
 *
 * Mirrors the Settings struct of orbit.go's natscontext package. Each field
 * is annotated with the JSON key it is read from; `nscUrl` and `nscCreds`
 * are not part of the JSON file but are resolved by invoking `nsc` when
 * #NSCLookup is set.
 */
typedef struct __natsContextSettings
{
    char *Name;                ///< JSON `name`.
    char *Description;         ///< JSON `description`.
    char *URL;                 ///< JSON `url`. Server URL(s), comma separated.
    char *nscUrl;              ///< Operator service URL(s) resolved via `nsc`.
    char *SocksProxy;          ///< JSON `socks_proxy`.
    char *Token;               ///< JSON `token`.
    char *User;                ///< JSON `user`.
    char *Password;            ///< JSON `password`.
    char *Creds;               ///< JSON `creds`. Path to a credentials file.
    char *nscCreds;            ///< Credentials path resolved via `nsc`.
    char *NKey;                ///< JSON `nkey`. Path to an nkey seed file.
    char *Cert;                ///< JSON `cert`. Path to a client certificate.
    char *Key;                 ///< JSON `key`. Path to a client key.
    char *CA;                  ///< JSON `ca`. Path to a CA bundle.
    char *NSCLookup;           ///< JSON `nsc`. `nsc` lookup, e.g. `nsc://operator/account/user`.
    char *JSDomain;            ///< JSON `jetstream_domain`.
    char *JSAPIPrefix;         ///< JSON `jetstream_api_prefix`.
    char *JSEventPrefix;       ///< JSON `jetstream_event_prefix`.
    char *InboxPrefix;         ///< JSON `inbox_prefix`.
    char *UserJwt;             ///< JSON `user_jwt`.
    char *ColorScheme;         ///< JSON `color_scheme`. Used by the CLI only.
    bool  TLSFirst;            ///< JSON `tls_first`.
    char *WinCertStoreType;    ///< JSON `windows_cert_store`.
    char *WinCertStoreMatchBy; ///< JSON `windows_cert_match_by`.
    char *WinCertStoreMatch;   ///< JSON `windows_cert_match`.
    char **WinCertStoreCaMatch;  ///< JSON `windows_ca_certs_match`.
    int   WinCertStoreCaMatchLen; ///< Number of entries in #WinCertStoreCaMatch.

} natsContextSettings;

void
natsContextSettings_Destroy(natsContextSettings *settings);

natsStatus
natsContext_Connect(natsConnection **nc, natsContextSettings **settings,
                    char *name, natsOptions *opts);

#if defined(__cplusplus)
}
#endif
#endif // NATS_CONTEXT_H_
