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

#ifndef NATS_CONTEXT_SOCKS_H_
#define NATS_CONTEXT_SOCKS_H_

#include <nats/nats.h>

/** \brief Returns the proxy connection handler for a `socks_proxy` setting.
 *
 * The handler reaches each NATS server through the SOCKS5 proxy at 'url' and
 * is installed on a `natsOptions` with #natsOptions_SetProxyConnHandler,
 * together with the returned closure.
 *
 * 'url' is `[socks5://][user[:password]@]host[:port]`, the form the `nats` CLI
 * writes, with the port defaulting to 1080 and the credentials, when given,
 * used for username/password authentication (RFC 1929). A `socks5h` scheme is
 * accepted and behaves the same: host names are handed to the proxy to
 * resolve either way.
 *
 * The closure belongs to the library and stays valid for the life of the
 * process — see the note on interning in socks.c — so nothing has to be kept
 * alive, or freed, by the caller.
 *
 * @param url the proxy URL.
 * @param handler out-param: the connection handler.
 * @param closure out-param: the closure to pass alongside it.
 * @return #NATS_OK, #NATS_INVALID_ARG if 'url' is not a SOCKS5 proxy URL, or
 * #NATS_NO_MEMORY.
 */
natsStatus
natsSocks_ProxyHandler(const char *url, natsProxyConnHandler *handler, void **closure);

#endif // NATS_CONTEXT_SOCKS_H_
