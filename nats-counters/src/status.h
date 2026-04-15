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

#ifndef ORBIT_STATUS_H_
#define ORBIT_STATUS_H_

#ifdef __cplusplus
extern "C" {
#endif

// TODO: Consider just using natsStatus instead of defining a separate enum.
//
typedef enum
{
    ORBIT_COUNTERS_OK = 0,              ///< Success.

    ORBIT_COUNTERS_ERR,                 ///< Generic error.
    ORBIT_COUNTERS_INVALID_ARG,         ///< An invalid argument was passed.
    ORBIT_COUNTERS_NO_MEMORY,           ///< Memory allocation failed.
    ORBIT_COUNTERS_NOT_FOUND,           ///< Subject has not been written, or no increment present.
    ORBIT_COUNTERS_INVALID_CONFIG,      ///< Stream is missing AllowDirect or AllowMsgCounter.
    ORBIT_COUNTERS_OVERFLOW,            ///< Value exceeds the range of long long.
    ORBIT_COUNTERS_PARSE_ERR,           ///< Failed to parse counter message body or header.
    ORBIT_COUNTERS_NATS_ERR,            ///< An underlying NATS client error occurred.

} orbitCountersStatus;

/** \brief Returns a static string describing the status code.
 *
 * @param s the status code.
 * @return a human-readable string.
 */
const char *orbitCountersStatus_GetText(orbitCountersStatus s);

#ifdef __cplusplus
}
#endif
#endif /* ORBIT_STATUS_H_ */
