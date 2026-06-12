// Copyright 2015-2018 The NATS Authors
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

#ifndef N_WIN_H_
#define N_WIN_H_

#include <winsock2.h>
#include <ws2tcpip.h>

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#undef WIN32_LEAN_AND_MEAN

#define _CRT_SECURE_NO_WARNINGS

// These pragmas are MSVC-only; mingw/gcc links Ws2_32 via CMake (ORBIT_EXTRA_LIB)
// and would otherwise warn on the unknown pragmas.
#if defined(_MSC_VER)
#pragma comment(lib, "Ws2_32.lib")
#pragma warning(disable : 4996)
#endif

typedef CRITICAL_SECTION    natsMutex;
typedef CONDITION_VARIABLE  natsCondition;

// Fills *(tm) from *(secs) (UTC); evaluates to true on success. gmtime_s takes
// its arguments in the opposite order from POSIX gmtime_r and returns an
// errno_t (0 on success).
#define nats_gmtime(secs, tm)   (gmtime_s((tm), (secs)) == 0)

#endif /* N_WIN_H_ */
