<p align="center">
  <img src="orbit.png">
</p>

Orbit is a set of independent utilities around the NATS ecosystem that aims to boost productivity and provide a higher abstraction layer for [nats.c](https://github.com/nats-io/nats.c) clients.

Note that these libraries will evolve rapidly and API guarantees are not made until the specific project has a v1.0.0 version.

# Utilities

| Module | Library | Description | Docs | Version |
| ------ | ------- | ----------- | ---- | ------- |
| nats extra | `nats_extra` | Core NATS request-many (scatter/gather with timeout, stall, count or sentinel stop) | [README.md](nats-extra/README.md) | 0.1.0 |
| jetstream extra | `jetstream_extra` | Batched DIRECT.GET (sync + async) and flow-controlled fast batch publish | [README.md](jetstream-extra/README.md) | 0.1.0 |
| nats counters | `nats_counters` | Distributed counters using JetStream | [README.md](nats-counters/README.md) | 0.1.0 |
| kv codec | `kv_codec` | Transparent key/value encoding for KeyValue buckets (Base64, path, custom, chained codecs) | [README.md](kv-codec/README.md) | 0.1.0 |
| nats context | `nats_context` | Connect using a `nats` CLI context, including SOCKS5 proxy support | [README.md](nats-context/README.md) | 0.1.0 |
| nats sysclient | `nats_sysclient` | Typed client for the `$SYS` monitoring endpoints (VARZ, STATSZ, CONNZ, SUBSZ, HEALTHZ, JSZ) | [README.md](nats-sysclient/README.md) | 0.1.0 |

Some libraries build on others; link the dependencies as well when using them:

| Library | Depends on |
| ------- | ---------- |
| `jetstream_extra` | `nats_extra` |
| `nats_counters` | `jetstream_extra` |
| `nats_sysclient` | `nats_extra` |

Every library also links the internal `orbit_utils` helper library (not a public API; its headers are not installed).

# Building

## Prerequisites

- CMake 3.13 or newer
- A C99 compiler (gcc, clang, or MSVC)
- [nats.c](https://github.com/nats-io/nats.c)
- Threads (POSIX threads on Linux/macOS)
- OpenSSL (optional, picked up automatically if present)
- `nats-server` on `PATH` to run the tests

## Build

```sh
cmake -S . -B build
cmake --build build
```

The default build type is `Release`. Pass `-DCMAKE_BUILD_TYPE=Debug` (or `RelWithDebInfo`, `MinSizeRel`) to change it. Libraries are written to `build/lib`, executables (examples and test suites) to `build/bin`.

## Build options

| Option | Default | Description |
| ------ | ------- | ----------- |
| `ORBIT_BUILD_LIB_STATIC` | `ON` | Build the static libraries (`<lib>_static`) |
| `ORBIT_BUILD_LIB_SHARED` | `ON` | Build the shared libraries |
| `ORBIT_BUILD_EXAMPLES` | `OFF` | Build example programs |
| `ORBIT_BUILD_STATIC_EXAMPLES` | `OFF` | Statically link examples (required to build examples without the shared libraries) |
| `ORBIT_BUILD_DEV_MODE` | `OFF` | Enable extra type-safety checks (`-DDEV_MODE`) |
| `ORBIT_COVERAGE` | `OFF` | Enable code coverage flags |
| `ORBIT_COMPILER_HARDENING` | `OFF` | Enable compiler hardening flags |
| `ORBIT_BUILD_ARCH` | `64` | Set to `32` for a 32-bit build (Unix only) |
| `BUILD_TESTING` | `ON` (CTest default) | Build the test suites |

Example, building the static libraries with examples enabled:

```sh
cmake -S . -B build -DORBIT_BUILD_LIB_SHARED=OFF -DORBIT_BUILD_STATIC_EXAMPLES=ON -DORBIT_BUILD_EXAMPLES=ON
cmake --build build
```

Examples are named `<module>-<example>`, e.g. `build/bin/nats-counters-basic_counter`.

## Install

```sh
cmake --install build --prefix /usr/local
```

Headers are installed flat under `include/`, except `nats_sysclient`'s, which go under `include/nats-sysclient/` because of their generic names (`varz.h`, `connz.h`, ...).

## Sanitizers

`ORBIT_SANITIZE` (as a CMake variable or environment variable) only adds `-fno-omit-frame-pointer -fno-optimize-sibling-calls`; it does not enable a sanitizer by itself. Pass the `-fsanitize` flags through `CMAKE_C_FLAGS` as well:

```sh
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DORBIT_SANITIZE=address \
  -DCMAKE_C_FLAGS="-fsanitize=address" -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address" \
  -DCMAKE_SHARED_LINKER_FLAGS="-fsanitize=address"
cmake --build build-asan
```

## Tests

The test suites spawn their own `nats-server` for each test, so no server needs to be running — the `nats-server` binary just needs to be on `PATH`. Set `NATS_TEST_SERVER_EXE` to point at a different binary if needed, or `NATS_TEST_KEEP_SERVER_OUTPUT=1` to keep the per-test server log.

```sh
ctest --test-dir build --output-on-failure
```

Each module's tests carry a prefix, so a single module can be run with `-R`:

| Module | Prefix |
| ------ | ------ |
| nats extra | `nex_` |
| jetstream extra | `jsx_` |
| kv codec | `kvc_` |
| nats sysclient | `sys_` |

```sh
ctest --test-dir build -R '^kvc_'
```

To run under valgrind:

```sh
cd build
ctest -T memcheck
```
