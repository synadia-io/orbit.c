<p align="center">
  <img src="orbit.png">
</p>

Orbit is a set of independent utilities around NATS ecosystem that aims to boost productivity and provide higher abstraction layer for NATS clients.

Note that these libraries will evolve rapidly and API guarantees are not made until the specific project has a v1.0.0 version.

# Building

## Prerequisites

- CMake 3.13 or newer
- A C99 compiler (gcc, clang, or MSVC)
- [cnats](https://github.com/nats-io/nats.c)
- Threads (POSIX threads on Linux/macOS)
- OpenSSL (optional, picked up automatically if present)

## Build

```sh
cmake -S . -B build
cmake --build build
```

The default build type is `Release`. Pass `-DCMAKE_BUILD_TYPE=Debug` (or `RelWithDebInfo`, `MinSizeRel`) to change it.

## Build options

| Option | Default | Description |
| ------ | ------- | ----------- |
| `ORBIT_BUILD_LIB_STATIC` | `ON` | Build the static library |
| `ORBIT_BUILD_LIB_SHARED` | `ON` | Build the shared library |
| `ORBIT_BUILD_EXAMPLES` | `OFF` | Build example programs |
| `ORBIT_BUILD_STATIC_EXAMPLES` | `OFF` | Statically link examples |
| `ORBIT_BUILD_DEV_MODE` | `OFF` | Enable extra type-safety checks (`-DDEV_MODE`) |
| `ORBIT_COVERAGE` | `OFF` | Enable code coverage flags |
| `ORBIT_COMPILER_HARDENING` | `OFF` | Enable compiler hardening flags |
| `BUILD_TESTING` | `ON` (CTest default) | Build the test suite |

Example, building the static library with examples enabled:

```sh
cmake -S . -B build -DORBIT_BUILD_LIB_SHARED=OFF -DORBIT_BUILD_EXAMPLES=ON
cmake --build build
```

## Sanitizers

Pass `-DORBIT_SANITIZE=<sanitizer>` when configuring to enable sanitizer-friendly compile flags:

```sh
cmake -S . -B build -DORBIT_SANITIZE=address
cmake --build build
```

## Tests

The test suite spawns its own `nats-server` for each test, so no server needs to be running — the `nats-server` binary just needs to be on `PATH`. Set `NATS_TEST_SERVER_EXE` to point at a different binary if needed, or `NATS_TEST_KEEP_SERVER_OUTPUT=1` to keep the per-test server log.

```sh
cd build
make test
```

To run under valgrind:

```sh
cd build
make test ARGS="-T memcheck"
```

# Utilities

This is a list of the current utilities.

| Module | Description                                   | Docs                        | Version |
| ------ | --------------------------------------------- | --------------------------- | ------- |
| nats counters | Distributed counters using JetStream | [README.md](nats-counters/README.md) |
