# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

Librdkafka.jl is a Julia wrapper for Apache Kafka. It uses a three-layer architecture:
- **C++ layer** (`deps/src/main.cpp`): wraps [modern-cpp-kafka](https://github.com/morganstanley/modern-cpp-kafka) headers via CxxWrap to expose producer/consumer functions
- **FFI layer** (`src/ffi/`): `native.jl` loads the compiled `libkafka` shared library via CxxWrap; `bindings.jl` provides cached Julia function wrappers around the C++ exports
- **Julia API** (`src/`): high-level `KafkaProducer` and `KafkaConsumer` types with idiomatic Julia interfaces

## Build Commands

### Build native wrapper (required before using the package)
```bash
cmake -S deps/src -B deps/src/build -DCMAKE_BUILD_TYPE=Release
cmake --build deps/src/build --config Release
```

The built `libkafka.{so,dylib,dll}` is output to `deps/src/build/lib/`. At runtime, the Julia code searches `deps/lib/` first, then `deps/src/build/lib/` (see `src/ffi/native.jl`).

For CI or deployment, copy the library to `deps/lib/`:
```bash
mkdir -p deps/lib && cp deps/src/build/lib/libkafka.$(julia -e 'using Libdl; print(Libdl.dlext)') deps/lib/
```

### Install Julia dependencies
```bash
julia --project=. -e 'using Pkg; Pkg.instantiate()'
```

### Run tests
```bash
julia --project=. -e 'using Pkg; Pkg.test()'
```

Unit tests (`test/unit.jl`) run without a Kafka broker. Integration tests (`test/integration.jl`) require `KAFKA_BOOTSTRAP_SERVERS` env var and are skipped otherwise.

### Build documentation
```bash
julia --project=docs docs/make.jl
```

## Architecture Notes

- **Data types** are defined directly in `src/Librdkafka.jl`: `Topic`, `Partition`, `TopicPartition`, `Assignment`, `ConsumerRecord`
- **Resource management**: `KafkaProducer` and `KafkaConsumer` register Julia finalizers for automatic cleanup; they also support explicit `close()`
- **Logging** (`src/logging.jl`): bridges C++ logs into Julia's Logging framework via a background timer (50ms drain interval) with mutex-protected buffers. Configurable sinks: Julia logger, stdout, or file
- **Record parsing** (`src/record_parser.jl`): binary format parser that reconstructs `ConsumerRecord` from raw Kafka data
- **Bindings module alias**: the FFI bindings are available as `_B` throughout the codebase (`const _B = Bindings`)

## CI Matrix

Tested on Julia 1.10, 1.11, 1.12 across Ubuntu 24.04 (x64, `.so`) and macOS 15 (arm64, `.dylib`). CI config is in `.github/workflow-config.yml`. The reusable build+test action is `.github/actions/build-test/action.yml`.

## Key Dependencies

- `CxxWrap.jl` — Julia/C++ interop (CMake finds it via `CxxWrap.CxxWrapCore.prefix_path()`)
- `librdkafka_jll` — prebuilt librdkafka binary artifact
- `CyrusSASL_jll` — SASL authentication support
- CMake auto-discovers these JLL artifacts if system libraries aren't found

## Version and Registry

Version is in `Project.toml`. Published to a custom Julia registry (Green). Bump version in `Project.toml` before releasing (see PR template checklist).
