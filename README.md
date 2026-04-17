<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Arrow

[![docs](https://img.shields.io/badge/docs-latest-blue&logo=julia)](https://arrow.apache.org/julia/)
[![CI](https://github.com/apache/arrow-julia/workflows/CI/badge.svg)](https://github.com/apache/arrow-julia/actions?query=workflow%3ACI)
[![codecov](https://app.codecov.io/gh/apache/arrow-julia/branch/main/graph/badge.svg)](https://app.codecov.io/gh/apache/arrow-julia)

[![deps](https://juliahub.com/docs/Arrow/deps.svg)](https://juliahub.com/ui/Packages/Arrow/QnF3w?t=2)
[![version](https://juliahub.com/docs/Arrow/version.svg)](https://juliahub.com/ui/Packages/Arrow/QnF3w)
[![pkgeval](https://juliahub.com/docs/Arrow/pkgeval.svg)](https://juliahub.com/ui/Packages/Arrow/QnF3w)

This is a pure Julia implementation of the [Apache Arrow](https://arrow.apache.org) data standard.  This package provides Julia `AbstractVector` objects for
referencing data that conforms to the Arrow standard.  This allows users to seamlessly interface Arrow formatted data with a great deal of existing Julia code.

Please see this [document](https://arrow.apache.org/docs/format/Columnar.html#physical-memory-layout) for a description of the Arrow memory layout.

## Installation

The package can be installed by typing in the following in a Julia REPL:

```julia
julia> using Pkg; Pkg.add("Arrow")
```

Arrow.jl currently requires Julia `1.12+`.

## Local Development

When developing on Arrow.jl it is recommended that you run the following to ensure that any
changes to ArrowTypes.jl are immediately available to Arrow.jl without requiring a release:

```sh
julia --project -e 'using Pkg; Pkg.develop(path="src/ArrowTypes")'
```

Current write-path notes:
  * `Arrow.tobuffer` includes a direct single-partition fast path for eligible inputs
  * `Arrow.tobuffer(Tables.partitioner(...))` also includes a targeted direct multi-record-batch path for single-column top-level strings and single-column non-missing binary/code-units columns
  * `Arrow.write(io, Tables.partitioner(...))` now reuses that same targeted direct multi-record-batch path instead of always going through the legacy `Writer` orchestration
  * multi-column partitions, dictionary-encoded top-level columns, map-heavy inputs, and missing-binary partitions retain the existing writer path

## Format Support

This implementation supports the 1.0 version of the specification, including support for:
  * All primitive data types
  * All nested data types
  * Dictionary encodings and messages
  * Dictionary-encoded `CategoricalArray` interop, including missing-value roundtrips through `Arrow.Table`, `copy`, and `DataFrame(...; copycols=true)`
  * Extension types
  * Lightweight schema/field metadata overlays via `Arrow.withmetadata(...)` for Tables.jl-compatible sources before serialization
  * Base Julia `Enum` logical types via the `JuliaLang.Enum` extension label, with native Julia roundtrips back to the original enum type while `convert=false` and non-Julia consumers still see the primitive storage type
  * View-backed Utf8/Binary columns, including recovery from under-reported variadic buffer counts by inferring the required external buffers from valid view elements
  * Streaming, file, record batch, and replacement and isdelta dictionary messages

It currently doesn't include support for:
  * Tensor or sparse tensor IPC payload semantics; Arrow.jl now recognizes those message headers explicitly and rejects them with precise errors instead of falling through to a generic unsupported-message path
  * C data interface
  * Writing Run-End Encoded arrays; Arrow.jl now reads REE arrays and exposes them as read-only vectors, but still rejects REE on write paths

Flight RPC status:
  * Experimental `Arrow.Flight` support is available in-tree
  * Requires Julia `1.12+`
  * Includes generated protocol bindings for the `FlightService` RPC surface while keeping the gRPC client constructors in the modular client boundary under `src/flight/client/` instead of in the generated protocol module
  * Keeps the top-level Flight module shell thin, with exports and generated-protocol setup split out of `src/flight/Flight.jl`
  * Includes high-level `FlightData <-> Arrow IPC` helpers for `Arrow.Table`, `Arrow.Stream`, and DoPut/DoExchange payload generation, `Arrow.Flight.pathdescriptor(...)` for PATH descriptors without manual proto assembly, opt-in `app_metadata` surfacing through `include_app_metadata=true` on `Arrow.Flight.stream(...)` / `Arrow.Flight.table(...)`, explicit batch-wise `app_metadata=...` emission on `Arrow.Flight.flightdata(...)`, `Arrow.Flight.putflightdata!(...)`, and source-based `Arrow.Flight.doexchange(...)`, and a reusable `Arrow.Flight.withappmetadata(...)` wrapper so source-level batch metadata can stay attached without manual keyword threading
  * Keeps the Flight IPC conversion layer modular under `src/flight/convert/`, with `src/flight/convert.jl` retained as a thin entrypoint
  * Owns Flight protocol, descriptor, IPC, and server/runtime surfaces only; package-owned interop and performance proofs run through external Python clients instead of a Julia Flight client runtime
  * Includes a transport-agnostic server core (`Service`, `ServerCallContext`, `ServiceDescriptor`, `MethodDescriptor`) for local Flight method dispatch, path lookup, handler testing, packaged backend capability checks through `Arrow.Flight.flight_server_backend_capabilities(...)`, transport-neutral gRPC-over-HTTP/2 framing helpers, high-level `DoExchange` assembly through `Arrow.Flight.exchangeservice(...)`, `Arrow.Flight.tableservice(...)`, and `Arrow.Flight.streamservice(...)`, and source-based local invocation through `Arrow.Flight.doexchange(service, context, source; ...)`, `Arrow.Flight.table(service, context, source; ...)`, and `Arrow.Flight.stream(service, context, source; ...)`
  * Keeps the transport-agnostic server core modular under `src/flight/server/`, with `src/flight/server.jl` retained as a thin entrypoint
  * Includes built-in `PureHTTP2.jl` transport helpers in the Flight server core for package-owned h2c listeners, unary RPCs, client-streaming, server-streaming, and live bidirectional `DoExchange` gRPC-over-HTTP/2 handling through `Arrow.Flight.purehttp2_flight_server(...)`; long-lived connection and handler workers now run on Julia's thread pool instead of sticky `@async` tasks so CPU-heavy Flight callbacks can overlap on multi-threaded runtimes, and the listener now exposes a bounded `max_active_requests` admission gate so overload returns a gRPC status instead of silently growing unbounded active compute work
  * The packaged Flight server backend contract now reports the built-in `:purehttp2` path as the only default live listener profile, retires `:grpcserver`, and exposes a weakdep-backed `:nghttp2` profile only when `Nghttp2Wrapper.jl` is loaded; that backend is currently limited to unary plus buffered server-streaming methods with trailer-borne `grpc-status`
  * Includes package-owned live Python-client coverage for authenticated `ListFlights`, `GetFlightInfo`, `GetSchema`, `DoGet`, `DoPut`, `DoExchange`, `ListActions`, and `DoAction` through `test/flight_purehttp2.jl`
  * Keeps targeted Flight verification modular under `test/flight/`, with `test/flight.jl` retained as the shared default entrypoint for generated protocol, server-core, and IPC coverage, and the PureHTTP2/nghttp2 listener proofs isolated in dedicated runner files
  * Includes `test/flight_purehttp2.jl` as the PureHTTP2-first temporary-environment runner for shared Flight interop coverage plus package-owned listener proofs
  * Includes `test/flight_purehttp2_perf.jl` as a focused large-transport runner that benchmarks large-response `DoGet` on the package-owned `PureHTTP2` listener through a reusable backend-factory seam
  * Includes `test/flight_nghttp2_probe.jl` as a substrate probe that verifies `Nghttp2Wrapper.jl` exports the low-level session / callback / submit hooks needed for the Flight adapter, proves a small `PureHTTP2` client interop smoke against `Nghttp2Wrapper.HTTP2Server`, and measures a raw 2 MiB h2c response on the C-wrapper server without widening default CI
  * Includes `test/flight_nghttp2.jl` as the focused weakdep-backed nghttp2 listener runner; it proves live Python-client unary plus server-streaming Flight calls over `Nghttp2Wrapper.jl` and prints same-harness large `DoGet` comparison numbers against the default `PureHTTP2` backend
  * The current nghttp2 backend still does not support request-streaming `Handshake`, `DoPut`, or `DoExchange`, so `PureHTTP2` remains the canonical package-owned backend and `test/flight_purehttp2_perf.jl` remains the default large-transport proof for the product lane
  * `Handshake` token propagation and `PollFlightInfo` currently remain server-core/local proofs because the current external Python client surfaces used in tests do not cover those contracts directly
  * Dedicated CI jobs now exercise the Flight interop suite on stable and nightly Linux through `test/flight_purehttp2.jl`; the built-in PureHTTP2 server substrate is the package-owned live backend direction, with Python-client smoke coverage on the same listener surface

Third-party data formats:
  * CSV, parquet and avro support via the existing [CSV.jl](https://github.com/JuliaData/CSV.jl), [Parquet.jl](https://github.com/JuliaIO/Parquet.jl) and [Avro.jl](https://github.com/JuliaData/Avro.jl) packages
  * Other Tables.jl-compatible packages automatically supported ([DataFrames.jl](https://github.com/JuliaData/DataFrames.jl), [JSONTables.jl](https://github.com/JuliaData/JSONTables.jl), [JuliaDB.jl](https://github.com/JuliaData/JuliaDB.jl), [SQLite.jl](https://github.com/JuliaDatabases/SQLite.jl), [MySQL.jl](https://github.com/JuliaDatabases/MySQL.jl), [JDBC.jl](https://github.com/JuliaDatabases/JDBC.jl), [ODBC.jl](https://github.com/JuliaDatabases/ODBC.jl), [XLSX.jl](https://github.com/felipenoris/XLSX.jl), etc.)
  * No current Julia packages support ORC

Canonical extension highlights:
  * `UUID` now writes the canonical `arrow.uuid` extension name by default while retaining reader compatibility with legacy `JuliaLang.UUID` metadata
  * `Arrow.TimestampWithOffset{U}` provides a canonical `arrow.timestamp_with_offset` logical type without conflating offset-only semantics with `ZonedDateTime`
  * `Arrow.Bool8` provides an explicit opt-in writer/reader surface for the canonical `arrow.bool8` extension without changing the default packed-bit `Bool` path
  * `Arrow.JSONText{String}` provides a text-backed logical type for the canonical `arrow.json` extension without parsing payloads during read or write
  * `arrow.opaque` now reads as the underlying storage type without warning, and explicit writer metadata can be generated with `Arrow.opaquemetadata(type_name, vendor_name)`
  * `Arrow.variantmetadata()`, `Arrow.fixedshapetensormetadata(...)`, and `Arrow.variableshapetensormetadata(...)` generate canonical metadata strings for advanced canonical extensions
  * `arrow.fixed_shape_tensor` and `arrow.variable_shape_tensor` are recognized on read as canonical passthrough extensions over their storage types, and Arrow.jl now validates their canonical metadata plus top-level storage shape before accepting them
  * `arrow.parquet.variant` is recognized on read as a canonical passthrough extension over its storage type; Arrow.jl currently validates that its canonical metadata is the required empty string, but does not yet implement deeper variant semantics or an automatic writer surface
  * Legacy `JuliaLang.ZonedDateTime-UTC` and `JuliaLang.ZonedDateTime` files remain readable for backward compatibility

See the [full documentation](https://arrow.apache.org/julia/) for details on reading and writing arrow data.
