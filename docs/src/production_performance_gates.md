```@raw html
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
```

# Production Performance Gates

Arrow.jl keeps shared CI performance checks conservative because GitHub-hosted
wall-clock timing is noisy. Production readiness should instead be proven on
the deployment class of hardware with explicit environment floors and retained
receipts.

The current production gate profile has three layers:

1. C Data and C Stream same-process FFI receipts prove allocation bounds,
   release behavior, and pointer identity for CPU buffers.
2. IPC receipts prove metadata reads, direct `Arrow.tobuffer` fast-path
   behavior, physical buffer scans, and materialized consumer scans.
3. Flight receipts prove end-to-end large and concurrent DoGet, DoPut, and
   DoExchange transport on the package-owned listener path.

Run the whole profile with:

```sh
julia --project=test test/production_performance_gates.jl
```

Set `ARROW_PRODUCTION_PERFORMANCE_REPORTS=ipc,cdata` or another comma-separated
subset of `ipc`, `cdata`, and `flight` when a production environment wants to
run only part of the profile.

## IPC Gate

Run:

```sh
julia --project=test test/ipc_performance_report.jl
```

The IPC report accepts allocation and timing limits through `ARROW_IPC_MAX_*`
environment variables. Shared CI currently gates read allocation and physical
scan allocation. Deployment profiles may additionally gate write timing,
direct `Arrow.tobuffer` timing, and materialized scan timing after collecting a
stable baseline on the target host class.

## C Data and C Stream Gate

Run:

```sh
julia --project=test test/cdata_validation_report.jl
```

The C Data report warms the runtime path before printing base export/import,
C Stream setup/import, scan allocation, timing, checksum, and pointer identity
receipts. CI gates steady-state allocation regressions. Production profiles
should retain the full report output because pointer identity is the evidence
for same-process CPU zero-copy claims.

## Flight Gate

Run:

```sh
julia --project=test test/flight_purehttp2_perf.jl
```

Flight wall-clock floors are deployment-local. Set positive values for the
operations that matter to the service:

```sh
ARROW_FLIGHT_PYARROW_DOGET_MIN_THROUGHPUT_MIB_PER_SEC=...
ARROW_FLIGHT_PYARROW_DOPUT_MIN_THROUGHPUT_MIB_PER_SEC=...
ARROW_FLIGHT_PYARROW_DOEXCHANGE_MIN_THROUGHPUT_MIB_PER_SEC=...
ARROW_FLIGHT_PYARROW_DOGET_CONCURRENT_MIN_THROUGHPUT_MIB_PER_SEC=...
ARROW_FLIGHT_PYARROW_DOPUT_CONCURRENT_MIN_THROUGHPUT_MIB_PER_SEC=...
ARROW_FLIGHT_PYARROW_DOEXCHANGE_CONCURRENT_MIN_THROUGHPUT_MIB_PER_SEC=...
```

Use `ARROW_FLIGHT_PYARROW_CONCURRENT_CLIENTS`,
`ARROW_FLIGHT_PYARROW_REQUESTS_PER_CLIENT`,
`ARROW_FLIGHT_PYARROW_SOAK_ROUNDS`,
`ARROW_FLIGHT_PYARROW_REUSED_DOPUT_REQUESTS`, and
`ARROW_FLIGHT_PYARROW_REUSED_DOPUT_SOAK_ROUNDS` to shape the local soak. The
default values are intentionally small enough for package validation; a
production profile should increase them to match expected concurrency and data
volume.

Network Flight receipts are transport receipts, not end-to-end zero-copy
claims. Flight serializes Arrow IPC payloads over the wire. Same-process
zero-copy claims belong to C Data and C Stream receipts.

## Flight SQL and ADBC

Flight SQL and ADBC need separate performance gates after their APIs exist.
Flight SQL should measure metadata commands, query execution, prepared
statement binding, ingestion, and session actions against an external Flight
SQL endpoint. ADBC should measure query streams, bulk ingestion, cancellation,
partitioned result sets, and driver-manager overhead across accepted drivers.

Until those protocol surfaces are implemented, the production performance gate
for this PR is limited to C Data, C Stream, IPC, and Flight transport.
