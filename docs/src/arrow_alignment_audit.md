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

# Full Arrow Alignment Audit

This page is the package-level synchronization table for Arrow.jl against the
official Apache Arrow specification surface. It is intentionally wider than the
C Data workstream: the table covers columnar layout, IPC, canonical extension
types, integration tests, C interfaces, Flight, Flight SQL, ADBC, and related
experimental specifications.

Official baseline reviewed for this audit:

- [Specifications](https://arrow.apache.org/docs/format/index.html)
- [Arrow Columnar Format](https://arrow.apache.org/docs/format/Columnar.html)
- [Canonical Extension Types](https://arrow.apache.org/docs/format/CanonicalExtensions.html)
- [Other Data Structures](https://arrow.apache.org/docs/format/Other.html)
- [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
- [Arrow C Stream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html)
- [Arrow C Device Data Interface](https://arrow.apache.org/docs/format/CDeviceDataInterface.html)
- [Arrow PyCapsule Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html)
- [Statistics Schema](https://arrow.apache.org/docs/format/StatisticsSchema.html)
- [Dissociated IPC Protocol](https://arrow.apache.org/docs/format/DissociatedIPC.html)
- [Arrow Flight RPC](https://arrow.apache.org/docs/format/Flight.html)
- [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
- [ADBC](https://arrow.apache.org/docs/format/ADBC.html)
- [Security Considerations](https://arrow.apache.org/docs/format/Security.html)
- [Integration Testing](https://arrow.apache.org/docs/format/Integration.html)

Status labels:

- `Covered`: implemented and guarded by focused tests or integration receipts.
- `Partial`: supported for a documented subset, with a remaining audit item.
- `Explicitly rejected`: unsupported data is recognized and rejected with a
  precise error rather than falling through accidentally.
- `Not implemented`: no package-owned runtime or helper surface exists yet.
- `Deferred`: adjacent or experimental Arrow surface that requires a separate
  design before implementation.

## Alignment Matrix

| ID | Official Arrow area | Alignment target | Current Arrow.jl status | Drift or risk | Evidence | Priority | Next action |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ARROW-AUDIT-001 | Specification version and support claims | Public docs should describe support layout-by-layout against the current Arrow format, not only as a broad version claim. | Covered by this audit slice. README now names core columnar and IPC support instead of pinning support claims to the old 1.0 wording. | Future docs can drift if new official layouts land without updating this matrix. | `README.md`, `src/metadata/Schema.jl`, `src/arraytypes/views.jl`, `src/arraytypes/runendencoded.jl`. | P0 | Update this row whenever official Arrow format version coverage changes. |
| ARROW-AUDIT-002 | Null, primitive, boolean, fixed-width physical layouts | Read and write standard fixed-size layouts with validity, values, and packed boolean buffers. | Covered. | No known drift in the audited surface. | `src/arraytypes/primitive.jl`, `src/arraytypes/bool.jl`, `test/runtests.jl`, `test/cdata/base_contract_tests.jl`. | P0 | Keep existing roundtrip and C Data format mapping tests. |
| ARROW-AUDIT-003 | Decimal, date/time, timestamp, duration, and interval types | Preserve Arrow logical scalar storage, including decimal widths and interval units. | Covered for package runtime, C Data, and integration JSON coverage, including month-day-nano interval storage. | No known drift in the audited interval surface; broader integration parity remains tracked by `ARROW-AUDIT-021`. | `src/eltypes.jl`, `src/cdata/formats.jl`, `test/runtests.jl`, `test/arrowjson/modern-layouts.json`, `test/cdata/import_nested_tests.jl`. | P1 | Keep the month-day-nano fixture active in the integration closure. |
| ARROW-AUDIT-004 | Binary, Utf8, LargeBinary, and LargeUtf8 | Read and write variable-width values with offset and data buffers. | Covered. | No known drift in the audited surface. | `src/arraytypes/list.jl`, `src/table.jl`, `src/write.jl`, `test/runtests.jl`, `test/cdata/import_basic_tests.jl`. | P0 | Keep offset-bound validation and cross-language fixtures active. |
| ARROW-AUDIT-005 | BinaryView and Utf8View plus variadic buffers | Support view records and external variadic data buffers, including C Data's extra buffer-lengths buffer. | Covered for Arrow.jl read paths, writer support, RecordBatch variadic buffer counts, C Data export/import, integration JSON parsing, and physical JSON-to-IPC preservation of inline plus external view data. | Full upstream Archery parity remains a separate integration decision. | `src/arraytypes/views.jl`, `src/metadata/Message.jl`, `src/write.jl`, `src/table.jl`, `src/cdata/export.jl`, `src/cdata/import.jl`, `test/runtests.jl`, `test/arrowjson/modern-layouts.json`, `test/cdata/export_tests.jl`. | P1 | Keep view-layout fixtures active and decide the upstream Archery parity boundary. |
| ARROW-AUDIT-006 | List, LargeList, FixedSizeList | Support nested child arrays, offset widths, and fixed-size list child spans. | Covered. | No known drift in the audited surface. | `src/arraytypes/list.jl`, `src/arraytypes/fixedsizelist.jl`, `src/table.jl`, `src/write.jl`, `test/runtests.jl`, `test/cdata/import_nested_tests.jl`. | P0 | Keep nested roundtrip and C Data import/export tests. |
| ARROW-AUDIT-007 | ListView and LargeListView | Support offsets, sizes, and child arrays for out-of-order or repeated spans. | Covered for native `Arrow.ListView` IPC read/write, metadata mapping, C Data export/import, and integration JSON-to-IPC physical preservation. | Ordinary nested Julia vectors intentionally continue to write as `List` / `LargeList`; callers use native `Arrow.ListView` to request list-view physical layout. | `src/arraytypes/list.jl`, `src/table.jl`, `src/cdata/export.jl`, `src/cdata/import.jl`, `test/runtests.jl`, `test/arrowjson/modern-layouts.json`, `test/cdata/export_tests.jl`. | P1 | Keep native ListView IPC, C Data roundtrip, and integration JSON physical-layout coverage active. |
| ARROW-AUDIT-008 | Struct, Map, DenseUnion, SparseUnion | Preserve nested fields, map entries, union type ids, and dense offsets. | Covered for normal package use and C Data export/import. | Large-offset maps remain rejected in C Data because the C Data map format has no offset-width variant. | `src/arraytypes/struct.jl`, `src/arraytypes/map.jl`, `src/arraytypes/union.jl`, `src/cdata/export.jl`, `docs/src/cdata_alignment.md`. | P0 | Keep explicit rejection for C Data large-offset maps and document it only inside the C Data boundary. |
| ARROW-AUDIT-009 | Dictionary encoding and dictionary messages | Support dictionary arrays, replacement dictionaries, and `isDelta` dictionary messages. | Covered. | No known drift in the audited surface. | `src/arraytypes/dictencoded.jl`, `src/write.jl`, `test/runtests.jl`, `test/arrowjson/dictionary.json`. | P0 | Keep replacement and delta dictionary fixtures. |
| ARROW-AUDIT-010 | Run-End Encoded layout | Read and write REE arrays while preserving the bufferless parent and `run_ends` / `values` child contract. | Covered for native `Arrow.RunEndEncoded` IPC read/write, C Data export/import, and integration JSON-to-IPC physical preservation. | Ordinary Julia vectors intentionally do not auto-encode to REE; callers use native `Arrow.RunEndEncoded` to request this physical layout. | `src/arraytypes/runendencoded.jl`, `src/eltypes.jl`, `src/table.jl`, `test/run_end_encoded_small.arrow`, `test/runtests.jl`, `test/arrowjson/run-end-encoded.json`, `test/cdata/import_nested_tests.jl`. | P1 | Keep native REE IPC, C Data, and integration JSON coverage active. |
| ARROW-AUDIT-011 | IPC stream, file, record batch, and dictionary messages | Read and write Arrow IPC stream/file payloads, schema messages, record batches, dictionary batches, and end markers. | Covered for table-oriented IPC. | Tensor/SparseTensor messages are explicitly outside this row and tracked separately. | `src/table.jl`, `src/write.jl`, `src/metadata/Message.jl`, `test/runtests.jl`, `test/integrationtest.jl`. | P0 | Keep IPC tests and avoid widening this row to tensor payloads. |
| ARROW-AUDIT-012 | IPC compression and buffer alignment | Support compressed IPC buffers and alignment rules for serialized messages. | Covered for LZ4 and ZSTD. | No known drift in the audited surface. | `src/table.jl`, `src/write.jl`, `test/java_compress_len_neg_one.arrow`, `test/java_compressed_zero_length.arrow`, `test/runtests.jl`. | P1 | Keep edge-case fixtures for zero-length and negative compressed lengths. |
| ARROW-AUDIT-013 | Tensor and SparseTensor IPC messages | Optional non-columnar IPC messages should be recognized if present. | Explicitly rejected. Arrow.jl recognizes these message headers and reports precise unsupported-message errors. | These structures are not required for a columnar implementation, but they are part of the broader Arrow spec surface. | `src/metadata/Message.jl`, `src/table.jl`, `docs/src/manual.md`, `README.md`. | P2 | Decide whether Arrow.jl should implement standalone tensor readers/writers or keep explicit rejection. |
| ARROW-AUDIT-014 | Statistics schema | Represent standardized dataset statistics as Arrow data. | Not implemented as a package-owned helper. The schema can be manually represented with existing struct/map/dense-union support. | No ergonomic producer/consumer, validator, or docs surface for the experimental statistics schema. | Existing nested layout support in `src/arraytypes/`; no dedicated statistics module. | P3 | Defer until a consumer asks for statistics exchange; then add helpers without changing core IPC semantics. |
| ARROW-AUDIT-015 | Dissociated IPC Protocol | Separate IPC metadata from body buffers for high-performance or device/shared-memory transports. | Not implemented. | This is experimental and transport-heavy; implementing it would affect Flight/device/runtime boundaries. | No dedicated transport or metadata/body split module. | P3 | Keep deferred; revisit only with a transport design tied to Flight or device memory. |
| ARROW-AUDIT-016 | Canonical extensions: `arrow.uuid`, `arrow.json`, `arrow.bool8`, `arrow.timestamp_with_offset` | Use official extension names and metadata contracts for supported semantic wrappers. | Covered. | No known drift in the audited surface. | `src/eltypes.jl`, `src/ArrowTypes/src/ArrowTypes.jl`, `test/runtests.jl`, `test/flight/ipc_conversion.jl`. | P0 | Keep writer metadata and read compatibility tests. |
| ARROW-AUDIT-017 | Canonical tensor extensions: `arrow.fixed_shape_tensor`, `arrow.variable_shape_tensor` | Validate canonical metadata and storage shape while preserving storage values. | Partial. Arrow.jl recognizes and validates metadata/storage, then returns storage-backed values. | No high-level semantic tensor wrapper or automatic writer surface exists. | `src/eltypes.jl`, `test/runtests.jl`, `README.md`, `docs/src/manual.md`. | P2 | Decide whether to add semantic wrapper types or keep storage-preserving passthrough as the package policy. |
| ARROW-AUDIT-018 | Canonical extensions: `arrow.opaque`, `arrow.parquet.variant` | Preserve extension metadata and validate required metadata constraints. | Partial. `arrow.opaque` has metadata writer helpers; `arrow.parquet.variant` validates empty metadata and remains storage passthrough. | Deeper Parquet Variant semantics and automatic writer support are not implemented. | `src/eltypes.jl`, `test/runtests.jl`, `README.md`, `docs/src/manual.md`. | P2 | Add a variant semantics design only if Arrow.jl chooses to own Parquet Variant interpretation. |
| ARROW-AUDIT-019 | Custom extension metadata | Preserve application extension names and metadata without corrupting storage fallback. | Covered for Julia extension types and raw metadata preservation. | Extension deserialization must remain robust against malformed metadata. | `src/logicaltypes.jl`, `src/eltypes.jl`, `test/runtests.jl`, `test/cdata/import_basic_tests.jl`. | P1 | Pair future extension additions with negative metadata tests. |
| ARROW-AUDIT-020 | Security and robustness guidance | Reject malformed layouts safely, validate dangerous metadata, and keep untrusted IPC/C Data risks documented. | Partial. IPC List/Binary/Utf8/Map/ListView offset validation, IPC BinaryView/Utf8View span validation, IPC Utf8/LargeUtf8/Utf8View byte validation, canonical extension metadata/storage negative tests, C Data Binary/UTF-8 offset validation, C Data UTF-8/UTF-8 view byte validation, C Data ListView negative offset/size/span validation, and public security guidance are covered; no broad fuzz/regression corpus or public validator API exists yet. | Untrusted IPC and C Data surfaces still need larger malformed-file coverage before broader security claims. | `src/table.jl`, `src/arraytypes/list.jl`, `src/arraytypes/views.jl`, `src/eltypes.jl`, `src/logicaltypes_builtin.jl`, `src/cdata/import.jl`, `test/runtests.jl`, `test/cdata/base_contract_tests.jl`, `test/cdata/import_*_tests.jl`, `docs/src/security.md`. | P1 | Extend malformed C Data coverage to any remaining nested edge cases and future official extension combinations; then decide whether to expose a public validator API. |
| ARROW-AUDIT-021 | Official integration testing | Provide JSON-to-Arrow, Arrow-to-JSON, and validation entry points that can be composed with cross-language integration tests. | Partial. The harness exists and now recognizes `utf8view`, `binaryview`, `listview`, `largelistview`, `runendencoded`, month-day-nano intervals, binary/fixed-size-binary hex JSON values, schema/field metadata, canonical extension storage+metadata fixtures, physical JSON-to-IPC preservation for view/list-view/REE layouts, an Archery-style IPC JSON CLI entry point with `--integration`, `--arrow`, `--json`, and `--mode`, plus package-local IPC file-to-stream and stream-to-file converter modes. | Remaining gap is upstream Archery tester registration for automated cross-language runner participation. | `test/integrationtest.jl`, `test/arrowjson.jl`, `test/arrowjson/modern-layouts.json`, `test/arrowjson/run-end-encoded.json`, `test/arrowjson/canonical-extensions.json`, `src/arraytypes/views.jl`, `src/write.jl`, `src/metadata/Message.jl`. | P1 | Decide whether to pursue upstream Archery tester registration from this package branch or keep the local executable as the integration contract. |
| ARROW-AUDIT-022 | C Data Interface | Export/import `ArrowSchema` and `ArrowArray` base structs with release-governed same-process lifetimes. | Covered for the documented base C Data scope. | Adjacent C Stream, C Device, and PyCapsule interfaces are separate rows, not implied by C Data. | `src/cdata.jl`, `src/cdata/`, `include/arrow_julia_cdata.h`, `test/cdata.jl`, `test/cdata_validation_report.jl`, `docs/src/cdata_alignment.md`. | P0 | Keep the C Data alignment page as the detailed submatrix. |
| ARROW-AUDIT-023 | C Stream Interface | Expose `ArrowArrayStream` callbacks for same-process pull-style streaming. | Not implemented. | Current C Data support exports/imports arrays and schemas, not streams. | `docs/src/cdata_alignment.md`, absence of `ArrowArrayStream` runtime types. | P2 | Design a separate `Arrow.CStream` module or document it as a non-goal. |
| ARROW-AUDIT-024 | C Device Data Interface | Exchange Arrow arrays/streams with device placement and synchronization metadata. | Deferred. | Official surface is experimental and requires a device memory ownership model. | No device memory module; C Data is CPU-buffer oriented. | P3 | Defer until Arrow.jl has a concrete GPU/device consumer and memory-manager design. |
| ARROW-AUDIT-025 | PyCapsule Interface | Provide Python capsule protocol methods for C Data, C Stream, and C Device objects. | Not implemented. | Arrow.jl exposes Julia FFI pointers, not Python object protocols. | `docs/src/cdata_alignment.md`; no Python capsule producer/consumer code. | P2 | Treat as a Python interop slice after C Stream direction is decided. |
| ARROW-AUDIT-026 | Flight RPC protocol surface | Support Flight service methods, descriptors, IPC payloads, errors, and auth metadata. | Covered for the current package-owned Flight scope. Generated protocol, server core, gRPCServer listener path, DoGet/DoPut/DoExchange helpers, `CancelFlightInfo` action/result payload helpers, explicit Julia client-runtime boundary helpers, and Python-client smoke coverage including `Handshake` token propagation, `PollFlightInfo`, and `CancelFlightInfo` `DoAction` exist. | No known drift in the package-owned Flight scope. Arrow.jl explicitly does not ship an in-package Julia Flight gRPC client runtime yet; external interop proof is owned by Python clients generated from the package `Flight.proto` or by PyArrow. | `src/flight/`, `test/flight.jl`, `test/flight_purehttp2.jl`, `test/flight_grpcserver.jl`, `test/flight/live_service_support.jl`, `README.md`. | P1 | If Arrow.jl accepts a Julia Flight client runtime, add it through a dedicated dependency/API design and live client test matrix. |
| ARROW-AUDIT-027 | Flight listener backends and transport performance | Package a clear live listener backend and performance evidence for large data movement. | Partial. `gRPCServer.jl` is the canonical packaged listener; nghttp2 is a bounded weakdep backend; large DoGet, large DoPut, large DoExchange, concurrent DoGet, connection-isolated concurrent DoPut, and concurrent DoExchange performance receipts exist for the canonical listener path. | Backend parity remains incomplete: nghttp2 still lacks request-streaming Flight methods, and same-client/reused DoPut upload soak receipts are not yet tracked. | `src/flight/server/backend_profiles.jl`, `test/flight_purehttp2_perf.jl`, `test/flight_nghttp2.jl`, `test/flight_grpcserver.jl`. | P2 | Extend backend parity and same-client upload-reuse performance receipts after the canonical gRPCServer path remains stable in CI. |
| ARROW-AUDIT-028 | Flight SQL | Interact with SQL databases through Flight SQL commands and actions. | Not implemented. | Flight SQL is a separate protocol layered on Flight; Arrow.jl currently owns Flight primitives, not SQL client/server semantics. | No Flight SQL module, generated bindings, or tests. | P3 | Defer to a dedicated package/module design if Arrow.jl should own SQL semantics. |
| ARROW-AUDIT-029 | ADBC | Provide database connectivity APIs returning Arrow streams. | Not implemented. | ADBC is a broader database access API standard and may belong outside Arrow.jl core. | No ADBC module or DB driver API. | P3 | Keep out of scope unless a Julia ADBC package boundary is accepted. |
| ARROW-AUDIT-030 | Performance and zero-copy governance | Back zero-copy and performance claims with repeatable receipts. | Partial. C Data has allocation and pointer-identity receipts; IPC has stream/file write, metadata-read, and scan receipts; Flight has large DoGet, large DoPut, large DoExchange, concurrent DoGet, connection-isolated concurrent DoPut, and concurrent DoExchange performance receipts. | Same-client/reused Flight upload performance does not yet have the same audit-gated baseline, and IPC timing remains informational by default rather than a strict wall-clock gate. | `test/cdata_validation_report.jl`, `test/ipc_performance_report.jl`, `test/flight_purehttp2_perf.jl`, CI workflow entries. | P2 | Add same-client Flight upload-reuse receipts only after the canonical gRPCServer path remains stable in CI. |
| ARROW-AUDIT-031 | Documentation truthfulness and discoverability | Public docs should expose the full support matrix and avoid stale unsupported lists. | Covered by this audit slice once merged. | Future implementation slices can drift unless rows are updated when features land. | `docs/src/arrow_alignment_audit.md`, `docs/src/cdata_alignment.md`, `README.md`, `docs/make.jl`. | P0 | Update this page in the same PR as any feature that changes a row status. |

## Immediate Closure Plan

1. Documentation truth baseline:
   keep this page and the C Data alignment submatrix in docs navigation, and
   keep README support claims synchronized with the matrix.
2. Format and integration closure:
   keep the new fixtures for `Utf8View` / `BinaryView`, `ListView` /
   `LargeListView`, REE read behavior, month-day-nano intervals, and
   canonical extension metadata active; next decide upstream Archery tester
   registration.
3. Core format decision:
   decide whether standalone Tensor/SparseTensor messages are in scope for
   Arrow.jl or explicitly documented as unsupported.
4. Canonical extension closure:
   decide whether tensor extensions and `arrow.parquet.variant` remain
   storage-preserving passthroughs or receive semantic Julia wrappers.
5. Robustness closure:
   extend the validation corpus for untrusted IPC, C Data soundness checks,
   and future official extension combinations.
6. Interop interface closure:
   design C Stream first, then decide PyCapsule on top of the accepted stream
   and array ownership model; keep C Device deferred until a device-memory
   owner exists.
7. Flight closure:
   keep the package-owned protocol/server boundary explicit; revisit a Julia
   Flight client runtime only through a dedicated dependency/API design.
8. Adjacent protocol decisions:
   keep Flight SQL, ADBC, Statistics Schema, and Dissociated IPC as explicit
   deferred rows unless a dedicated consumer and module boundary are accepted.

## High-Priority Audit IDs

The next implementation plan should start with:

| Order | ID | Why |
| --- | --- | --- |
| 1 | ARROW-AUDIT-020 | Security validation is the highest-risk surface before expanding interop claims. |
| 2 | ARROW-AUDIT-021 | Integration coverage still needs an upstream Archery registration decision. |
| 3 | ARROW-AUDIT-030 | Flight same-client upload reuse and strict performance gates remain the next production evidence gap. |
| 4 | ARROW-AUDIT-017 | Tensor extension wrappers remain a documented semantic-policy gap. |
| 5 | ARROW-AUDIT-018 | Parquet Variant and opaque extension policies still need deeper producer/semantic decisions. |
