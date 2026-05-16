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

# C Data Interface Alignment

This page tracks the current Arrow.jl alignment with Apache Arrow's C
interfaces. The implemented surface is the same-process
[Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
for `ArrowSchema` and `ArrowArray` pointers. The
[Arrow C Stream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html),
[Arrow C Device data interface](https://arrow.apache.org/docs/format/CDeviceDataInterface.html),
and
[Arrow PyCapsule Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html)
are adjacent specifications and are tracked separately below.

Status labels:

- `Covered`: implemented and covered by focused tests.
- `Partial`: implemented for the listed subset, with a recorded audit item.
- `Not implemented`: a known gap inside the C Data boundary.
- `Out of scope`: an adjacent Arrow C interface or transport surface, not a
  C Data base-struct feature.

## Synchronization Matrix

| Area | Official Arrow contract | Arrow.jl export | Arrow.jl import | Evidence | Status | Next action |
| --- | --- | --- | --- | --- | --- | --- |
| Base struct ABI | `ArrowSchema` and `ArrowArray` carry format, metadata, children, dictionary, buffers, release callbacks, and producer private data. | `Arrow.CData.ArrowSchema`, `Arrow.CData.ArrowArray`, `exporttable`, and `exporttable!`. | `importtable` borrows a top-level struct array. | `src/cdata/core.jl`, `src/cdata/export.jl`, `src/cdata/import.jl`, `test/cdata.jl`. | Covered | Keep ABI field order and release behavior guarded by focused tests. |
| Lifetime and release callbacks | Consumers release only the base structures; producers release children, dictionaries, and private data. Released structures set `release` to `NULL`. | Export owners retain schema strings, metadata bytes, child refs, pointer arrays, dictionaries, and source buffers. | Imported tables call base `release` callbacks through `release!`. | C smoke, embedded C smoke, release-idempotency tests. | Covered | Keep C smoke in CI and avoid exposing child-release helpers as public API. |
| Same-process zero-copy | C Data exposes pointers to existing Arrow memory; it is not an IPC, persistence, or cross-process transport. | Exported arrays point at Arrow.jl-owned buffers. | Imported primitive, bitmap, offset, data, view, list, union, and REE buffers are borrowed with `own=false`. | `test/cdata_validation_report.jl`, pointer-identity assertions in `test/cdata.jl`. | Covered | Keep reporting local export/import/scan timings and pointer identity. |
| C consumer header | The C Data specification is small enough to copy into consumers. | `include/arrow_julia_cdata.h` exposes package-owned struct/release helpers. | Consumers can validate and release exported base structs from C. | `header_path`, `test/cdata_smoke.c`, `test/cdata_embed_smoke.c`. | Covered | Keep the header limited to C Data helpers, not a high-level table builder. |
| Schema and field metadata bytes | `ArrowSchema.metadata` uses the official native-endian key/value byte layout; absent metadata can be `NULL`. Extension type names and extension metadata are encoded there. | Schema and field metadata are exported from `Arrow.getmetadata` and `Arrow.withmetadata`. | Imported tables and columns expose raw schema/field metadata through `Arrow.getmetadata`, including `ARROW:extension:*` keys as a storage-preserving fallback. | Metadata export/import tests and `ImportedTable` metadata fields. | Covered | None. |
| Null layout | Format `n` represents null arrays with no buffers. | `NullVector` exports as `n`. | Format `n` imports as a missing-valued vector. | Null/logical scalar tests. | Covered | None. |
| Primitive integers and floats | Fixed-width primitive formats use one optional validity bitmap and one data buffer. | Primitive storage exports standard format strings. | Imported columns borrow the data buffer, with nullable wrappers when needed. | Primitive and nullable primitive tests. | Covered | None. |
| Boolean | Format `b` uses bit-packed boolean values plus optional validity. | `BoolVector` exports as `b`. | Imported boolean columns borrow packed data and validity bitmaps. | Boolean import tests. | Covered | None. |
| UTF-8 and binary | Formats `u`, `U`, `z`, and `Z` use offsets plus data buffers. | String, large string, binary, and large binary Arrow columns export standard layouts. | Imported columns borrow offsets and data buffers. | UTF-8, large UTF-8, binary, and large binary tests. | Covered | None. |
| UTF-8 view and binary view | Formats `vu` and `vz` use view records, variadic data buffers, and a variadic-buffer-lengths buffer in C Data. | Arrow.jl view columns export `vu` and `vz` with the extra C Data lengths buffer. | Imported view columns validate spans and borrow inline, view, data, and lengths storage. | View export/import tests. | Covered | None. |
| List and large list | Formats `+l` and `+L` use offsets and one child array. | List columns export normal and large offset widths. | Imported list columns borrow offsets and child arrays. | List and large-list tests. | Covered | None. |
| List-view and large list-view | Formats `+vl` and `+vL` use offsets, sizes, and one child array. | Not exported because Arrow.jl does not currently expose a native list-view producer vector. | Imported list-view columns borrow offsets, sizes, and child arrays. | Hand-built C Data list-view import tests. | Partial | `CDATA-AUDIT-003`. |
| Fixed-size binary and fixed-size list | Formats `w:N` and `+w:N` use fixed-width data or one fixed-size-list child. | Fixed-size binary/list columns export standard layouts. | Imported columns borrow data or child arrays. | Fixed-size tests. | Covered | None. |
| Struct | Format `+s` uses one validity buffer and child arrays. | Top-level tables and nested struct columns export as struct arrays. | Non-null top-level structs and nested struct columns import. Nullable top-level structs are rejected. | Struct tests and top-level validation tests. | Partial | `CDATA-AUDIT-004`. |
| Map | Format `+m` uses Int32 offsets and one entries struct child. | Standard Int32-offset map columns export. Large-offset Arrow.jl map views are rejected because C Data map format has no offset-width variant. | Imported map columns borrow Int32 offsets and entries. | Map tests and explicit large-offset rejection. | Covered | None. |
| Dictionary | Parent schema/array describe indices; dictionary schema/array describe values. | Dictionary columns export parent indices plus dictionary values. | Imported dictionary columns borrow index and dictionary buffers. Nested dictionary chains are rejected. | Dictionary tests and nested-dictionary guards. | Partial | `CDATA-AUDIT-005`. |
| Dense and sparse union | Formats `+ud:<ids>` and `+us:<ids>` carry type IDs and optional dense offsets. | Dense and sparse union columns export declared UInt8 type IDs. | Imported union columns borrow type IDs, dense offsets, and child arrays. | Union tests. | Covered | None. |
| Run-end encoded | Format `+r` uses `run_ends` and `values` children. | Run-end encoded columns export `+r`. | Imported REE columns borrow both child arrays. | REE tests. | Covered | None. |
| Logical scalar storage | Decimal, date, time, timestamp, duration, and interval storage use C Data format strings over physical buffers. | Arrow.jl-surfaced decimal, date, time, timestamp, duration, year-month interval, and day-time interval storage exports. | The same surfaced logical scalar formats import over borrowed physical buffers. Month-day-nano interval is not currently surfaced. | Logical scalar tests and format mapping in `src/cdata/formats.jl`. | Partial | `CDATA-AUDIT-006`. |
| Array offsets | Consumers may receive arrays with non-zero `ArrowArray.offset` when buffers are large enough for `length + offset`. | Export emits zero-offset arrays. | Import honors non-zero offsets for supported top-level and child arrays, including bit-level validity and boolean buffers. | Offset-aware import tests in `test/cdata.jl`. | Covered | None. |
| C Stream Interface | `ArrowArrayStream` is a pull-style stream of schemas and arrays using callbacks. | Not exported as C Stream. | Not imported as C Stream. | Out-of-scope docs boundary. | Out of scope | `CDATA-AUDIT-008`. |
| C Device data interface | Device arrays and device streams carry memory-manager and device placement information. | Not exported as C Device. | Not imported as C Device. | Out-of-scope docs boundary. | Out of scope | `CDATA-AUDIT-009`. |
| PyCapsule Interface | Python objects can expose Arrow C Data, Stream, and Device interfaces through capsule methods. | No PyCapsule producer. | No PyCapsule consumer. | Out-of-scope docs boundary. | Out of scope | `CDATA-AUDIT-010`. |

## Audit Tracker

| ID | Gap or risk | Evidence | Owner surface | Priority | Tracking status | Landing condition |
| --- | --- | --- | --- | --- | --- | --- |
| CDATA-AUDIT-001 | Imported C Data metadata is borrowed but not exposed through a high-level `Arrow.getmetadata` surface on `ImportedTable` or imported columns. | Imported tables now decode the C Data metadata byte layout and metadata-bearing imported columns use a delegating wrapper that preserves low-level buffer access. | `src/cdata/core.jl`, `src/cdata/imported_vectors.jl`, `src/cdata/import.jl`, `test/cdata/import_basic_tests.jl`. | P1 | Closed | Landed when imported schema and field metadata became readable without copying data buffers, with tests for schema and column metadata. |
| CDATA-AUDIT-002 | Extension type reconstruction from C Data metadata is not implemented. | C Data import now preserves `ARROW:extension:name` and `ARROW:extension:metadata` on imported storage vectors as a documented raw fallback; it does not reconstruct Julia extension wrapper values. | `src/cdata/imported_vectors.jl`, `src/cdata/import.jl`, `test/cdata/import_basic_tests.jl`, `docs/src/manual.md`. | P1 | Closed | Landed when C Data import recognized extension metadata through `Arrow.getmetadata` and documented raw storage fallback behavior. |
| CDATA-AUDIT-003 | List-view export has no native Arrow.jl producer vector shape. | `src/cdata/formats.jl` throws for list-view export; tests build list-view C Data manually for import. | Arrow vector type surface and `src/cdata/export.jl`. | P2 | Open | A native producer shape exists and `exporttable` can emit `+vl` / `+vL` with tests. |
| CDATA-AUDIT-004 | Nullable top-level struct import is rejected. | `importtable` requires top-level `array.null_count == 0`. | `src/cdata/import.jl`. | P2 | Open | Import either supports top-level struct validity or documents a stable reason to keep it rejected. |
| CDATA-AUDIT-005 | Nested dictionary chains are rejected on import. | `_import_dictionary_column` rejects dictionary schemas or arrays that themselves carry dictionaries. | `src/cdata/import.jl`, imported vector wrappers. | P2 | Open | Nested dictionary values import correctly or remain explicitly documented as unsupported with a targeted negative test. |
| CDATA-AUDIT-006 | Month-day-nano interval (`tin`) is not represented in the current C Data logical scalar mapping. | `src/cdata/formats.jl` maps `tiM` and `tiD`, but not `tin`. | Arrow logical scalar types and `src/cdata/formats.jl`. | P2 | Open | If Arrow.jl exposes month-day-nano interval storage, C Data export/import maps it and tests it; otherwise the docs keep it listed as not surfaced. |
| CDATA-AUDIT-007 | Non-zero `ArrowArray.offset` import was rejected. | Offset-aware import tests now cover top-level struct slicing plus primitive, boolean, UTF-8, list, dictionary, fixed-size, view, and run-end encoded child arrays. | `src/cdata/import.jl` and imported vector wrappers. | P1 | Closed | Landed when import borrowed non-zero-offset arrays without misaddressing validity, offsets, values, or child buffers. |
| CDATA-AUDIT-008 | C Stream is not part of `Arrow.CData`. | C Stream uses `ArrowArrayStream`, not just base `ArrowSchema` / `ArrowArray`. | Future C stream module or explicit non-goal. | P3 | Deferred | A separate design decides whether Arrow.jl should expose `ArrowArrayStream`; until then docs keep it out of C Data support claims. |
| CDATA-AUDIT-009 | C Device data is not part of `Arrow.CData`. | Device arrays add device and memory-manager semantics beyond host buffers. | Future device-interface module or explicit non-goal. | P3 | Deferred | A separate design decides whether Arrow.jl should expose device arrays/streams. |
| CDATA-AUDIT-010 | PyCapsule producer/consumer methods are not implemented. | Arrow.jl currently exposes Julia FFI pointers, not Python object capsule protocols. | Python interop boundary, if adopted. | P3 | Deferred | A package-owned interop design defines capsule ownership, release behavior, and tests against Python consumers. |
| CDATA-AUDIT-011 | Performance governance is a local receipt, not a CI gate. | `test/cdata_validation_report.jl` reports timings and zero-copy checks but does not enforce thresholds. | `test/cdata_validation_report.jl`, CI workflow policy. | P2 | Open | A documented threshold policy or CI job gates obvious regressions while avoiding noisy wall-clock failures. |
| CDATA-AUDIT-012 | PR CI matrix status must stay separate from local C Data validation. | Local `test/cdata.jl`, docs, and validation report can pass before the full GitHub matrix completes. | GitHub Actions and PR review notes. | P1 | Tracking | PR status reports distinguish local validation, passing focused checks, and still-pending matrix jobs. |

## Follow-Up Order

1. Keep `CDATA-AUDIT-008`, `CDATA-AUDIT-009`, and `CDATA-AUDIT-010` outside the
   current PR unless a separate interface design is accepted.
