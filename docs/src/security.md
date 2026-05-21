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

# Security and Robustness

Arrow.jl reads Arrow IPC, C Data, C Stream, and Flight payloads as in-process
data, and exposes C Stream producers over the same C Data ownership model. It
does not sandbox parsing, allocate data in a separate trust domain, or turn
untrusted input into a security boundary by itself. Applications that accept
Arrow data from untrusted peers should keep normal process isolation, resource
limits, and transport authentication outside Arrow.jl.
For IPC streams or files, `Arrow.validate(input; convert=false)` runs the
reader-side structural checks, requires at least one schema message, and
returns `nothing` on success or throws the same diagnostic `ArgumentError` that
`Arrow.Table` would throw for malformed metadata or buffers. Use
`Arrow.validate(input; stream=true)` to validate by iterating the batch-wise
`Arrow.Stream` reader path.

## Checked Layout Boundaries

The package keeps targeted negative tests for layout classes that can otherwise
lead to unsafe indexing or misleading semantic interpretation:

- unsupported Tensor and SparseTensor IPC message headers are recognized and
  rejected explicitly;
- truncated IPC message length, metadata, and body sections are rejected after
  a continuation marker has started a message;
- IPC field nodes validate non-negative lengths, non-negative null counts, and
  null counts bounded by their logical lengths before buffer materialization;
- variable-size List, LargeList, Binary, Utf8, Map, ListView, BinaryView, and
  Utf8View IPC layouts validate offset or view span counts, monotonicity where
  required, non-negative spans, and child/data bounds before materialization;
- IPC Utf8, LargeUtf8, and Utf8View string values validate UTF-8 bytes for
  non-null slots before exposing Julia strings;
- IPC dictionary-encoded columns validate non-null index slots against the
  dictionary length before exposing borrowed `DictEncoded` vectors;
- IPC FixedSizeList columns validate that child arrays cover the declared
  fixed-size logical length before exposing tuple values;
- IPC Run-End Encoded columns validate run-end type, run/value counts,
  strictly increasing positive run ends, non-null run ends, and final coverage
  before exposing logical values;
- IPC dense and sparse union columns validate type ids, dense unions validate
  child offsets, and sparse unions validate child array lengths before
  exposing borrowed union vectors;
- canonical extension metadata and storage contracts are parsed and validated
  for the supported canonical extension names before Arrow.jl returns
  converted semantic values;
- C Data import rejects released schema/array inputs and validates format
  strings, metadata entry counts and string byte lengths, base struct shape,
  length/null-count consistency, child counts, child pointer entries, buffers,
  release callbacks, non-zero offsets, dictionary schema/array peers, nested
  dictionaries, Binary/UTF-8 offsets, UTF-8 bytes, view buffers, UTF-8 view
  bytes, dictionary indices, list-view spans, and run-end encoded child shape
  inside the documented same-process ownership model.

## Current Limits

These checks are not a complete malicious-input certification. The remaining
tracked work is to add a larger regression corpus for malformed IPC files,
C Data soundness cases, and future official extension combinations, then decide
whether Arrow.jl should expose a public validation API separate from normal read
paths.
