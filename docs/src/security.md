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

Arrow.jl reads Arrow IPC, C Data, and Flight payloads as in-process data. It
does not sandbox parsing, allocate data in a separate trust domain, or turn
untrusted input into a security boundary by itself. Applications that accept
Arrow data from untrusted peers should keep normal process isolation, resource
limits, and transport authentication outside Arrow.jl.

## Checked Layout Boundaries

The package keeps targeted negative tests for layout classes that can otherwise
lead to unsafe indexing or misleading semantic interpretation:

- unsupported Tensor and SparseTensor IPC message headers are recognized and
  rejected explicitly;
- variable-size List, LargeList, Binary, Utf8, Map, ListView, BinaryView, and
  Utf8View IPC layouts validate offset or view span counts, monotonicity where
  required, non-negative spans, and child/data bounds before materialization;
- IPC Utf8, LargeUtf8, and Utf8View string values validate UTF-8 bytes for
  non-null slots before exposing Julia strings;
- canonical extension metadata is parsed and validated for the supported
  canonical extension names before Arrow.jl returns converted semantic values;
- C Data import validates base struct shape, child counts, buffers, release
  callbacks, non-zero offsets, nested dictionaries, view buffers, list-view
  spans, and run-end encoded children inside the documented same-process
  ownership model.

## Current Limits

These checks are not a complete malicious-input certification. The remaining
tracked work is to add a larger regression corpus for malformed IPC files,
malformed extension metadata combinations, and C Data soundness cases, then
decide whether Arrow.jl should expose a public validation API separate from
normal read paths.
