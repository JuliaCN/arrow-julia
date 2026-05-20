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

# Archery Integration Adapter

This directory contains the Arrow.jl IPC adapter surface intended for Apache
Arrow Archery integration testing.

The executable wrapper:

```sh
dev/archery/arrow-julia-json-integration-test --integration \
  --json test/arrowjson/primitive-empty.json \
  --arrow /tmp/primitive.arrow \
  --mode JSON_TO_ARROW
```

delegates to `test/integrationtest.jl` under the package test environment. It
supports the Archery IPC operations used by `Tester`: `JSON_TO_ARROW`,
`VALIDATE`, `FILE_TO_STREAM`, and `STREAM_TO_FILE`.

`tester_julia.py` mirrors the Python `Tester` class shape from Apache Arrow's
`dev/archery/archery/integration/` package.

`apache-arrow-archery-julia-registration.patch` is a patch artifact for the
Apache Arrow monorepo. It adds a Julia tester module, registers
`--with-julia`, and wires the selector through
`ARCHERY_INTEGRATION_WITH_JULIA`. Apply it from an Apache Arrow checkout and
point `ARROW_JULIA_ROOT` at this package checkout:

```sh
cd /path/to/apache-arrow
git apply /path/to/arrow-julia/dev/archery/apache-arrow-archery-julia-registration.patch
ARROW_JULIA_ROOT=/path/to/arrow-julia \
  ARCHERY_INTEGRATION_WITH_JULIA=1 \
  archery integration --run-ipc --target-implementations=julia -k primitive
```

This repository keeps the patch as an auditable registration artifact. Actual
upstream Archery participation is complete only after the patch is applied and
validated in the Apache Arrow monorepo.

Upstream apply receipt:

- Apache Arrow checkout: `603eeec8f6d75fa3d029be0aefbf1405a0dde69b`.
- `git apply --check dev/archery/apache-arrow-archery-julia-registration.patch`
  passed from the Apache Arrow repository root.
- `git apply dev/archery/apache-arrow-archery-julia-registration.patch`
  passed from the Apache Arrow repository root.
- `python -m py_compile dev/archery/archery/integration/tester_julia.py`
  passed after applying the patch.
- `rg -n "with_julia|ARCHERY_INTEGRATION_WITH_JULIA|JuliaTester|command=\"FILE_TO_STREAM\"" dev/archery/archery`
  found the expected registration points.

Upstream runner receipts:

- Apache Arrow checkout: `603eeec8f6d75fa3d029be0aefbf1405a0dde69b`.
- Command shape for the focused selectors:

  ```sh
  ARROW_JULIA_ROOT=/path/to/arrow-julia \
    ARCHERY_INTEGRATION_WITH_JULIA=true \
    archery integration \
      --run-ipc \
      --target-implementations=julia \
      --with-julia=true \
      --serial \
      --stop-on-error \
      --tempdir /tmp/arrow-julia-archery-primitive \
      -k <selector>
  ```

- `primitive` selector result: `0 failures, 0 skips`.
- `primitive` covered upstream IPC JSON cases:
  `generated_primitive_no_batches.json`, `generated_primitive.json`, and
  `generated_primitive_zerolength.json`.
- `dictionary` selector result: `0 failures, 0 skips`.
- `dictionary` covered upstream IPC JSON cases:
  `generated_dictionary.json`, `generated_dictionary_unsigned.json`, and
  `generated_nested_dictionary.json`.
- `nested` selector result: `0 failures, 0 skips`.
- `nested` covered upstream IPC JSON cases:
  `generated_nested.json`, `generated_recursive_nested.json`,
  `generated_nested_large_offsets.json`, and
  `generated_nested_dictionary.json`.
- `map` selector result: `0 failures, 0 skips`.
- `map` covered upstream IPC JSON cases:
  `generated_map.json` and `generated_map_non_canonical.json`.
- `decimal` selector result: `0 failures, 0 skips`.
- `decimal` covered upstream IPC JSON cases:
  `generated_decimal.json`, `generated_decimal256.json`,
  `generated_decimal32.json`, and `generated_decimal64.json`.
- `union` selector result: `0 failures, 0 skips`.
- `union` covered upstream IPC JSON cases:
  `generated_union.json`.
- Each covered case executed Julia producer plus Julia consumer validation
  through file and stream paths.

Local validation:

```sh
python3 -m py_compile dev/archery/tester_julia.py
julia --project=test test/archery_adapter_smoke.jl
julia --project=test test/archery_upstream_patch_smoke.jl
```
