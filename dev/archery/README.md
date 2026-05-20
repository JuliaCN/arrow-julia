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

Local validation:

```sh
python3 -m py_compile dev/archery/tester_julia.py
julia --project=test test/archery_adapter_smoke.jl
julia --project=test test/archery_upstream_patch_smoke.jl
```
