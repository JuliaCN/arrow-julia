# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
from pathlib import Path

from .tester import Tester
from .util import log, run_cmd


def _default_arrow_julia_root():
    local_root = Path(__file__).resolve().parents[2]
    if (local_root / "test" / "integrationtest.jl").is_file():
        return str(local_root)
    cwd = Path.cwd()
    if (cwd / "test" / "integrationtest.jl").is_file():
        return str(cwd)
    raise RuntimeError(
        "Set ARROW_JULIA_ROOT to an Arrow.jl checkout containing "
        "test/integrationtest.jl"
    )


_ARROW_JULIA_ROOT = os.environ.get("ARROW_JULIA_ROOT")
if _ARROW_JULIA_ROOT is None:
    _ARROW_JULIA_ROOT = _default_arrow_julia_root()
_INTEGRATION_EXE = os.environ.get(
    "ARROW_JULIA_INTEGRATION_EXE",
    os.path.join(_ARROW_JULIA_ROOT, "dev", "archery", "arrow-julia-json-integration-test"),
)


class JuliaTester(Tester):
    PRODUCER = True
    CONSUMER = True

    name = "Julia"

    def _run(
        self,
        arrow_path=None,
        json_path=None,
        input_path=None,
        output_path=None,
        command="VALIDATE",
    ):
        cmd = [_INTEGRATION_EXE, "--integration"]

        if arrow_path is not None:
            cmd.append("--arrow=" + str(arrow_path))

        if json_path is not None:
            cmd.append("--json=" + str(json_path))

        if input_path is not None:
            cmd.append("--input=" + str(input_path))

        if output_path is not None:
            cmd.append("--output=" + str(output_path))

        cmd.append("--mode=" + command)

        if self.debug:
            log(" ".join(cmd))

        run_cmd(cmd)

    def validate(self, json_path, arrow_path, quirks=None):
        return self._run(arrow_path, json_path, command="VALIDATE")

    def json_to_file(self, json_path, arrow_path):
        return self._run(arrow_path, json_path, command="JSON_TO_ARROW")

    def stream_to_file(self, stream_path, file_path):
        return self._run(
            input_path=stream_path,
            output_path=file_path,
            command="STREAM_TO_FILE",
        )

    def file_to_stream(self, file_path, stream_path):
        return self._run(
            input_path=file_path,
            output_path=stream_path,
            command="FILE_TO_STREAM",
        )
