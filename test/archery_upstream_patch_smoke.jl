# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

using Test

const TEST_ROOT = @__DIR__
const PACKAGE_ROOT = dirname(TEST_ROOT)
const ARCHERY_DIR = joinpath(PACKAGE_ROOT, "dev", "archery")
const UPSTREAM_PATCH =
    joinpath(ARCHERY_DIR, "apache-arrow-archery-julia-registration.patch")
const LOCAL_TESTER = joinpath(ARCHERY_DIR, "tester_julia.py")

@testset "upstream Archery registration patch smoke" begin
    @test isfile(UPSTREAM_PATCH)
    @test isfile(LOCAL_TESTER)

    patch = read(UPSTREAM_PATCH, String)
    tester = read(LOCAL_TESTER, String)

    @test occursin("diff --git a/dev/archery/archery/cli.py", patch)
    @test occursin("diff --git a/dev/archery/archery/integration/runner.py", patch)
    @test occursin("dev/archery/archery/integration/tester_julia.py", patch)
    @test occursin("--with-julia", patch)
    @test occursin("ARCHERY_INTEGRATION_WITH_JULIA", patch)
    @test occursin("with_julia=False", patch)
    @test occursin("from .tester_julia import JuliaTester", patch)
    @test occursin("append_tester(\"julia\", JuliaTester(**kwargs))", patch)
    @test occursin("class JuliaTester(Tester):", patch)
    @test occursin("ARROW_JULIA_ROOT", patch)
    @test occursin("ARROW_JULIA_INTEGRATION_EXE", patch)
    @test occursin("command=\"VALIDATE\"", patch)
    @test occursin("command=\"JSON_TO_ARROW\"", patch)
    @test occursin("command=\"STREAM_TO_FILE\"", patch)
    @test occursin("command=\"FILE_TO_STREAM\"", patch)

    @test occursin("command=\"VALIDATE\"", tester)
    @test occursin("command=\"JSON_TO_ARROW\"", tester)
    @test occursin("command=\"STREAM_TO_FILE\"", tester)
    @test occursin("command=\"FILE_TO_STREAM\"", tester)
    @test !occursin("os.environ.get(\"ARROW_JULIA_ROOT\", _default_arrow_julia_root())", tester)
    @test !occursin("return self._run(arrow_path, json_path, \"VALIDATE\")", tester)
    @test !occursin("return self._run(arrow_path, json_path, \"JSON_TO_ARROW\")", tester)
end
