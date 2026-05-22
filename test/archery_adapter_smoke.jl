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
const ARCHERY_WRAPPER =
    joinpath(PACKAGE_ROOT, "dev", "archery", "arrow-julia-json-integration-test")
const ARCHERY_TESTER = joinpath(PACKAGE_ROOT, "dev", "archery", "tester_julia.py")
const FIXTURE = joinpath(TEST_ROOT, "arrowjson", "primitive-empty.json")

function run_archery_adapter(args...)
    run(pipeline(`$(ARCHERY_WRAPPER) $(collect(args))`; stdout=devnull))
    return
end

@testset "Archery adapter smoke" begin
    @test isfile(ARCHERY_WRAPPER)
    @test isfile(ARCHERY_TESTER)
    mktempdir() do dir
        arrowfile = joinpath(dir, "primitive-empty.arrow")
        streamfile = joinpath(dir, "primitive-empty.stream")
        convertedfile = joinpath(dir, "primitive-empty.converted.arrow")

        run_archery_adapter(
            "--integration",
            "--json",
            FIXTURE,
            "--arrow",
            arrowfile,
            "--mode",
            "JSON_TO_ARROW",
        )
        @test isfile(arrowfile)

        run_archery_adapter(
            "--integration",
            "--json",
            FIXTURE,
            "--arrow",
            arrowfile,
            "--mode",
            "VALIDATE",
        )

        run_archery_adapter(
            "--integration",
            "--input",
            arrowfile,
            "--output",
            streamfile,
            "--mode",
            "FILE_TO_STREAM",
        )
        @test isfile(streamfile)

        run_archery_adapter(
            "--integration",
            "--input",
            streamfile,
            "--output",
            convertedfile,
            "--mode",
            "STREAM_TO_FILE",
        )
        @test isfile(convertedfile)

        run_archery_adapter(
            "--integration",
            "--json",
            FIXTURE,
            "--arrow",
            convertedfile,
            "--mode",
            "VALIDATE",
        )
    end
end
