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

const TEST_PROJECT = dirname(Base.active_project())
const INTEGRATION_SCRIPT = joinpath(@__DIR__, "integrationtest.jl")
const ARROW_JSON_DIR = joinpath(@__DIR__, "arrowjson")

function integration_cmd(args...)
    return `$(Base.julia_cmd()) --startup-file=no --project=$(TEST_PROJECT) $(INTEGRATION_SCRIPT) $(collect(args))`
end

function run_integration_cli(args...)
    run(pipeline(integration_cmd(args...); stdout=devnull))
    return
end

function assert_json_ipc_roundtrip(jsonfile, dir; basename)
    arrowfile = joinpath(dir, "$basename.arrow")
    generatedjson = joinpath(dir, "$basename.roundtrip.json")
    streamfile = joinpath(dir, "$basename.stream")
    convertedfile = joinpath(dir, "$basename.converted.arrow")

    run_integration_cli(
        "--integration",
        "--json",
        jsonfile,
        "--arrow",
        arrowfile,
        "--mode",
        "json-to-arrow",
    )
    @test isfile(arrowfile)

    run_integration_cli(
        "--integration",
        "--json",
        jsonfile,
        "--arrow",
        arrowfile,
        "--mode",
        "validate",
    )

    run_integration_cli(
        "--integration",
        "--json",
        generatedjson,
        "--arrow",
        arrowfile,
        "--mode",
        "arrow-to-json",
    )
    @test isfile(generatedjson)

    run_integration_cli(
        "--integration",
        "--json",
        generatedjson,
        "--arrow",
        arrowfile,
        "--mode",
        "validate",
    )

    run_integration_cli(
        "--integration",
        "--input",
        arrowfile,
        "--output",
        streamfile,
        "--mode",
        "file-to-stream",
    )
    @test isfile(streamfile)

    run_integration_cli(
        "--integration",
        "--json",
        jsonfile,
        "--arrow",
        streamfile,
        "--mode",
        "validate",
    )

    run_integration_cli(
        "--integration",
        "--input",
        streamfile,
        "--output",
        convertedfile,
        "--mode",
        "stream-to-file",
    )
    @test isfile(convertedfile)

    run_integration_cli(
        "--integration",
        "--json",
        jsonfile,
        "--arrow",
        convertedfile,
        "--mode",
        "validate",
    )
    return
end

function assert_json_ipc_validate(jsonfile, dir; basename)
    arrowfile = joinpath(dir, "$basename.arrow")
    run_integration_cli(
        "--integration",
        "--json",
        jsonfile,
        "--arrow",
        arrowfile,
        "--mode",
        "json-to-arrow",
    )
    @test isfile(arrowfile)
    run_integration_cli(
        "--integration",
        "--json",
        jsonfile,
        "--arrow",
        arrowfile,
        "--mode",
        "validate",
    )
    return
end

function write_null_trivial_json(path)
    write(
        path,
        """
        {
          "schema": {
            "fields": [
              {
                "name": "f0",
                "type": {
                  "name": "null"
                },
                "nullable": true,
                "children": []
              }
            ]
          },
          "batches": [
            {
              "count": 0,
              "columns": [
                {
                  "name": "f0",
                  "count": 0
                }
              ]
            }
          ]
        }
        """,
    )
    return path
end

@testset "integration CLI subprocess smoke" begin
    mktempdir() do dir
        assert_json_ipc_roundtrip(
            write_null_trivial_json(joinpath(dir, "null-trivial.json")),
            dir;
            basename="null-trivial",
        )
        assert_json_ipc_roundtrip(
            joinpath(ARROW_JSON_DIR, "primitive-empty.json"),
            dir;
            basename="primitive-empty",
        )
        assert_json_ipc_roundtrip(
            joinpath(ARROW_JSON_DIR, "modern-layouts.json"),
            dir;
            basename="modern-layouts",
        )
        assert_json_ipc_roundtrip(
            joinpath(ARROW_JSON_DIR, "canonical-extensions.json"),
            dir;
            basename="canonical-extensions",
        )
        assert_json_ipc_validate(
            joinpath(ARROW_JSON_DIR, "nested.json"),
            dir;
            basename="nested",
        )
        assert_json_ipc_validate(
            joinpath(ARROW_JSON_DIR, "map-non-canonical.json"),
            dir;
            basename="map-non-canonical",
        )
        assert_json_ipc_validate(
            joinpath(ARROW_JSON_DIR, "decimal256.json"),
            dir;
            basename="decimal256",
        )
        assert_json_ipc_validate(
            joinpath(ARROW_JSON_DIR, "union-custom-ids.json"),
            dir;
            basename="union-custom-ids",
        )
    end
end
