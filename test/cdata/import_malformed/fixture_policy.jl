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

@testset "malformed C Data import fixture policy" begin
    expected_files = sort([
        "child_pointers.jl",
        "dictionary_peers.jl",
        "fixture_policy.jl",
        "format_strings.jl",
        "metadata_bytes.jl",
        "released_inputs.jl",
        "support.jl",
    ])
    actual_files = sort(basename.(filter(endswith(".jl"), readdir(@__DIR__; join=true))))
    @test actual_files == expected_files

    entrypoint = read(joinpath(dirname(@__DIR__), "import_malformed_tests.jl"), String)
    for file in setdiff(expected_files, ["support.jl"])
        @test occursin(file, entrypoint)
    end

    for file in setdiff(expected_files, ["fixture_policy.jl", "support.jl"])
        source = read(joinpath(@__DIR__, file), String)
        @test occursin("@testset", source)
        @test !occursin("Random.", source)
        @test !occursin("rand(", source)
    end
end
