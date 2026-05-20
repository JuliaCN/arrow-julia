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
using Arrow
using ArrowTypes
using Tables
using Dates
using PooledArrays
using TimeZones
using UUIDs
using Sockets
using CategoricalArrays
using DataAPI
using FilePathsBase
using DataFrames
using JSON3
using OffsetArrays
import Random: randstring
using TestSetExtensions: ExtendedTestSet

# this formulation tests the loaded ArrowTypes, even if it's not the dev version
# within the mono-repo
include(joinpath(dirname(pathof(ArrowTypes)), "../test/tests.jl"))

include(joinpath(@__DIR__, "testtables.jl"))
include(joinpath(@__DIR__, "testappend.jl"))
include(joinpath(@__DIR__, "integrationtest.jl"))
include(joinpath(@__DIR__, "dates.jl"))
include(joinpath(@__DIR__, "cdata.jl"))
include(joinpath(@__DIR__, "flight.jl"))

struct CustomStruct
    x::Int
    y::Float64
    z::String
end

struct CustomStruct2{sym}
    x::Int
end

module EnumRoundtripModule
@enum RankingStrategy lexical = 1 semantic = 2 hybrid = 3
end

module WideEnumRoundtripModule
@enum WideRanking::UInt64 tiny = 1 colossal = 0xffffffffffffffff
end

@testset ExtendedTestSet "Arrow" begin
    include(joinpath(@__DIR__, "runtests", "roundtrip_integration.jl"))

    @testset "misc" begin
        include(joinpath(@__DIR__, "runtests", "misc_core_layouts.jl"))
        include(joinpath(@__DIR__, "runtests", "misc_validation_views.jl"))
        include(joinpath(@__DIR__, "runtests", "misc_validation_nodes.jl"))
        include(joinpath(@__DIR__, "runtests", "misc_validation_unions.jl"))
        include(joinpath(@__DIR__, "runtests", "misc_issue_roundtrips.jl"))
        include(joinpath(@__DIR__, "runtests", "misc_extensions.jl"))
        include(joinpath(@__DIR__, "runtests", "misc_tensor_display.jl"))
        include(joinpath(@__DIR__, "runtests", "misc_table_issues.jl"))
    end # @testset "misc"

    include(joinpath(@__DIR__, "runtests", "metadata.jl"))
end
