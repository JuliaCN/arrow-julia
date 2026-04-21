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

using Pkg
using TOML

const TEST_ROOT = @__DIR__
const ARROW_ROOT = normpath(joinpath(TEST_ROOT, ".."))
const ARROWTYPES_ROOT = joinpath(ARROW_ROOT, "src", "ArrowTypes")
const GRPCSERVER_UUID = Base.UUID("608c6337-0d7d-447f-bb69-0f5674ee3959")
const PUREHTTP2_UUID = Base.UUID("7d1e1b98-28e7-4969-8df9-5a308937986a")

auto_local_flight_dev_deps() = get(ENV, "ARROW_FLIGHT_DISABLE_AUTO_LOCAL_DEPS", "0") != "1"

function maybe_git_root(path::AbstractString)
    try
        return readchomp(pipeline(`git -C $path rev-parse --show-toplevel`; stderr=devnull))
    catch
        return nothing
    end
end

function flight_purehttp2_roots(path::AbstractString)
    roots = String[]
    current = abspath(path)
    while true
        root = maybe_git_root(current)
        if !isnothing(root) && root ∉ roots
            push!(roots, root)
        end
        parent = dirname(current)
        parent == current && break
        current = parent
    end
    return roots
end

function maybe_locate_purehttp2()
    if haskey(ENV, "ARROW_FLIGHT_PUREHTTP2_PATH")
        candidate = abspath(ENV["ARROW_FLIGHT_PUREHTTP2_PATH"])
        isdir(candidate) || error("ARROW_FLIGHT_PUREHTTP2_PATH does not exist: $candidate")
        return candidate
    end

    auto_local_flight_dev_deps() || return nothing

    for root in flight_purehttp2_roots(TEST_ROOT)
        for candidate in (
            joinpath(root, ".data", "PureHTTP2.jl"),
            joinpath(root, "PureHTTP2.jl"),
            joinpath(root, ".cache", "vendor", "PureHTTP2.jl"),
            "/tmp/PureHTTP2.jl",
        )
            isdir(candidate) && return candidate
        end
    end

    return nothing
end

function maybe_locate_grpcserver()
    if haskey(ENV, "ARROW_FLIGHT_GRPCSERVER_PATH")
        candidate = abspath(ENV["ARROW_FLIGHT_GRPCSERVER_PATH"])
        isdir(candidate) || error("ARROW_FLIGHT_GRPCSERVER_PATH does not exist: $candidate")
        return candidate
    end

    auto_local_flight_dev_deps() || return nothing

    for root in flight_purehttp2_roots(TEST_ROOT)
        for candidate in (
            joinpath(root, ".data", "gRPCServer.jl"),
            joinpath(root, "gRPCServer.jl"),
            joinpath(root, ".cache", "vendor", "gRPCServer.jl"),
            "/tmp/gRPCServer.jl",
        )
            isdir(candidate) && return candidate
        end
    end

    return nothing
end

function strip_temp_source_override!(
    project_path::AbstractString,
    package_name::AbstractString,
)
    project = TOML.parsefile(project_path)
    sources = get(project, "sources", nothing)
    sources isa AbstractDict || return nothing
    haskey(sources, package_name) || return nothing
    delete!(sources, package_name)
    isempty(sources) && delete!(project, "sources")
    open(project_path, "w") do io
        TOML.print(io, project)
    end
    return nothing
end

const TEMP_ENV = mktempdir()
const TEMP_PROJECT = joinpath(TEMP_ENV, "Project.toml")
cp(joinpath(TEST_ROOT, "Project.toml"), TEMP_PROJECT)

local_grpcserver = maybe_locate_grpcserver()
!isnothing(local_grpcserver) && strip_temp_source_override!(TEMP_PROJECT, "gRPCServer")
local_purehttp2 = maybe_locate_purehttp2()
!isnothing(local_purehttp2) && strip_temp_source_override!(TEMP_PROJECT, "PureHTTP2")

Pkg.activate(TEMP_ENV)
dev_packages = PackageSpec[PackageSpec(path=ARROW_ROOT), PackageSpec(path=ARROWTYPES_ROOT)]
if !isnothing(local_grpcserver)
    push!(
        dev_packages,
        PackageSpec(name="gRPCServer", uuid=GRPCSERVER_UUID, path=local_grpcserver),
    )
end
if !isnothing(local_purehttp2)
    push!(
        dev_packages,
        PackageSpec(name="PureHTTP2", uuid=PUREHTTP2_UUID, path=local_purehttp2),
    )
end
Pkg.develop(dev_packages)
Pkg.instantiate()

using Test
using Arrow
using JSON3
using gRPCServer
using PureHTTP2
using Tables

ENV["ARROW_FLIGHT_INCLUDE_GRPCSERVER"] = "0"
include(joinpath(TEST_ROOT, "flight.jl"))
include(joinpath(TEST_ROOT, "flight", "purehttp2_extension.jl"))
