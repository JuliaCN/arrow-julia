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

function maybe_git_root(path::AbstractString)
    try
        return readchomp(pipeline(`git -C $path rev-parse --show-toplevel`; stderr=devnull))
    catch
        return nothing
    end
end

function flight_grpcserver_roots(path::AbstractString)
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

function maybe_locate_grpcserver()
    if !haskey(ENV, "ARROW_FLIGHT_GRPCSERVER_PATH")
        return nothing
    end

    candidate = abspath(ENV["ARROW_FLIGHT_GRPCSERVER_PATH"])
    isdir(candidate) || error("ARROW_FLIGHT_GRPCSERVER_PATH does not exist: $candidate")
    return candidate
end

function grpcserver_source_spec()
    project = TOML.parsefile(joinpath(ARROW_ROOT, "Project.toml"))
    sources = get(project, "sources", Dict{String,Any}())
    haskey(sources, "gRPCServer") || error("Arrow Project.toml is missing [sources].gRPCServer")

    source = sources["gRPCServer"]
    if haskey(source, "path")
        path = source["path"]
        grpcserver_path = isabspath(path) ? normpath(path) : normpath(joinpath(ARROW_ROOT, path))
        return (; mode=:develop, spec=PackageSpec(path=grpcserver_path))
    end

    haskey(source, "url") || error("Arrow Project.toml [sources].gRPCServer is missing url")
    kwargs = Dict{Symbol,Any}(:name => "gRPCServer", :url => source["url"])
    haskey(source, "rev") && (kwargs[:rev] = source["rev"])
    return (; mode=:add, spec=PackageSpec(; kwargs...))
end

const TEMP_ENV = mktempdir()
cp(joinpath(TEST_ROOT, "Project.toml"), joinpath(TEMP_ENV, "Project.toml"))

Pkg.activate(TEMP_ENV)
Pkg.develop([PackageSpec(path=ARROW_ROOT), PackageSpec(path=ARROWTYPES_ROOT)])

local_grpcserver = maybe_locate_grpcserver()
if isnothing(local_grpcserver)
    grpcserver_source = grpcserver_source_spec()
    if grpcserver_source.mode == :develop
        Pkg.develop(grpcserver_source.spec)
    else
        Pkg.add(grpcserver_source.spec)
    end
else
    Pkg.develop(PackageSpec(path=local_grpcserver))
end
Pkg.instantiate()

using Test
using Arrow
using Tables

include(joinpath(TEST_ROOT, "flight", "support.jl"))
include(joinpath(TEST_ROOT, "flight", "live_service_support.jl"))
include(joinpath(TEST_ROOT, "flight", "grpcserver_extension.jl"))
