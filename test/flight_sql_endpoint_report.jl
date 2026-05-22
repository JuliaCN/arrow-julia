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

auto_local_flight_sql_endpoint_deps() =
    get(ENV, "ARROW_FLIGHT_DISABLE_AUTO_LOCAL_DEPS", "0") != "1"

function maybe_git_root(path::AbstractString)
    try
        return readchomp(pipeline(`git -C $path rev-parse --show-toplevel`; stderr=devnull))
    catch
        return nothing
    end
end

function flight_sql_endpoint_roots(path::AbstractString)
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

function maybe_locate_dependency(name::AbstractString, env_name::AbstractString)
    if haskey(ENV, env_name)
        candidate = abspath(ENV[env_name])
        isdir(candidate) || error("$env_name does not exist: $candidate")
        return candidate
    end

    auto_local_flight_sql_endpoint_deps() || return nothing

    for root in flight_sql_endpoint_roots(TEST_ROOT)
        for candidate in (
            joinpath(root, ".data", name),
            joinpath(root, name),
            joinpath(root, ".cache", "vendor", name),
            joinpath("/tmp", name),
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

local_grpcserver = maybe_locate_dependency("gRPCServer.jl", "ARROW_FLIGHT_GRPCSERVER_PATH")
!isnothing(local_grpcserver) && strip_temp_source_override!(TEMP_PROJECT, "gRPCServer")
local_purehttp2 = maybe_locate_dependency("PureHTTP2.jl", "ARROW_FLIGHT_PUREHTTP2_PATH")
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

using Arrow
using JSON3
using gRPCServer
using PureHTTP2
using Tables
using Test

ENV["ARROW_FLIGHT_INCLUDE_GRPCSERVER"] = "0"
include(joinpath(TEST_ROOT, "flight", "support.jl"))
include(joinpath(TEST_ROOT, "flight", "live_service_support.jl"))
include(joinpath(TEST_ROOT, "flight", "purehttp2_extension", "support.jl"))

function optional_float_limit(name::AbstractString)
    value = strip(get(ENV, name, ""))
    isempty(value) && return nothing
    parsed = tryparse(Float64, value)
    parsed !== nothing && parsed >= 0 ||
        throw(ArgumentError("$name must be a non-negative number"))
    return parsed
end

function enforce_max(label::AbstractString, value, limit)
    limit === nothing && return nothing
    println("$(label)_max=$(limit)")
    value <= limit || error("$(label)=$(value) exceeded configured max $(limit)")
    return nothing
end

function print_endpoint_report(result)
    println("arrow_flight_sql_endpoint_report")
    println("julia_num_threads=$(Threads.nthreads())")
    println("query_rows=$(Int(result["query_rows"]))")
    println("prepared_rows=$(Int(result["prepared_rows"]))")
    println("ingested_rows=$(Int(result["ingested_rows"]))")
    println("query_bytes=$(Int(result["query_bytes"]))")
    println("prepared_bytes=$(Int(result["prepared_bytes"]))")
    for label in ("query", "prepared", "ingest")
        value = Float64(result["$(label)_ms"])
        println("flight_sql_endpoint_$(label)_ms=$(round(value; digits=3))")
    end
    return nothing
end

function main()
    python = FlightTestSupport.pyarrow_flight_python(
        required_modules=FLIGHT_SQL_ENDPOINT_REQUIRED_MODULES,
    )
    isnothing(python) && error(
        "Flight SQL endpoint report requires Python modules: " *
        join(FLIGHT_SQL_ENDPOINT_REQUIRED_MODULES, ", "),
    )

    protocol = Arrow.Flight.Protocol
    fixture = flight_sql_endpoint_fixture(protocol)
    service = flight_sql_endpoint_service(protocol, fixture)
    server = Arrow.Flight.grpcserver_flight_server(
        service;
        host="127.0.0.1",
        port=0,
        request_capacity=4,
        response_capacity=4,
    )

    try
        purehttp2_extension_wait_for_live_server(server.host, server.port)
        result = flight_live_python_sql_endpoint_smoke(server.host, server.port, fixture)
        result !== nothing || error("Flight SQL endpoint smoke did not run")
        Bool(result["ok"]) || error("Flight SQL endpoint smoke failed")
        print_endpoint_report(result)
        enforce_max(
            "flight_sql_endpoint_query_ms",
            Float64(result["query_ms"]),
            optional_float_limit("ARROW_FLIGHT_SQL_ENDPOINT_MAX_QUERY_MS"),
        )
        enforce_max(
            "flight_sql_endpoint_prepared_ms",
            Float64(result["prepared_ms"]),
            optional_float_limit("ARROW_FLIGHT_SQL_ENDPOINT_MAX_PREPARED_MS"),
        )
        enforce_max(
            "flight_sql_endpoint_ingest_ms",
            Float64(result["ingest_ms"]),
            optional_float_limit("ARROW_FLIGHT_SQL_ENDPOINT_MAX_INGEST_MS"),
        )
    finally
        Arrow.Flight.stop!(server; force=true)
    end

    return nothing
end

main()
