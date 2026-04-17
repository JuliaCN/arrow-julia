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

using JSON3

const FLIGHT_LIVE_PYARROW_GETFLIGHTINFO = raw"""
import json
import pyarrow.flight as fl
import sys

host = sys.argv[1]
port = int(sys.argv[2])
deadline = float(sys.argv[3])
descriptor = fl.FlightDescriptor.for_path(*sys.argv[4:])
client = fl.FlightClient(f"grpc://{host}:{port}")

try:
    info = client.get_flight_info(descriptor, options=fl.FlightCallOptions(timeout=deadline))
    print(json.dumps({
        "ok": True,
        "total_records": info.total_records,
        "total_bytes": info.total_bytes,
    }))
except Exception as exc:  # noqa: BLE001
    print(json.dumps({
        "ok": False,
        "type": type(exc).__name__,
        "message": str(exc),
    }))
"""

function flight_live_pyarrow_getflightinfo(
    host::AbstractString,
    port::Integer,
    descriptor;
    deadline::Real=30.0,
)
    python = FlightTestSupport.pyarrow_flight_python()
    isnothing(python) && return nothing
    output = readchomp(
        Cmd([
            python,
            "-c",
            FLIGHT_LIVE_PYARROW_GETFLIGHTINFO,
            host,
            string(port),
            string(Float64(deadline)),
            descriptor.path...,
        ]),
    )
    return JSON3.read(output)
end

function purehttp2_extension_test_live_listener(fixture)
    protocol = fixture.protocol
    live_fixture = flight_live_fixture(protocol)
    live_service = flight_live_service(protocol, live_fixture)
    server = Arrow.Flight.purehttp2_flight_server(
        live_service;
        host="127.0.0.1",
        port=0,
        request_capacity=4,
        response_capacity=4,
    )

    try
        purehttp2_extension_wait_for_live_server(server.host, server.port)
        @test isopen(server)
        @test server.port > 0
        pyarrow_smoke_ran =
            flight_live_pyarrow_smoke(server.host, server.port, live_fixture)
        @test pyarrow_smoke_ran || isnothing(FlightTestSupport.pyarrow_flight_python())
    finally
        Arrow.Flight.stop!(server; force=true)
        @test !isopen(server)
    end
end

function purehttp2_extension_test_concurrent_listener(fixture)
    Threads.nthreads() > 1 || return nothing
    !isnothing(FlightTestSupport.pyarrow_flight_python()) || return nothing

    protocol = fixture.protocol
    active_handlers = Threads.Atomic{Int}(0)
    max_active_handlers = Ref(0)
    max_active_lock = ReentrantLock()
    descriptor = fixture.descriptor
    info = protocol.FlightInfo(
        UInt8[],
        descriptor,
        protocol.FlightEndpoint[],
        7,
        42,
        false,
        UInt8[],
    )

    service = Arrow.Flight.Service(
        getflightinfo=(ctx, req) -> begin
            @test req.path == descriptor.path
            current_active = Threads.atomic_add!(active_handlers, 1) + 1
            lock(max_active_lock) do
                max_active_handlers[] = max(max_active_handlers[], current_active)
            end
            deadline = time_ns() + 400_000_000
            while time_ns() < deadline
            end
            Threads.atomic_add!(active_handlers, -1)
            return info
        end,
    )

    server = Arrow.Flight.purehttp2_flight_server(
        service;
        host="127.0.0.1",
        port=0,
        request_capacity=4,
        response_capacity=4,
    )

    try
        purehttp2_extension_wait_for_live_server(server.host, server.port)
        warm_info = flight_live_pyarrow_getflightinfo(server.host, server.port, descriptor)
        @test !isnothing(warm_info)
        @test Bool(warm_info["ok"])
        @test Int(warm_info["total_records"]) == info.total_records

        start_gate = Channel{Nothing}(2)
        tasks = [
            Threads.@spawn begin
                take!(start_gate)
                request_info =
                    flight_live_pyarrow_getflightinfo(server.host, server.port, descriptor)
                @test !isnothing(request_info)
                @test Bool(request_info["ok"])
                @test Int(request_info["total_records"]) == info.total_records
                @test Int(request_info["total_bytes"]) == info.total_bytes
            end for _ = 1:2
        ]

        put!(start_gate, nothing)
        put!(start_gate, nothing)
        fetch.(tasks)
        @test max_active_handlers[] >= 2
    finally
        Arrow.Flight.stop!(server; force=true)
    end
end

function purehttp2_extension_test_overload_listener(fixture)
    Threads.nthreads() > 1 || return nothing
    !isnothing(FlightTestSupport.pyarrow_flight_python()) || return nothing

    protocol = fixture.protocol
    descriptor = fixture.descriptor
    info = protocol.FlightInfo(
        UInt8[],
        descriptor,
        protocol.FlightEndpoint[],
        11,
        17,
        false,
        UInt8[],
    )
    handler_started = Channel{Nothing}(1)
    release_handler = Channel{Nothing}(1)

    service = Arrow.Flight.Service(
        getflightinfo=(ctx, req) -> begin
            @test req.path == descriptor.path
            put!(handler_started, nothing)
            take!(release_handler)
            return info
        end,
    )

    server = Arrow.Flight.purehttp2_flight_server(
        service;
        host="127.0.0.1",
        port=0,
        max_active_requests=1,
        request_capacity=4,
        response_capacity=4,
    )

    try
        purehttp2_extension_wait_for_live_server(server.host, server.port)
        @test getfield(server, :request_gate).max_active_requests == 1

        first_task = Threads.@spawn begin
            request_info =
                flight_live_pyarrow_getflightinfo(server.host, server.port, descriptor)
            @test !isnothing(request_info)
            @test Bool(request_info["ok"])
            @test Int(request_info["total_records"]) == info.total_records
            @test Int(request_info["total_bytes"]) == info.total_bytes
        end

        take!(handler_started)

        overloaded_result =
            flight_live_pyarrow_getflightinfo(server.host, server.port, descriptor; deadline=5)
        @test !isnothing(overloaded_result)
        @test !Bool(overloaded_result["ok"])
        @test occursin("resource exhausted", lowercase(String(overloaded_result["message"])))
        @test occursin("active request limit 1 reached", String(overloaded_result["message"]))

        put!(release_handler, nothing)
        fetch(first_task)
    finally
        Arrow.Flight.stop!(server; force=true)
    end
end
