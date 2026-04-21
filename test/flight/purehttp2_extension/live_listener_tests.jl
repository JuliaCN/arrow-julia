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

const FLIGHT_LIVE_PYARROW_LARGE_DOEXCHANGE = raw"""
import pyarrow as pa
import pyarrow.flight as fl
import sys

host = sys.argv[1]
port = int(sys.argv[2])
warm_timeout = float(sys.argv[3])
large_timeout = float(sys.argv[4])
large_bytes = int(sys.argv[5])
descriptor = fl.FlightDescriptor.for_path(*sys.argv[6:])
client = fl.FlightClient(f"grpc://{host}:{port}")

schema = pa.schema(
    [
        ("doc_id", pa.string()),
        ("vector_score", pa.float64()),
    ],
    metadata={b"wendao.schema_version": b"v1"},
)

def exchange_once(size: int, timeout: float) -> list[str]:
    payload = "x" * size
    table = pa.Table.from_arrays(
        [pa.array([payload]), pa.array([1.0])],
        schema=schema,
    )
    writer, reader = client.do_exchange(
        descriptor,
        fl.FlightCallOptions(timeout=timeout),
    )
    writer.begin(table.schema)
    writer.write_table(table)
    writer.done_writing()
    response = reader.read_all()
    return response.column(0).to_pylist()

warm_rows = exchange_once(16, warm_timeout)
assert warm_rows == ["1"]
large_rows = exchange_once(large_bytes, large_timeout)
assert large_rows == ["1"]
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

function flight_live_pyarrow_large_doexchange(
    host::AbstractString,
    port::Integer,
    descriptor;
    warm_timeout::Real=60.0,
    large_timeout::Real=20.0,
    large_bytes::Integer=65_536,
)
    python = FlightTestSupport.pyarrow_flight_python()
    isnothing(python) && return false
    run(
        Cmd([
            python,
            "-c",
            FLIGHT_LIVE_PYARROW_LARGE_DOEXCHANGE,
            host,
            string(port),
            string(Float64(warm_timeout)),
            string(Float64(large_timeout)),
            string(Int(large_bytes)),
            descriptor.path...,
        ]),
    )
    return true
end
function purehttp2_extension_test_live_listener(fixture)
    protocol = fixture.protocol
    live_fixture = flight_live_fixture(protocol)
    live_service = flight_live_service(protocol, live_fixture)
    server = Arrow.Flight.grpcserver_flight_server(
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
            try
                sleep(0.4)
                return info
            finally
                Threads.atomic_add!(active_handlers, -1)
            end
        end,
    )

    server = Arrow.Flight.grpcserver_flight_server(
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
                request_info = flight_live_pyarrow_getflightinfo(
                    server.host,
                    server.port,
                    descriptor,
                )
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

    server = Arrow.Flight.grpcserver_flight_server(
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
        @test getfield(server, :request_gate).active_requests[] == 0

        first_task = Threads.@spawn begin
            request_info =
                flight_live_pyarrow_getflightinfo(server.host, server.port, descriptor)
            @test !isnothing(request_info)
            @test Bool(request_info["ok"])
            @test Int(request_info["total_records"]) == info.total_records
            @test Int(request_info["total_bytes"]) == info.total_bytes
        end

        take!(handler_started)

        overloaded_result = flight_live_pyarrow_getflightinfo(
            server.host,
            server.port,
            descriptor;
            deadline=5,
        )
        @test !isnothing(overloaded_result)
        @test !Bool(overloaded_result["ok"])
        @test occursin(
            "deadline exceeded",
            lowercase(String(overloaded_result["message"])),
        )
        @test occursin(
            "timeout error",
            lowercase(String(overloaded_result["message"])),
        )

        put!(release_handler, nothing)
        fetch(first_task)
        @test getfield(server, :request_gate).active_requests[] == 0
    finally
        Arrow.Flight.stop!(server; force=true)
    end
end

function purehttp2_extension_test_large_pyarrow_doexchange_listener(fixture)
    !isnothing(FlightTestSupport.pyarrow_flight_python()) || return nothing

    descriptor = Arrow.Flight.pathdescriptor(("rerank",))
    service = Arrow.Flight.streamservice(
        stream -> begin
            rows = 0
            for batch in stream
                rows += length(Tables.columntable(batch).doc_id)
            end
            return (doc_id=[string(rows)], vector_score=[1.0])
        end;
        descriptor=descriptor,
    )

    server = Arrow.Flight.grpcserver_flight_server(
        service;
        host="127.0.0.1",
        port=0,
        request_capacity=8,
        response_capacity=8,
    )

    try
        purehttp2_extension_wait_for_live_server(server.host, server.port)
        @test flight_live_pyarrow_large_doexchange(server.host, server.port, descriptor)
    finally
        Arrow.Flight.stop!(server; force=true)
    end
end

function purehttp2_extension_test_concurrent_large_doget_listener()
    !isnothing(FlightTestSupport.pyarrow_flight_python()) || return nothing

    protocol = Arrow.Flight.Protocol
    fixture = flight_live_transport_fixture(
        protocol;
        batch_count=2,
        rows_per_batch=256,
        payload_bytes=4_096,
    )
    active_handlers = Threads.Atomic{Int}(0)
    max_active_handlers = Ref(0)
    max_active_lock = ReentrantLock()

    service = Arrow.Flight.Service(
        getflightinfo=(ctx, req) -> begin
            @test req.path == fixture.descriptor.path
            return fixture.info
        end,
        doget=(ctx, req, response) -> begin
            @test req.ticket == fixture.ticket.ticket
            current_active = Threads.atomic_add!(active_handlers, 1) + 1
            lock(max_active_lock) do
                max_active_handlers[] = max(max_active_handlers[], current_active)
            end
            try
                sleep(0.05)
                Arrow.Flight.putflightdata!(
                    response,
                    Tables.partitioner(fixture.batches);
                    descriptor=fixture.descriptor,
                    metadata=fixture.dataset_metadata,
                    colmetadata=fixture.dataset_colmetadata,
                    close=true,
                )
                return :doget_ok
            finally
                Threads.atomic_add!(active_handlers, -1)
            end
        end,
    )

    server = Arrow.Flight.grpcserver_flight_server(
        service;
        host="127.0.0.1",
        port=0,
        max_active_requests=2,
        request_capacity=8,
        response_capacity=8,
    )

    try
        purehttp2_extension_wait_for_live_server(server.host, server.port)
        result = flight_live_pyarrow_concurrent_doget(
            server.host,
            server.port,
            fixture;
            concurrent_clients=2,
            requests_per_client=2,
        )
        @test !isnothing(result)
        @test Int(result["concurrent_clients"]) == 2
        @test Int(result["requests_per_client"]) == 2
        @test Int(result["total_requests"]) == 4
        @test Int(result["wall_ns"]) > 0
        @test Int(result["request_median_ns"]) > 0
        @test Int(result["request_p95_ns"]) >= Int(result["request_median_ns"])
        @test Int(result["request_p99_ns"]) >= Int(result["request_p95_ns"])
        @test Int(result["request_max_ns"]) >= Int(result["request_p99_ns"])
        @test max_active_handlers[] >= 2
        @test getfield(server, :request_gate).active_requests[] == 0
    finally
        Arrow.Flight.stop!(server; force=true)
    end
end
