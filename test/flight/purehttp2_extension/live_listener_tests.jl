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

        warm_grpc = gRPCClient.gRPCCURL()
        gRPCClient.grpc_init(warm_grpc)
        try
            warm_client = Arrow.Flight.Client(
                "grpc://$(server.host):$(server.port)";
                grpc=warm_grpc,
                deadline=30,
            )
            warm_info = Arrow.Flight.getflightinfo(warm_client, descriptor)
            @test warm_info.total_records == info.total_records
        finally
            gRPCClient.grpc_shutdown(warm_grpc)
        end

        start_gate = Channel{Nothing}(2)
        tasks = [
            Threads.@spawn begin
                grpc = gRPCClient.gRPCCURL()
                gRPCClient.grpc_init(grpc)
                try
                    client = Arrow.Flight.Client(
                        "grpc://$(server.host):$(server.port)";
                        grpc=grpc,
                        deadline=30,
                    )
                    take!(start_gate)
                    request_info = Arrow.Flight.getflightinfo(client, descriptor)
                    @test request_info.total_records == info.total_records
                    @test request_info.total_bytes == info.total_bytes
                finally
                    gRPCClient.grpc_shutdown(grpc)
                end
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
            grpc = gRPCClient.gRPCCURL()
            gRPCClient.grpc_init(grpc)
            try
                client = Arrow.Flight.Client(
                    "grpc://$(server.host):$(server.port)";
                    grpc=grpc,
                    deadline=30,
                )
                request_info = Arrow.Flight.getflightinfo(client, descriptor)
                @test request_info.total_records == info.total_records
                @test request_info.total_bytes == info.total_bytes
            finally
                gRPCClient.grpc_shutdown(grpc)
            end
        end

        take!(handler_started)

        overloaded_error = let
            grpc = gRPCClient.gRPCCURL()
            gRPCClient.grpc_init(grpc)
            try
                client = Arrow.Flight.Client(
                    "grpc://$(server.host):$(server.port)";
                    grpc=grpc,
                    deadline=30,
                )
                try
                    Arrow.Flight.getflightinfo(client, descriptor)
                    nothing
                catch error
                    error
                end
            finally
                gRPCClient.grpc_shutdown(grpc)
            end
        end

        @test overloaded_error isa gRPCClient.gRPCServiceCallException
        @test overloaded_error.grpc_status == gRPCClient.GRPC_RESOURCE_EXHAUSTED
        @test occursin("active request limit 1 reached", overloaded_error.message)

        put!(release_handler, nothing)
        fetch(first_task)
    finally
        Arrow.Flight.stop!(server; force=true)
    end
end
