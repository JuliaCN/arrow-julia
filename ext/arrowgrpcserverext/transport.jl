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

struct GRPCServerFlightService
    service::Flight.Service
    request_capacity::Int
    response_capacity::Int
    secure::Bool
    runtime::Union{Nothing,Flight.FlightServerRuntime}
end

function GRPCServerFlightService(
    service::Flight.Service,
    request_capacity::Integer,
    response_capacity::Integer,
    secure::Bool=false,
    runtime::Union{Nothing,Flight.FlightServerRuntime}=nothing,
)
    return GRPCServerFlightService(
        service,
        Int(request_capacity),
        Int(response_capacity),
        secure,
        runtime,
    )
end

mutable struct GRPCServerFlightServer
    service::Flight.Service
    server::gRPCServer.GRPCServer
    host::String
    port::Int
    request_capacity::Int
    response_capacity::Int
    runtime::Flight.FlightServerRuntime
end

function _grpcserver_construct_port(port::Integer)
    0 <= port <= 65535 || throw(ArgumentError("port must be between 0 and 65535"))
    # gRPCServer's current constructor rejects zero even though its HTTP
    # backend and HTTP.port(::GRPCServer) support an ephemeral listener.  Use
    # the upstream test-suite bridge until the constructor accepts port=0;
    # unlike pre-binding a socket, this leaves no close/rebind race window.
    # Upstream API request: https://github.com/JuliaIO/gRPCServer.jl/issues/4
    return port == 0 ? 1 : Int(port)
end

function _wait_for_grpcserver_listener(
    server::gRPCServer.GRPCServer;
    timeout_sec::Real=5.0,
    settle_sec::Real=0.1,
)
    deadline = time() + timeout_sec

    while time() < deadline
        if server.status == gRPCServer.ServerStatus.RUNNING
            sleep(settle_sec)
            return nothing
        end
        if !isnothing(server.last_error)
            throw(server.last_error)
        end
        sleep(0.05)
    end

    error("gRPCServer Flight listener did not reach RUNNING state before timeout")
end

function Flight.grpcserver_flight_server(
    service::Flight.Service;
    host::AbstractString="127.0.0.1",
    port::Integer=8815,
    max_concurrent_requests::Integer=1024,
    request_capacity::Integer=Flight.DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=Flight.DEFAULT_STREAM_BUFFER,
    max_inflight_bytes::Integer=2 * 1024 * 1024 * 1024,
    call_reservation_bytes::Union{Nothing,Integer}=nothing,
    cleanup_grace_seconds::Real=1.0,
    configure_server::Function=server -> nothing,
    server_kwargs...,
)
    max_concurrent_requests > 0 ||
        throw(ArgumentError("max_concurrent_requests must be positive"))
    request_capacity > 0 || throw(ArgumentError("request_capacity must be positive"))
    response_capacity > 0 || throw(ArgumentError("response_capacity must be positive"))

    actual_host = String(host)
    construct_port = _grpcserver_construct_port(port)
    grpc_server = gRPCServer.GRPCServer(
        actual_host,
        construct_port;
        max_concurrent_requests=Int(max_concurrent_requests),
        server_kwargs...,
    )
    port == 0 && (grpc_server.port = 0)
    reservation = if isnothing(call_reservation_bytes)
        Int64(grpc_server.config.max_receive_message_length) +
        Int64(grpc_server.config.max_send_message_length)
    else
        Int64(call_reservation_bytes)
    end
    runtime = Flight.FlightServerRuntime(
        max_active_calls=Int(max_concurrent_requests),
        max_reserved_bytes=max_inflight_bytes,
        call_reservation_bytes=reservation,
        cleanup_grace_seconds=cleanup_grace_seconds,
    )
    configure_server(grpc_server)
    configured_service = GRPCServerFlightService(
        service,
        Int(request_capacity),
        Int(response_capacity),
        !isnothing(grpc_server.config.tls),
        runtime,
    )
    gRPCServer.register!(grpc_server, configured_service)
    gRPCServer.start!(grpc_server)
    try
        _wait_for_grpcserver_listener(grpc_server)
    catch error
        grpc_server.status in
        (gRPCServer.ServerStatus.RUNNING, gRPCServer.ServerStatus.DRAINING) &&
            gRPCServer.stop!(grpc_server; force=true)
        rethrow(error)
    end
    actual_port = Int(gRPCServer.HTTP.port(grpc_server))
    return GRPCServerFlightServer(
        service,
        grpc_server,
        actual_host,
        actual_port,
        Int(request_capacity),
        Int(response_capacity),
        runtime,
    )
end

Flight.flight_server_metrics(server::GRPCServerFlightServer) =
    Flight.flight_server_metrics(server.runtime)

function Flight.stop!(server::GRPCServerFlightServer; force::Bool=false, timeout::Real=0.0)
    gRPCServer.stop!(server.server; force=force, timeout=Float64(timeout))
    return server
end

Base.close(server::GRPCServerFlightServer) = Flight.stop!(server)
Base.isopen(server::GRPCServerFlightServer) =
    server.server.status != gRPCServer.ServerStatus.STOPPED
