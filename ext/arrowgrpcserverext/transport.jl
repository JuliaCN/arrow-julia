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

mutable struct GRPCServerRequestGate
    max_active_requests::Int
    active_requests::Int
    lock::ReentrantLock
end

function GRPCServerRequestGate(max_active_requests::Integer)
    max_active_requests > 0 ||
        throw(ArgumentError("max_active_requests must be positive"))
    return GRPCServerRequestGate(Int(max_active_requests), 0, ReentrantLock())
end

struct GRPCServerFlightService
    service::Flight.Service
    request_capacity::Int
    response_capacity::Int
    request_gate::GRPCServerRequestGate
end

mutable struct GRPCServerFlightServer
    service::Flight.Service
    server::gRPCServer.GRPCServer
    host::String
    port::Int
    request_capacity::Int
    response_capacity::Int
    request_gate::GRPCServerRequestGate
    accept_task::Union{Nothing,Task}
end

function _grpcserver_bind_address(host::AbstractString)
    if host == "0.0.0.0" || isempty(host)
        return Sockets.IPv4(0)
    elseif host == "::"
        return Sockets.IPv6(0)
    end

    try
        return parse(Sockets.IPv4, host)
    catch
        try
            return parse(Sockets.IPv6, host)
        catch
            return Sockets.getaddrinfo(String(host))
        end
    end
end

function _grpcserver_bind_port(host::AbstractString, port::Integer)
    0 <= port <= 65535 || throw(ArgumentError("port must be between 0 and 65535"))
    port != 0 && return Int(port)

    socket = Sockets.listen(_grpcserver_bind_address(host), 0)
    _, actual_port = getsockname(socket)
    close(socket)
    return Int(actual_port)
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

function _try_acquire_request!(gate::GRPCServerRequestGate)
    return lock(gate.lock) do
        gate.active_requests >= gate.max_active_requests && return false
        gate.active_requests += 1
        return true
    end
end

function _release_request!(gate::GRPCServerRequestGate)
    lock(gate.lock) do
        gate.active_requests > 0 && (gate.active_requests -= 1)
    end
    return nothing
end

function _throw_request_limit_error(gate::GRPCServerRequestGate)
    throw(
        gRPCServer.GRPCError(
            gRPCServer.StatusCode.RESOURCE_EXHAUSTED,
            "active request limit $(gate.max_active_requests) reached",
        ),
    )
end

function Flight.grpcserver_flight_server(
    service::Flight.Service;
    host::AbstractString="127.0.0.1",
    port::Integer=8815,
    max_active_requests::Integer=DEFAULT_MAX_ACTIVE_REQUESTS,
    request_capacity::Integer=Flight.DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=Flight.DEFAULT_STREAM_BUFFER,
)
    max_active_requests > 0 || throw(ArgumentError("max_active_requests must be positive"))
    request_capacity > 0 || throw(ArgumentError("request_capacity must be positive"))
    response_capacity > 0 || throw(ArgumentError("response_capacity must be positive"))

    actual_host = String(host)
    actual_port = _grpcserver_bind_port(actual_host, port)
    request_gate = GRPCServerRequestGate(Int(max_active_requests))
    configured_service = GRPCServerFlightService(
        service,
        Int(request_capacity),
        Int(response_capacity),
        request_gate,
    )
    grpc_server = gRPCServer.GRPCServer(
        actual_host,
        actual_port;
        max_concurrent_requests=Int(max_active_requests),
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
    return GRPCServerFlightServer(
        service,
        grpc_server,
        actual_host,
        actual_port,
        Int(request_capacity),
        Int(response_capacity),
        request_gate,
        grpc_server.accept_task,
    )
end

function Flight.stop!(server::GRPCServerFlightServer; force::Bool=false)
    gRPCServer.stop!(server.server; force=force)
    server.accept_task = nothing
    return server
end

Base.close(server::GRPCServerFlightServer) = Flight.stop!(server)
Base.isopen(server::GRPCServerFlightServer) =
    server.server.status != gRPCServer.ServerStatus.STOPPED
