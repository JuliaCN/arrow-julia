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

using Sockets

const GRPCSERVER_COMPARE_STREAM_BUFFER = 16

function grpcserver_compare_live_port()
    socket = Sockets.listen(parse(Sockets.IPv4, "127.0.0.1"), 0)
    _, port = getsockname(socket)
    close(socket)
    return Int(port)
end

function grpcserver_compare_wait_for_live_server(host::AbstractString, port::Integer)
    deadline = time() + 5.0
    last_error = nothing
    while time() < deadline
        try
            socket = Sockets.connect(parse(Sockets.IPv4, host), port)
            close(socket)
            return nothing
        catch error
            last_error = error
        end
        sleep(0.05)
    end
    detail =
        isnothing(last_error) ? "unknown readiness failure" : sprint(showerror, last_error)
    error("gRPCServer compare server did not become ready on $(host):$(port): $(detail)")
end

function grpcserver_compare_method_type(method::Arrow.Flight.MethodDescriptor)
    if method.request_streaming
        return method.response_streaming ? gRPCServer.MethodType.BIDI_STREAMING :
               gRPCServer.MethodType.CLIENT_STREAMING
    end
    return method.response_streaming ? gRPCServer.MethodType.SERVER_STREAMING :
           gRPCServer.MethodType.UNARY
end

function grpcserver_compare_call_context(context::gRPCServer.ServerContext)
    headers = Arrow.Flight.HeaderPair[
        String(name) => (value isa String ? value : Vector{UInt8}(value)) for
        (name, value) in pairs(context.metadata)
    ]
    peer = string(context.peer.address, ":", context.peer.port)
    return Arrow.Flight.ServerCallContext(
        headers=headers,
        peer=peer,
        secure=(context.peer.certificate !== nothing),
    )
end

function grpcserver_compare_rethrow_status(error::Arrow.Flight.FlightStatusError)
    throw(gRPCServer.GRPCError(gRPCServer.StatusCode.T(error.code), error.message))
end

function grpcserver_compare_transport_context(context::gRPCServer.ServerContext)
    return grpcserver_compare_call_context(context)
end

function grpcserver_compare_on_status_error(error::Arrow.Flight.FlightStatusError)
    return grpcserver_compare_rethrow_status(error)
end

function grpcserver_compare_handler(
    service::Arrow.Flight.Service,
    method::Arrow.Flight.TransportMethodDescriptor,
)
    if !method.method.request_streaming && !method.method.response_streaming
        return (context, request) -> Arrow.Flight.transport_unary_call(
            service,
            grpcserver_compare_transport_context(context),
            method,
            request;
            on_status_error=grpcserver_compare_on_status_error,
        )
    elseif !method.method.request_streaming && method.method.response_streaming
        return (context, request, stream) -> begin
            Arrow.Flight.transport_server_streaming_call(
                service,
                grpcserver_compare_transport_context(context),
                method,
                request,
                message -> gRPCServer.send!(stream, message);
                response_capacity=GRPCSERVER_COMPARE_STREAM_BUFFER,
                on_status_error=grpcserver_compare_on_status_error,
            )
            gRPCServer.close!(stream)
            return nothing
        end
    elseif method.method.request_streaming && !method.method.response_streaming
        return (context, stream) -> Arrow.Flight.transport_client_streaming_call(
            service,
            grpcserver_compare_transport_context(context),
            method,
            stream;
            request_capacity=GRPCSERVER_COMPARE_STREAM_BUFFER,
            on_status_error=grpcserver_compare_on_status_error,
        )
    end

    return (context, stream) -> begin
        Arrow.Flight.transport_bidi_streaming_call(
            service,
            grpcserver_compare_transport_context(context),
            method,
            stream,
            message -> gRPCServer.send!(stream, message);
            request_capacity=GRPCSERVER_COMPARE_STREAM_BUFFER,
            response_capacity=GRPCSERVER_COMPARE_STREAM_BUFFER,
            on_status_error=grpcserver_compare_on_status_error,
        )
        gRPCServer.close!(stream)
        return nothing
    end
end

function gRPCServer.service_descriptor(service::Arrow.Flight.Service)
    descriptor = Arrow.Flight.transportdescriptor(service)
    registry = gRPCServer.get_type_registry()
    methods = Dict{String,gRPCServer.MethodDescriptor}()
    for method in descriptor.methods
        registry[method.request_type_name] = method.method.request_type
        registry[method.response_type_name] = method.method.response_type
        methods[method.method.name] = gRPCServer.MethodDescriptor(
            method.method.name,
            grpcserver_compare_method_type(method.method),
            method.request_type_name,
            method.response_type_name,
            grpcserver_compare_handler(service, method),
        )
    end
    return gRPCServer.ServiceDescriptor(descriptor.name, methods, nothing)
end

function grpcserver_compare_transport()
    return flight_live_transport_backend(
        backend=:grpcserver,
        start_server=service -> begin
            host = "127.0.0.1"
            port = grpcserver_compare_live_port()
            server = gRPCServer.GRPCServer(host, port)
            gRPCServer.register!(server, service)
            gRPCServer.start!(server)
            return (server=server, host=host, port=port)
        end,
        wait_for_server=server ->
            grpcserver_compare_wait_for_live_server(server.host, server.port),
        stop_server=server -> gRPCServer.stop!(server.server; force=true),
        endpoint=server -> (server.host, server.port),
    )
end

function purehttp2_compare_transport()
    return flight_live_transport_backend(
        backend=:purehttp2,
        start_server=service -> Arrow.Flight.purehttp2_flight_server(
            service;
            host="127.0.0.1",
            port=0,
            request_capacity=4,
            response_capacity=4,
        ),
        wait_for_server=server ->
            purehttp2_extension_wait_for_live_server(server.host, server.port),
        stop_server=server -> Arrow.Flight.stop!(server; force=true),
        endpoint=server -> (server.host, server.port),
    )
end

function transport_compare_service(protocol, fixture, data_messages)
    return Arrow.Flight.Service(
        getflightinfo=(ctx, req) -> begin
            @test req.path == fixture.descriptor.path
            return fixture.info
        end,
        doget=(ctx, req, response) -> begin
            @test req.ticket == fixture.ticket.ticket
            for message in data_messages
                put!(response, message)
            end
            close(response)
            return :doget_ok
        end,
    )
end

function _transport_compare_message_bytes(messages)
    total = 0
    for message in messages
        total += length(Arrow.Flight.grpcmessage(message))
    end
    return total
end

function transport_compare_backend_attempt(
    protocol,
    transport,
    fixture,
    data_messages;
    iterations::Integer,
)
    response_bytes = _transport_compare_message_bytes(data_messages)
    service = transport_compare_service(protocol, fixture, data_messages)
    server = transport.start_server(service)
    try
        transport.wait_for_server(server)
        host, port = transport.endpoint(server)
        return FlightTestSupport.with_test_grpc_handle() do grpc
            client = Arrow.Flight.Client("grpc://$(host):$(port)"; grpc=grpc, deadline=60)
            info = Arrow.Flight.getflightinfo(client, fixture.descriptor)
            if info.total_records != fixture.total_records
                return (
                    backend=transport.backend,
                    supported=false,
                    detail="GetFlightInfo total_records mismatch: expected $(fixture.total_records), got $(info.total_records)",
                    metric=nothing,
                )
            end

            doget_messages = _flight_live_transport_doget(client, info.endpoint[1].ticket)
            if length(doget_messages) != length(data_messages)
                return (
                    backend=transport.backend,
                    supported=false,
                    detail="DoGet message count mismatch: expected $(length(data_messages)), got $(length(doget_messages))",
                    metric=nothing,
                )
            end
            doget_table = Arrow.Flight.table(doget_messages; schema=info)
            if length(doget_table.id) != fixture.total_records
                return (
                    backend=transport.backend,
                    supported=false,
                    detail="DoGet table length mismatch: expected $(fixture.total_records), got $(length(doget_table.id))",
                    metric=nothing,
                )
            end
            if isempty(doget_table.payload) || first(doget_table.payload) != fixture.payload_value
                return (
                    backend=transport.backend,
                    supported=false,
                    detail="DoGet payload validation failed on the first row",
                    metric=nothing,
                )
            end

            return (
                backend=transport.backend,
                supported=true,
                detail=nothing,
                metric=_flight_live_transport_measure(
                    transport.backend,
                    :doget;
                    request_bytes=0,
                    response_bytes=response_bytes,
                    iterations=iterations,
                    run_once=() ->
                        _flight_live_transport_doget(client, info.endpoint[1].ticket),
                ),
            )
        end
    finally
        transport.stop_server(server)
    end
end

function transport_compare_test_large_doget(;
    iterations::Integer=3,
    rows_per_batch::Integer=512,
    payload_bytes::Integer=4_096,
)
    fixture = flight_live_transport_fixture(
        Arrow.Flight.Protocol;
        batch_count=1,
        rows_per_batch=rows_per_batch,
        payload_bytes=payload_bytes,
    )
    data_messages = fixture.messages[2:end]
    @test length(data_messages) == 1

    purehttp2_result = transport_compare_backend_attempt(
        Arrow.Flight.Protocol,
        purehttp2_compare_transport(),
        fixture,
        data_messages;
        iterations=iterations,
    )
    @test purehttp2_result.supported
    purehttp2_metric = purehttp2_result.metric

    grpcserver_result = transport_compare_backend_attempt(
        Arrow.Flight.Protocol,
        grpcserver_compare_transport(),
        fixture,
        data_messages;
        iterations=iterations,
    )
    @test purehttp2_metric.total_bytes >= 2 * 1024 * 1024
    @test purehttp2_metric.median_ns > 0
    metrics = [purehttp2_metric]
    if grpcserver_result.supported
        grpcserver_metric = grpcserver_result.metric
        @test !isnothing(grpcserver_metric)
        @test grpcserver_metric.median_ns > 0
        @test purehttp2_metric.total_bytes == grpcserver_metric.total_bytes
        push!(metrics, grpcserver_metric)
        flight_live_transport_print_metrics(stdout, metrics)
        comparison = flight_live_transport_print_comparison(
            stdout,
            purehttp2_metric,
            grpcserver_metric,
        )
        return (
            metrics=metrics,
            comparison=comparison,
            grpcserver_supported=true,
            grpcserver_detail=nothing,
        )
    end

    flight_live_transport_print_metrics(stdout, metrics)
    println(
        stdout,
        "grpcserver doget unsupported reason=\"$(grpcserver_result.detail)\"",
    )
    return (
        metrics=metrics,
        comparison=nothing,
        grpcserver_supported=false,
        grpcserver_detail=grpcserver_result.detail,
    )
end
