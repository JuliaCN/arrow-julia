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

const EXCLUDED_PUREHTTP2_REQUEST_HEADERS = Set(["content-type", "te"])
const DEFAULT_MAX_ACTIVE_REQUESTS = max(Threads.nthreads() * 8, 32)

mutable struct PureHTTP2RequestGate
    lock::ReentrantLock
    max_active_requests::Int
    active_requests::Int
end

mutable struct PureHTTP2FlightServer
    service::Service
    listener::Sockets.TCPServer
    host::String
    port::Int
    request_capacity::Int
    response_capacity::Int
    request_gate::PureHTTP2RequestGate
    state_lock::ReentrantLock
    accept_task::Union{Nothing,Task}
    connections::Dict{Task,Sockets.TCPSocket}
end

mutable struct PureHTTP2LiveResponseWriter
    conn::PureHTTP2.HTTP2Connection
    io::IO
    stream_id::UInt32
    transport_lock::ReentrantLock
    headers_sent::Bool
    closed::Bool
end

mutable struct PureHTTP2LiveStreamSession
    method::TransportMethodDescriptor
    context::ServerCallContext
    request_messages::Any
    task::Task
    writer::PureHTTP2LiveResponseWriter
    received_bytes::Int
    tail::Vector{UInt8}
    request_closed::Bool
end

function _purehttp2_request_stream(conn::PureHTTP2.HTTP2Connection, stream_id::UInt32)
    stream = PureHTTP2.get_stream(conn, stream_id)
    isnothing(stream) && throw(
        ArgumentError("PureHTTP2 Flight transport could not find stream $(stream_id)"),
    )
    return stream
end

function _purehttp2_request_gate(max_active_requests::Integer)
    max_active_requests > 0 || throw(ArgumentError("max_active_requests must be positive"))
    return PureHTTP2RequestGate(ReentrantLock(), Int(max_active_requests), 0)
end

function _purehttp2_try_acquire_request!(gate::PureHTTP2RequestGate)
    return lock(gate.lock) do
        gate.active_requests < gate.max_active_requests || return false
        gate.active_requests += 1
        return true
    end
end

function _purehttp2_release_request!(gate::PureHTTP2RequestGate)
    lock(gate.lock) do
        gate.active_requests > 0 || return nothing
        gate.active_requests -= 1
    end
    return nothing
end

function _purehttp2_overload_error(gate::PureHTTP2RequestGate)
    return FlightStatusError(
        FLIGHT_STATUS_RESOURCE_EXHAUSTED,
        "Arrow Flight PureHTTP2 server is overloaded: active request limit $(gate.max_active_requests) reached",
    )
end

function purehttp2_call_context(
    stream::PureHTTP2.HTTP2Stream;
    peer::Union{Nothing,String}=nothing,
    secure::Bool=false,
)
    headers = ServerHeaderPair[]
    for (name, value) in stream.request_headers
        startswith(name, ":") && continue
        lowercase_name = lowercase(name)
        lowercase_name in EXCLUDED_PUREHTTP2_REQUEST_HEADERS && continue
        if endswith(lowercase_name, "-bin")
            push!(headers, String(name) => Base64.base64decode(String(value)))
        else
            push!(headers, String(name) => String(value))
        end
    end
    return ServerCallContext(headers=headers, peer=peer, secure=secure)
end

function purehttp2_route_method(service::Service, stream::PureHTTP2.HTTP2Stream)
    request_method = PureHTTP2.get_method(stream)
    request_method == "POST" ||
        throw(ArgumentError("PureHTTP2 Flight transport expects HTTP/2 POST requests"))

    content_type = PureHTTP2.get_content_type(stream)
    grpccontenttype(content_type) || throw(
        ArgumentError(
            "PureHTTP2 Flight transport expects an application/grpc content-type",
        ),
    )

    PureHTTP2.get_header(stream, "te") == "trailers" ||
        throw(ArgumentError("PureHTTP2 Flight transport expects te: trailers"))

    path = PureHTTP2.get_path(stream)
    isnothing(path) && throw(ArgumentError("PureHTTP2 Flight transport requires a :path"))

    method = lookuptransportmethod(service, path)
    isnothing(method) && throw(ArgumentError("Unknown Arrow Flight RPC path: $(path)"))
    return method
end

function _decode_purehttp2_unary_request(
    stream::PureHTTP2.HTTP2Stream,
    method::TransportMethodDescriptor,
)
    messages = _decode_purehttp2_request_messages(stream, method)
    length(messages) == 1 || throw(
        ArgumentError(
            "PureHTTP2 Flight transport expected exactly one gRPC message for $(method.method.name), got $(length(messages))",
        ),
    )
    return only(messages)
end

function _decode_purehttp2_request_messages(
    stream::PureHTTP2.HTTP2Stream,
    method::TransportMethodDescriptor,
)
    return decodegrpcmessages(method.method.request_type, PureHTTP2.peek_data(stream))
end

function _purehttp2_emit_message!(
    frames::Vector{PureHTTP2.Frame},
    conn::PureHTTP2.HTTP2Connection,
    stream_id::UInt32,
    response_message,
)
    append!(frames, _purehttp2_send_data(conn, stream_id, grpcmessage(response_message)))
    return nothing
end

function _purehttp2_send_data(
    conn::PureHTTP2.HTTP2Connection,
    stream_id::UInt32,
    data::AbstractVector{UInt8};
    end_stream::Bool=false,
)
    payload = data isa Vector{UInt8} ? data : Vector{UInt8}(data)
    sender = PureHTTP2.DataSender(conn.flow_controller, conn.remote_settings.max_frame_size)
    frames = PureHTTP2.send_data_frames(sender, stream_id, payload; end_stream=end_stream)
    sent_bytes = sum(Int(frame.header.length) for frame in frames)
    sent_bytes == length(payload) || throw(
        ArgumentError(
            "PureHTTP2 Flight transport could emit only $(sent_bytes) of $(length(payload)) bytes before flow control blocked the stream",
        ),
    )

    stream = PureHTTP2.get_stream(conn, stream_id)
    if !isnothing(stream)
        for frame in frames
            frame_length = Int(frame.header.length)
            if stream.send_window < frame_length
                stream.send_window = frame_length
            end
            PureHTTP2.send_data!(
                stream,
                frame_length,
                PureHTTP2.has_flag(frame.header, PureHTTP2.FrameFlags.END_STREAM),
            )
        end
    end

    return frames
end

function _purehttp2_write_frames_unlocked!(io::IO, frames::Vector{PureHTTP2.Frame})
    isempty(frames) && return nothing
    for frame in frames
        write(io, PureHTTP2.encode_frame(frame))
    end
    return nothing
end

function _purehttp2_write_frames!(
    transport_lock::ReentrantLock,
    io::IO,
    frames::Vector{PureHTTP2.Frame},
)
    lock(transport_lock) do
        _purehttp2_write_frames_unlocked!(io, frames)
    end
    return nothing
end

function _purehttp2_with_transport_lock(f::Function, transport_lock::ReentrantLock, io::IO)
    lock(transport_lock) do
        frames = f()
        _purehttp2_write_frames_unlocked!(io, frames)
    end
    return nothing
end

function _purehttp2_ensure_live_headers!(writer::PureHTTP2LiveResponseWriter)
    writer.headers_sent && return nothing
    _purehttp2_with_transport_lock(writer.transport_lock, writer.io) do
        frames =
            PureHTTP2.send_headers(writer.conn, writer.stream_id, grpcresponseheaders())
        writer.headers_sent = true
        return frames
    end
    return nothing
end

function _purehttp2_emit_live_message!(
    writer::PureHTTP2LiveResponseWriter,
    response_message,
)
    writer.closed && return nothing
    _purehttp2_ensure_live_headers!(writer)
    _purehttp2_with_transport_lock(writer.transport_lock, writer.io) do
        return _purehttp2_send_data(
            writer.conn,
            writer.stream_id,
            grpcmessage(response_message),
        )
    end
    return nothing
end

function _purehttp2_finish_live_response!(writer::PureHTTP2LiveResponseWriter)
    writer.closed && return nothing
    _purehttp2_ensure_live_headers!(writer)
    _purehttp2_with_transport_lock(writer.transport_lock, writer.io) do
        writer.closed = true
        return PureHTTP2.send_trailers(
            writer.conn,
            writer.stream_id,
            grpcresponsetrailers(),
        )
    end
    return nothing
end

function _purehttp2_fail_live_response!(writer::PureHTTP2LiveResponseWriter, error)
    writer.closed && return nothing
    status, message = grpcstatus(error)
    try
        if !writer.headers_sent
            _purehttp2_with_transport_lock(writer.transport_lock, writer.io) do
                frames = PureHTTP2.Frame[]
                append!(
                    frames,
                    PureHTTP2.send_headers(
                        writer.conn,
                        writer.stream_id,
                        grpcresponseheaders(),
                    ),
                )
                append!(
                    frames,
                    PureHTTP2.send_trailers(
                        writer.conn,
                        writer.stream_id,
                        grpcresponsetrailers(status; message=message),
                    ),
                )
                writer.headers_sent = true
                writer.closed = true
                return frames
            end
        else
            _purehttp2_with_transport_lock(writer.transport_lock, writer.io) do
                writer.closed = true
                return PureHTTP2.send_trailers(
                    writer.conn,
                    writer.stream_id,
                    grpcresponsetrailers(status; message=message),
                )
            end
        end
    catch
        lock(writer.transport_lock) do
            writer.closed = true
            rst = PureHTTP2.send_rst_stream(
                writer.conn,
                writer.stream_id,
                UInt32(PureHTTP2.ErrorCode.INTERNAL_ERROR),
            )
            _purehttp2_write_frames_unlocked!(writer.io, PureHTTP2.Frame[rst])
        end
    end
    return nothing
end

function _purehttp2_start_live_session(
    conn::PureHTTP2.HTTP2Connection,
    io::IO,
    service::Service,
    stream_id::UInt32,
    method::TransportMethodDescriptor,
    context::ServerCallContext,
    transport_lock::ReentrantLock;
    request_gate::Union{Nothing,PureHTTP2RequestGate}=nothing,
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
)
    request_messages = Channel{method.method.request_type}(request_capacity)
    writer = PureHTTP2LiveResponseWriter(conn, io, stream_id, transport_lock, false, false)
    task = _transport_spawn() do
        try
            if method.method.response_streaming
                emit =
                    response_message ->
                        _purehttp2_emit_live_message!(writer, response_message)
                transport_bidi_streaming_live_call(
                    service,
                    context,
                    method,
                    request_messages,
                    emit;
                    response_capacity=response_capacity,
                )
            else
                response_message = transport_client_streaming_live_call(
                    service,
                    context,
                    method,
                    request_messages;
                )
                _purehttp2_emit_live_message!(writer, response_message)
            end
            _purehttp2_finish_live_response!(writer)
        catch error
            _purehttp2_fail_live_response!(writer, error)
        finally
            isopen(request_messages) && close(request_messages)
            isnothing(request_gate) || _purehttp2_release_request!(request_gate)
        end
    end
    return PureHTTP2LiveStreamSession(
        method,
        context,
        request_messages,
        task,
        writer,
        0,
        UInt8[],
        false,
    )
end

function _purehttp2_pump_live_request!(
    session::PureHTTP2LiveStreamSession,
    stream::PureHTTP2.HTTP2Stream,
)
    payload = PureHTTP2.peek_data(stream)
    if length(payload) > session.received_bytes
        append!(session.tail, payload[(session.received_bytes + 1):end])
        session.received_bytes = length(payload)
        frames, remainder = splitcompletegrpcmessages(session.tail)
        session.tail = remainder
        for frame in frames
            put!(
                session.request_messages,
                decodegrpcmessage(session.method.method.request_type, frame),
            )
        end
    end

    if stream.end_stream_received && !session.request_closed
        isempty(session.tail) || throw(
            ArgumentError(
                "PureHTTP2 Flight transport received a truncated gRPC frame for $(session.method.method.name)",
            ),
        )
        isopen(session.request_messages) && close(session.request_messages)
        session.request_closed = true
    end
    return nothing
end

_purehttp2_resolve_host(host::Sockets.IPAddr) = host
_purehttp2_resolve_host(host::AbstractString) = Sockets.getaddrinfo(host)

function _purehttp2_bound_listener(listener::Sockets.TCPServer)
    address, port = Sockets.getsockname(listener)
    return string(address), Int(port)
end

function _purehttp2_peer(socket::Sockets.TCPSocket)
    try
        address, port = Sockets.getpeername(socket)
        return "$(address):$(port)"
    catch
        return nothing
    end
end

function _purehttp2_track_connection!(
    server::PureHTTP2FlightServer,
    task::Task,
    socket::Sockets.TCPSocket,
)
    lock(server.state_lock) do
        server.connections[task] = socket
    end
    return nothing
end

function _purehttp2_untrack_connection!(server::PureHTTP2FlightServer, task::Task)
    lock(server.state_lock) do
        pop!(server.connections, task, nothing)
    end
    return nothing
end

function _purehttp2_swallow_connection_error(server::PureHTTP2FlightServer, error)
    !isopen(server.listener) && return error isa EOFError ||
           error isa Base.IOError ||
           error isa PureHTTP2.ConnectionError

    if error isa EOFError
        return true
    end

    if error isa Base.IOError
        return error.code == Libc.ECONNRESET || error.code == Libc.EPIPE
    end

    return false
end

function _purehttp2_connection_task!(
    server::PureHTTP2FlightServer,
    socket::Sockets.TCPSocket,
)
    try
        purehttp2_serve_grpc_connection!(
            server.service,
            socket;
            peer=_purehttp2_peer(socket),
            request_gate=server.request_gate,
            request_capacity=server.request_capacity,
            response_capacity=server.response_capacity,
        )
    catch error
        _purehttp2_swallow_connection_error(server, error) || @warn(
            "PureHTTP2 Flight connection terminated",
            exception=(error, catch_backtrace()),
            peer=_purehttp2_peer(socket),
        )
    finally
        _purehttp2_untrack_connection!(server, current_task())
        isopen(socket) && close(socket)
    end
    return nothing
end

function _purehttp2_accept_loop!(server::PureHTTP2FlightServer)
    while isopen(server.listener)
        socket = try
            accept(server.listener)
        catch error
            if !isopen(server.listener) && error isa Base.IOError
                break
            end
            rethrow()
        end
        task = _transport_spawn() do
            _purehttp2_connection_task!(server, socket)
        end
        _purehttp2_track_connection!(server, task, socket)
    end
    return nothing
end

function purehttp2_serve_grpc_connection!(
    service::Service,
    conn::PureHTTP2.HTTP2Connection,
    io::IO;
    max_frame_size::Int=PureHTTP2.DEFAULT_MAX_FRAME_SIZE,
    peer::Union{Nothing,String}=nothing,
    secure::Bool=false,
    request_gate::Union{Nothing,PureHTTP2RequestGate}=nothing,
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
)
    preface_bytes = read(io, length(PureHTTP2.CONNECTION_PREFACE))
    if length(preface_bytes) < length(PureHTTP2.CONNECTION_PREFACE)
        throw(
            PureHTTP2.ConnectionError(
                PureHTTP2.ErrorCode.PROTOCOL_ERROR,
                "Truncated connection preface",
            ),
        )
    end

    success, preface_response = PureHTTP2.process_preface(conn, preface_bytes)
    success || throw(
        PureHTTP2.ConnectionError(
            PureHTTP2.ErrorCode.PROTOCOL_ERROR,
            "Invalid connection preface",
        ),
    )

    transport_lock = ReentrantLock()
    _purehttp2_write_frames!(transport_lock, io, preface_response)

    live_sessions = Dict{UInt32,PureHTTP2LiveStreamSession}()
    handled_streams = Set{UInt32}()

    while !PureHTTP2.is_closed(conn)
        header_bytes = read(io, PureHTTP2.FRAME_HEADER_SIZE)
        length(header_bytes) < PureHTTP2.FRAME_HEADER_SIZE && break

        header = PureHTTP2.decode_frame_header(header_bytes)
        if header.length > max_frame_size
            throw(
                PureHTTP2.ConnectionError(
                    PureHTTP2.ErrorCode.FRAME_SIZE_ERROR,
                    "Frame size $(header.length) exceeds max $(max_frame_size)",
                ),
            )
        end

        payload = header.length == 0 ? UInt8[] : read(io, Int(header.length))
        if length(payload) < header.length
            throw(
                PureHTTP2.ConnectionError(
                    PureHTTP2.ErrorCode.PROTOCOL_ERROR,
                    "Truncated frame payload: got $(length(payload)) of $(header.length) bytes",
                ),
            )
        end

        frame = PureHTTP2.Frame(header, payload)
        response_frames = lock(transport_lock) do
            PureHTTP2.process_frame(conn, frame)
        end
        _purehttp2_write_frames!(transport_lock, io, response_frames)

        stream_id = header.stream_id
        stream_id == 0 && continue
        stream = PureHTTP2.get_stream(conn, stream_id)
        isnothing(stream) && continue
        stream.headers_complete || continue
        stream_id in handled_streams && continue

        method = purehttp2_route_method(service, stream)

        if method.method.request_streaming
            if !haskey(live_sessions, stream_id)
                context = purehttp2_call_context(stream; peer=peer, secure=secure)
                if !isnothing(request_gate) &&
                   !_purehttp2_try_acquire_request!(request_gate)
                    overload_frames = _purehttp2_status_frames(
                        conn,
                        stream_id,
                        _purehttp2_overload_error(request_gate);
                        include_headers=true,
                    )
                    _purehttp2_write_frames!(transport_lock, io, overload_frames)
                    push!(handled_streams, stream_id)
                    continue
                end
                live_sessions[stream_id] = _purehttp2_start_live_session(
                    conn,
                    io,
                    service,
                    stream_id,
                    method,
                    context,
                    transport_lock;
                    request_gate=request_gate,
                    request_capacity=request_capacity,
                    response_capacity=response_capacity,
                )
            end
            _purehttp2_pump_live_request!(live_sessions[stream_id], stream)
            yield()
        elseif stream.end_stream_received && stream_id ∉ handled_streams
            frames =
                if !isnothing(request_gate) &&
                   !_purehttp2_try_acquire_request!(request_gate)
                    _purehttp2_status_frames(
                        conn,
                        stream_id,
                        _purehttp2_overload_error(request_gate);
                        include_headers=true,
                    )
                else
                    try
                        purehttp2_handle_grpc_stream!(
                            conn,
                            service,
                            stream_id;
                            peer=peer,
                            secure=secure,
                            request_capacity=request_capacity,
                            response_capacity=response_capacity,
                        )
                    finally
                        isnothing(request_gate) || _purehttp2_release_request!(request_gate)
                    end
                end
            _purehttp2_write_frames!(transport_lock, io, frames)
            push!(handled_streams, stream_id)
        end
    end

    for session in values(live_sessions)
        session.request_closed ||
            (isopen(session.request_messages) && close(session.request_messages))
        wait(session.task)
    end
    return nothing
end

function purehttp2_serve_grpc_connection!(service::Service, io::IO; kwargs...)
    conn = PureHTTP2.HTTP2Connection()
    purehttp2_serve_grpc_connection!(service, conn, io; kwargs...)
    return conn
end

function purehttp2_flight_server(
    service::Service;
    host::Union{Sockets.IPAddr,AbstractString}=Sockets.IPv4("127.0.0.1"),
    port::Integer=0,
    max_active_requests::Integer=DEFAULT_MAX_ACTIVE_REQUESTS,
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
)
    listener = Sockets.listen(_purehttp2_resolve_host(host), Int(port))
    bound_host, bound_port = _purehttp2_bound_listener(listener)
    server = PureHTTP2FlightServer(
        service,
        listener,
        bound_host,
        bound_port,
        Int(request_capacity),
        Int(response_capacity),
        _purehttp2_request_gate(max_active_requests),
        ReentrantLock(),
        nothing,
        Dict{Task,Sockets.TCPSocket}(),
    )
    server.accept_task = _transport_spawn() do
        _purehttp2_accept_loop!(server)
    end
    return server
end

function stop!(server::PureHTTP2FlightServer; force::Bool=false)
    isopen(server.listener) && close(server.listener)
    if force
        sockets = lock(server.state_lock) do
            collect(values(server.connections))
        end
        for socket in sockets
            isopen(socket) && close(socket)
        end
    end
    !isnothing(server.accept_task) && wait(server.accept_task)
    tasks = lock(server.state_lock) do
        collect(keys(server.connections))
    end
    for task in tasks
        wait(task)
    end
    return nothing
end

Base.close(server::PureHTTP2FlightServer) = stop!(server)
Base.isopen(server::PureHTTP2FlightServer) = isopen(server.listener)

function _purehttp2_status_frames(
    conn::PureHTTP2.HTTP2Connection,
    stream_id::UInt32,
    error;
    include_headers::Bool,
)
    status, message = grpcstatus(error)
    frames = PureHTTP2.Frame[]
    if include_headers
        append!(frames, PureHTTP2.send_headers(conn, stream_id, grpcresponseheaders()))
    end
    append!(
        frames,
        PureHTTP2.send_trailers(
            conn,
            stream_id,
            grpcresponsetrailers(status; message=message),
        ),
    )
    return frames
end

function purehttp2_handle_grpc_stream!(
    conn::PureHTTP2.HTTP2Connection,
    service::Service,
    stream_id::UInt32;
    peer::Union{Nothing,String}=nothing,
    secure::Bool=false,
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
)
    stream = _purehttp2_request_stream(conn, stream_id)
    method = purehttp2_route_method(service, stream)
    context = purehttp2_call_context(stream; peer=peer, secure=secure)
    frames = PureHTTP2.Frame[]
    headers_sent = false

    try
        append!(frames, PureHTTP2.send_headers(conn, stream_id, grpcresponseheaders()))
        headers_sent = true

        if method.method.request_streaming
            request_messages = _decode_purehttp2_request_messages(stream, method)
            if method.method.response_streaming
                emit =
                    response_message ->
                        _purehttp2_emit_message!(frames, conn, stream_id, response_message)
                transport_bidi_streaming_call(
                    service,
                    context,
                    method,
                    request_messages,
                    emit;
                    request_capacity=request_capacity,
                    response_capacity=response_capacity,
                )
            else
                response_message = transport_client_streaming_call(
                    service,
                    context,
                    method,
                    request_messages;
                    request_capacity=request_capacity,
                )
                _purehttp2_emit_message!(frames, conn, stream_id, response_message)
            end
        else
            request = _decode_purehttp2_unary_request(stream, method)
            if method.method.response_streaming
                emit =
                    response_message ->
                        _purehttp2_emit_message!(frames, conn, stream_id, response_message)
                transport_server_streaming_call(
                    service,
                    context,
                    method,
                    request,
                    emit;
                    response_capacity=response_capacity,
                )
            else
                response_message = transport_unary_call(service, context, method, request)
                _purehttp2_emit_message!(frames, conn, stream_id, response_message)
            end
        end

        append!(frames, PureHTTP2.send_trailers(conn, stream_id, grpcresponsetrailers()))
        return frames
    catch error
        append!(
            frames,
            _purehttp2_status_frames(
                conn,
                stream_id,
                error;
                include_headers=(!headers_sent),
            ),
        )
        return frames
    end
end
