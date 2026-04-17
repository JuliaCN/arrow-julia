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

const EXCLUDED_NGHTTP2_REQUEST_HEADERS = Set(["content-type", "te"])

mutable struct Nghttp2FlightServer
    service::Flight.Service
    listener::Sockets.TCPServer
    host::String
    port::Int
    request_capacity::Int
    response_capacity::Int
    state_lock::ReentrantLock
    accept_task::Union{Nothing,Task}
    connections::Dict{Task,Sockets.TCPSocket}
end

mutable struct Nghttp2FlightStreamState
    id::Int32
    method::String
    path::String
    headers::Vector{Nghttp2Wrapper.NVPair}
    body::Vector{UInt8}
end

Nghttp2FlightStreamState(id::Int32) =
    Nghttp2FlightStreamState(id, "", "", Nghttp2Wrapper.NVPair[], UInt8[])

mutable struct Nghttp2ResponseBodySource
    data::Vector{UInt8}
    offset::Int
    keep_open::Bool
end

Nghttp2ResponseBodySource(data::Vector{UInt8}; keep_open::Bool=false) =
    Nghttp2ResponseBodySource(data, 0, keep_open)

mutable struct Nghttp2FlightConnectionContext
    server::Nghttp2FlightServer
    peer::Union{Nothing,String}
    streams::Dict{Int32,Nghttp2FlightStreamState}
    session_ptr::Ptr{Cvoid}
    io_stream::IO
    lock::ReentrantLock
    response_bodies::Dict{Int32,Nghttp2ResponseBodySource}
    pending_trailers::Dict{Int32,Vector{Tuple{String,String}}}
end

struct Nghttp2DataProvider
    source::Ptr{Cvoid}
    read_callback::Ptr{Cvoid}
end

_nghttp2_resolve_host(host::Sockets.IPAddr) = host
_nghttp2_resolve_host(host::AbstractString) = Sockets.getaddrinfo(host)

function _nghttp2_bound_listener(listener::Sockets.TCPServer)
    address, port = Sockets.getsockname(listener)
    return string(address), Int(port)
end

function _nghttp2_peer(socket::Sockets.TCPSocket)
    try
        address, port = Sockets.getpeername(socket)
        return "$(address):$(port)"
    catch
        return nothing
    end
end

function _nghttp2_track_connection!(
    server::Nghttp2FlightServer,
    task::Task,
    socket::Sockets.TCPSocket,
)
    lock(server.state_lock) do
        server.connections[task] = socket
    end
    return nothing
end

function _nghttp2_untrack_connection!(server::Nghttp2FlightServer, task::Task)
    lock(server.state_lock) do
        pop!(server.connections, task, nothing)
    end
    return nothing
end

function _nghttp2_swallow_connection_error(server::Nghttp2FlightServer, error)
    !isopen(server.listener) && return error isa EOFError ||
           error isa Base.IOError ||
           error isa Nghttp2Wrapper.Nghttp2Error

    if error isa EOFError
        return true
    end

    if error isa Base.IOError
        return error.code == Libc.ECONNRESET || error.code == Libc.EPIPE
    end

    return false
end

function _nghttp2_header(headers::Vector{Nghttp2Wrapper.NVPair}, name::AbstractString)
    needle = lowercase(String(name))
    for header in headers
        if lowercase(String(copy(header.name))) == needle
            return String(copy(header.value))
        end
    end
    return nothing
end

function _nghttp2_call_context(
    headers::Vector{Nghttp2Wrapper.NVPair};
    peer::Union{Nothing,String}=nothing,
    secure::Bool=false,
)
    context_headers = Flight.ServerHeaderPair[]
    for header in headers
        name = String(copy(header.name))
        startswith(name, ":") && continue
        lowercase_name = lowercase(name)
        lowercase_name in EXCLUDED_NGHTTP2_REQUEST_HEADERS && continue
        value = String(copy(header.value))
        if endswith(lowercase_name, "-bin")
            push!(context_headers, name => Base64.base64decode(value))
        else
            push!(context_headers, name => value)
        end
    end
    return Flight.ServerCallContext(headers=context_headers, peer=peer, secure=secure)
end

function _nghttp2_route_method(service::Flight.Service, state::Nghttp2FlightStreamState)
    state.method == "POST" ||
        throw(ArgumentError("Nghttp2 Flight transport expects HTTP/2 POST requests"))

    content_type = _nghttp2_header(state.headers, "content-type")
    Flight.grpccontenttype(content_type) || throw(
        ArgumentError("Nghttp2 Flight transport expects an application/grpc content-type"),
    )

    te_header = _nghttp2_header(state.headers, "te")
    !isnothing(te_header) && lowercase(te_header) == "trailers" ||
        throw(ArgumentError("Nghttp2 Flight transport expects te: trailers"))

    isempty(state.path) &&
        throw(ArgumentError("Nghttp2 Flight transport requires a :path header"))

    method = Flight.lookuptransportmethod(service, state.path)
    isnothing(method) && throw(
        Flight.FlightStatusError(
            Flight.FLIGHT_STATUS_UNIMPLEMENTED,
            "Arrow Flight RPC path $(state.path) is not implemented",
        ),
    )

    method.method.request_streaming && throw(
        Flight.FlightStatusError(
            Flight.FLIGHT_STATUS_UNIMPLEMENTED,
            "Arrow Flight nghttp2 backend does not yet support request-streaming RPC $(method.method.name)",
        ),
    )

    return method
end

function _nghttp2_decode_unary_request(
    state::Nghttp2FlightStreamState,
    method::Flight.TransportMethodDescriptor,
)
    messages = Flight.decodegrpcmessages(method.method.request_type, state.body)
    length(messages) == 1 || throw(
        ArgumentError(
            "Nghttp2 Flight transport expected exactly one gRPC message for $(method.method.name), got $(length(messages))",
        ),
    )
    return only(messages)
end

function _nghttp2_buffer_response_messages!(body::Vector{UInt8}, response_message)
    append!(body, Flight.grpcmessage(response_message))
    return nothing
end

function _nghttp2_collect_grpc_response(
    service::Flight.Service,
    context::Flight.ServerCallContext,
    method::Flight.TransportMethodDescriptor,
    request;
    response_capacity::Integer,
)
    body = UInt8[]
    if method.method.response_streaming
        emit =
            response_message -> _nghttp2_buffer_response_messages!(body, response_message)
        Flight.transport_server_streaming_call(
            service,
            context,
            method,
            request,
            emit;
            response_capacity=response_capacity,
        )
    else
        response_message = Flight.transport_unary_call(service, context, method, request)
        _nghttp2_buffer_response_messages!(body, response_message)
    end
    return (
        headers=Flight.grpcresponseheaders(),
        body=body,
        trailers=Flight.grpcresponsetrailers(),
    )
end

function _nghttp2_error_response(error)
    status, message = Flight.grpcstatus(error)
    return (
        headers=Flight.grpcresponseheaders(),
        body=UInt8[],
        trailers=Flight.grpcresponsetrailers(status; message=message),
    )
end

function _nghttp2_dispatch_request!(
    ctx::Nghttp2FlightConnectionContext,
    state::Nghttp2FlightStreamState,
)
    method = _nghttp2_route_method(ctx.server.service, state)
    context = _nghttp2_call_context(state.headers; peer=ctx.peer, secure=false)
    request = _nghttp2_decode_unary_request(state, method)
    return _nghttp2_collect_grpc_response(
        ctx.server.service,
        context,
        method,
        request;
        response_capacity=ctx.server.response_capacity,
    )
end

function _nghttp2_submit_checked(rv::Integer)
    rv < 0 && Nghttp2Wrapper.check_error(rv)
    return nothing
end

function _nghttp2_session_send_all(session_ptr::Ptr{Cvoid})
    output = UInt8[]
    while true
        nbytes, data_ptr = Nghttp2Wrapper.nghttp2_session_mem_send2(session_ptr)
        nbytes < 0 && Nghttp2Wrapper.check_error(nbytes)
        nbytes == 0 && break
        append!(output, unsafe_wrap(Array, data_ptr, nbytes; own=false))
    end
    return output
end

function _nghttp2_submit_data(
    session_ptr::Ptr{Cvoid},
    flags::Integer,
    stream_id::Integer,
    data_prd::Ptr{Cvoid},
)
    return ccall(
        (:nghttp2_submit_data, Nghttp2Wrapper.nghttp2_jll.libnghttp2),
        Cint,
        (Ptr{Cvoid}, UInt8, Int32, Ptr{Cvoid}),
        session_ptr,
        flags,
        stream_id,
        data_prd,
    )
end

function _nghttp2_submit_headers!(
    session_ptr::Ptr{Cvoid},
    flags::Integer,
    stream_id::Integer,
    headers::Vector{Tuple{String,String}},
)
    nva = Nghttp2Wrapper.NVPair[
        Nghttp2Wrapper.NVPair(name, value) for (name, value) in headers
    ]
    rv = Nghttp2Wrapper.with_nva(nva) do nva_ptr, nvlen
        Nghttp2Wrapper.nghttp2_submit_headers(
            session_ptr,
            flags,
            stream_id,
            Ptr{Cvoid}(C_NULL),
            nva_ptr,
            nvlen,
            Ptr{Cvoid}(C_NULL),
        )
    end
    _nghttp2_submit_checked(rv)
    return nothing
end

function _nghttp2_submit_response!(
    ctx::Nghttp2FlightConnectionContext,
    stream_id::Int32,
    response,
)
    _nghttp2_submit_headers!(
        ctx.session_ptr,
        Nghttp2Wrapper.NGHTTP2_FLAG_NONE,
        stream_id,
        response.headers,
    )

    body_source =
        Nghttp2ResponseBodySource(response.body; keep_open=(!isempty(response.trailers)))
    lock(ctx.lock) do
        ctx.response_bodies[stream_id] = body_source
        isempty(response.trailers) || (ctx.pending_trailers[stream_id] = response.trailers)
    end

    data_prd = Ref(
        Nghttp2DataProvider(
            pointer_from_objref(body_source),
            _nghttp2_data_source_read_cb_ptr(),
        ),
    )
    GC.@preserve body_source data_prd begin
        rv = _nghttp2_submit_data(
            ctx.session_ptr,
            Nghttp2Wrapper.NGHTTP2_FLAG_NONE,
            stream_id,
            Base.unsafe_convert(Ptr{Cvoid}, data_prd),
        )
        _nghttp2_submit_checked(rv)
    end

    return nothing
end

function _nghttp2_ctx(user_data::Ptr{Cvoid})::Nghttp2FlightConnectionContext
    return unsafe_pointer_to_objref(user_data)::Nghttp2FlightConnectionContext
end

function _nghttp2_on_header_cb(
    session_ptr::Ptr{Cvoid},
    frame_ptr::Ptr{Cvoid},
    name_ptr::Ptr{UInt8},
    namelen::Csize_t,
    value_ptr::Ptr{UInt8},
    valuelen::Csize_t,
    flags::UInt8,
    user_data::Ptr{Cvoid},
)::Cint
    try
        ctx = _nghttp2_ctx(user_data)
        stream_id = unsafe_load(Ptr{Int32}(frame_ptr + sizeof(Csize_t)))
        name = unsafe_string(name_ptr, namelen)
        value = unsafe_string(value_ptr, valuelen)
        lock(ctx.lock) do
            state = get!(ctx.streams, stream_id) do
                Nghttp2FlightStreamState(stream_id)
            end
            if name == ":method"
                state.method = value
            elseif name == ":path"
                state.path = value
            else
                push!(state.headers, Nghttp2Wrapper.NVPair(name, value))
            end
        end
    catch
    end
    return Cint(0)
end

function _nghttp2_on_data_chunk_cb(
    session_ptr::Ptr{Cvoid},
    flags::UInt8,
    stream_id::Int32,
    data_ptr::Ptr{UInt8},
    len::Csize_t,
    user_data::Ptr{Cvoid},
)::Cint
    try
        ctx = _nghttp2_ctx(user_data)
        chunk = copy(unsafe_wrap(Array, data_ptr, len; own=false))
        lock(ctx.lock) do
            haskey(ctx.streams, stream_id) && append!(ctx.streams[stream_id].body, chunk)
        end
    catch
    end
    return Cint(0)
end

function _nghttp2_on_stream_close_cb(
    session_ptr::Ptr{Cvoid},
    stream_id::Int32,
    error_code::UInt32,
    user_data::Ptr{Cvoid},
)::Cint
    try
        ctx = _nghttp2_ctx(user_data)
        lock(ctx.lock) do
            pop!(ctx.streams, stream_id, nothing)
            pop!(ctx.response_bodies, stream_id, nothing)
            pop!(ctx.pending_trailers, stream_id, nothing)
        end
    catch
    end
    return Cint(0)
end

function _nghttp2_on_frame_recv_cb(
    session_ptr::Ptr{Cvoid},
    frame_ptr::Ptr{Cvoid},
    user_data::Ptr{Cvoid},
)::Cint
    try
        ctx = _nghttp2_ctx(user_data)
        stream_id = unsafe_load(Ptr{Int32}(frame_ptr + sizeof(Csize_t)))
        frame_type = unsafe_load(Ptr{UInt8}(frame_ptr + sizeof(Csize_t) + 4))
        frame_flags = unsafe_load(Ptr{UInt8}(frame_ptr + sizeof(Csize_t) + 5))

        has_end_stream = (frame_flags & Nghttp2Wrapper.NGHTTP2_FLAG_END_STREAM) != 0
        is_headers = frame_type == Nghttp2Wrapper.NGHTTP2_HEADERS
        is_data = frame_type == Nghttp2Wrapper.NGHTTP2_DATA

        if has_end_stream && (is_headers || is_data) && stream_id > 0
            state = lock(ctx.lock) do
                pop!(ctx.streams, stream_id, nothing)
            end
            isnothing(state) && return Cint(0)

            response = try
                _nghttp2_dispatch_request!(ctx, state)
            catch error
                _nghttp2_error_response(error)
            end
            _nghttp2_submit_response!(ctx, stream_id, response)
        end
    catch error
        @warn "Nghttp2 Flight frame receive callback failed" exception=(
            error,
            catch_backtrace(),
        )
    end
    return Cint(0)
end

function _nghttp2_data_source_read_cb(
    session_ptr::Ptr{Cvoid},
    stream_id::Int32,
    buf::Ptr{UInt8},
    len::Csize_t,
    data_flags::Ptr{UInt32},
    source::Ptr{Cvoid},
    user_data::Ptr{Cvoid},
)::Cssize_t
    try
        src_ptr = unsafe_load(Ptr{Ptr{Cvoid}}(source))
        body_source = unsafe_pointer_to_objref(src_ptr)::Nghttp2ResponseBodySource

        remaining = length(body_source.data) - body_source.offset
        to_copy = min(Int(len), remaining)
        if to_copy > 0
            GC.@preserve body_source unsafe_copyto!(
                buf,
                pointer(body_source.data, body_source.offset + 1),
                to_copy,
            )
            body_source.offset += to_copy
        end

        if body_source.offset >= length(body_source.data)
            flags = unsafe_load(data_flags) | Nghttp2Wrapper.NGHTTP2_DATA_FLAG_EOF
            body_source.keep_open &&
                (flags |= Nghttp2Wrapper.NGHTTP2_DATA_FLAG_NO_END_STREAM)
            unsafe_store!(data_flags, flags)

            ctx = _nghttp2_ctx(user_data)
            trailers = nothing
            lock(ctx.lock) do
                pop!(ctx.response_bodies, stream_id, nothing)
                trailers = pop!(ctx.pending_trailers, stream_id, nothing)
            end
            if trailers isa Vector{Tuple{String,String}}
                _nghttp2_submit_headers!(
                    session_ptr,
                    Nghttp2Wrapper.NGHTTP2_FLAG_END_STREAM,
                    stream_id,
                    trailers,
                )
            end
        end

        return Cssize_t(to_copy)
    catch
        return Cssize_t(Nghttp2Wrapper.NGHTTP2_ERR_CALLBACK_FAILURE)
    end
end

_nghttp2_on_header_cb_ptr() = @cfunction(
    _nghttp2_on_header_cb,
    Cint,
    (Ptr{Cvoid}, Ptr{Cvoid}, Ptr{UInt8}, Csize_t, Ptr{UInt8}, Csize_t, UInt8, Ptr{Cvoid}),
)
_nghttp2_on_data_chunk_cb_ptr() = @cfunction(
    _nghttp2_on_data_chunk_cb,
    Cint,
    (Ptr{Cvoid}, UInt8, Int32, Ptr{UInt8}, Csize_t, Ptr{Cvoid}),
)
_nghttp2_on_stream_close_cb_ptr() =
    @cfunction(_nghttp2_on_stream_close_cb, Cint, (Ptr{Cvoid}, Int32, UInt32, Ptr{Cvoid}),)
_nghttp2_on_frame_recv_cb_ptr() =
    @cfunction(_nghttp2_on_frame_recv_cb, Cint, (Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}),)
_nghttp2_data_source_read_cb_ptr() = @cfunction(
    _nghttp2_data_source_read_cb,
    Cssize_t,
    (Ptr{Cvoid}, Int32, Ptr{UInt8}, Csize_t, Ptr{UInt32}, Ptr{Cvoid}, Ptr{Cvoid}),
)

function _nghttp2_read_tcp_chunk!(io, buf::Vector{UInt8})
    try
        buf[1] = read(io, UInt8)
        available = bytesavailable(io)
        if available > 0
            extra = min(available, length(buf) - 1)
            unsafe_read(io, pointer(buf, 2), UInt(extra))
            return 1 + Int(extra)
        end
        return 1
    catch
        return 0
    end
end

function _nghttp2_connection_loop!(server::Nghttp2FlightServer, socket::Sockets.TCPSocket)
    ctx = Nghttp2FlightConnectionContext(
        server,
        _nghttp2_peer(socket),
        Dict{Int32,Nghttp2FlightStreamState}(),
        C_NULL,
        socket,
        ReentrantLock(),
        Dict{Int32,Nghttp2ResponseBodySource}(),
        Dict{Int32,Vector{Tuple{String,String}}}(),
    )

    callbacks = Nghttp2Wrapper.Callbacks()
    Nghttp2Wrapper.nghttp2_session_callbacks_set_on_header_callback(
        callbacks.ptr,
        _nghttp2_on_header_cb_ptr(),
    )
    Nghttp2Wrapper.nghttp2_session_callbacks_set_on_data_chunk_recv_callback(
        callbacks.ptr,
        _nghttp2_on_data_chunk_cb_ptr(),
    )
    Nghttp2Wrapper.nghttp2_session_callbacks_set_on_frame_recv_callback(
        callbacks.ptr,
        _nghttp2_on_frame_recv_cb_ptr(),
    )
    Nghttp2Wrapper.nghttp2_session_callbacks_set_on_stream_close_callback(
        callbacks.ptr,
        _nghttp2_on_stream_close_cb_ptr(),
    )

    rv, session_ptr =
        Nghttp2Wrapper.nghttp2_session_server_new(callbacks.ptr, pointer_from_objref(ctx))
    _nghttp2_submit_checked(rv)
    ctx.session_ptr = session_ptr

    GC.@preserve ctx begin
        try
            _nghttp2_submit_checked(
                Nghttp2Wrapper.nghttp2_submit_settings(
                    session_ptr,
                    Nghttp2Wrapper.NGHTTP2_FLAG_NONE,
                    Ptr{Nghttp2Wrapper.Nghttp2SettingsEntry}(C_NULL),
                    0,
                ),
            )
            outdata = _nghttp2_session_send_all(session_ptr)
            isempty(outdata) || write(socket, outdata)

            buf = Vector{UInt8}(undef, 65536)
            while isopen(server.listener) && isopen(socket)
                nbytes = _nghttp2_read_tcp_chunk!(socket, buf)
                nbytes == 0 && break
                _nghttp2_submit_checked(
                    Nghttp2Wrapper.nghttp2_session_mem_recv2(session_ptr, buf[1:nbytes]),
                )
                outdata = _nghttp2_session_send_all(session_ptr)
                isempty(outdata) || write(socket, outdata)
            end
        finally
            Nghttp2Wrapper.nghttp2_session_del(session_ptr)
            close(callbacks)
        end
    end
    return nothing
end

function _nghttp2_connection_task!(server::Nghttp2FlightServer, socket::Sockets.TCPSocket)
    try
        _nghttp2_connection_loop!(server, socket)
    catch error
        _nghttp2_swallow_connection_error(server, error) || @warn(
            "Nghttp2 Flight connection terminated",
            exception=(error, catch_backtrace()),
            peer=_nghttp2_peer(socket),
        )
    finally
        _nghttp2_untrack_connection!(server, current_task())
        isopen(socket) && close(socket)
    end
    return nothing
end

function _nghttp2_accept_loop!(server::Nghttp2FlightServer)
    while isopen(server.listener)
        socket = try
            accept(server.listener)
        catch error
            if !isopen(server.listener) && error isa Base.IOError
                break
            end
            rethrow()
        end
        task = @async _nghttp2_connection_task!(server, socket)
        _nghttp2_track_connection!(server, task, socket)
    end
    return nothing
end

function Flight.nghttp2_flight_server(
    service::Flight.Service;
    host::Union{Sockets.IPAddr,AbstractString}=Sockets.IPv4("127.0.0.1"),
    port::Integer=0,
    request_capacity::Integer=Flight.DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=Flight.DEFAULT_STREAM_BUFFER,
)
    listener = Sockets.listen(_nghttp2_resolve_host(host), Int(port))
    bound_host, bound_port = _nghttp2_bound_listener(listener)
    server = Nghttp2FlightServer(
        service,
        listener,
        bound_host,
        bound_port,
        Int(request_capacity),
        Int(response_capacity),
        ReentrantLock(),
        nothing,
        Dict{Task,Sockets.TCPSocket}(),
    )
    server.accept_task = @async _nghttp2_accept_loop!(server)
    return server
end

function Flight.stop!(server::Nghttp2FlightServer; force::Bool=false)
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

Base.close(server::Nghttp2FlightServer) = Flight.stop!(server)
Base.isopen(server::Nghttp2FlightServer) = isopen(server.listener)
