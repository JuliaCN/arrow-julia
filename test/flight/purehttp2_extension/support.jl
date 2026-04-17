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

using Base64
using PureHTTP2
using Tables
using Sockets

purehttp2_extension_app_metadata_string(payload::AbstractVector{UInt8}) =
    String(copy(payload))
purehttp2_extension_app_metadata_strings(payloads) =
    purehttp2_extension_app_metadata_string.(payloads)

function purehttp2_extension_fixture()
    protocol = Arrow.Flight.Protocol
    seen_unary_context = Ref{Union{Nothing,Arrow.Flight.ServerCallContext}}(nothing)
    seen_unary_request = Ref{Union{Nothing,protocol.FlightDescriptor}}(nothing)
    seen_stream_context = Ref{Union{Nothing,Arrow.Flight.ServerCallContext}}(nothing)
    seen_stream_request = Ref{Union{Nothing,protocol.Ticket}}(nothing)
    seen_handshake_context = Ref{Union{Nothing,Arrow.Flight.ServerCallContext}}(nothing)
    seen_handshake_messages = Ref{Any}(nothing)
    seen_doput_context = Ref{Union{Nothing,Arrow.Flight.ServerCallContext}}(nothing)
    seen_doput_batches = Ref{Any}(nothing)
    seen_exchange_context = Ref{Union{Nothing,Arrow.Flight.ServerCallContext}}(nothing)
    seen_exchange_batches = Ref{Any}(nothing)

    descriptor_type = protocol.var"FlightDescriptor.DescriptorType"
    descriptor =
        protocol.FlightDescriptor(descriptor_type.PATH, UInt8[], ["server", "dataset"])
    ticket = protocol.Ticket(b"ticket-1")
    endpoint = protocol.FlightEndpoint(ticket, protocol.Location[], nothing, UInt8[])
    handshake_token = b"secret:test"
    handshake_requests = [
        protocol.HandshakeRequest(UInt64(0), b"test"),
        protocol.HandshakeRequest(UInt64(0), b"p4ssw0rd"),
    ]
    dataset_metadata = Dict("dataset" => "native")
    dataset_colmetadata = Dict(:name => Dict("lang" => "en"))
    dataset_app_metadata = ["put:0", "put:1"]
    messages = Arrow.Flight.flightdata(
        Tables.partitioner((
            (id=Int64[1, 2], name=["one", "two"]),
            (id=Int64[3], name=["three"]),
        ));
        descriptor=descriptor,
        metadata=dataset_metadata,
        colmetadata=dataset_colmetadata,
        app_metadata=dataset_app_metadata,
    )
    exchange_metadata = Dict("dataset" => "exchange")
    exchange_colmetadata = Dict(:name => Dict("lang" => "exchange"))
    exchange_app_metadata = ["exchange:0"]
    exchange_messages = Arrow.Flight.flightdata(
        Tables.partitioner(((id=Int64[10], name=["ten"]),));
        descriptor=descriptor,
        metadata=exchange_metadata,
        colmetadata=exchange_colmetadata,
        app_metadata=exchange_app_metadata,
    )

    service = Arrow.Flight.Service(
        handshake=(ctx, request, response) -> begin
            incoming = collect(request)
            seen_handshake_context[] = ctx
            seen_handshake_messages[] = incoming
            put!(response, protocol.HandshakeResponse(UInt64(0), handshake_token))
            close(response)
            return :handshake_ok
        end,
        getflightinfo=(ctx, req) -> begin
            seen_unary_context[] = ctx
            seen_unary_request[] = req
            return protocol.FlightInfo(UInt8[], req, [endpoint], 7, 42, false, UInt8[])
        end,
        doget=(ctx, ticket, response) -> begin
            seen_stream_context[] = ctx
            seen_stream_request[] = ticket
            put!(
                response,
                protocol.FlightData(nothing, UInt8[0x01], UInt8[0x02], UInt8[0x03]),
            )
            put!(
                response,
                protocol.FlightData(nothing, UInt8[0x04], UInt8[0x05], UInt8[0x06]),
            )
            close(response)
            return :doget_ok
        end,
        doput=(ctx, request, response) -> begin
            incoming = collect(Arrow.Flight.stream(request; include_app_metadata=true))
            seen_doput_context[] = ctx
            seen_doput_batches[] = incoming
            put!(response, protocol.PutResult(b"stored"))
            close(response)
            return :doput_ok
        end,
        doexchange=(ctx, request, response) -> begin
            incoming = collect(Arrow.Flight.stream(request; include_app_metadata=true))
            seen_exchange_context[] = ctx
            seen_exchange_batches[] = incoming
            Arrow.Flight.putflightdata!(
                response,
                Arrow.Flight.withappmetadata(
                    Tables.partitioner(getproperty.(incoming, :table));
                    app_metadata=getproperty.(incoming, :app_metadata),
                );
                close=true,
            )
            return :doexchange_ok
        end,
    )

    failure_service = Arrow.Flight.Service(
        getflightinfo=(ctx, req) ->
            throw(Arrow.Flight.FlightStatusError(Int32(16), "bad token")),
    )

    return (;
        protocol,
        service,
        failure_service,
        descriptor,
        ticket,
        dataset_metadata,
        dataset_colmetadata,
        handshake_token,
        handshake_requests,
        messages,
        exchange_metadata,
        exchange_colmetadata,
        dataset_app_metadata,
        exchange_messages,
        exchange_app_metadata,
        unary_headers=Tuple{String,String}[
            ("authorization", "Bearer purehttp2"),
            ("auth-token-bin", base64encode(UInt8[0x01, 0x02])),
        ],
        seen_unary_context,
        seen_unary_request,
        seen_stream_context,
        seen_stream_request,
        seen_handshake_context,
        seen_handshake_messages,
        seen_doput_context,
        seen_doput_batches,
        seen_exchange_context,
        seen_exchange_batches,
    )
end

function purehttp2_extension_request(
    rpc_name::AbstractString,
    request;
    headers::Vector{Tuple{String,String}}=Tuple{String,String}[],
)
    return purehttp2_extension_messages_request(rpc_name, (request,); headers=headers)
end

function _purehttp2_extension_chunk_payload(
    payload::Vector{UInt8},
    chunk_size::Union{Nothing,Integer},
)
    isnothing(chunk_size) && return [payload]
    chunk_size > 0 || throw(ArgumentError("chunk_size must be positive"))
    chunks = Vector{Vector{UInt8}}()
    index = firstindex(payload)
    final = lastindex(payload)
    while index <= final
        stop = min(index + chunk_size - 1, final)
        push!(chunks, payload[index:stop])
        index = stop + 1
    end
    return chunks
end

function purehttp2_extension_messages_request(
    rpc_name::AbstractString,
    requests;
    headers::Vector{Tuple{String,String}}=Tuple{String,String}[],
    chunk_size::Union{Nothing,Integer}=nothing,
)
    conn = PureHTTP2.HTTP2Connection()
    stream = PureHTTP2.create_stream(conn, UInt32(1))
    stream.request_headers = Tuple{String,String}[
        (":method", "POST"),
        (":path", "/arrow.flight.protocol.FlightService/$(rpc_name)"),
        (":authority", "localhost"),
        ("content-type", "application/grpc+proto"),
        ("te", "trailers"),
    ]
    append!(stream.request_headers, headers)

    payloads = Vector{Vector{UInt8}}()
    for request in requests
        append!(
            payloads,
            _purehttp2_extension_chunk_payload(
                Arrow.Flight.grpcmessage(request),
                chunk_size,
            ),
        )
    end

    PureHTTP2.receive_headers!(stream, isempty(payloads))
    for (index, payload) in enumerate(payloads)
        PureHTTP2.receive_data!(stream, payload, index == length(payloads))
    end
    return conn, stream
end

function purehttp2_extension_decode_headers(frames::Vector{PureHTTP2.Frame})
    decoder = PureHTTP2.HPACKDecoder(PureHTTP2.DEFAULT_HEADER_TABLE_SIZE)
    blocks = Vector{Vector{Tuple{String,String}}}()
    block_payload = UInt8[]

    for frame in frames
        if frame.header.frame_type == PureHTTP2.FrameType.HEADERS
            empty!(block_payload)
            append!(block_payload, frame.payload)
            if PureHTTP2.has_flag(frame.header, PureHTTP2.FrameFlags.END_HEADERS)
                push!(blocks, PureHTTP2.decode_headers(decoder, copy(block_payload)))
                empty!(block_payload)
            end
        elseif frame.header.frame_type == PureHTTP2.FrameType.CONTINUATION
            append!(block_payload, frame.payload)
            if PureHTTP2.has_flag(frame.header, PureHTTP2.FrameFlags.END_HEADERS)
                push!(blocks, PureHTTP2.decode_headers(decoder, copy(block_payload)))
                empty!(block_payload)
            end
        end
    end

    return blocks
end

function purehttp2_extension_decode_data(
    ::Type{T},
    frames::Vector{PureHTTP2.Frame},
) where {T}
    payload = UInt8[]
    for frame in frames
        frame.header.frame_type == PureHTTP2.FrameType.DATA || continue
        append!(payload, frame.payload)
    end
    return Arrow.Flight.decodegrpcmessages(T, payload)
end

mutable struct PureHTTP2ExtensionPairedIO <: IO
    incoming::Base.BufferStream
    outgoing::Base.BufferStream
end

Base.read(io::PureHTTP2ExtensionPairedIO, n::Int) = read(io.incoming, n)
Base.write(io::PureHTTP2ExtensionPairedIO, x::UInt8) = write(io.outgoing, x)
Base.unsafe_write(io::PureHTTP2ExtensionPairedIO, p::Ptr{UInt8}, n::UInt) =
    unsafe_write(io.outgoing, p, n)
Base.close(io::PureHTTP2ExtensionPairedIO) = (close(io.incoming); close(io.outgoing))

function purehttp2_extension_live_port()
    socket = Sockets.listen(parse(Sockets.IPv4, "127.0.0.1"), 0)
    _, port = getsockname(socket)
    close(socket)
    return Int(port)
end

function purehttp2_extension_wait_for_live_server(host::AbstractString, port::Integer)
    deadline = time() + 5.0
    last_error = nothing
    while time() < deadline
        try
            socket = Sockets.connect(Sockets.getaddrinfo(host), port)
            purehttp2_extension_write_client_preface!(socket)
            header_bytes = read(socket, PureHTTP2.FRAME_HEADER_SIZE)
            length(header_bytes) == PureHTTP2.FRAME_HEADER_SIZE ||
                error("PureHTTP2 readiness probe did not receive a full frame header")
            header = PureHTTP2.decode_frame_header(header_bytes)
            header.length == 0 || read(socket, Int(header.length))
            close(socket)
            return nothing
        catch error
            last_error = error
        end
        sleep(0.05)
    end
    detail =
        isnothing(last_error) ? "unknown readiness failure" : sprint(showerror, last_error)
    error("PureHTTP2 Flight test server did not become ready on $(host):$(port): $(detail)")
end

function purehttp2_extension_header_block(
    rpc_name::AbstractString;
    headers::Vector{Tuple{String,String}}=Tuple{String,String}[],
)
    encoder = PureHTTP2.HPACKEncoder()
    request_headers = Tuple{String,String}[
        (":method", "POST"),
        (":path", "/arrow.flight.protocol.FlightService/$(rpc_name)"),
        (":scheme", "http"),
        (":authority", "localhost"),
        ("content-type", "application/grpc+proto"),
        ("te", "trailers"),
    ]
    append!(request_headers, headers)
    return PureHTTP2.encode_headers(encoder, request_headers)
end

function purehttp2_extension_write_client_preface!(io::IO)
    write(io, PureHTTP2.CONNECTION_PREFACE)
    write(io, PureHTTP2.encode_frame(PureHTTP2.settings_frame(Tuple{UInt16,UInt32}[])))
    return nothing
end

function purehttp2_extension_collect_stream_frames(
    io::IO,
    stream_id::UInt32,
    count::Integer;
    timeout_secs::Real=2.0,
)
    seen = Ref(Tuple{UInt32,UInt8,UInt8}[])
    task = @async begin
        frames = PureHTTP2.Frame[]
        while length(frames) < count
            header_bytes = read(io, PureHTTP2.FRAME_HEADER_SIZE)
            length(header_bytes) < PureHTTP2.FRAME_HEADER_SIZE &&
                error("Unexpected EOF while reading HTTP/2 frame header")
            header = PureHTTP2.decode_frame_header(header_bytes)
            payload = header.length == 0 ? UInt8[] : read(io, Int(header.length))
            length(payload) < header.length &&
                error("Unexpected EOF while reading HTTP/2 frame payload")
            frame = PureHTTP2.Frame(header, payload)
            push!(
                seen[],
                (frame.header.stream_id, frame.header.frame_type, frame.header.flags),
            )
            frame.header.stream_id == stream_id && push!(frames, frame)
        end
        return frames
    end

    deadline = time() + timeout_secs
    while !istaskdone(task) && time() < deadline
        sleep(0.01)
    end
    if !istaskdone(task)
        close(io)
        error(
            "Timed out waiting for $(count) frame(s) on stream $(stream_id); " *
            "seen=$(seen[])",
        )
    end
    return fetch(task)
end
