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

const GRPC_ENVELOPE_HEADER_SIZE = 5
const GRPC_CONTENT_TYPE = "application/grpc+proto"
const GRPC_STATUS_OK = Int32(0)
const GRPC_STATUS_INTERNAL = Int32(13)

function grpccontenttype(content_type::Union{Nothing,AbstractString})
    isnothing(content_type) && return false
    normalized = strip(lowercase(String(content_type)))
    return normalized == "application/grpc" ||
           startswith(normalized, "application/grpc+") ||
           startswith(normalized, "application/grpc;")
end

function _grpc_message_length(frame::AbstractVector{UInt8})
    length(frame) >= GRPC_ENVELOPE_HEADER_SIZE ||
        throw(ArgumentError("gRPC message frame must be at least 5 bytes"))

    return (UInt32(frame[2]) << 24) | (UInt32(frame[3]) << 16) | (UInt32(frame[4]) << 8) |
           UInt32(frame[5])
end

function _grpc_message_payload(frame::AbstractVector{UInt8})
    frame[1] == 0x00 ||
        throw(ArgumentError("Compressed gRPC messages are not supported in Arrow.Flight"))

    payload_length = Int(_grpc_message_length(frame))
    frame_length = GRPC_ENVELOPE_HEADER_SIZE + payload_length
    length(frame) == frame_length || throw(
        ArgumentError(
            "gRPC message frame length $(length(frame)) does not match encoded payload length $(frame_length)",
        ),
    )

    return Vector{UInt8}(view(frame, (GRPC_ENVELOPE_HEADER_SIZE + 1):frame_length))
end

function splitgrpcmessages(payload::AbstractVector{UInt8})
    frames, tail = splitcompletegrpcmessages(payload)
    isempty(tail) ||
        throw(ArgumentError("Trailing gRPC payload is shorter than one message envelope"))
    return frames
end

function splitcompletegrpcmessages(payload::AbstractVector{UInt8})
    isempty(payload) && return Vector{UInt8}[], UInt8[]

    frames = Vector{Vector{UInt8}}()
    offset = firstindex(payload)
    final = lastindex(payload)

    while offset <= final
        remaining = final - offset + 1
        if remaining < GRPC_ENVELOPE_HEADER_SIZE
            return frames, Vector{UInt8}(view(payload, offset:final))
        end

        message_length = Int(
            (UInt32(payload[offset + 1]) << 24) | (UInt32(payload[offset + 2]) << 16) |
            (UInt32(payload[offset + 3]) << 8) | UInt32(payload[offset + 4]),
        )
        frame_length = GRPC_ENVELOPE_HEADER_SIZE + message_length
        frame_end = offset + frame_length - 1
        if frame_end > final
            return frames, Vector{UInt8}(view(payload, offset:final))
        end

        push!(frames, Vector{UInt8}(view(payload, offset:frame_end)))
        offset = frame_end + 1
    end

    return frames, UInt8[]
end

function _protobuf_bytes(message)
    io = IOBuffer()
    encoder = ProtoBuf.ProtoEncoder(io)
    ProtoBuf.encode(encoder, message)
    return take!(io)
end

function grpcmessage(message)
    payload =
        message isa AbstractVector{UInt8} ? Vector{UInt8}(message) :
        _protobuf_bytes(message)
    payload_length = length(payload)
    payload_length <= typemax(UInt32) ||
        throw(ArgumentError("gRPC message payload length exceeds UInt32 framing limit"))

    framed = Vector{UInt8}(undef, GRPC_ENVELOPE_HEADER_SIZE + payload_length)
    framed[1] = 0x00
    framed[2] = UInt8((payload_length >> 24) & 0xff)
    framed[3] = UInt8((payload_length >> 16) & 0xff)
    framed[4] = UInt8((payload_length >> 8) & 0xff)
    framed[5] = UInt8(payload_length & 0xff)
    copyto!(framed, GRPC_ENVELOPE_HEADER_SIZE + 1, payload, 1, payload_length)
    return framed
end

function decodegrpcmessage(::Type{Vector{UInt8}}, frame::AbstractVector{UInt8})
    return _grpc_message_payload(frame)
end

function decodegrpcmessage(::Type{T}, frame::AbstractVector{UInt8}) where {T}
    payload = _grpc_message_payload(frame)
    decoder = ProtoBuf.ProtoDecoder(IOBuffer(payload))
    return ProtoBuf.decode(decoder, T)
end

function decodegrpcmessages(::Type{T}, payload::AbstractVector{UInt8}) where {T}
    return [decodegrpcmessage(T, frame) for frame in splitgrpcmessages(payload)]
end

function grpcresponseheaders(;
    content_type::AbstractString=GRPC_CONTENT_TYPE,
    metadata::Vector{Tuple{String,String}}=Tuple{String,String}[],
)
    headers =
        Tuple{String,String}[(":status", "200"), ("content-type", String(content_type))]
    append!(headers, metadata)
    return headers
end

function grpcresponsetrailers(
    status::Integer=GRPC_STATUS_OK;
    message::Union{Nothing,AbstractString}=nothing,
    metadata::Vector{Tuple{String,String}}=Tuple{String,String}[],
)
    trailers = Tuple{String,String}[("grpc-status", string(status))]
    if !isnothing(message) && !isempty(message)
        push!(trailers, ("grpc-message", String(message)))
    end
    append!(trailers, metadata)
    return trailers
end

grpcstatus(error::FlightStatusError) = (error.code, error.message)
grpcstatus(error) = (GRPC_STATUS_INTERNAL, sprint(showerror, error))
