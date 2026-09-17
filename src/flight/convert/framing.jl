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

const _IPC_CONTINUATION = UInt32(0xffffffff)

_require_flight_little_endian() =
    Base.ENDIAN_BOM == UInt32(0x04030201) ||
    throw(ArgumentError("Arrow Flight IPC requires a little-endian host"))

@inline function _read_i32(bytes::AbstractVector{UInt8}, pos::Int)
    pos >= 1 && pos + 3 <= length(bytes) ||
        throw(ArgumentError("truncated Arrow IPC framing"))
    return reinterpret(Int32, Vector{UInt8}(@view bytes[pos:(pos + 3)]))[1]
end

@inline function _read_u32(bytes::AbstractVector{UInt8}, pos::Int)
    pos >= 1 && pos + 3 <= length(bytes) ||
        throw(ArgumentError("truncated Arrow IPC framing"))
    return reinterpret(UInt32, Vector{UInt8}(@view bytes[pos:(pos + 3)]))[1]
end

_padding_length(n::Integer, alignment::Integer=DEFAULT_IPC_ALIGNMENT) =
    mod(-Int(n), Int(alignment))

function _write_zeros(io::IO, n::Integer)
    n <= 0 && return 0
    return Base.write(io, zeros(UInt8, Int(n)))
end

function _flight_message_header(data_header::AbstractVector{UInt8})
    isempty(data_header) &&
        throw(ArgumentError("FlightData message is missing the Arrow IPC header"))
    bytes = Vector{UInt8}(data_header)
    return ArrowParent.FB.getrootas(ArrowParent.Meta.Message, bytes, 0)
end

_flight_message_header(message::Protocol.FlightData) =
    _flight_message_header(message.data_header)

function _write_framed_message(
    io::IO,
    data_header::AbstractVector{UInt8},
    data_body::AbstractVector{UInt8},
    alignment::Integer,
)
    _require_flight_little_endian()
    alignment == DEFAULT_IPC_ALIGNMENT || throw(
        ArgumentError("Arrow 3 Flight IPC uses the standard 8-byte alignment"),
    )
    header = Vector{UInt8}(data_header)
    msg = _flight_message_header(header)
    bodylen = Int(msg.bodyLength)
    bodylen >= 0 || throw(ArgumentError("negative Arrow IPC body length"))
    length(data_body) == bodylen || throw(
        ArgumentError(
            "FlightData body length $(length(data_body)) does not match Arrow IPC header $bodylen",
        ),
    )
    metalen = length(header) + _padding_length(length(header), alignment)
    Base.write(io, _IPC_CONTINUATION)
    Base.write(io, Int32(metalen))
    Base.write(io, header)
    _write_zeros(io, metalen - length(header))
    Base.write(io, data_body)
    return nothing
end

function _write_end_marker(io::IO)
    _require_flight_little_endian()
    Base.write(io, _IPC_CONTINUATION)
    Base.write(io, Int32(0))
    return nothing
end

function _split_ipc_stream(bytes::AbstractVector{UInt8})
    _require_flight_little_endian()
    data = Vector{UInt8}(bytes)
    region = ArrowParent.AC.heapregion(data)
    try
        ArrowParent.framemessages(region, ArrowParent.Limits())
    finally
        ArrowParent.AC.release!(region)
    end

    MessagePart = NamedTuple{
        (:header, :body, :kind),
        Tuple{Vector{UInt8},Vector{UInt8},Any},
    }
    messages = MessagePart[]
    pos = 1
    while pos <= length(data)
        length(data) - pos + 1 >= 8 ||
            throw(ArgumentError("truncated Arrow IPC prefix at byte $(pos - 1)"))
        _read_u32(data, pos) == _IPC_CONTINUATION ||
            throw(ArgumentError("missing Arrow IPC continuation marker"))
        metalen = Int(_read_i32(data, pos + 4))
        metalen == 0 && break
        metalen > 0 || throw(ArgumentError("negative Arrow IPC metadata length"))
        metastart = pos + 8
        metaend = metastart + metalen - 1
        metaend <= length(data) || throw(ArgumentError("truncated Arrow IPC metadata"))
        header = Vector{UInt8}(@view data[metastart:metaend])
        msg = _flight_message_header(header)
        bodylen = Int(msg.bodyLength)
        bodylen >= 0 || throw(ArgumentError("negative Arrow IPC body length"))
        bodystart = metaend + 1
        bodyend = bodystart + bodylen - 1
        bodyend <= length(data) || throw(ArgumentError("truncated Arrow IPC body"))
        body = bodylen == 0 ? UInt8[] : Vector{UInt8}(@view data[bodystart:bodyend])
        push!(messages, (header=header, body=body, kind=msg.header))
        pos = bodyend + 1
    end
    return messages
end
