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

mutable struct FlightBodyBuffer <: IO
    data::Vector{UInt8}
    pos::Int
end

FlightBodyBuffer(n::Integer) = FlightBodyBuffer(Vector{UInt8}(undef, n), 1)

@inline function _ensureflightbodycapacity(io::FlightBodyBuffer, n::Integer)
    io.pos + n - 1 <= length(io.data) || throw(
        ArgumentError("FlightData body buffer exceeded expected Arrow IPC body length"),
    )
    return nothing
end

@inline function Base.unsafe_write(io::FlightBodyBuffer, p::Ptr{UInt8}, n::UInt)
    len = Int(n)
    _ensureflightbodycapacity(io, len)
    data = io.data
    GC.@preserve data unsafe_copyto!(pointer(data, io.pos), p, len)
    io.pos += len
    return len
end

@inline function Base.write(io::FlightBodyBuffer, x::UInt8)
    _ensureflightbodycapacity(io, 1)
    @inbounds io.data[io.pos] = x
    io.pos += 1
    return 1
end

@inline function Base.write(io::FlightBodyBuffer, data::StridedVector{UInt8})
    GC.@preserve data begin
        return Base.unsafe_write(io, pointer(data), UInt(length(data)))
    end
end

@inline function Base.write(io::FlightBodyBuffer, data::Vector{UInt8})
    GC.@preserve data begin
        return Base.unsafe_write(io, pointer(data), UInt(length(data)))
    end
end

function ArrowParent.writezeros(io::FlightBodyBuffer, n::Integer)
    n <= 0 && return 0
    _ensureflightbodycapacity(io, n)
    fill!(view(io.data, (io.pos):(io.pos + n - 1)), 0x00)
    io.pos += n
    return n
end

function ArrowParent.writearray(
    io::FlightBodyBuffer,
    ::Type{UInt8},
    col::ArrowParent.ToList{UInt8,stringtype},
) where {stringtype}
    total = length(col)
    _ensureflightbodycapacity(io, total)
    pos = io.pos
    body = io.data
    off = ArrowParent._tolistoffset(col)
    data = ArrowParent._tolistdata(col)
    if off == 0
        for chunk in data
            chunk === missing && continue
            bytes = stringtype ? ArrowParent._codeunits(chunk) : chunk
            n = stringtype ? ArrowParent._ncodeunits(chunk) : length(bytes)
            GC.@preserve body bytes begin
                unsafe_copyto!(pointer(body, pos), pointer(bytes), n)
            end
            pos += n
        end
    else
        len = length(data)
        @inbounds for i = 1:len
            chunk = data[i + off]
            chunk === missing && continue
            bytes = stringtype ? ArrowParent._codeunits(chunk) : chunk
            n = stringtype ? ArrowParent._ncodeunits(chunk) : length(bytes)
            GC.@preserve body bytes begin
                unsafe_copyto!(pointer(body, pos), pointer(bytes), n)
            end
            pos += n
        end
    end
    io.pos = pos
    return total
end

function _takeflightbody!(io::FlightBodyBuffer)
    len = io.pos - 1
    len == length(io.data) || resize!(io.data, len)
    return io.data
end

function _message_body(msg::ArrowParent.Message, alignment::Integer)
    msg.columns === nothing && return UInt8[]
    io = FlightBodyBuffer(Int(msg.bodylen))
    for col in Tables.Columns(msg.columns)
        ArrowParent.writebuffer(io, col, alignment)
    end
    return _takeflightbody!(io)
end

function _flightdata_message(
    msg::ArrowParent.Message;
    descriptor::Union{Nothing,Protocol.FlightDescriptor}=nothing,
    app_metadata::AbstractVector{UInt8}=UInt8[],
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
)
    body = _message_body(msg, alignment)
    length(body) == msg.bodylen ||
        throw(ArgumentError("FlightData body length mismatch while encoding Arrow IPC"))
    return Protocol.FlightData(
        descriptor,
        Vector{UInt8}(msg.msgflatbuf),
        Vector{UInt8}(app_metadata),
        body,
    )
end

function _write_framed_message(
    io::IO,
    data_header::AbstractVector{UInt8},
    data_body::AbstractVector{UInt8},
    alignment::Integer,
)
    metalen = ArrowParent.padding(length(data_header), alignment)
    Base.write(io, ArrowParent.CONTINUATION_INDICATOR_BYTES)
    Base.write(io, Int32(metalen))
    Base.write(io, data_header)
    ArrowParent.writezeros(io, ArrowParent.paddinglength(length(data_header), alignment))
    Base.write(io, data_body)
    return
end

function _write_end_marker(io::IO)
    Base.write(io, ArrowParent.CONTINUATION_INDICATOR_BYTES)
    Base.write(io, Int32(0))
    return
end
