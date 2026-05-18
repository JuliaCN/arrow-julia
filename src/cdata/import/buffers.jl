# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

_logical_offset(array::ArrowArray) = Int(array.offset)
_logical_length(array::ArrowArray) = Int(array.length)

_validity_nbytes(len::Integer, offset::Integer=0) = len == 0 ? 0 : Int(cld(len + offset, 8))

function _buffer_pointer(::Type{T}, ptr::Ptr{Cvoid}, offset::Integer=0) where {T}
    return Ptr{T}(ptr) + Int(offset) * sizeof(T)
end

function _wrap_buffer(::Type{T}, ptr::Ptr{Cvoid}, len::Integer, offset::Integer=0) where {T}
    len == 0 && return T[]
    ptr == C_NULL && throw(ArgumentError("cannot wrap C_NULL buffer"))
    return unsafe_wrap(Vector{T}, _buffer_pointer(T, ptr, offset), Int(len); own=false)
end

function _wrap_byte_buffer(ptr::Ptr{Cvoid}, len::Integer, offset::Integer=0)
    len == 0 && return UInt8[]
    ptr == C_NULL && throw(ArgumentError("cannot wrap C_NULL byte buffer"))
    return unsafe_wrap(Vector{UInt8}, Ptr{UInt8}(ptr) + Int(offset), Int(len); own=false)
end

function _validity_vector(array::ArrowArray, name::Symbol)
    array.buffers == C_NULL && throw(ArgumentError("column $name has C_NULL buffers"))
    validity_ptr = unsafe_load(array.buffers, 1)
    if array.length > 0 && array.null_count != 0 && validity_ptr == C_NULL
        throw(ArgumentError("nullable column $name has C_NULL validity buffer"))
    end
    nbytes = _validity_nbytes(array.length, array.offset)
    nbytes == 0 && return UInt8[]
    validity_ptr == C_NULL && return UInt8[]
    return unsafe_wrap(Vector{UInt8}, Ptr{UInt8}(validity_ptr), nbytes; own=false)
end

function _validity_bit(validity::Vector{UInt8}, i::Int, offset::Int=0)
    isempty(validity) && return true
    physical = i + offset
    byte = @inbounds validity[((physical - 1) >>> 3) + 1]
    shift = (physical - 1) & 7
    mask = UInt8(1) << shift
    return (byte & mask) != 0
end

function _bitmap_vector(
    array::ArrowArray,
    name::Symbol,
    buffer_index::Integer,
    label::AbstractString,
)
    array.buffers == C_NULL && throw(ArgumentError("column $name has C_NULL buffers"))
    nbytes = _validity_nbytes(array.length, array.offset)
    nbytes == 0 && return UInt8[]
    data_ptr = unsafe_load(array.buffers, buffer_index)
    data_ptr == C_NULL && throw(ArgumentError("$label column $name has C_NULL data buffer"))
    return unsafe_wrap(Vector{UInt8}, Ptr{UInt8}(data_ptr), nbytes; own=false)
end
