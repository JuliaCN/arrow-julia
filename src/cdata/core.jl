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

"""
    Arrow.CData.ArrowSchema

ABI-compatible representation of the Apache Arrow C Data Interface
`ArrowSchema` struct.
"""
struct ArrowSchema
    format::Ptr{UInt8}
    name::Ptr{UInt8}
    metadata::Ptr{UInt8}
    flags::Int64
    n_children::Int64
    children::Ptr{Ptr{ArrowSchema}}
    dictionary::Ptr{ArrowSchema}
    release::Ptr{Cvoid}
    private_data::Ptr{Cvoid}
end

"""
    Arrow.CData.ArrowArray

ABI-compatible representation of the Apache Arrow C Data Interface
`ArrowArray` struct.
"""
struct ArrowArray
    length::Int64
    null_count::Int64
    offset::Int64
    n_buffers::Int64
    n_children::Int64
    buffers::Ptr{Ptr{Cvoid}}
    children::Ptr{Ptr{ArrowArray}}
    dictionary::Ptr{ArrowArray}
    release::Ptr{Cvoid}
    private_data::Ptr{Cvoid}
end

"""
    Arrow.CData.ArrowArrayStream

ABI-compatible representation of the Apache Arrow C Stream Interface
`ArrowArrayStream` struct.
"""
struct ArrowArrayStream
    get_schema::Ptr{Cvoid}
    get_next::Ptr{Cvoid}
    get_last_error::Ptr{Cvoid}
    release::Ptr{Cvoid}
    private_data::Ptr{Cvoid}
end

abstract type ExportHandle end

mutable struct SchemaHandle <: ExportHandle
    format::Vector{UInt8}
    name::Vector{UInt8}
    metadata::Vector{UInt8}
    children::Vector{Base.RefValue{ArrowSchema}}
    child_handles::Vector{SchemaHandle}
    child_ptrs::Vector{Ptr{ArrowSchema}}
    dictionary::Union{Nothing,Base.RefValue{ArrowSchema}}
    dictionary_handle::Union{Nothing,SchemaHandle}
    dictionary_ptr::Ptr{ArrowSchema}
    private_data::Ptr{Cvoid}
end

mutable struct ArrayHandle <: ExportHandle
    source::Any
    buffers::Vector{Ptr{Cvoid}}
    children::Vector{Base.RefValue{ArrowArray}}
    child_handles::Vector{ArrayHandle}
    child_ptrs::Vector{Ptr{ArrowArray}}
    dictionary::Union{Nothing,Base.RefValue{ArrowArray}}
    dictionary_handle::Union{Nothing,ArrayHandle}
    dictionary_ptr::Ptr{ArrowArray}
    private_data::Ptr{Cvoid}
end

struct SchemaExport
    ref::Base.RefValue{ArrowSchema}
    handle::SchemaHandle
end

struct ArrayExport
    ref::Base.RefValue{ArrowArray}
    handle::ArrayHandle
end

const EMPTY_SCHEMA_EXPORTS = SchemaExport[]
const EMPTY_ARRAY_EXPORTS = ArrayExport[]
const EMPTY_SCHEMA_REFS = Base.RefValue{ArrowSchema}[]
const EMPTY_ARRAY_REFS = Base.RefValue{ArrowArray}[]
const EMPTY_SCHEMA_HANDLES = SchemaHandle[]
const EMPTY_ARRAY_HANDLES = ArrayHandle[]
const EMPTY_SCHEMA_PTRS = Ptr{ArrowSchema}[]
const EMPTY_ARRAY_PTRS = Ptr{ArrowArray}[]
const EMPTY_CSTRING = UInt8[0x00]
const CSTRING_FORMAT_NULL = UInt8[UInt8('n'), 0x00]
const CSTRING_FORMAT_BOOL = UInt8[UInt8('b'), 0x00]
const CSTRING_FORMAT_INT8 = UInt8[UInt8('c'), 0x00]
const CSTRING_FORMAT_UINT8 = UInt8[UInt8('C'), 0x00]
const CSTRING_FORMAT_INT16 = UInt8[UInt8('s'), 0x00]
const CSTRING_FORMAT_UINT16 = UInt8[UInt8('S'), 0x00]
const CSTRING_FORMAT_INT32 = UInt8[UInt8('i'), 0x00]
const CSTRING_FORMAT_UINT32 = UInt8[UInt8('I'), 0x00]
const CSTRING_FORMAT_INT64 = UInt8[UInt8('l'), 0x00]
const CSTRING_FORMAT_UINT64 = UInt8[UInt8('L'), 0x00]
const CSTRING_FORMAT_FLOAT16 = UInt8[UInt8('e'), 0x00]
const CSTRING_FORMAT_FLOAT32 = UInt8[UInt8('f'), 0x00]
const CSTRING_FORMAT_FLOAT64 = UInt8[UInt8('g'), 0x00]
const CSTRING_FORMAT_UTF8 = UInt8[UInt8('u'), 0x00]
const CSTRING_FORMAT_LARGE_UTF8 = UInt8[UInt8('U'), 0x00]
const CSTRING_FORMAT_BINARY = UInt8[UInt8('z'), 0x00]
const CSTRING_FORMAT_LARGE_BINARY = UInt8[UInt8('Z'), 0x00]
const CSTRING_FORMAT_UTF8_VIEW = UInt8[UInt8('v'), UInt8('u'), 0x00]
const CSTRING_FORMAT_BINARY_VIEW = UInt8[UInt8('v'), UInt8('z'), 0x00]
const CSTRING_FORMAT_STRUCT = UInt8[UInt8('+'), UInt8('s'), 0x00]
const CSTRING_FORMAT_LIST = UInt8[UInt8('+'), UInt8('l'), 0x00]
const CSTRING_FORMAT_LARGE_LIST = UInt8[UInt8('+'), UInt8('L'), 0x00]
const CSTRING_FORMAT_LIST_VIEW = UInt8[UInt8('+'), UInt8('v'), UInt8('l'), 0x00]
const CSTRING_FORMAT_LARGE_LIST_VIEW = UInt8[UInt8('+'), UInt8('v'), UInt8('L'), 0x00]
const CSTRING_FORMAT_MAP = UInt8[UInt8('+'), UInt8('m'), 0x00]
const CSTRING_FORMAT_RUN_END_ENCODED = UInt8[UInt8('+'), UInt8('r'), 0x00]

"""
    Arrow.CData.ExportedTable

Owner object returned by [`Arrow.CData.exporttable`](@ref). Keep this object
alive for as long as a C consumer may access [`schema_ptr`](@ref) or
[`array_ptr`](@ref).
"""
mutable struct ExportedTable
    schema::SchemaExport
    array::ArrayExport
    schema_base::Ptr{ArrowSchema}
    array_base::Ptr{ArrowArray}
end

function ExportedTable(schema::SchemaExport, array::ArrayExport)
    return ExportedTable(
        schema,
        array,
        Base.unsafe_convert(Ptr{ArrowSchema}, schema.ref),
        Base.unsafe_convert(Ptr{ArrowArray}, array.ref),
    )
end

const HANDLE_LOCK = ReentrantLock()
const LIVE_HANDLES = Dict{Ptr{Cvoid},ExportHandle}()
const EMPTY_METADATA_BYTES = UInt8[]

function _retain!(handle::ExportHandle)
    ptr = Ptr{Cvoid}(pointer_from_objref(handle))
    Base.@lock HANDLE_LOCK begin
        LIVE_HANDLES[ptr] = handle
    end
    return ptr
end

function _release_handle!(ptr::Ptr{Cvoid})
    ptr == C_NULL && return nothing
    Base.@lock HANDLE_LOCK begin
        pop!(LIVE_HANDLES, ptr, nothing)
    end
    return nothing
end

_retained_handle_count() = Base.@lock HANDLE_LOCK length(LIVE_HANDLES)

function _cstring(value::AbstractString)
    bytes = Vector{UInt8}(codeunits(value))
    push!(bytes, 0x00)
    return bytes
end

_cstring_ptr(bytes::Vector{UInt8}) = pointer(bytes)

function _schema_format_cstring(value::AbstractString)
    value == "n" && return CSTRING_FORMAT_NULL
    value == "b" && return CSTRING_FORMAT_BOOL
    value == "c" && return CSTRING_FORMAT_INT8
    value == "C" && return CSTRING_FORMAT_UINT8
    value == "s" && return CSTRING_FORMAT_INT16
    value == "S" && return CSTRING_FORMAT_UINT16
    value == "i" && return CSTRING_FORMAT_INT32
    value == "I" && return CSTRING_FORMAT_UINT32
    value == "l" && return CSTRING_FORMAT_INT64
    value == "L" && return CSTRING_FORMAT_UINT64
    value == "e" && return CSTRING_FORMAT_FLOAT16
    value == "f" && return CSTRING_FORMAT_FLOAT32
    value == "g" && return CSTRING_FORMAT_FLOAT64
    value == "u" && return CSTRING_FORMAT_UTF8
    value == "U" && return CSTRING_FORMAT_LARGE_UTF8
    value == "z" && return CSTRING_FORMAT_BINARY
    value == "Z" && return CSTRING_FORMAT_LARGE_BINARY
    value == "vu" && return CSTRING_FORMAT_UTF8_VIEW
    value == "vz" && return CSTRING_FORMAT_BINARY_VIEW
    value == "+s" && return CSTRING_FORMAT_STRUCT
    value == "+l" && return CSTRING_FORMAT_LIST
    value == "+L" && return CSTRING_FORMAT_LARGE_LIST
    value == "+vl" && return CSTRING_FORMAT_LIST_VIEW
    value == "+vL" && return CSTRING_FORMAT_LARGE_LIST_VIEW
    value == "+m" && return CSTRING_FORMAT_MAP
    value == "+r" && return CSTRING_FORMAT_RUN_END_ENCODED
    return _cstring(value)
end

_schema_name_cstring(value::AbstractString) = value == "" ? EMPTY_CSTRING : _cstring(value)

function _append_int32!(bytes::Vector{UInt8}, value::Integer)
    value > typemax(Int32) &&
        throw(ArgumentError("Arrow C Data metadata length exceeds Int32 range"))
    value < typemin(Int32) &&
        throw(ArgumentError("Arrow C Data metadata length is outside Int32 range"))
    append!(bytes, reinterpret(UInt8, Int32[value]))
    return bytes
end

function _metadata_bytes(metadata)
    metadata === nothing && return EMPTY_METADATA_BYTES
    entries = [(String(key), String(metadata[key])) for key in keys(metadata)]
    isempty(entries) && return EMPTY_METADATA_BYTES
    sort!(entries; by=first)
    bytes = UInt8[]
    _append_int32!(bytes, length(entries))
    for (key, value) in entries
        key_bytes = codeunits(key)
        value_bytes = codeunits(value)
        _append_int32!(bytes, length(key_bytes))
        append!(bytes, key_bytes)
        _append_int32!(bytes, length(value_bytes))
        append!(bytes, value_bytes)
    end
    return bytes
end

_metadata_ptr(bytes::Vector{UInt8}) = isempty(bytes) ? Ptr{UInt8}(C_NULL) : pointer(bytes)

function _read_metadata_int32(ptr::Ptr{UInt8}, offset::Base.RefValue{Int})
    value = unsafe_load(Ptr{Int32}(ptr + offset[]))
    offset[] += sizeof(Int32)
    return Int(value)
end

function _read_metadata_string(ptr::Ptr{UInt8}, offset::Base.RefValue{Int})
    len = _read_metadata_int32(ptr, offset)
    len >= 0 || throw(ArgumentError("Arrow C Data metadata has negative byte length"))
    value = len == 0 ? "" : unsafe_string(ptr + offset[], len)
    offset[] += len
    return value
end

function _metadata_from_ptr(ptr::Ptr{UInt8})
    ptr == C_NULL && return nothing
    offset = Ref(0)
    count = _read_metadata_int32(ptr, offset)
    count >= 0 || throw(ArgumentError("Arrow C Data metadata has negative entry count"))
    entries = Pair{String,String}[]
    sizehint!(entries, count)
    for _ = 1:count
        key = _read_metadata_string(ptr, offset)
        value = _read_metadata_string(ptr, offset)
        push!(entries, key => value)
    end
    return toidict(entries)
end

"""
    Arrow.CData.header_path()

Return the package path to `arrow_julia_cdata.h`, the C consumer header for
the Arrow C Data Interface structs and release helpers used by this module.
"""
header_path() = normpath(joinpath(@__DIR__, "..", "..", "include", "arrow_julia_cdata.h"))
