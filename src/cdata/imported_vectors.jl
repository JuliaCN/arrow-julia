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
    Arrow.CData.ImportedStringVector

Borrowed UTF-8 string vector imported from Arrow C Data Interface offsets and
data buffers. Keep the owning [`ImportedTable`](@ref) live until done.
"""
struct ImportedStringVector{O<:Union{Int32,Int64}} <: AbstractVector{String}
    offsets::Vector{O}
    data::Vector{UInt8}
    len::Int
end

Base.IndexStyle(::Type{<:ImportedStringVector}) = Base.IndexLinear()
Base.size(vector::ImportedStringVector) = (vector.len,)

function Base.getindex(vector::ImportedStringVector, i::Int)
    @boundscheck checkbounds(vector, i)
    lo = Int(@inbounds vector.offsets[i]) + 1
    hi = Int(@inbounds vector.offsets[i + 1])
    nbytes = hi - lo + 1
    nbytes == 0 && return ""
    return unsafe_string(pointer(vector.data, lo), nbytes)
end

"""
    Arrow.CData.ImportedBinaryVector

Borrowed binary vector imported from Arrow C Data Interface offsets, data, and
optional validity buffers. Elements are byte views into the imported data
buffer. Keep the owning [`ImportedTable`](@ref) live until done.
"""
struct ImportedBinaryVector{T,O<:Union{Int32,Int64}} <: AbstractVector{T}
    offsets::Vector{O}
    data::Vector{UInt8}
    validity::Vector{UInt8}
    len::Int
    validity_offset::Int
end

function ImportedBinaryVector(
    offsets::Vector{O},
    data::Vector{UInt8},
    validity::Vector{UInt8},
    len::Int,
    nullable::Bool,
    validity_offset::Int=0,
) where {O<:Union{Int32,Int64}}
    T = AbstractVector{UInt8}
    ET = nullable ? Union{Missing,T} : T
    return ImportedBinaryVector{ET,O}(offsets, data, validity, len, validity_offset)
end

Base.IndexStyle(::Type{<:ImportedBinaryVector}) = Base.IndexLinear()
Base.size(vector::ImportedBinaryVector) = (vector.len,)

function Base.getindex(vector::ImportedBinaryVector, i::Int)
    @boundscheck checkbounds(vector, i)
    _validity_bit(vector.validity, i, vector.validity_offset) || return missing
    lo = Int(@inbounds vector.offsets[i]) + 1
    hi = Int(@inbounds vector.offsets[i + 1])
    return @view vector.data[lo:hi]
end

"""
    Arrow.CData.ImportedStringViewVector

Borrowed UTF-8 view vector imported from Arrow C Data Interface views,
variadic data, variadic length, and optional validity buffers. Keep the owning
[`ImportedTable`](@ref) live until done.
"""
struct ImportedStringViewVector{T} <: AbstractVector{T}
    views::Vector{ViewElement}
    inline::Vector{UInt8}
    buffers::Vector{Vector{UInt8}}
    lengths::Vector{Int64}
    validity::Vector{UInt8}
    len::Int
    validity_offset::Int
end

"""
    Arrow.CData.ImportedBinaryViewVector

Borrowed binary view vector imported from Arrow C Data Interface views,
variadic data, variadic length, and optional validity buffers. Keep the owning
[`ImportedTable`](@ref) live until done.
"""
struct ImportedBinaryViewVector{T} <: AbstractVector{T}
    views::Vector{ViewElement}
    inline::Vector{UInt8}
    buffers::Vector{Vector{UInt8}}
    lengths::Vector{Int64}
    validity::Vector{UInt8}
    len::Int
    validity_offset::Int
end

function ImportedStringViewVector(
    views::Vector{ViewElement},
    inline::Vector{UInt8},
    buffers::Vector{Vector{UInt8}},
    lengths::Vector{Int64},
    validity::Vector{UInt8},
    len::Int,
    nullable::Bool,
    validity_offset::Int=0,
)
    T = nullable ? Union{Missing,String} : String
    return ImportedStringViewVector{T}(
        views,
        inline,
        buffers,
        lengths,
        validity,
        len,
        validity_offset,
    )
end

function ImportedBinaryViewVector(
    views::Vector{ViewElement},
    inline::Vector{UInt8},
    buffers::Vector{Vector{UInt8}},
    lengths::Vector{Int64},
    validity::Vector{UInt8},
    len::Int,
    nullable::Bool,
    validity_offset::Int=0,
)
    T = AbstractVector{UInt8}
    ET = nullable ? Union{Missing,T} : T
    return ImportedBinaryViewVector{ET}(
        views,
        inline,
        buffers,
        lengths,
        validity,
        len,
        validity_offset,
    )
end

Base.IndexStyle(::Type{<:ImportedStringViewVector}) = Base.IndexLinear()
Base.IndexStyle(::Type{<:ImportedBinaryViewVector}) = Base.IndexLinear()
Base.size(vector::Union{ImportedStringViewVector,ImportedBinaryViewVector}) = (vector.len,)

@inline _view_inline_start(i::Integer) =
    (i - 1) * VIEW_ELEMENT_BYTES + VIEW_LENGTH_BYTES + 1

function _view_buffer_span(
    vector::Union{ImportedStringViewVector,ImportedBinaryViewVector},
    i::Int,
)
    view = @inbounds vector.views[i]
    len = Int(view.length)
    if _viewisinline(len)
        return vector.inline, _view_inline_start(i), len
    end
    buffer_index = Int(view.bufindex) + 1
    @boundscheck checkbounds(vector.buffers, buffer_index)
    return vector.buffers[buffer_index], Int(view.offset) + 1, len
end

function Base.getindex(vector::ImportedStringViewVector, i::Int)
    @boundscheck checkbounds(vector, i)
    _validity_bit(vector.validity, i, vector.validity_offset) || return missing
    buffer, start, len = _view_buffer_span(vector, i)
    len == 0 && return ""
    return unsafe_string(pointer(buffer, start), len)
end

function Base.getindex(vector::ImportedBinaryViewVector, i::Int)
    @boundscheck checkbounds(vector, i)
    _validity_bit(vector.validity, i, vector.validity_offset) || return missing
    buffer, start, len = _view_buffer_span(vector, i)
    len == 0 && return @view vector.inline[1:0]
    return @view buffer[start:(start + len - 1)]
end

"""
    Arrow.CData.ImportedFixedSizeBinaryVector

Borrowed fixed-size-binary vector imported from Arrow C Data Interface
bit-width metadata and data buffers. Keep the owning [`ImportedTable`](@ref)
live until done.
"""
struct ImportedFixedSizeBinaryVector{T,N} <: AbstractVector{T}
    data::Vector{UInt8}
    validity::Vector{UInt8}
    len::Int
    validity_offset::Int
end

function ImportedFixedSizeBinaryVector(
    ::Val{N},
    data::Vector{UInt8},
    validity::Vector{UInt8},
    len::Int,
    nullable::Bool,
    validity_offset::Int=0,
) where {N}
    T = NTuple{N,UInt8}
    ET = nullable ? Union{Missing,T} : T
    return ImportedFixedSizeBinaryVector{ET,N}(data, validity, len, validity_offset)
end

Base.IndexStyle(::Type{<:ImportedFixedSizeBinaryVector}) = Base.IndexLinear()
Base.size(vector::ImportedFixedSizeBinaryVector) = (vector.len,)

function Base.getindex(vector::ImportedFixedSizeBinaryVector{T,N}, i::Int) where {T,N}
    @boundscheck checkbounds(vector, i)
    _validity_bit(vector.validity, i, vector.validity_offset) || return missing
    offset = (i - 1) * N
    return ntuple(j -> @inbounds(vector.data[offset + j]), Val(N))
end

"""
    Arrow.CData.ImportedNullablePrimitiveVector

Borrowed nullable primitive vector imported from Arrow C Data Interface data
and validity buffers. Keep the owning [`ImportedTable`](@ref) live until done.
"""
struct ImportedNullablePrimitiveVector{T} <: AbstractVector{Union{Missing,T}}
    data::Vector{T}
    validity::Vector{UInt8}
    len::Int
    validity_offset::Int
end

Base.IndexStyle(::Type{<:ImportedNullablePrimitiveVector}) = Base.IndexLinear()
Base.size(vector::ImportedNullablePrimitiveVector) = (vector.len,)

function Base.getindex(vector::ImportedNullablePrimitiveVector{T}, i::Int) where {T}
    @boundscheck checkbounds(vector, i)
    _validity_bit(vector.validity, i, vector.validity_offset) || return missing
    return @inbounds vector.data[i]
end

"""
    Arrow.CData.ImportedNullableStringVector

Borrowed nullable UTF-8 string vector imported from Arrow C Data Interface
offsets, data, and validity buffers. Keep the owning [`ImportedTable`](@ref)
live until done.
"""
struct ImportedNullableStringVector{O<:Union{Int32,Int64}} <:
       AbstractVector{Union{Missing,String}}
    values::ImportedStringVector{O}
    validity::Vector{UInt8}
    validity_offset::Int
end

Base.IndexStyle(::Type{<:ImportedNullableStringVector}) = Base.IndexLinear()
Base.size(vector::ImportedNullableStringVector) = size(vector.values)

function Base.getindex(vector::ImportedNullableStringVector, i::Int)
    @boundscheck checkbounds(vector, i)
    _validity_bit(vector.validity, i, vector.validity_offset) || return missing
    return @inbounds vector.values[i]
end

"""
    Arrow.CData.ImportedBoolVector

Borrowed boolean vector imported from Arrow C Data Interface bit-packed data
and optional validity buffers. Keep the owning [`ImportedTable`](@ref) live
until done.
"""
struct ImportedBoolVector{T} <: AbstractVector{T}
    data::Vector{UInt8}
    validity::Vector{UInt8}
    len::Int
    validity_offset::Int
    data_offset::Int
end

function ImportedBoolVector(
    data::Vector{UInt8},
    validity::Vector{UInt8},
    len::Int,
    nullable::Bool,
    validity_offset::Int=0,
    data_offset::Int=0,
)
    T = nullable ? Union{Missing,Bool} : Bool
    return ImportedBoolVector{T}(data, validity, len, validity_offset, data_offset)
end

Base.IndexStyle(::Type{<:ImportedBoolVector}) = Base.IndexLinear()
Base.size(vector::ImportedBoolVector) = (vector.len,)

function Base.getindex(vector::ImportedBoolVector, i::Int)
    @boundscheck checkbounds(vector, i)
    _validity_bit(vector.validity, i, vector.validity_offset) || return missing
    return _validity_bit(vector.data, i, vector.data_offset)
end

"""
    Arrow.CData.ImportedDictionaryVector

Borrowed dictionary encoded vector imported from Arrow C Data Interface
indices, optional validity, and dictionary value buffers. Keep the owning
[`ImportedTable`](@ref) live until done.
"""
struct ImportedDictionaryVector{T,I,D<:AbstractVector} <: AbstractVector{T}
    indices::Vector{I}
    dictionary::D
    validity::Vector{UInt8}
    len::Int
    validity_offset::Int
end

function ImportedDictionaryVector(
    indices::Vector{I},
    dictionary::D,
    validity::Vector{UInt8},
    len::Int,
    nullable::Bool,
    validity_offset::Int=0,
) where {I,D<:AbstractVector}
    T = nullable ? Union{Missing,eltype(dictionary)} : eltype(dictionary)
    return ImportedDictionaryVector{T,I,D}(
        indices,
        dictionary,
        validity,
        len,
        validity_offset,
    )
end

Base.IndexStyle(::Type{<:ImportedDictionaryVector}) = Base.IndexLinear()
Base.size(vector::ImportedDictionaryVector) = (vector.len,)

function Base.getindex(vector::ImportedDictionaryVector, i::Int)
    @boundscheck checkbounds(vector, i)
    _validity_bit(vector.validity, i, vector.validity_offset) || return missing
    index = Int(@inbounds vector.indices[i]) + 1
    @boundscheck checkbounds(vector.dictionary, index)
    return @inbounds vector.dictionary[index]
end

"""
    Arrow.CData.ImportedListVector

Borrowed variable-size list vector imported from Arrow C Data Interface
offsets, optional validity, and child value buffers. Keep the owning
[`ImportedTable`](@ref) live until done.
"""
struct ImportedListVector{T,O<:Union{Int32,Int64},V<:AbstractVector} <: AbstractVector{T}
    offsets::Vector{O}
    values::V
    validity::Vector{UInt8}
    len::Int
    validity_offset::Int
end

function ImportedListVector(
    offsets::Vector{O},
    values::V,
    validity::Vector{UInt8},
    len::Int,
    nullable::Bool,
    validity_offset::Int=0,
) where {O<:Union{Int32,Int64},V<:AbstractVector}
    T = AbstractVector{eltype(values)}
    ET = nullable ? Union{Missing,T} : T
    return ImportedListVector{ET,O,V}(offsets, values, validity, len, validity_offset)
end

Base.IndexStyle(::Type{<:ImportedListVector}) = Base.IndexLinear()
Base.size(vector::ImportedListVector) = (vector.len,)

function Base.getindex(vector::ImportedListVector, i::Int)
    @boundscheck checkbounds(vector, i)
    _validity_bit(vector.validity, i, vector.validity_offset) || return missing
    lo = Int(@inbounds vector.offsets[i]) + 1
    hi = Int(@inbounds vector.offsets[i + 1])
    return @view vector.values[lo:hi]
end

"""
    Arrow.CData.ImportedListViewVector

Borrowed list-view vector imported from Arrow C Data Interface offsets, sizes,
optional validity, and child value buffers. Keep the owning [`ImportedTable`](@ref)
live until done.
"""
struct ImportedListViewVector{T,O<:Union{Int32,Int64},V<:AbstractVector} <:
       AbstractVector{T}
    offsets::Vector{O}
    sizes::Vector{O}
    values::V
    validity::Vector{UInt8}
    len::Int
    validity_offset::Int
end

function ImportedListViewVector(
    offsets::Vector{O},
    sizes::Vector{O},
    values::V,
    validity::Vector{UInt8},
    len::Int,
    nullable::Bool,
    validity_offset::Int=0,
) where {O<:Union{Int32,Int64},V<:AbstractVector}
    T = AbstractVector{eltype(values)}
    ET = nullable ? Union{Missing,T} : T
    return ImportedListViewVector{ET,O,V}(
        offsets,
        sizes,
        values,
        validity,
        len,
        validity_offset,
    )
end

Base.IndexStyle(::Type{<:ImportedListViewVector}) = Base.IndexLinear()
Base.size(vector::ImportedListViewVector) = (vector.len,)

function Base.getindex(vector::ImportedListViewVector, i::Int)
    @boundscheck checkbounds(vector, i)
    _validity_bit(vector.validity, i, vector.validity_offset) || return missing
    size = Int(@inbounds vector.sizes[i])
    size == 0 && return @view vector.values[1:0]
    lo = Int(@inbounds vector.offsets[i]) + 1
    return @view vector.values[lo:(lo + size - 1)]
end

"""
    Arrow.CData.ImportedFixedSizeListVector

Borrowed fixed-size-list vector imported from Arrow C Data Interface child
arrays. Keep the owning [`ImportedTable`](@ref) live until done.
"""
struct ImportedFixedSizeListVector{T,N,V<:AbstractVector} <: AbstractVector{T}
    values::V
    validity::Vector{UInt8}
    len::Int
    validity_offset::Int
    value_offset::Int
end

function ImportedFixedSizeListVector(
    ::Val{N},
    values::V,
    validity::Vector{UInt8},
    len::Int,
    nullable::Bool,
    value_offset::Int=0,
    validity_offset::Int=0,
) where {N,V<:AbstractVector}
    T = NTuple{N,eltype(values)}
    ET = nullable ? Union{Missing,T} : T
    return ImportedFixedSizeListVector{ET,N,V}(
        values,
        validity,
        len,
        validity_offset,
        value_offset,
    )
end

Base.IndexStyle(::Type{<:ImportedFixedSizeListVector}) = Base.IndexLinear()
Base.size(vector::ImportedFixedSizeListVector) = (vector.len,)

function Base.getindex(vector::ImportedFixedSizeListVector{T,N}, i::Int) where {T,N}
    @boundscheck checkbounds(vector, i)
    _validity_bit(vector.validity, i, vector.validity_offset) || return missing
    offset = vector.value_offset + (i - 1) * N
    return ntuple(j -> @inbounds(vector.values[offset + j]), Val(N))
end

"""
    Arrow.CData.ImportedMapVector

Borrowed map vector imported from Arrow C Data Interface offsets and entries
struct child arrays. Row access materializes a `Dict` from borrowed entries.
Keep the owning [`ImportedTable`](@ref) live until done.
"""
struct ImportedMapVector{T,O<:Union{Int32,Int64},E<:AbstractVector} <: AbstractVector{T}
    offsets::Vector{O}
    entries::E
    validity::Vector{UInt8}
    len::Int
    validity_offset::Int
end

function _map_entry_types(entries::AbstractVector)
    entry_type = Base.nonmissingtype(eltype(entries))
    entry_type <: NamedTuple ||
        throw(ArgumentError("map entries must import as NamedTuple values"))
    fieldnames(entry_type) == (:key, :value) ||
        throw(ArgumentError("map entries must expose key and value fields"))
    return fieldtype(entry_type, :key), fieldtype(entry_type, :value)
end

function ImportedMapVector(
    offsets::Vector{O},
    entries::E,
    validity::Vector{UInt8},
    len::Int,
    nullable::Bool,
    validity_offset::Int=0,
) where {O<:Union{Int32,Int64},E<:AbstractVector}
    K, V = _map_entry_types(entries)
    T = Dict{K,V}
    ET = nullable ? Union{Missing,T} : T
    return ImportedMapVector{ET,O,E}(offsets, entries, validity, len, validity_offset)
end

Base.IndexStyle(::Type{<:ImportedMapVector}) = Base.IndexLinear()
Base.size(vector::ImportedMapVector) = (vector.len,)

function Base.getindex(vector::ImportedMapVector{T}, i::Int) where {T}
    @boundscheck checkbounds(vector, i)
    _validity_bit(vector.validity, i, vector.validity_offset) || return missing
    K, V = _map_entry_types(vector.entries)
    result = Dict{K,V}()
    lo = Int(@inbounds vector.offsets[i]) + 1
    hi = Int(@inbounds vector.offsets[i + 1])
    for index = lo:hi
        entry = @inbounds vector.entries[index]
        result[entry.key] = entry.value
    end
    return result
end

"""
    Arrow.CData.ImportedDenseUnionVector

Borrowed dense union vector imported from Arrow C Data Interface type id,
offset, and child array buffers. Keep the owning [`ImportedTable`](@ref) live
until done.
"""
struct ImportedDenseUnionVector{T,C<:Tuple} <: AbstractVector{T}
    type_ids::Vector{UInt8}
    offsets::Vector{Int32}
    children::C
    type_id_to_child::Dict{UInt8,Int}
    len::Int
end

"""
    Arrow.CData.ImportedSparseUnionVector

Borrowed sparse union vector imported from Arrow C Data Interface type id and
child array buffers. Keep the owning [`ImportedTable`](@ref) live until done.
"""
struct ImportedSparseUnionVector{T,C<:Tuple} <: AbstractVector{T}
    type_ids::Vector{UInt8}
    children::C
    type_id_to_child::Dict{UInt8,Int}
    len::Int
end

function _union_eltype(children::Tuple)
    child_types = Tuple(eltype(child) for child in children)
    return Union{child_types...}
end

function _union_type_id_map(declared_ids::Vector{UInt8})
    return Dict(id => index for (index, id) in enumerate(declared_ids))
end

function ImportedDenseUnionVector(
    type_ids::Vector{UInt8},
    offsets::Vector{Int32},
    children::C,
    declared_ids::Vector{UInt8},
    len::Int,
) where {C<:Tuple}
    length(type_ids) == len ||
        throw(ArgumentError("dense union type id length must match row count"))
    length(offsets) == len ||
        throw(ArgumentError("dense union offset length must match row count"))
    length(children) == length(declared_ids) ||
        throw(ArgumentError("dense union child count must match declared type ids"))
    return ImportedDenseUnionVector{_union_eltype(children),C}(
        type_ids,
        offsets,
        children,
        _union_type_id_map(declared_ids),
        len,
    )
end

function ImportedSparseUnionVector(
    type_ids::Vector{UInt8},
    children::C,
    declared_ids::Vector{UInt8},
    len::Int,
) where {C<:Tuple}
    length(type_ids) == len ||
        throw(ArgumentError("sparse union type id length must match row count"))
    length(children) == length(declared_ids) ||
        throw(ArgumentError("sparse union child count must match declared type ids"))
    return ImportedSparseUnionVector{_union_eltype(children),C}(
        type_ids,
        children,
        _union_type_id_map(declared_ids),
        len,
    )
end

Base.IndexStyle(::Type{<:ImportedDenseUnionVector}) = Base.IndexLinear()
Base.IndexStyle(::Type{<:ImportedSparseUnionVector}) = Base.IndexLinear()
Base.size(vector::Union{ImportedDenseUnionVector,ImportedSparseUnionVector}) = (vector.len,)

function _union_child_index(vector, type_id::UInt8)
    child_index = get(vector.type_id_to_child, type_id, 0)
    child_index == 0 &&
        throw(ArgumentError("union value references undeclared type id $(Int(type_id))"))
    return child_index
end

function Base.getindex(vector::ImportedDenseUnionVector, i::Int)
    @boundscheck checkbounds(vector, i)
    type_id = @inbounds vector.type_ids[i]
    child_index = _union_child_index(vector, type_id)
    offset = Int(@inbounds vector.offsets[i]) + 1
    @boundscheck checkbounds(vector.children[child_index], offset)
    return @inbounds vector.children[child_index][offset]
end

function Base.getindex(vector::ImportedSparseUnionVector, i::Int)
    @boundscheck checkbounds(vector, i)
    type_id = @inbounds vector.type_ids[i]
    child_index = _union_child_index(vector, type_id)
    return @inbounds vector.children[child_index][i]
end

"""
    Arrow.CData.ImportedRunEndEncodedVector

Borrowed run-end encoded vector imported from Arrow C Data Interface child
arrays. Logical indexing searches the borrowed `run_ends` child and indexes the
matching borrowed `values` child. Keep the owning [`ImportedTable`](@ref) live
until done.
"""
struct ImportedRunEndEncodedVector{T,R<:AbstractVector,V<:AbstractVector} <:
       AbstractVector{T}
    run_ends::R
    values::V
    len::Int
    offset::Int
end

function _assert_run_end_type(::Type{T}) where {T}
    T === Int16 ||
        T === Int32 ||
        T === Int64 ||
        throw(
            ArgumentError("run-end encoded run_ends must use signed 16/32/64-bit integers"),
        )
    return nothing
end

function _assert_run_end_shape(
    run_ends::AbstractVector,
    values::AbstractVector,
    len::Int,
    offset::Int,
)
    _assert_run_end_type(eltype(run_ends))
    length(run_ends) == length(values) ||
        throw(ArgumentError("run-end encoded run_ends and values lengths must match"))
    if len == 0
        return nothing
    end
    isempty(run_ends) &&
        throw(ArgumentError("non-empty run-end encoded arrays must have at least one run"))
    previous = 0
    for run_end in run_ends
        current = Int(run_end)
        current > previous ||
            throw(ArgumentError("run-end encoded run_ends must be strictly increasing"))
        previous = current
    end
    previous >= offset + len ||
        throw(ArgumentError("run-end encoded final run_end must cover logical length"))
    return nothing
end

function ImportedRunEndEncodedVector(
    run_ends::R,
    values::V,
    len::Int,
    offset::Int=0,
) where {R,V}
    _assert_run_end_shape(run_ends, values, len, offset)
    return ImportedRunEndEncodedVector{eltype(values),R,V}(run_ends, values, len, offset)
end

Base.IndexStyle(::Type{<:ImportedRunEndEncodedVector}) = Base.IndexLinear()
Base.size(vector::ImportedRunEndEncodedVector) = (vector.len,)

function Base.getindex(vector::ImportedRunEndEncodedVector, i::Int)
    @boundscheck checkbounds(vector, i)
    physical = searchsortedfirst(vector.run_ends, i + vector.offset)
    physical <= length(vector.values) ||
        throw(ArgumentError("run-end encoded value has no matching physical run"))
    return @inbounds vector.values[physical]
end

"""
    Arrow.CData.ImportedStructVector

Borrowed struct vector imported from Arrow C Data Interface child arrays.
Elements are `NamedTuple` rows assembled from borrowed child columns. Keep the
owning [`ImportedTable`](@ref) live until done.
"""
struct ImportedStructVector{T} <: AbstractVector{T}
    names::Vector{Symbol}
    columns::Vector{AbstractVector}
    validity::Vector{UInt8}
    len::Int
    validity_offset::Int
    row_offset::Int
end

function ImportedStructVector(
    names::Vector{Symbol},
    columns::Vector{AbstractVector},
    validity::Vector{UInt8},
    len::Int,
    nullable::Bool,
    row_offset::Int=0,
    validity_offset::Int=0,
)
    field_types = Tuple(eltype.(columns))
    row_type = NamedTuple{Tuple(names),Tuple{field_types...}}
    T = nullable ? Union{Missing,row_type} : row_type
    return ImportedStructVector{T}(
        names,
        columns,
        validity,
        len,
        validity_offset,
        row_offset,
    )
end

Base.IndexStyle(::Type{<:ImportedStructVector}) = Base.IndexLinear()
Base.size(vector::ImportedStructVector) = (vector.len,)

function Base.getindex(vector::ImportedStructVector, i::Int)
    @boundscheck checkbounds(vector, i)
    _validity_bit(vector.validity, i, vector.validity_offset) || return missing
    physical = i + vector.row_offset
    values = Tuple(@inbounds column[physical] for column in vector.columns)
    return NamedTuple{Tuple(vector.names)}(values)
end

"""
    Arrow.CData.ImportedTable

Borrowed Tables.jl-compatible view imported from Arrow C Data Interface base
struct pointers. Release it explicitly with [`Arrow.CData.release!`](@ref).
"""
mutable struct ImportedTable
    schema_base::Ptr{ArrowSchema}
    array_base::Ptr{ArrowArray}
    names::Vector{Symbol}
    columns::Vector{AbstractVector}
end

schema_ptr(table::ImportedTable) = table.schema_base
array_ptr(table::ImportedTable) = table.array_base
schema(table::ImportedTable) = unsafe_load(schema_ptr(table))
array(table::ImportedTable) = unsafe_load(array_ptr(table))
isreleased(table::ImportedTable) =
    schema(table).release == C_NULL && array(table).release == C_NULL

Tables.istable(::Type{ImportedTable}) = true
Tables.columnaccess(::Type{ImportedTable}) = true
Tables.columns(table::ImportedTable) = table
Tables.columnnames(table::ImportedTable) = table.names
Tables.schema(table::ImportedTable) =
    Tables.Schema(Tuple(table.names), Tuple(eltype.(table.columns)))
Tables.getcolumn(table::ImportedTable, index::Int) = table.columns[index]
function Tables.getcolumn(table::ImportedTable, name::Symbol)
    index = findfirst(==(name), table.names)
    index === nothing && throw(KeyError(name))
    return table.columns[index]
end

function release!(table::ImportedTable)
    arr = array(table)
    arr.release == C_NULL || ccall(arr.release, Cvoid, (Ptr{ArrowArray},), array_ptr(table))
    sch = schema(table)
    sch.release == C_NULL ||
        ccall(sch.release, Cvoid, (Ptr{ArrowSchema},), schema_ptr(table))
    return table
end
