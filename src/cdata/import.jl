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

_import_format(schema::ArrowSchema) =
    schema.format == C_NULL ? throw(ArgumentError("ArrowSchema format is C_NULL")) :
    unsafe_string(schema.format)

function _assert_import_live(schema::ArrowSchema, array::ArrowArray)
    schema.release == C_NULL && throw(ArgumentError("cannot import released ArrowSchema"))
    array.release == C_NULL && throw(ArgumentError("cannot import released ArrowArray"))
    return nothing
end

function _assert_import_column_layout(array::ArrowArray, name::Symbol)
    array.length >= 0 || throw(ArgumentError("column $name has negative length"))
    array.null_count >= -1 || throw(ArgumentError("column $name has invalid null count"))
    array.offset >= 0 || throw(ArgumentError("column $name has negative offset"))
    return nothing
end

_nullable_field(schema::ArrowSchema, array::ArrowArray) =
    schema.flags & ARROW_FLAG_NULLABLE != 0 || array.null_count != 0

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

function _assert_binary_like_offsets!(offsets::Vector, name::Symbol, label::AbstractString)
    isempty(offsets) && throw(ArgumentError("$label column $name has empty offsets"))
    previous = Int(first(offsets))
    previous >= 0 || throw(ArgumentError("$label column $name has negative offset"))
    for offset in Iterators.drop(offsets, 1)
        current = Int(offset)
        current >= previous ||
            throw(ArgumentError("$label column $name offsets must be nondecreasing"))
        previous = current
    end
    return previous
end

function _assert_utf8_spans!(
    offsets::Vector,
    data::Vector{UInt8},
    validity::Vector{UInt8},
    len::Integer,
    name::Symbol,
    validity_offset::Int,
)
    for i = 1:Int(len)
        _validity_bit(validity, i, validity_offset) || continue
        start = Int(@inbounds offsets[i]) + 1
        stop = Int(@inbounds offsets[i + 1])
        stop < start && continue
        isvalid(String, @view data[start:stop]) ||
            throw(ArgumentError("UTF-8 column $name value at index $i is not valid UTF-8"))
    end
    return nothing
end

function _import_null_column(array::ArrowArray, name::Symbol)
    _assert_import_column_layout(array, name)
    array.n_children == 0 ||
        throw(ArgumentError("null column $name must not expose children"))
    array.n_buffers == 0 ||
        throw(ArgumentError("null column $name must not expose buffers"))
    return fill(missing, Int(array.length))
end

function _import_bool_column(schema::ArrowSchema, array::ArrowArray, name::Symbol)
    _assert_import_column_layout(array, name)
    array.n_buffers == 2 ||
        throw(ArgumentError("bool column $name must expose two buffers"))
    nullable = _nullable_field(schema, array)
    validity = nullable ? _validity_vector(array, name) : UInt8[]
    data = _bitmap_vector(array, name, 2, "bool")
    offset = _logical_offset(array)
    return ImportedBoolVector(
        data,
        validity,
        _logical_length(array),
        nullable,
        offset,
        offset,
    )
end

function _import_primitive_column(schema::ArrowSchema, array::ArrowArray, name::Symbol)
    _assert_import_column_layout(array, name)
    array.n_buffers == 2 ||
        throw(ArgumentError("primitive column $name must expose two buffers"))
    array.buffers == C_NULL &&
        throw(ArgumentError("primitive column $name has C_NULL buffers"))
    data_ptr = unsafe_load(array.buffers, 2)
    T = _storage_type_for_format(_import_format(schema))
    nullable = _nullable_field(schema, array)
    validity = nullable ? _validity_vector(array, name) : UInt8[]
    array.length == 0 &&
        return nullable ? ImportedNullablePrimitiveVector(T[], validity, 0, 0) : T[]
    data_ptr == C_NULL &&
        throw(ArgumentError("primitive column $name has C_NULL data buffer"))
    offset = _logical_offset(array)
    data = _wrap_buffer(T, data_ptr, array.length, offset)
    return nullable ?
           ImportedNullablePrimitiveVector(data, validity, _logical_length(array), offset) :
           data
end

function _import_string_column(
    schema::ArrowSchema,
    array::ArrowArray,
    name::Symbol,
    ::Type{O},
) where {O<:Union{Int32,Int64}}
    _assert_import_column_layout(array, name)
    array.n_buffers == 3 ||
        throw(ArgumentError("UTF-8 column $name must expose three buffers"))
    array.buffers == C_NULL && throw(ArgumentError("UTF-8 column $name has C_NULL buffers"))
    offsets_ptr = unsafe_load(array.buffers, 2)
    data_ptr = unsafe_load(array.buffers, 3)
    offsets_ptr == C_NULL && throw(ArgumentError("UTF-8 column $name has C_NULL offsets"))
    offset = _logical_offset(array)
    offsets = _wrap_buffer(O, offsets_ptr, _logical_length(array) + 1, offset)
    data_len = _assert_binary_like_offsets!(offsets, name, "UTF-8")
    data_len > 0 &&
        data_ptr == C_NULL &&
        throw(ArgumentError("UTF-8 column $name has C_NULL data buffer"))
    data =
        data_len == 0 ? UInt8[] :
        unsafe_wrap(Vector{UInt8}, Ptr{UInt8}(data_ptr), data_len; own=false)
    values = ImportedStringVector(offsets, data, Int(array.length))
    nullable = _nullable_field(schema, array)
    validity = nullable ? _validity_vector(array, name) : UInt8[]
    _assert_utf8_spans!(offsets, data, validity, _logical_length(array), name, offset)
    return nullable ? ImportedNullableStringVector(values, validity, offset) : values
end

function _import_binary_column(
    schema::ArrowSchema,
    array::ArrowArray,
    name::Symbol,
    ::Type{O},
) where {O<:Union{Int32,Int64}}
    _assert_import_column_layout(array, name)
    array.n_buffers == 3 ||
        throw(ArgumentError("binary column $name must expose three buffers"))
    array.buffers == C_NULL &&
        throw(ArgumentError("binary column $name has C_NULL buffers"))
    offsets_ptr = unsafe_load(array.buffers, 2)
    data_ptr = unsafe_load(array.buffers, 3)
    offsets_ptr == C_NULL && throw(ArgumentError("binary column $name has C_NULL offsets"))
    offset = _logical_offset(array)
    offsets = _wrap_buffer(O, offsets_ptr, _logical_length(array) + 1, offset)
    data_len = _assert_binary_like_offsets!(offsets, name, "binary")
    data_len > 0 &&
        data_ptr == C_NULL &&
        throw(ArgumentError("binary column $name has C_NULL data buffer"))
    data =
        data_len == 0 ? UInt8[] :
        unsafe_wrap(Vector{UInt8}, Ptr{UInt8}(data_ptr), data_len; own=false)
    nullable = _nullable_field(schema, array)
    validity = nullable ? _validity_vector(array, name) : UInt8[]
    return ImportedBinaryVector(
        offsets,
        data,
        validity,
        _logical_length(array),
        nullable,
        offset,
    )
end

function _assert_view_spans!(
    views::Vector{ViewElement},
    buffers::Vector{Vector{UInt8}},
    validity::Vector{UInt8},
    name::Symbol,
    validity_offset::Int,
)
    for i in eachindex(views)
        _validity_bit(validity, i, validity_offset) || continue
        view = @inbounds views[i]
        len = Int(view.length)
        len >= 0 || throw(ArgumentError("view column $name has negative value length"))
        if _viewisinline(len)
            len <= VIEW_INLINE_BYTES ||
                throw(ArgumentError("view column $name has invalid inline length"))
            continue
        end
        buffer_index = Int(view.bufindex) + 1
        1 <= buffer_index <= length(buffers) ||
            throw(ArgumentError("view column $name references missing variadic buffer"))
        offset = Int(view.offset)
        offset >= 0 || throw(ArgumentError("view column $name has negative data offset"))
        offset + len <= length(buffers[buffer_index]) || throw(
            ArgumentError("view column $name references bytes outside variadic buffer"),
        )
    end
    return nothing
end

function _assert_utf8_view_spans!(
    views::Vector{ViewElement},
    inline::Vector{UInt8},
    buffers::Vector{Vector{UInt8}},
    validity::Vector{UInt8},
    name::Symbol,
    validity_offset::Int,
)
    for i in eachindex(views)
        _validity_bit(validity, i, validity_offset) || continue
        view = @inbounds views[i]
        len = Int(view.length)
        len == 0 && continue
        valid = if _viewisinline(len)
            start = _view_inline_start(i)
            isvalid(String, @view inline[start:(start + len - 1)])
        else
            buffer = @inbounds buffers[Int(view.bufindex) + 1]
            start = Int(view.offset) + 1
            isvalid(String, @view buffer[start:(start + len - 1)])
        end
        valid || throw(
            ArgumentError("UTF-8 view column $name value at index $i is not valid UTF-8"),
        )
    end
    return nothing
end

function _import_view_column(
    schema::ArrowSchema,
    array::ArrowArray,
    name::Symbol,
    utf8::Bool,
)
    schema.n_children == 0 ||
        throw(ArgumentError("view column $name must not expose child schemas"))
    array.n_children == 0 ||
        throw(ArgumentError("view column $name must not expose child arrays"))
    _assert_import_column_layout(array, name)
    array.n_buffers >= 3 || throw(
        ArgumentError("view column $name must expose validity, views, and lengths buffers"),
    )
    array.buffers == C_NULL && throw(ArgumentError("view column $name has C_NULL buffers"))
    views_ptr = unsafe_load(array.buffers, 2)
    offset = _logical_offset(array)
    array.length > 0 &&
        views_ptr == C_NULL &&
        throw(ArgumentError("view column $name has C_NULL views buffer"))
    views =
        array.length == 0 ? ViewElement[] :
        _wrap_buffer(ViewElement, views_ptr, _logical_length(array), offset)
    inline =
        array.length == 0 ? UInt8[] :
        _wrap_byte_buffer(
            views_ptr,
            _logical_length(array) * VIEW_ELEMENT_BYTES,
            offset * VIEW_ELEMENT_BYTES,
        )
    n_variadic = Int(array.n_buffers) - 3
    lengths_ptr = unsafe_load(array.buffers, Int(array.n_buffers))
    n_variadic > 0 &&
        lengths_ptr == C_NULL &&
        throw(ArgumentError("view column $name has C_NULL variadic lengths buffer"))
    lengths =
        n_variadic == 0 ? Int64[] :
        unsafe_wrap(Vector{Int64}, Ptr{Int64}(lengths_ptr), n_variadic; own=false)
    buffers = Vector{UInt8}[]
    for buffer_index = 1:n_variadic
        data_ptr = unsafe_load(array.buffers, buffer_index + 2)
        data_len = Int(@inbounds lengths[buffer_index])
        data_len >= 0 ||
            throw(ArgumentError("view column $name has negative variadic buffer length"))
        data_len > 0 &&
            data_ptr == C_NULL &&
            throw(ArgumentError("view column $name has C_NULL variadic data buffer"))
        push!(
            buffers,
            data_len == 0 ? UInt8[] :
            unsafe_wrap(Vector{UInt8}, Ptr{UInt8}(data_ptr), data_len; own=false),
        )
    end
    nullable = _nullable_field(schema, array)
    validity = nullable ? _validity_vector(array, name) : UInt8[]
    _assert_view_spans!(views, buffers, validity, name, offset)
    if utf8
        _assert_utf8_view_spans!(views, inline, buffers, validity, name, offset)
        return ImportedStringViewVector(
            views,
            inline,
            buffers,
            lengths,
            validity,
            Int(array.length),
            nullable,
            offset,
        )
    end
    return ImportedBinaryViewVector(
        views,
        inline,
        buffers,
        lengths,
        validity,
        Int(array.length),
        nullable,
        offset,
    )
end

function _fixed_size_from_format(format::AbstractString, prefix::AbstractString)
    startswith(format, prefix) || throw(
        ArgumentError("Arrow C Data import does not support fixed-size format $format"),
    )
    size_text = format[(lastindex(prefix) + 1):end]
    list_size = tryparse(Int, size_text)
    list_size === nothing &&
        throw(ArgumentError("Arrow C Data import has invalid fixed-size format $format"))
    list_size > 0 ||
        throw(ArgumentError("Arrow C Data import fixed-size format must be positive"))
    return list_size
end

function _import_fixed_size_binary_column(
    schema::ArrowSchema,
    array::ArrowArray,
    name::Symbol,
    format::AbstractString,
)
    schema.n_children == 0 ||
        throw(ArgumentError("fixed-size-binary column $name must not expose children"))
    array.n_children == 0 ||
        throw(ArgumentError("fixed-size-binary column $name must not expose child arrays"))
    _assert_import_column_layout(array, name)
    array.n_buffers == 2 ||
        throw(ArgumentError("fixed-size-binary column $name must expose two buffers"))
    array.buffers == C_NULL &&
        throw(ArgumentError("fixed-size-binary column $name has C_NULL buffers"))
    width = _fixed_size_from_format(format, "w:")
    data_ptr = unsafe_load(array.buffers, 2)
    nbytes = width * Int(array.length)
    offset = _logical_offset(array)
    data =
        nbytes == 0 ? UInt8[] :
        data_ptr == C_NULL ?
        throw(ArgumentError("fixed-size-binary column $name has C_NULL data buffer")) :
        _wrap_byte_buffer(data_ptr, nbytes, offset * width)
    nullable = _nullable_field(schema, array)
    validity = nullable ? _validity_vector(array, name) : UInt8[]
    return ImportedFixedSizeBinaryVector(
        Val(width),
        data,
        validity,
        Int(array.length),
        nullable,
        offset,
    )
end

function _import_fixed_size_list_column(
    schema::ArrowSchema,
    array::ArrowArray,
    name::Symbol,
    format::AbstractString,
)
    schema.n_children == 1 ||
        throw(ArgumentError("fixed-size-list column $name must expose one child schema"))
    array.n_children == 1 ||
        throw(ArgumentError("fixed-size-list column $name must expose one child array"))
    schema.children == C_NULL &&
        throw(ArgumentError("fixed-size-list column $name has C_NULL children"))
    array.children == C_NULL &&
        throw(ArgumentError("fixed-size-list column $name has C_NULL child arrays"))
    _assert_import_column_layout(array, name)
    array.n_buffers == 1 ||
        throw(ArgumentError("fixed-size-list column $name must expose one validity buffer"))
    array.buffers == C_NULL &&
        throw(ArgumentError("fixed-size-list column $name has C_NULL buffers"))
    list_size = _fixed_size_from_format(format, "+w:")
    _, values =
        _import_column(unsafe_load(schema.children, 1), unsafe_load(array.children, 1))
    length(values) >= list_size * Int(array.length) ||
        throw(ArgumentError("fixed-size-list column $name child array is too short"))
    offset = _logical_offset(array)
    length(values) >= list_size * (offset + Int(array.length)) ||
        throw(ArgumentError("fixed-size-list column $name child array is too short"))
    nullable = _nullable_field(schema, array)
    validity = nullable ? _validity_vector(array, name) : UInt8[]
    return ImportedFixedSizeListVector(
        Val(list_size),
        values,
        validity,
        Int(array.length),
        nullable,
        offset * list_size,
        offset,
    )
end

function _import_dictionary_column(schema::ArrowSchema, array::ArrowArray, name::Symbol)
    schema.dictionary == C_NULL &&
        throw(ArgumentError("dictionary column $name has C_NULL dictionary schema"))
    array.dictionary == C_NULL &&
        throw(ArgumentError("dictionary column $name has C_NULL dictionary array"))
    schema.n_children == 0 || throw(
        ArgumentError(
            "Arrow C Data import does not yet support nested dictionary column $name",
        ),
    )
    array.n_children == 0 || throw(
        ArgumentError(
            "Arrow C Data import does not yet support nested dictionary array $name",
        ),
    )
    _assert_import_column_layout(array, name)
    array.n_buffers == 2 ||
        throw(ArgumentError("dictionary column $name must expose two index buffers"))
    array.buffers == C_NULL &&
        throw(ArgumentError("dictionary column $name has C_NULL buffers"))

    _, dictionary = _import_column(schema.dictionary, array.dictionary)

    index_ptr = unsafe_load(array.buffers, 2)
    I = _storage_type_for_format(_import_format(schema))
    nullable = _nullable_field(schema, array)
    validity = nullable ? _validity_vector(array, name) : UInt8[]
    array.length == 0 &&
        return ImportedDictionaryVector(I[], dictionary, validity, 0, nullable, 0)
    index_ptr == C_NULL &&
        throw(ArgumentError("dictionary column $name has C_NULL indices buffer"))
    offset = _logical_offset(array)
    indices = _wrap_buffer(I, index_ptr, _logical_length(array), offset)
    return ImportedDictionaryVector(
        indices,
        dictionary,
        validity,
        Int(array.length),
        nullable,
        offset,
    )
end

function _list_offset_type_for_format(format::AbstractString)
    format == "+l" && return Int32
    format == "+L" && return Int64
    throw(ArgumentError("Arrow C Data import does not support list format $format"))
end

function _list_view_offset_type_for_format(format::AbstractString)
    format == "+vl" && return Int32
    format == "+vL" && return Int64
    throw(ArgumentError("Arrow C Data import does not support list-view format $format"))
end

function _assert_list_offsets!(offsets::Vector, values::AbstractVector, name::Symbol)
    isempty(offsets) && throw(ArgumentError("list column $name has empty offsets"))
    previous = Int(first(offsets))
    previous >= 0 || throw(ArgumentError("list column $name has negative offset"))
    for offset in Iterators.drop(offsets, 1)
        current = Int(offset)
        current >= previous ||
            throw(ArgumentError("list column $name offsets must be nondecreasing"))
        previous = current
    end
    previous <= length(values) ||
        throw(ArgumentError("list column $name offsets exceed child value length"))
    return nothing
end

function _assert_list_view_spans!(
    offsets::Vector,
    sizes::Vector,
    values::AbstractVector,
    name::Symbol,
)
    length(offsets) == length(sizes) ||
        throw(ArgumentError("list-view column $name offsets and sizes length mismatch"))
    for i in eachindex(offsets)
        offset = Int(@inbounds offsets[i])
        size = Int(@inbounds sizes[i])
        offset >= 0 || throw(ArgumentError("list-view column $name has negative offset"))
        size >= 0 || throw(ArgumentError("list-view column $name has negative size"))
        offset + size <= length(values) ||
            throw(ArgumentError("list-view column $name span exceeds child value length"))
    end
    return nothing
end

function _import_list_column(
    schema::ArrowSchema,
    array::ArrowArray,
    name::Symbol,
    format::AbstractString,
)
    schema.n_children == 1 ||
        throw(ArgumentError("list column $name must expose one child schema"))
    array.n_children == 1 ||
        throw(ArgumentError("list column $name must expose one child array"))
    schema.children == C_NULL &&
        throw(ArgumentError("list column $name has C_NULL children"))
    array.children == C_NULL &&
        throw(ArgumentError("list column $name has C_NULL child arrays"))
    _assert_import_column_layout(array, name)
    array.n_buffers == 2 ||
        throw(ArgumentError("list column $name must expose validity and offsets buffers"))
    array.buffers == C_NULL && throw(ArgumentError("list column $name has C_NULL buffers"))
    offsets_ptr = unsafe_load(array.buffers, 2)
    offsets_ptr == C_NULL && throw(ArgumentError("list column $name has C_NULL offsets"))

    _, values =
        _import_column(unsafe_load(schema.children, 1), unsafe_load(array.children, 1))
    O = _list_offset_type_for_format(format)
    offset = _logical_offset(array)
    offsets = _wrap_buffer(O, offsets_ptr, _logical_length(array) + 1, offset)
    _assert_list_offsets!(offsets, values, name)
    nullable = _nullable_field(schema, array)
    validity = nullable ? _validity_vector(array, name) : UInt8[]
    return ImportedListVector(
        offsets,
        values,
        validity,
        _logical_length(array),
        nullable,
        offset,
    )
end

function _import_list_view_column(
    schema::ArrowSchema,
    array::ArrowArray,
    name::Symbol,
    format::AbstractString,
)
    schema.n_children == 1 ||
        throw(ArgumentError("list-view column $name must expose one child schema"))
    array.n_children == 1 ||
        throw(ArgumentError("list-view column $name must expose one child array"))
    schema.children == C_NULL &&
        throw(ArgumentError("list-view column $name has C_NULL children"))
    array.children == C_NULL &&
        throw(ArgumentError("list-view column $name has C_NULL child arrays"))
    _assert_import_column_layout(array, name)
    array.n_buffers == 3 || throw(
        ArgumentError(
            "list-view column $name must expose validity, offsets, and sizes buffers",
        ),
    )
    array.buffers == C_NULL &&
        throw(ArgumentError("list-view column $name has C_NULL buffers"))
    offsets_ptr = unsafe_load(array.buffers, 2)
    sizes_ptr = unsafe_load(array.buffers, 3)
    array.length > 0 &&
        offsets_ptr == C_NULL &&
        throw(ArgumentError("list-view column $name has C_NULL offsets"))
    array.length > 0 &&
        sizes_ptr == C_NULL &&
        throw(ArgumentError("list-view column $name has C_NULL sizes"))

    _, values =
        _import_column(unsafe_load(schema.children, 1), unsafe_load(array.children, 1))
    O = _list_view_offset_type_for_format(format)
    offset = _logical_offset(array)
    offsets =
        array.length == 0 ? O[] :
        _wrap_buffer(O, offsets_ptr, _logical_length(array), offset)
    sizes =
        array.length == 0 ? O[] : _wrap_buffer(O, sizes_ptr, _logical_length(array), offset)
    _assert_list_view_spans!(offsets, sizes, values, name)
    nullable = _nullable_field(schema, array)
    validity = nullable ? _validity_vector(array, name) : UInt8[]
    return ImportedListViewVector(
        offsets,
        sizes,
        values,
        validity,
        Int(array.length),
        nullable,
        offset,
    )
end

function _import_map_column(schema::ArrowSchema, array::ArrowArray, name::Symbol)
    schema.n_children == 1 ||
        throw(ArgumentError("map column $name must expose one entries schema"))
    array.n_children == 1 ||
        throw(ArgumentError("map column $name must expose one entries array"))
    schema.children == C_NULL &&
        throw(ArgumentError("map column $name has C_NULL children"))
    array.children == C_NULL &&
        throw(ArgumentError("map column $name has C_NULL child arrays"))
    _assert_import_column_layout(array, name)
    array.n_buffers == 2 ||
        throw(ArgumentError("map column $name must expose validity and offsets buffers"))
    array.buffers == C_NULL && throw(ArgumentError("map column $name has C_NULL buffers"))
    offsets_ptr = unsafe_load(array.buffers, 2)
    offsets_ptr == C_NULL && throw(ArgumentError("map column $name has C_NULL offsets"))

    _, entries =
        _import_column(unsafe_load(schema.children, 1), unsafe_load(array.children, 1))
    offset = _logical_offset(array)
    offsets = unsafe_wrap(
        Vector{Int32},
        _buffer_pointer(Int32, offsets_ptr, offset),
        Int(array.length) + 1;
        own=false,
    )
    _assert_list_offsets!(offsets, entries, name)
    nullable = _nullable_field(schema, array)
    validity = nullable ? _validity_vector(array, name) : UInt8[]
    return ImportedMapVector(
        offsets,
        entries,
        validity,
        _logical_length(array),
        nullable,
        offset,
    )
end

function _parse_union_format(format::AbstractString)
    dense = startswith(format, "+ud:")
    sparse = startswith(format, "+us:")
    dense ||
        sparse ||
        throw(ArgumentError("Arrow C Data import does not support union format $format"))
    id_text = format[5:end]
    isempty(id_text) &&
        throw(ArgumentError("Arrow C Data import union format must declare type ids"))
    declared_ids = UInt8[]
    for part in split(id_text, ",")
        id = tryparse(Int, part)
        id === nothing &&
            throw(ArgumentError("Arrow C Data import has invalid union type id in $format"))
        0 <= id <= typemax(UInt8) ||
            throw(ArgumentError("Arrow C Data import union type id out of UInt8 range"))
        push!(declared_ids, UInt8(id))
    end
    length(unique(declared_ids)) == length(declared_ids) ||
        throw(ArgumentError("Arrow C Data import requires unique union type ids"))
    return dense, declared_ids
end

function _import_union_type_ids(array::ArrowArray, name::Symbol)
    array.buffers == C_NULL && throw(ArgumentError("union column $name has C_NULL buffers"))
    type_ids_ptr = unsafe_load(array.buffers, 1)
    array.length == 0 && return UInt8[]
    type_ids_ptr == C_NULL && throw(ArgumentError("union column $name has C_NULL type ids"))
    return _wrap_buffer(UInt8, type_ids_ptr, _logical_length(array), _logical_offset(array))
end

function _assert_union_type_ids!(
    type_ids::Vector{UInt8},
    declared_ids::Vector{UInt8},
    name::Symbol,
)
    declared = Set(declared_ids)
    for type_id in type_ids
        type_id in declared || throw(
            ArgumentError(
                "union column $name references undeclared type id $(Int(type_id))",
            ),
        )
    end
    return nothing
end

function _assert_dense_union_offsets!(
    offsets::Vector{Int32},
    type_ids::Vector{UInt8},
    children::Tuple,
    declared_ids::Vector{UInt8},
    name::Symbol,
)
    id_to_child = _union_type_id_map(declared_ids)
    for (index, type_id) in enumerate(type_ids)
        child_index = id_to_child[type_id]
        offset = Int(@inbounds offsets[index]) + 1
        1 <= offset <= length(children[child_index]) ||
            throw(ArgumentError("dense union column $name has child offset out of bounds"))
    end
    return nothing
end

function _assert_sparse_union_children!(children::Tuple, len::Int, name::Symbol)
    for child in children
        length(child) == len || throw(
            ArgumentError("sparse union column $name child length must match row count"),
        )
    end
    return nothing
end

function _import_union_column(
    schema::ArrowSchema,
    array::ArrowArray,
    name::Symbol,
    format::AbstractString,
)
    dense, declared_ids = _parse_union_format(format)
    schema.n_children == length(declared_ids) ||
        throw(ArgumentError("union column $name child schema count must match type ids"))
    array.n_children == length(declared_ids) ||
        throw(ArgumentError("union column $name child array count must match type ids"))
    schema.children == C_NULL &&
        throw(ArgumentError("union column $name has C_NULL children"))
    array.children == C_NULL &&
        throw(ArgumentError("union column $name has C_NULL child arrays"))
    _assert_import_column_layout(array, name)
    array.null_count == 0 ||
        throw(ArgumentError("union column $name must not expose parent nulls"))
    expected_buffers = dense ? 2 : 1
    array.n_buffers == expected_buffers ||
        throw(ArgumentError("union column $name has invalid buffer count"))

    children = Tuple(
        last(
            _import_column(
                unsafe_load(schema.children, index),
                unsafe_load(array.children, index),
            ),
        ) for index = 1:schema.n_children
    )
    type_ids = _import_union_type_ids(array, name)
    _assert_union_type_ids!(type_ids, declared_ids, name)
    if dense
        offsets_ptr = unsafe_load(array.buffers, 2)
        array.length > 0 &&
            offsets_ptr == C_NULL &&
            throw(ArgumentError("dense union column $name has C_NULL offsets"))
        offsets =
            array.length == 0 ? Int32[] :
            _wrap_buffer(Int32, offsets_ptr, _logical_length(array), _logical_offset(array))
        _assert_dense_union_offsets!(offsets, type_ids, children, declared_ids, name)
        return ImportedDenseUnionVector(
            type_ids,
            offsets,
            children,
            declared_ids,
            Int(array.length),
        )
    end
    _assert_sparse_union_children!(children, Int(array.length), name)
    return ImportedSparseUnionVector(type_ids, children, declared_ids, Int(array.length))
end

function _import_run_end_encoded_column(
    schema::ArrowSchema,
    array::ArrowArray,
    name::Symbol,
)
    schema.n_children == 2 ||
        throw(ArgumentError("run-end encoded column $name must expose two child schemas"))
    array.n_children == 2 ||
        throw(ArgumentError("run-end encoded column $name must expose two child arrays"))
    schema.children == C_NULL &&
        throw(ArgumentError("run-end encoded column $name has C_NULL children"))
    array.children == C_NULL &&
        throw(ArgumentError("run-end encoded column $name has C_NULL child arrays"))
    _assert_import_column_layout(array, name)
    array.null_count == 0 ||
        throw(ArgumentError("run-end encoded column $name must not expose parent nulls"))
    array.n_buffers == 0 ||
        throw(ArgumentError("run-end encoded column $name must not expose parent buffers"))
    run_ends_name, run_ends =
        _import_column(unsafe_load(schema.children, 1), unsafe_load(array.children, 1))
    values_name, values =
        _import_column(unsafe_load(schema.children, 2), unsafe_load(array.children, 2))
    run_ends_name == :run_ends ||
        throw(ArgumentError("run-end encoded column $name first child must be run_ends"))
    values_name == :values ||
        throw(ArgumentError("run-end encoded column $name second child must be values"))
    return ImportedRunEndEncodedVector(
        run_ends,
        values,
        Int(array.length),
        _logical_offset(array),
    )
end

function _import_struct_column(schema::ArrowSchema, array::ArrowArray, name::Symbol)
    schema.n_children == array.n_children ||
        throw(ArgumentError("struct column $name schema/array child count mismatch"))
    array.n_buffers == 1 ||
        throw(ArgumentError("struct column $name must expose one validity buffer"))
    array.buffers == C_NULL &&
        throw(ArgumentError("struct column $name has C_NULL buffers"))
    schema.n_children > 0 &&
        schema.children == C_NULL &&
        throw(ArgumentError("struct column $name has C_NULL children"))
    array.n_children > 0 &&
        array.children == C_NULL &&
        throw(ArgumentError("struct column $name has C_NULL child arrays"))
    _assert_import_column_layout(array, name)
    names = Symbol[]
    columns = AbstractVector[]
    for i = 1:schema.n_children
        child_name, child =
            _import_column(unsafe_load(schema.children, i), unsafe_load(array.children, i))
        push!(names, child_name)
        push!(columns, child)
    end
    nullable = _nullable_field(schema, array)
    validity = nullable ? _validity_vector(array, name) : UInt8[]
    return ImportedStructVector(
        names,
        columns,
        validity,
        _logical_length(array),
        nullable,
        _logical_offset(array),
        _logical_offset(array),
    )
end

_metadata_from_schema_ptr(schema_ptr::Ptr{ArrowSchema}) =
    _metadata_from_ptr(unsafe_load(schema_ptr).metadata)

function _import_column_data(schema_ptr::Ptr{ArrowSchema}, array_ptr::Ptr{ArrowArray})
    schema_ptr == C_NULL && throw(ArgumentError("child ArrowSchema pointer is C_NULL"))
    array_ptr == C_NULL && throw(ArgumentError("child ArrowArray pointer is C_NULL"))
    schema = unsafe_load(schema_ptr)
    array = unsafe_load(array_ptr)
    _assert_import_live(schema, array)
    name = schema.name == C_NULL ? Symbol("") : Symbol(unsafe_string(schema.name))
    if schema.dictionary != C_NULL || array.dictionary != C_NULL
        return name, _import_dictionary_column(schema, array, name)
    end
    format = _import_format(schema)
    format == "+m" && return name, _import_map_column(schema, array, name)
    format == "+r" && return name, _import_run_end_encoded_column(schema, array, name)
    (startswith(format, "+ud:") || startswith(format, "+us:")) &&
        return name, _import_union_column(schema, array, name, format)
    (format == "+vl" || format == "+vL") &&
        return name, _import_list_view_column(schema, array, name, format)
    (format == "+l" || format == "+L") &&
        return name, _import_list_column(schema, array, name, format)
    startswith(format, "+w:") &&
        return name, _import_fixed_size_list_column(schema, array, name, format)
    format == "+s" && return name, _import_struct_column(schema, array, name)
    schema.n_children == 0 ||
        throw(ArgumentError("Arrow C Data import does not yet support nested column $name"))
    array.n_children == 0 ||
        throw(ArgumentError("Arrow C Data import does not yet support nested array $name"))
    format == "vu" && return name, _import_view_column(schema, array, name, true)
    format == "vz" && return name, _import_view_column(schema, array, name, false)
    format == "u" && return name, _import_string_column(schema, array, name, Int32)
    format == "U" && return name, _import_string_column(schema, array, name, Int64)
    format == "z" && return name, _import_binary_column(schema, array, name, Int32)
    format == "Z" && return name, _import_binary_column(schema, array, name, Int64)
    startswith(format, "w:") &&
        return name, _import_fixed_size_binary_column(schema, array, name, format)
    format == "n" && return name, _import_null_column(array, name)
    format == "b" && return name, _import_bool_column(schema, array, name)
    return name, _import_primitive_column(schema, array, name)
end

function _import_column(schema_ptr::Ptr{ArrowSchema}, array_ptr::Ptr{ArrowArray})
    name, column = _import_column_data(schema_ptr, array_ptr)
    metadata = _metadata_from_schema_ptr(schema_ptr)
    return name, _wrap_imported_metadata(column, metadata)
end

"""
    Arrow.CData.importtable(schema_ptr, array_ptr) -> Arrow.CData.ImportedTable

Borrow a top-level Arrow C Data Interface struct array as a Tables.jl-compatible
Julia view. This import surface supports primitive, boolean, UTF-8, binary,
UTF-8 view, binary view, list, fixed-size, map, dense/sparse union, run-end
encoded, list-view, struct, dictionary encoded, null, and logical scalar
columns with nullable field validity. Call [`Arrow.CData.release!`](@ref) on
the imported table when done.
"""
function importtable(schema_ptr::Ptr{ArrowSchema}, array_ptr::Ptr{ArrowArray})
    schema_ptr == C_NULL &&
        throw(ArgumentError("ArrowSchema input pointer must not be C_NULL"))
    array_ptr == C_NULL &&
        throw(ArgumentError("ArrowArray input pointer must not be C_NULL"))
    schema = unsafe_load(schema_ptr)
    array = unsafe_load(array_ptr)
    _assert_import_live(schema, array)
    _import_format(schema) == "+s" || throw(
        ArgumentError("Arrow C Data import currently requires top-level struct format +s"),
    )
    schema.n_children == array.n_children ||
        throw(ArgumentError("Arrow C Data import schema/array child count mismatch"))
    array.offset >= 0 || throw(ArgumentError("top-level struct array has negative offset"))
    array.n_buffers == 1 ||
        throw(ArgumentError("top-level struct array must expose one buffer"))
    array.buffers == C_NULL &&
        throw(ArgumentError("top-level struct array has C_NULL buffers"))
    schema.n_children > 0 &&
        schema.children == C_NULL &&
        throw(ArgumentError("top-level struct schema has C_NULL children"))
    array.n_children > 0 &&
        array.children == C_NULL &&
        throw(ArgumentError("top-level struct array has C_NULL children"))
    names = Symbol[]
    imported_columns = AbstractVector[]
    top_validity =
        _nullable_field(schema, array) ?
        _validity_vector(array, Symbol("top-level struct")) : UInt8[]
    top_offset = _logical_offset(array)
    for i = 1:schema.n_children
        child_schema_ptr = unsafe_load(schema.children, i)
        child_array_ptr = unsafe_load(array.children, i)
        name, column = _import_column_data(child_schema_ptr, child_array_ptr)
        push!(names, name)
        metadata = _metadata_from_schema_ptr(child_schema_ptr)
        imported_column =
            top_offset == 0 ? column :
            begin
                stop = top_offset + _logical_length(array)
                length(column) >= stop || throw(
                    ArgumentError("top-level struct child $name is too short for offset"),
                )
                @view column[(top_offset + 1):stop]
            end
        imported_column =
            _wrap_imported_row_validity(imported_column, top_validity, top_offset)
        push!(imported_columns, _wrap_imported_metadata(imported_column, metadata))
    end
    return ImportedTable(
        schema_ptr,
        array_ptr,
        names,
        imported_columns,
        _metadata_from_ptr(schema.metadata),
    )
end
