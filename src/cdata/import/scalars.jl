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
