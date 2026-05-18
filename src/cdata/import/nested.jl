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
    _assert_dictionary_indices!(indices, dictionary, validity, offset, name)
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
    _assert_list_offsets!(offsets, entries, name, "map")
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
