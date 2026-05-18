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

_metadata_from_schema_ptr(schema_ptr::Ptr{ArrowSchema}) =
    _metadata_from_ptr(unsafe_load(schema_ptr).metadata)

function _import_column_data(schema_ptr::Ptr{ArrowSchema}, array_ptr::Ptr{ArrowArray})
    schema_ptr == C_NULL && throw(ArgumentError("child ArrowSchema pointer is C_NULL"))
    array_ptr == C_NULL && throw(ArgumentError("child ArrowArray pointer is C_NULL"))
    schema = unsafe_load(schema_ptr)
    array = unsafe_load(array_ptr)
    _assert_import_live(schema, array)
    name = schema.name == C_NULL ? Symbol("") : Symbol(unsafe_string(schema.name))
    _assert_import_schema_layout(schema, name)
    _assert_import_column_layout(array, name)
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
    _assert_import_schema_layout(schema, Symbol("top-level struct"))
    _assert_import_column_layout(array, Symbol("top-level struct"))
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
