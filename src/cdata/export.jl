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

function _schema_export(
    format::AbstractString,
    name::AbstractString;
    flags::Integer=0,
    metadata=nothing,
    children::Vector{SchemaExport}=SchemaExport[],
    dictionary::Union{Nothing,SchemaExport}=nothing,
)
    strings = [_cstring(format), _cstring(name)]
    metadata_bytes = _metadata_bytes(metadata)
    child_refs = [child.ref for child in children]
    child_handles = [child.handle for child in children]
    child_ptrs = Ptr{ArrowSchema}[
        Base.unsafe_convert(Ptr{ArrowSchema}, child.ref) for child in children
    ]
    dictionary_ref = dictionary === nothing ? nothing : dictionary.ref
    dictionary_handle = dictionary === nothing ? nothing : dictionary.handle
    dictionary_ptr =
        dictionary === nothing ?
        Ptr{ArrowSchema}(C_NULL) :
        Base.unsafe_convert(Ptr{ArrowSchema}, dictionary.ref)
    handle = SchemaHandle(
        strings,
        metadata_bytes,
        child_refs,
        child_handles,
        child_ptrs,
        dictionary_ref,
        dictionary_handle,
        dictionary_ptr,
        C_NULL,
    )
    private_data = _retain!(handle)
    handle.private_data = private_data
    schema = ArrowSchema(
        _cstring_ptr(strings[1]),
        _cstring_ptr(strings[2]),
        _metadata_ptr(metadata_bytes),
        Int64(flags),
        Int64(length(children)),
        _children_ptr(child_ptrs),
        dictionary_ptr,
        _schema_release_callback_ptr(),
        private_data,
    )
    return SchemaExport(Ref(schema), handle)
end

function _unwrap_column(column)
    unwrapped = _metadatavectordata(column)
    unwrapped === column && return column
    return _unwrap_column(unwrapped)
end

function _schema_export_for_column(name::Symbol, column)
    metadata = getmetadata(column)
    col = _unwrap_column(column)
    flags = _nullable_flags(eltype(col))
    if col isa NullVector
        return _schema_export("n", String(name); flags=flags, metadata=metadata)
    elseif col isa DictEncoded
        indices = getfield(col, :indices)
        encoding = getfield(col, :encoding)
        encoding_schema = _schema_export_for_column(Symbol(""), getfield(encoding, :data))
        getfield(encoding, :isOrdered) &&
            (flags |= ARROW_FLAG_DICTIONARY_ORDERED)
        return _schema_export(
            _format_for_storage_type(eltype(indices)),
            String(name);
            flags=flags,
            metadata=metadata,
            dictionary=encoding_schema,
        )
    elseif col isa List
        format = _list_format(col)
        if liststringtype(col)
            return _schema_export(format, String(name); flags=flags, metadata=metadata)
        end
        child_schema = _schema_export_for_column(:item, getfield(col, :data))
        return _schema_export(
            format,
            String(name);
            flags=flags,
            metadata=metadata,
            children=SchemaExport[child_schema],
        )
    elseif col isa View
        return _schema_export(
            _view_format(col),
            String(name);
            flags=flags,
            metadata=metadata,
        )
    elseif col isa Map
        _assert_standard_map_offsets(col)
        _assert_map_offsets_shape(col)
        entries_schema = _schema_export_for_column(:entries, getfield(col, :data))
        return _schema_export(
            "+m",
            String(name);
            flags=flags,
            metadata=metadata,
            children=SchemaExport[entries_schema],
        )
    elseif col isa RunEndEncoded
        run_ends_schema = _schema_export_for_column(:run_ends, getfield(col, :run_ends))
        values_schema = _schema_export_for_column(:values, getfield(col, :values))
        return _schema_export(
            "+r",
            String(name);
            flags=flags,
            metadata=metadata,
            children=SchemaExport[run_ends_schema, values_schema],
        )
    elseif col isa Union{DenseUnion,SparseUnion}
        child_schemas =
            SchemaExport[_schema_export_for_column(Symbol(""), child) for
                         child in getfield(col, :data)]
        return _schema_export(
            _union_format(col),
            String(name);
            flags=flags,
            metadata=metadata,
            children=child_schemas,
        )
    elseif col isa Struct
        child_schemas =
            SchemaExport[_schema_export_for_column(field_name, child) for
                         (field_name, child) in zip(_struct_field_names(col), getfield(col, :data))]
        return _schema_export(
            "+s",
            String(name);
            flags=flags,
            metadata=metadata,
            children=child_schemas,
        )
    elseif col isa FixedSizeList
        list_size = _fixed_size_list_size(col)
        data = getfield(col, :data)
        if eltype(data) === UInt8
            return _schema_export(
                "w:$list_size",
                String(name);
                flags=flags,
                metadata=metadata,
            )
        end
        child_schema = _schema_export_for_column(:item, data)
        return _schema_export(
            "+w:$list_size",
            String(name);
            flags=flags,
            metadata=metadata,
            children=SchemaExport[child_schema],
        )
    elseif col isa Primitive
        data = getfield(col, :data)
        return _schema_export(
            _format_for_storage_type(eltype(data)),
            String(name);
            flags=flags,
            metadata=metadata,
        )
    elseif col isa BoolVector
        return _schema_export("b", String(name); flags=flags, metadata=metadata)
    end
    throw(
        ArgumentError(
            "Arrow C Data export currently supports NullVector, Primitive, BoolVector, List, View, FixedSizeList, Map, DenseUnion, SparseUnion, RunEndEncoded, Struct, and DictEncoded columns; got $(typeof(col)) for column $(name)",
        ),
    )
end

function _schema_export_for_table(table::Table)
    child_schemas =
        SchemaExport[_schema_export_for_column(name, column) for
                     (name, column) in zip(names(table), columns(table))]
    return _schema_export("+s", ""; metadata=getmetadata(table), children=child_schemas)
end

function _validity_pointer(column)
    validity = validitybitmap(column)
    validity.nc == 0 && return Ptr{Cvoid}(C_NULL)
    return Ptr{Cvoid}(pointer(validity.bytes, validity.pos))
end

function _primitive_data_pointer(data)
    isempty(data) && return Ptr{Cvoid}(C_NULL)
    return Ptr{Cvoid}(pointer(data))
end

function _bool_data_pointer(column::BoolVector)
    length(column) == 0 && return Ptr{Cvoid}(C_NULL)
    return Ptr{Cvoid}(pointer(getfield(column, :arrow), getfield(column, :pos)))
end

function _offsets_pointer(column::Union{List,Map})
    offsets = getfield(getfield(column, :offsets), :offsets)
    return _primitive_data_pointer(offsets)
end

function _list_data_pointer(column::List)
    data = getfield(column, :data)
    data isa AbstractVector{UInt8} ||
        throw(
            ArgumentError(
                "Arrow C Data export expected string/binary List data as UInt8 storage; got $(typeof(data))",
            ),
        )
    return _primitive_data_pointer(data)
end

function _view_variadic_lengths(column::View)
    return Int64[length(buffer) for buffer in getfield(column, :buffers)]
end

function _view_buffer_pointers(column::View, lengths::Vector{Int64})
    buffers = Ptr{Cvoid}[
        _validity_pointer(column),
        _primitive_data_pointer(getfield(column, :data)),
    ]
    append!(buffers, Ptr{Cvoid}[_primitive_data_pointer(buffer) for buffer in getfield(column, :buffers)])
    push!(buffers, _primitive_data_pointer(lengths))
    return buffers
end

function _array_export(
    source,
    len::Integer,
    null_count::Integer,
    buffers::Vector{Ptr{Cvoid}};
    children::Vector{ArrayExport}=ArrayExport[],
    dictionary::Union{Nothing,ArrayExport}=nothing,
)
    child_refs = [child.ref for child in children]
    child_handles = [child.handle for child in children]
    child_ptrs = Ptr{ArrowArray}[
        Base.unsafe_convert(Ptr{ArrowArray}, child.ref) for child in children
    ]
    dictionary_ref = dictionary === nothing ? nothing : dictionary.ref
    dictionary_handle = dictionary === nothing ? nothing : dictionary.handle
    dictionary_ptr =
        dictionary === nothing ?
        Ptr{ArrowArray}(C_NULL) :
        Base.unsafe_convert(Ptr{ArrowArray}, dictionary.ref)
    handle = ArrayHandle(
        source,
        buffers,
        child_refs,
        child_handles,
        child_ptrs,
        dictionary_ref,
        dictionary_handle,
        dictionary_ptr,
        C_NULL,
    )
    private_data = _retain!(handle)
    handle.private_data = private_data
    array = ArrowArray(
        Int64(len),
        Int64(null_count),
        Int64(0),
        Int64(length(buffers)),
        Int64(length(children)),
        isempty(buffers) ? Ptr{Ptr{Cvoid}}(C_NULL) : pointer(buffers),
        _children_ptr(child_ptrs),
        dictionary_ptr,
        _array_release_callback_ptr(),
        private_data,
    )
    return ArrayExport(Ref(array), handle)
end

function _array_export_for_column(name::Symbol, column)
    col = _unwrap_column(column)
    if col isa NullVector
        return _array_export(col, length(col), length(col), Ptr{Cvoid}[])
    elseif col isa DictEncoded
        indices = getfield(col, :indices)
        encoding = getfield(col, :encoding)
        dictionary_array = _array_export_for_column(Symbol(""), getfield(encoding, :data))
        buffers = Ptr{Cvoid}[_validity_pointer(col), _primitive_data_pointer(indices)]
        return _array_export(
            col,
            length(col),
            nullcount(col),
            buffers;
            dictionary=dictionary_array,
        )
    elseif col isa List
        buffers = Ptr{Cvoid}[_validity_pointer(col), _offsets_pointer(col)]
        if liststringtype(col)
            push!(buffers, _list_data_pointer(col))
            return _array_export(col, length(col), nullcount(col), buffers)
        end
        child_array = _array_export_for_column(:item, getfield(col, :data))
        return _array_export(
            col,
            length(col),
            nullcount(col),
            buffers;
            children=ArrayExport[child_array],
        )
    elseif col isa View
        _view_format(col)
        lengths = _view_variadic_lengths(col)
        buffers = _view_buffer_pointers(col, lengths)
        return _array_export((col, lengths), length(col), nullcount(col), buffers)
    elseif col isa Map
        _assert_standard_map_offsets(col)
        _assert_map_offsets_shape(col)
        buffers = Ptr{Cvoid}[_validity_pointer(col), _offsets_pointer(col)]
        entries_array = _array_export_for_column(:entries, getfield(col, :data))
        return _array_export(
            col,
            length(col),
            nullcount(col),
            buffers;
            children=ArrayExport[entries_array],
        )
    elseif col isa RunEndEncoded
        run_ends_array = _array_export_for_column(:run_ends, getfield(col, :run_ends))
        values_array = _array_export_for_column(:values, getfield(col, :values))
        return _array_export(
            col,
            length(col),
            0,
            Ptr{Cvoid}[];
            children=ArrayExport[run_ends_array, values_array],
        )
    elseif col isa DenseUnion
        buffers = Ptr{Cvoid}[
            _primitive_data_pointer(getfield(col, :typeIds)),
            _primitive_data_pointer(getfield(col, :offsets)),
        ]
        child_arrays =
            ArrayExport[_array_export_for_column(Symbol(""), child) for
                        child in getfield(col, :data)]
        return _array_export(
            col,
            length(col),
            nullcount(col),
            buffers;
            children=child_arrays,
        )
    elseif col isa SparseUnion
        buffers = Ptr{Cvoid}[_primitive_data_pointer(getfield(col, :typeIds))]
        child_arrays =
            ArrayExport[_array_export_for_column(Symbol(""), child) for
                        child in getfield(col, :data)]
        return _array_export(
            col,
            length(col),
            nullcount(col),
            buffers;
            children=child_arrays,
        )
    elseif col isa Struct
        child_arrays =
            ArrayExport[_array_export_for_column(field_name, child) for
                        (field_name, child) in zip(_struct_field_names(col), getfield(col, :data))]
        buffers = Ptr{Cvoid}[_validity_pointer(col)]
        return _array_export(
            col,
            length(col),
            nullcount(col),
            buffers;
            children=child_arrays,
        )
    elseif col isa FixedSizeList
        data = getfield(col, :data)
        buffers = Ptr{Cvoid}[_validity_pointer(col)]
        if eltype(data) === UInt8
            push!(buffers, _primitive_data_pointer(data))
            return _array_export(col, length(col), nullcount(col), buffers)
        end
        child_array = _array_export_for_column(:item, data)
        return _array_export(
            col,
            length(col),
            nullcount(col),
            buffers;
            children=ArrayExport[child_array],
        )
    elseif col isa Primitive
        buffers = Ptr{Cvoid}[_validity_pointer(col), _primitive_data_pointer(getfield(col, :data))]
        return _array_export(col, length(col), nullcount(col), buffers)
    elseif col isa BoolVector
        buffers = Ptr{Cvoid}[_validity_pointer(col), _bool_data_pointer(col)]
        return _array_export(col, length(col), nullcount(col), buffers)
    end
    throw(
        ArgumentError(
            "Arrow C Data export currently supports NullVector, Primitive, BoolVector, List, View, FixedSizeList, Map, DenseUnion, SparseUnion, RunEndEncoded, Struct, and DictEncoded columns; got $(typeof(col)) for column $(name)",
        ),
    )
end

function _table_length(table::Table)
    cols = columns(table)
    isempty(cols) && return 0
    len = length(first(cols))
    for col in Iterators.drop(cols, 1)
        length(col) == len || throw(ArgumentError("Arrow.Table columns have mismatched lengths"))
    end
    return len
end

function _array_export_for_table(table::Table)
    child_arrays =
        ArrayExport[_array_export_for_column(name, column) for
                    (name, column) in zip(names(table), columns(table))]
    return _array_export(
        table,
        _table_length(table),
        0,
        Ptr{Cvoid}[C_NULL];
        children=child_arrays,
    )
end

"""
    Arrow.CData.exporttable(table::Arrow.Table) -> Arrow.CData.ExportedTable

Export an `Arrow.Table` as a top-level Arrow C Data Interface struct array.

This producer supports null, primitive, boolean, string/binary view,
string/list, fixed-size, map, dense/sparse union, run-end encoded, struct, and
dictionary encoded columns. Logical scalar primitive storage uses standard C
Data format strings. Keep the returned `ExportedTable` alive while any C
consumer may access its schema or array pointers, then call
[`Arrow.CData.release!`](@ref) when the consumer is done.
"""
function exporttable(table::Table)
    return ExportedTable(_schema_export_for_table(table), _array_export_for_table(table))
end

"""
    Arrow.CData.exporttable!(
        schema_out::Ptr{Arrow.CData.ArrowSchema},
        array_out::Ptr{Arrow.CData.ArrowArray},
        table::Arrow.Table,
    ) -> Arrow.CData.ExportedTable

Export an `Arrow.Table` into caller-owned Arrow C Data Interface base structs.

The caller provides non-null `ArrowSchema*` and `ArrowArray*` storage. Julia
fills those base structs and retains producer-owned member memory through the
returned `ExportedTable` owner until the base structs are released.
"""
function exporttable!(schema_out::Ptr{ArrowSchema}, array_out::Ptr{ArrowArray}, table::Table)
    schema_out == C_NULL &&
        throw(ArgumentError("ArrowSchema output pointer must not be C_NULL"))
    array_out == C_NULL &&
        throw(ArgumentError("ArrowArray output pointer must not be C_NULL"))
    schema_export = _schema_export_for_table(table)
    array_export = _array_export_for_table(table)
    unsafe_store!(schema_out, schema_export.ref[])
    unsafe_store!(array_out, array_export.ref[])
    return ExportedTable(schema_export, array_export, schema_out, array_out)
end

"""
    Arrow.CData.schema(exported)

Return the current `ArrowSchema` value for an exported table.
"""
schema(exported::ExportedTable) = unsafe_load(schema_ptr(exported))

"""
    Arrow.CData.array(exported)

Return the current `ArrowArray` value for an exported table.
"""
array(exported::ExportedTable) = unsafe_load(array_ptr(exported))

"""
    Arrow.CData.schema_ptr(exported)

Return a pointer to the exported table's `ArrowSchema` base struct.
"""
schema_ptr(exported::ExportedTable) = exported.schema_base

"""
    Arrow.CData.array_ptr(exported)

Return a pointer to the exported table's `ArrowArray` base struct.
"""
array_ptr(exported::ExportedTable) = exported.array_base

"""
    Arrow.CData.isreleased(exported)

Return whether both exported base structs have been released.
"""
isreleased(exported::ExportedTable) =
    schema(exported).release == C_NULL && array(exported).release == C_NULL

"""
    Arrow.CData.release!(exported)

Release the exported table's array and schema base structs if they are still
live. Raw C consumers may equivalently call each base struct's `release`
callback.
"""
function release!(exported::ExportedTable)
    arr = array(exported)
    arr.release == C_NULL || ccall(arr.release, Cvoid, (Ptr{ArrowArray},), array_ptr(exported))
    sch = schema(exported)
    sch.release == C_NULL ||
        ccall(sch.release, Cvoid, (Ptr{ArrowSchema},), schema_ptr(exported))
    return exported
end
