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

@testset "imports non-null primitive and UTF-8 C Data tables" begin
    table = Arrow.Table(
        Arrow.tobuffer((id=Int32[1, 2], score=Float64[1.25, 2.5], name=["a", "bb"])),
    )
    exported = CData.exporttable(table)
    imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

    @test Tables.columnnames(imported) == [:id, :score, :name]
    @test Tables.getcolumn(imported, :id) == Int32[1, 2]
    @test Tables.getcolumn(imported, :score) == Float64[1.25, 2.5]
    @test collect(Tables.getcolumn(imported, :name)) == ["a", "bb"]
    @test Tables.schema(imported).names == (:id, :score, :name)

    id_array = _child_array(CData.array(exported), 1)
    score_array = _child_array(CData.array(exported), 2)
    name_array = _child_array(CData.array(exported), 3)
    @test pointer(Tables.getcolumn(imported, :id)) ==
          Ptr{Int32}(unsafe_load(id_array.buffers, 2))
    @test pointer(Tables.getcolumn(imported, :score)) ==
          Ptr{Float64}(unsafe_load(score_array.buffers, 2))
    @test pointer(Tables.getcolumn(imported, :name).offsets) ==
          Ptr{Int32}(unsafe_load(name_array.buffers, 2))
    @test pointer(Tables.getcolumn(imported, :name).data) ==
          Ptr{UInt8}(unsafe_load(name_array.buffers, 3))

    CData.release!(imported)
    @test CData.isreleased(imported)
    @test CData.isreleased(exported)
end

@testset "imports schema and field metadata" begin
    source = Arrow.withmetadata(
        (id=Int32[1, 2, 3], name=["alpha", "beta", "gamma"]);
        metadata=Dict("dataset" => "cdata", "stage" => "import"),
        colmetadata=Dict(
            :id => Dict("semantic.role" => "identifier"),
            :name => Dict("semantic.role" => "label", "semantic.domain" => "example"),
        ),
    )
    table = Arrow.Table(Arrow.tobuffer(source))
    exported = CData.exporttable(table)
    imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

    id = Tables.getcolumn(imported, :id)
    name = Tables.getcolumn(imported, :name)
    id_array = _child_array(CData.array(exported), 1)
    name_array = _child_array(CData.array(exported), 2)

    @test Arrow.getmetadata(imported) == Dict("dataset" => "cdata", "stage" => "import")
    @test Arrow.getmetadata(id) == Dict("semantic.role" => "identifier")
    @test Arrow.getmetadata(name) ==
          Dict("semantic.role" => "label", "semantic.domain" => "example")
    @test collect(id) == Int32[1, 2, 3]
    @test collect(name) == ["alpha", "beta", "gamma"]
    @test pointer(id) == Ptr{Int32}(unsafe_load(id_array.buffers, 2))
    @test pointer(name.offsets) == Ptr{Int32}(unsafe_load(name_array.buffers, 2))
    @test pointer(name.data) == Ptr{UInt8}(unsafe_load(name_array.buffers, 3))

    CData.release!(imported)
    @test CData.isreleased(imported)
    @test CData.isreleased(exported)
end

@testset "imports extension metadata as raw storage fallback" begin
    json_values =
        Arrow.JSONText{String}[Arrow.JSONText("{\"a\":1}"), Arrow.JSONText("[1,2,3]")]
    table = Arrow.Table(Arrow.tobuffer((payload=json_values,)); convert=false)
    exported = CData.exporttable(table)
    imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

    payload = Tables.getcolumn(imported, :payload)
    payload_schema = _child_schema(CData.schema(exported), 1)
    payload_array = _child_array(CData.array(exported), 1)
    payload_metadata = Arrow.getmetadata(payload)

    @test _cstring(payload_schema.format) == "u"
    @test payload isa CData.ImportedMetadataVector
    @test eltype(payload) === String
    @test collect(payload) == ["{\"a\":1}", "[1,2,3]"]
    @test payload_metadata["ARROW:extension:name"] == "arrow.json"
    @test payload_metadata["ARROW:extension:metadata"] == ""
    @test pointer(payload.offsets) == Ptr{Int32}(unsafe_load(payload_array.buffers, 2))
    @test pointer(payload.data) == Ptr{UInt8}(unsafe_load(payload_array.buffers, 3))

    CData.release!(imported)
    @test CData.isreleased(imported)
    @test CData.isreleased(exported)
end

@testset "imports large UTF-8 and binary C Data columns" begin
    large_table = Arrow.Table(Arrow.tobuffer((name=["a", "bb", ""],); largelists=true))
    large_exported = CData.exporttable(large_table)
    large_imported =
        CData.importtable(CData.schema_ptr(large_exported), CData.array_ptr(large_exported))

    large_name = Tables.getcolumn(large_imported, :name)
    large_schema = _child_schema(CData.schema(large_exported), 1)
    large_array = _child_array(CData.array(large_exported), 1)
    @test _cstring(large_schema.format) == "U"
    @test large_name isa CData.ImportedStringVector{Int64}
    @test collect(large_name) == ["a", "bb", ""]
    @test pointer(large_name.offsets) == Ptr{Int64}(unsafe_load(large_array.buffers, 2))
    @test pointer(large_name.data) == Ptr{UInt8}(unsafe_load(large_array.buffers, 3))

    CData.release!(large_imported)
    @test CData.isreleased(large_imported)
    @test CData.isreleased(large_exported)

    binary_table = Arrow.Table(
        Arrow.tobuffer((blob=[codeunits("a"), codeunits("bb"), codeunits("")],)),
    )
    binary_exported = CData.exporttable(binary_table)
    binary_imported = CData.importtable(
        CData.schema_ptr(binary_exported),
        CData.array_ptr(binary_exported),
    )

    blob = Tables.getcolumn(binary_imported, :blob)
    blob_schema = _child_schema(CData.schema(binary_exported), 1)
    blob_array = _child_array(CData.array(binary_exported), 1)
    @test _cstring(blob_schema.format) == "z"
    @test blob isa CData.ImportedBinaryVector
    @test [String(collect(value)) for value in blob] == ["a", "bb", ""]
    @test pointer(blob.offsets) == Ptr{Int32}(unsafe_load(blob_array.buffers, 2))
    @test pointer(blob.data) == Ptr{UInt8}(unsafe_load(blob_array.buffers, 3))

    CData.release!(binary_imported)
    @test CData.isreleased(binary_imported)
    @test CData.isreleased(binary_exported)

    nullable_binary_table = Arrow.Table(
        Arrow.tobuffer(
            (
                blob=Union{Missing,Base.CodeUnits{UInt8,String}}[
                    codeunits("a"),
                    missing,
                    codeunits("ccc"),
                ],
            );
            largelists=true,
        ),
    )
    nullable_binary_exported = CData.exporttable(nullable_binary_table)
    nullable_binary_imported = CData.importtable(
        CData.schema_ptr(nullable_binary_exported),
        CData.array_ptr(nullable_binary_exported),
    )

    maybe_blob = Tables.getcolumn(nullable_binary_imported, :blob)
    maybe_blob_schema = _child_schema(CData.schema(nullable_binary_exported), 1)
    maybe_blob_array = _child_array(CData.array(nullable_binary_exported), 1)
    @test _cstring(maybe_blob_schema.format) == "Z"
    @test maybe_blob isa CData.ImportedBinaryVector
    @test maybe_blob.offsets isa Vector{Int64}
    @test isequal(
        [ismissing(value) ? missing : String(collect(value)) for value in maybe_blob],
        Union{Missing,String}["a", missing, "ccc"],
    )
    @test pointer(maybe_blob.validity) ==
          Ptr{UInt8}(unsafe_load(maybe_blob_array.buffers, 1))

    CData.release!(nullable_binary_imported)
    @test CData.isreleased(nullable_binary_imported)
    @test CData.isreleased(nullable_binary_exported)
end

@testset "imports boolean C Data columns" begin
    table = Arrow.Table(
        Arrow.tobuffer((
            flag=Bool[true, false, true, true, false, false, true, false, true],
        )),
    )
    exported = CData.exporttable(table)
    imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

    flag = Tables.getcolumn(imported, :flag)
    flag_array = _child_array(CData.array(exported), 1)
    @test flag isa CData.ImportedBoolVector{Bool}
    @test collect(flag) == Bool[true, false, true, true, false, false, true, false, true]
    @test pointer(flag.data) == Ptr{UInt8}(unsafe_load(flag_array.buffers, 2))

    CData.release!(imported)
    @test CData.isreleased(imported)
    @test CData.isreleased(exported)

    nullable_table = Arrow.Table(
        Arrow.tobuffer((flag=Union{Missing,Bool}[true, missing, false, true, missing],),),
    )
    nullable_exported = CData.exporttable(nullable_table)
    nullable_imported = CData.importtable(
        CData.schema_ptr(nullable_exported),
        CData.array_ptr(nullable_exported),
    )
    maybe_flag = Tables.getcolumn(nullable_imported, :flag)
    maybe_flag_array = _child_array(CData.array(nullable_exported), 1)
    @test maybe_flag isa CData.ImportedBoolVector{Union{Missing,Bool}}
    @test isequal(
        collect(maybe_flag),
        Union{Missing,Bool}[true, missing, false, true, missing],
    )
    @test pointer(maybe_flag.data) == Ptr{UInt8}(unsafe_load(maybe_flag_array.buffers, 2))
    @test pointer(maybe_flag.validity) ==
          Ptr{UInt8}(unsafe_load(maybe_flag_array.buffers, 1))

    CData.release!(nullable_imported)
    @test CData.isreleased(nullable_imported)
    @test CData.isreleased(nullable_exported)
end

@testset "imports nullable primitive and UTF-8 C Data tables" begin
    table = Arrow.Table(
        Arrow.tobuffer((
            maybe=Union{Missing,Int32}[1, missing, 3],
            label=Union{Missing,String}["a", missing, "ccc"],
        ),),
    )
    exported = CData.exporttable(table)
    imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

    maybe = Tables.getcolumn(imported, :maybe)
    label = Tables.getcolumn(imported, :label)
    @test maybe isa CData.ImportedNullablePrimitiveVector{Int32}
    @test label isa CData.ImportedNullableStringVector{Int32}
    @test eltype(maybe) == Union{Missing,Int32}
    @test eltype(label) == Union{Missing,String}
    @test isequal(collect(maybe), Union{Missing,Int32}[1, missing, 3])
    @test isequal(collect(label), Union{Missing,String}["a", missing, "ccc"])

    maybe_array = _child_array(CData.array(exported), 1)
    label_array = _child_array(CData.array(exported), 2)
    @test pointer(maybe.data) == Ptr{Int32}(unsafe_load(maybe_array.buffers, 2))
    @test pointer(maybe.validity) == Ptr{UInt8}(unsafe_load(maybe_array.buffers, 1))
    @test pointer(label.values.offsets) == Ptr{Int32}(unsafe_load(label_array.buffers, 2))
    @test pointer(label.values.data) == Ptr{UInt8}(unsafe_load(label_array.buffers, 3))
    @test pointer(label.validity) == Ptr{UInt8}(unsafe_load(label_array.buffers, 1))

    CData.release!(imported)
    @test CData.isreleased(imported)
    @test CData.isreleased(exported)
end

function _nested_dictionary_export()
    inner_table =
        Arrow.Table(Arrow.tobuffer((color=["red", "blue", "red"],); dictencode=true))
    inner = Tables.getcolumn(inner_table, :color)
    outer_indices = Int8[0, 2, 1, 0]
    outer_encoding =
        Arrow.DictEncoding{eltype(inner),Int8,typeof(inner)}(42, inner, false, nothing)
    outer = Arrow.DictEncoded(
        UInt8[],
        Arrow.ValidityBitmap(fill(true, length(outer_indices))),
        outer_indices,
        outer_encoding,
        nothing,
    )
    top_schema = CData._schema_export(
        "+s",
        "";
        children=CData.SchemaExport[CData._schema_export_for_column(:color, outer)],
    )
    top_array = CData._array_export(
        (inner_table, outer),
        length(outer),
        0,
        Ptr{Cvoid}[C_NULL];
        children=CData.ArrayExport[CData._array_export_for_column(:color, outer)],
    )
    return CData.ExportedTable(top_schema, top_array)
end

@testset "imports dictionary encoded C Data columns" begin
    table = Arrow.Table(Arrow.tobuffer((color=["red", "blue", "red"],); dictencode=true))
    exported = CData.exporttable(table)
    imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

    color = Tables.getcolumn(imported, :color)
    color_array = _child_array(CData.array(exported), 1)
    dictionary_array = unsafe_load(color_array.dictionary)
    @test color isa CData.ImportedDictionaryVector
    @test eltype(color) == String
    @test collect(color) == ["red", "blue", "red"]
    @test Ptr{Cvoid}(pointer(color.indices)) == unsafe_load(color_array.buffers, 2)
    @test pointer(color.dictionary.offsets) ==
          Ptr{Int32}(unsafe_load(dictionary_array.buffers, 2))
    @test pointer(color.dictionary.data) ==
          Ptr{UInt8}(unsafe_load(dictionary_array.buffers, 3))

    CData.release!(imported)
    @test CData.isreleased(imported)
    @test CData.isreleased(exported)

    nullable_table = Arrow.Table(
        Arrow.tobuffer(
            (color=Union{Missing,String}["red", missing, "blue", "red"],);
            dictencode=true,
        ),
    )
    nullable_exported = CData.exporttable(nullable_table)
    nullable_imported = CData.importtable(
        CData.schema_ptr(nullable_exported),
        CData.array_ptr(nullable_exported),
    )

    maybe_color = Tables.getcolumn(nullable_imported, :color)
    maybe_color_array = _child_array(CData.array(nullable_exported), 1)
    @test maybe_color isa CData.ImportedDictionaryVector
    @test eltype(maybe_color) == Union{Missing,String}
    @test isequal(
        collect(maybe_color),
        Union{Missing,String}["red", missing, "blue", "red"],
    )
    @test Ptr{Cvoid}(pointer(maybe_color.indices)) ==
          unsafe_load(maybe_color_array.buffers, 2)
    @test pointer(maybe_color.validity) ==
          Ptr{UInt8}(unsafe_load(maybe_color_array.buffers, 1))

    CData.release!(nullable_imported)
    @test CData.isreleased(nullable_imported)
    @test CData.isreleased(nullable_exported)

    nested_exported = _nested_dictionary_export()
    nested_imported = CData.importtable(
        CData.schema_ptr(nested_exported),
        CData.array_ptr(nested_exported),
    )

    nested_color = Tables.getcolumn(nested_imported, :color)
    nested_color_array = _child_array(CData.array(nested_exported), 1)
    nested_dictionary_array = unsafe_load(nested_color_array.dictionary)
    value_dictionary_array = unsafe_load(nested_dictionary_array.dictionary)
    @test nested_color isa CData.ImportedDictionaryVector
    @test nested_color.dictionary isa CData.ImportedDictionaryVector
    @test eltype(nested_color) == String
    @test collect(nested_color) == ["red", "red", "blue", "red"]
    @test Ptr{Cvoid}(pointer(nested_color.indices)) ==
          unsafe_load(nested_color_array.buffers, 2)
    @test Ptr{Cvoid}(pointer(nested_color.dictionary.indices)) ==
          unsafe_load(nested_dictionary_array.buffers, 2)
    @test pointer(nested_color.dictionary.dictionary.offsets) ==
          Ptr{Int32}(unsafe_load(value_dictionary_array.buffers, 2))
    @test pointer(nested_color.dictionary.dictionary.data) ==
          Ptr{UInt8}(unsafe_load(value_dictionary_array.buffers, 3))

    CData.release!(nested_imported)
    @test CData.isreleased(nested_imported)
    @test CData.isreleased(nested_exported)
end

@testset "imports list C Data columns" begin
    table = Arrow.Table(Arrow.tobuffer((values=[Int64[1, 2], Int64[3], Int64[]],)))
    exported = CData.exporttable(table)
    imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

    values = Tables.getcolumn(imported, :values)
    values_array = _child_array(CData.array(exported), 1)
    item_array = unsafe_load(unsafe_load(values_array.children, 1))
    @test values isa CData.ImportedListVector
    @test eltype(values) == AbstractVector{Int64}
    @test map(collect, collect(values)) == [Int64[1, 2], Int64[3], Int64[]]
    @test pointer(values.offsets) == Ptr{Int32}(unsafe_load(values_array.buffers, 2))
    @test pointer(values.values) == Ptr{Int64}(unsafe_load(item_array.buffers, 2))

    CData.release!(imported)
    @test CData.isreleased(imported)
    @test CData.isreleased(exported)

    nullable_table = Arrow.Table(
        Arrow.tobuffer((
            values=Union{Missing,Vector{Int64}}[Int64[1, 2], missing, Int64[]],
        ),),
    )
    nullable_exported = CData.exporttable(nullable_table)
    nullable_imported = CData.importtable(
        CData.schema_ptr(nullable_exported),
        CData.array_ptr(nullable_exported),
    )
    maybe_values = Tables.getcolumn(nullable_imported, :values)
    maybe_values_array = _child_array(CData.array(nullable_exported), 1)
    @test maybe_values isa CData.ImportedListVector
    @test eltype(maybe_values) == Union{Missing,AbstractVector{Int64}}
    @test isequal(
        [ismissing(value) ? missing : collect(value) for value in maybe_values],
        Union{Missing,Vector{Int64}}[Int64[1, 2], missing, Int64[]],
    )
    @test pointer(maybe_values.validity) ==
          Ptr{UInt8}(unsafe_load(maybe_values_array.buffers, 1))

    CData.release!(nullable_imported)
    @test CData.isreleased(nullable_imported)
    @test CData.isreleased(nullable_exported)

    large_table =
        Arrow.Table(Arrow.tobuffer((values=[Int64[1], Int64[]],); largelists=true))
    large_exported = CData.exporttable(large_table)
    large_imported =
        CData.importtable(CData.schema_ptr(large_exported), CData.array_ptr(large_exported))
    large_values = Tables.getcolumn(large_imported, :values)
    @test large_values isa CData.ImportedListVector
    @test large_values.offsets isa Vector{Int64}
    @test map(collect, collect(large_values)) == [Int64[1], Int64[]]
    CData.release!(large_imported)
    @test CData.isreleased(large_imported)
    @test CData.isreleased(large_exported)
end
