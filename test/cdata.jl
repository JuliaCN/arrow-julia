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

using Test
using Arrow
using Dates
using Libdl
using Tables

const CData = Arrow.CData

_cstring(ptr::Ptr{UInt8}) = ptr == C_NULL ? nothing : unsafe_string(ptr)

function _child_schema(schema::CData.ArrowSchema, index::Integer)
    return unsafe_load(unsafe_load(schema.children, index))
end

function _child_array(array::CData.ArrowArray, index::Integer)
    return unsafe_load(unsafe_load(array.children, index))
end

function _metadata_dict(ptr::Ptr{UInt8})
    ptr == C_NULL && return Dict{String,String}()
    offset = 0
    function read_int32()
        value = unsafe_load(Ptr{Int32}(ptr + offset))
        offset += 4
        return Int(value)
    end
    function read_string()
        len = read_int32()
        value = len == 0 ? "" : unsafe_string(ptr + offset, len)
        offset += len
        return value
    end
    count = read_int32()
    metadata = Dict{String,String}()
    for _ = 1:count
        key = read_string()
        value = read_string()
        metadata[key] = value
    end
    return metadata
end

function _compile_cdata_smoke()
    cc = get(ENV, "CC", "cc")
    source = joinpath(@__DIR__, "cdata_smoke.c")
    output = joinpath(mktempdir(), "libarrow_julia_cdata_smoke.$(Libdl.dlext)")
    shared_flag = Sys.isapple() ? "-dynamiclib" : "-shared"
    include_dir = dirname(CData.header_path())
    run(`$cc $shared_flag -fPIC -I$include_dir $source -o $output`)
    return output
end

_julia_include_dir() = abspath(Sys.BINDIR, Base.INCLUDEDIR)
_julia_lib_dir() = abspath(Sys.BINDIR, Base.LIBDIR)

function _compile_cdata_embed_smoke()
    cc = get(ENV, "CC", "cc")
    source = joinpath(@__DIR__, "cdata_embed_smoke.c")
    output = joinpath(mktempdir(), "arrow_julia_cdata_embed_smoke")
    include_dir = dirname(CData.header_path())
    julia_include = _julia_include_dir()
    julia_include_private = joinpath(julia_include, "julia")
    julia_lib = _julia_lib_dir()
    julia_private_lib = joinpath(julia_lib, "julia")
    rpath_julia = "-Wl,-rpath,$julia_lib"
    rpath_private = "-Wl,-rpath,$julia_private_lib"
    run(
        `$cc -I$include_dir -I$julia_include -I$julia_include_private $source -L$julia_lib -ljulia $rpath_julia $rpath_private -o $output`,
    )
    return output
end

function _active_project_dir()
    project = Base.active_project()
    return project === nothing ? (@__DIR__) : dirname(project)
end

function _current_load_path()
    separator = Sys.iswindows() ? ";" : ":"
    return join(Base.LOAD_PATH, separator)
end

function _run_cdata_embed_smoke(executable)
    julia_lib = _julia_lib_dir()
    julia_private_lib = joinpath(julia_lib, "julia")
    lib_path_var = Sys.isapple() ? "DYLD_LIBRARY_PATH" : "LD_LIBRARY_PATH"
    existing = get(ENV, lib_path_var, "")
    lib_path = join(
        isempty(existing) ? (julia_lib, julia_private_lib) :
        (julia_lib, julia_private_lib, existing),
        Sys.iswindows() ? ";" : ":",
    )
    withenv(
        "JULIA_PROJECT" => _active_project_dir(),
        "JULIA_LOAD_PATH" => _current_load_path(),
        lib_path_var => lib_path,
    ) do
        run(`$executable`)
    end
    return true
end

@testset "Arrow C Data Interface" begin
    @testset "exposes a C consumer header" begin
        @test isfile(CData.header_path())
        @test basename(CData.header_path()) == "arrow_julia_cdata.h"
    end

    @testset "exports primitive and boolean table columns" begin
        table = Arrow.Table(
            Arrow.tobuffer((
                id=Int32[1, 2, 3],
                score=Float64[1.5, 2.5, 3.5],
                flag=Bool[true, false, true],
            )),
        )
        exported = CData.exporttable(table)

        schema = CData.schema(exported)
        array = CData.array(exported)

        @test _cstring(schema.format) == "+s"
        @test _cstring(schema.name) == ""
        @test schema.n_children == 3
        @test array.length == 3
        @test array.null_count == 0
        @test array.n_buffers == 1
        @test array.n_children == 3
        @test unsafe_load(array.buffers, 1) == C_NULL

        id_schema = _child_schema(schema, 1)
        score_schema = _child_schema(schema, 2)
        flag_schema = _child_schema(schema, 3)
        @test _cstring(id_schema.format) == "i"
        @test _cstring(id_schema.name) == "id"
        @test _cstring(score_schema.format) == "g"
        @test _cstring(score_schema.name) == "score"
        @test _cstring(flag_schema.format) == "b"
        @test _cstring(flag_schema.name) == "flag"

        id_column = Tables.getcolumn(table, :id)
        score_column = Tables.getcolumn(table, :score)
        flag_column = Tables.getcolumn(table, :flag)
        id_array = _child_array(array, 1)
        score_array = _child_array(array, 2)
        flag_array = _child_array(array, 3)

        @test id_array.n_buffers == 2
        @test score_array.n_buffers == 2
        @test flag_array.n_buffers == 2
        @test unsafe_load(id_array.buffers, 1) == C_NULL
        @test unsafe_load(score_array.buffers, 1) == C_NULL
        @test unsafe_load(flag_array.buffers, 1) == C_NULL
        @test unsafe_load(id_array.buffers, 2) ==
              Ptr{Cvoid}(pointer(getfield(id_column, :data)))
        @test unsafe_load(score_array.buffers, 2) ==
              Ptr{Cvoid}(pointer(getfield(score_column, :data)))
        @test unsafe_load(flag_array.buffers, 2) == Ptr{Cvoid}(
            pointer(getfield(flag_column, :arrow), getfield(flag_column, :pos)),
        )

        CData.release!(exported)
        @test CData.isreleased(exported)
    end

    @testset "exports nullable primitive validity bitmap" begin
        table = Arrow.Table(Arrow.tobuffer((maybe=Union{Missing,Int64}[1, missing, 3],)))
        exported = CData.exporttable(table)
        schema = CData.schema(exported)
        array = CData.array(exported)
        maybe_schema = _child_schema(schema, 1)
        maybe_array = _child_array(array, 1)

        @test _cstring(maybe_schema.format) == "l"
        @test maybe_schema.flags & CData.ARROW_FLAG_NULLABLE == CData.ARROW_FLAG_NULLABLE
        @test maybe_array.length == 3
        @test maybe_array.null_count == 1
        @test unsafe_load(maybe_array.buffers, 1) != C_NULL

        CData.release!(exported)
        @test CData.isreleased(exported)
    end

    @testset "exports string columns and schema metadata" begin
        source = Arrow.withmetadata(
            (name=["alpha", "beta", ""], maybe=Union{Missing,String}["x", missing, "yz"]);
            metadata=Dict("dataset" => "strings"),
            colmetadata=Dict(
                :name => Dict("role" => "label"),
                :maybe => Dict("role" => "optional"),
            ),
        )
        table = Arrow.Table(Arrow.tobuffer(source))
        exported = CData.exporttable(table)

        schema = CData.schema(exported)
        array = CData.array(exported)
        name_schema = _child_schema(schema, 1)
        maybe_schema = _child_schema(schema, 2)
        name_array = _child_array(array, 1)
        maybe_array = _child_array(array, 2)
        name_column = Tables.getcolumn(table, :name)
        maybe_column = Tables.getcolumn(table, :maybe)

        @test _metadata_dict(schema.metadata) == Dict("dataset" => "strings")
        @test _cstring(name_schema.format) == "u"
        @test _cstring(maybe_schema.format) == "u"
        @test _metadata_dict(name_schema.metadata) == Dict("role" => "label")
        @test _metadata_dict(maybe_schema.metadata) == Dict("role" => "optional")
        @test maybe_schema.flags & CData.ARROW_FLAG_NULLABLE == CData.ARROW_FLAG_NULLABLE
        @test name_array.n_buffers == 3
        @test maybe_array.n_buffers == 3
        @test unsafe_load(name_array.buffers, 1) == C_NULL
        @test unsafe_load(name_array.buffers, 2) ==
              Ptr{Cvoid}(pointer(getfield(getfield(name_column, :offsets), :offsets)))
        @test unsafe_load(name_array.buffers, 3) ==
              Ptr{Cvoid}(pointer(getfield(name_column, :data)))
        @test maybe_array.null_count == 1
        @test unsafe_load(maybe_array.buffers, 1) != C_NULL
        @test unsafe_load(maybe_array.buffers, 2) ==
              Ptr{Cvoid}(pointer(getfield(getfield(maybe_column, :offsets), :offsets)))
        @test unsafe_load(maybe_array.buffers, 3) ==
              Ptr{Cvoid}(pointer(getfield(maybe_column, :data)))

        CData.release!(exported)
        @test CData.isreleased(exported)
    end

    @testset "exports list columns with child arrays" begin
        table = Arrow.Table(Arrow.tobuffer((values=[[Int64(1), 2], [Int64(3)], Int64[]],)))
        exported = CData.exporttable(table)

        schema = CData.schema(exported)
        array = CData.array(exported)
        values_schema = _child_schema(schema, 1)
        values_array = _child_array(array, 1)
        item_schema = _child_schema(values_schema, 1)
        item_array = _child_array(values_array, 1)
        values_column = Tables.getcolumn(table, :values)
        item_column = getfield(values_column, :data)

        @test _cstring(values_schema.format) == "+l"
        @test values_schema.n_children == 1
        @test _cstring(item_schema.format) == "l"
        @test _cstring(item_schema.name) == "item"
        @test values_array.n_buffers == 2
        @test values_array.n_children == 1
        @test unsafe_load(values_array.buffers, 1) == C_NULL
        @test unsafe_load(values_array.buffers, 2) ==
              Ptr{Cvoid}(pointer(getfield(getfield(values_column, :offsets), :offsets)))
        @test item_array.length == 3
        @test unsafe_load(item_array.buffers, 2) ==
              Ptr{Cvoid}(pointer(getfield(item_column, :data)))

        CData.release!(exported)
        @test CData.isreleased(exported)
    end

    @testset "exports dictionary encoded columns" begin
        table =
            Arrow.Table(Arrow.tobuffer((color=["red", "blue", "red"],); dictencode=true))
        exported = CData.exporttable(table)

        schema = CData.schema(exported)
        array = CData.array(exported)
        color_schema = _child_schema(schema, 1)
        color_array = _child_array(array, 1)
        color_column = Tables.getcolumn(table, :color)
        dictionary_schema_ptr = color_schema.dictionary
        dictionary_array_ptr = color_array.dictionary
        dictionary_schema = unsafe_load(dictionary_schema_ptr)
        dictionary_array = unsafe_load(dictionary_array_ptr)

        @test _cstring(color_schema.format) == "c"
        @test dictionary_schema_ptr != C_NULL
        @test dictionary_array_ptr != C_NULL
        @test _cstring(dictionary_schema.format) == "u"
        @test dictionary_array.length ==
              length(getfield(getfield(color_column, :encoding), :data))
        @test color_array.n_buffers == 2
        @test unsafe_load(color_array.buffers, 1) == C_NULL
        @test unsafe_load(color_array.buffers, 2) ==
              Ptr{Cvoid}(pointer(getfield(color_column, :indices)))
        @test dictionary_array.n_buffers == 3

        CData.release!(exported)
        @test CData.isreleased(exported)
        @test unsafe_load(dictionary_schema_ptr).release == C_NULL
        @test unsafe_load(dictionary_array_ptr).release == C_NULL
    end

    @testset "exports into caller-owned base structs" begin
        before = CData._retained_handle_count()
        table = Arrow.Table(Arrow.tobuffer((id=Int32[1, 2], name=["a", "b"])))
        schema_ref = Ref{CData.ArrowSchema}()
        array_ref = Ref{CData.ArrowArray}()
        schema_out = Base.unsafe_convert(Ptr{CData.ArrowSchema}, schema_ref)
        array_out = Base.unsafe_convert(Ptr{CData.ArrowArray}, array_ref)

        exported = CData.exporttable!(schema_out, array_out, table)
        schema = unsafe_load(schema_out)
        array = unsafe_load(array_out)

        @test CData.schema_ptr(exported) == schema_out
        @test CData.array_ptr(exported) == array_out
        @test _cstring(schema.format) == "+s"
        @test array.length == 2
        @test schema.release != C_NULL
        @test array.release != C_NULL
        @test schema.private_data != C_NULL
        @test array.private_data != C_NULL
        @test CData._retained_handle_count() >= before + 6

        ccall(array.release, Cvoid, (Ptr{CData.ArrowArray},), array_out)
        @test CData.array(exported).release == C_NULL
        @test !CData.isreleased(exported)
        ccall(schema.release, Cvoid, (Ptr{CData.ArrowSchema},), schema_out)
        @test CData.isreleased(exported)
        CData.release!(exported)
        @test CData.isreleased(exported)
        @test CData._retained_handle_count() == before

        @test_throws ArgumentError CData.exporttable!(
            Ptr{CData.ArrowSchema}(C_NULL),
            array_out,
            table,
        )
        @test_throws ArgumentError CData.exporttable!(
            schema_out,
            Ptr{CData.ArrowArray}(C_NULL),
            table,
        )
    end

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

    @testset "imports large UTF-8 and binary C Data columns" begin
        large_table = Arrow.Table(Arrow.tobuffer((name=["a", "bb", ""],); largelists=true))
        large_exported = CData.exporttable(large_table)
        large_imported = CData.importtable(
            CData.schema_ptr(large_exported),
            CData.array_ptr(large_exported),
        )

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
        @test collect(flag) ==
              Bool[true, false, true, true, false, false, true, false, true]
        @test pointer(flag.data) == Ptr{UInt8}(unsafe_load(flag_array.buffers, 2))

        CData.release!(imported)
        @test CData.isreleased(imported)
        @test CData.isreleased(exported)

        nullable_table = Arrow.Table(
            Arrow.tobuffer((
                flag=Union{Missing,Bool}[true, missing, false, true, missing],
            ),),
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
        @test pointer(maybe_flag.data) ==
              Ptr{UInt8}(unsafe_load(maybe_flag_array.buffers, 2))
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
        @test pointer(label.values.offsets) ==
              Ptr{Int32}(unsafe_load(label_array.buffers, 2))
        @test pointer(label.values.data) == Ptr{UInt8}(unsafe_load(label_array.buffers, 3))
        @test pointer(label.validity) == Ptr{UInt8}(unsafe_load(label_array.buffers, 1))

        CData.release!(imported)
        @test CData.isreleased(imported)
        @test CData.isreleased(exported)
    end

    @testset "imports dictionary encoded C Data columns" begin
        table =
            Arrow.Table(Arrow.tobuffer((color=["red", "blue", "red"],); dictencode=true))
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
        large_imported = CData.importtable(
            CData.schema_ptr(large_exported),
            CData.array_ptr(large_exported),
        )
        large_values = Tables.getcolumn(large_imported, :values)
        @test large_values isa CData.ImportedListVector
        @test large_values.offsets isa Vector{Int64}
        @test map(collect, collect(large_values)) == [Int64[1], Int64[]]
        CData.release!(large_imported)
        @test CData.isreleased(large_imported)
        @test CData.isreleased(large_exported)
    end

    @testset "C smoke consumer validates and releases caller-owned structs" begin
        smoke_lib = _compile_cdata_smoke()
        before = CData._retained_handle_count()
        table = Arrow.Table(Arrow.tobuffer((id=Int32[1, 2], name=["a", "b"])))
        schema_ref = Ref{CData.ArrowSchema}()
        array_ref = Ref{CData.ArrowArray}()
        schema_out = Base.unsafe_convert(Ptr{CData.ArrowSchema}, schema_ref)
        array_out = Base.unsafe_convert(Ptr{CData.ArrowArray}, array_ref)
        exported = CData.exporttable!(schema_out, array_out, table)
        smoke_handle = Libdl.dlopen(smoke_lib)
        validate = Libdl.dlsym(smoke_handle, :arrow_julia_cdata_smoke_validate)
        release_array = Libdl.dlsym(smoke_handle, :arrow_julia_cdata_smoke_release_array)
        release_schema = Libdl.dlsym(smoke_handle, :arrow_julia_cdata_smoke_release_schema)

        @test ccall(
            validate,
            Cint,
            (Ptr{CData.ArrowSchema}, Ptr{CData.ArrowArray}),
            schema_out,
            array_out,
        ) == 0
        @test ccall(release_array, Cint, (Ptr{CData.ArrowArray},), array_out) == 0
        @test CData.array(exported).release == C_NULL
        @test !CData.isreleased(exported)
        @test ccall(release_schema, Cint, (Ptr{CData.ArrowSchema},), schema_out) == 0
        @test CData.isreleased(exported)
        @test CData._retained_handle_count() == before
    end

    @testset "embedded C process exports and releases C Data structs" begin
        executable = _compile_cdata_embed_smoke()
        @test _run_cdata_embed_smoke(executable)
    end

    @testset "release callbacks release child structs and registry handles" begin
        before = CData._retained_handle_count()
        table = Arrow.Table(Arrow.tobuffer((id=Int32[1, 2], flag=Bool[true, false])))
        exported = CData.exporttable(table)
        @test CData._retained_handle_count() >= before + 6

        schema = CData.schema(exported)
        array = CData.array(exported)
        schema_child_ptr = unsafe_load(schema.children, 1)
        array_child_ptr = unsafe_load(array.children, 1)

        CData.release!(exported)

        @test CData.schema(exported).release == C_NULL
        @test CData.array(exported).release == C_NULL
        @test unsafe_load(schema_child_ptr).release == C_NULL
        @test unsafe_load(array_child_ptr).release == C_NULL
        @test CData._retained_handle_count() == before
    end

    @testset "exports and imports nested struct columns" begin
        maybe_type = Union{Missing,NamedTuple{(:x, :label),Tuple{Int32,String}}}
        table = Arrow.Table(
            Arrow.tobuffer((
                point=[(x=Int32(1), label="a"), (x=Int32(2), label="bb")],
                maybe=maybe_type[(x=Int32(3), label="ccc"), missing],
            ),),
        )
        exported = CData.exporttable(table)
        imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

        schema = CData.schema(exported)
        array = CData.array(exported)
        point_schema = _child_schema(schema, 1)
        maybe_schema = _child_schema(schema, 2)
        point_array = _child_array(array, 1)
        maybe_array = _child_array(array, 2)
        point_x_array = unsafe_load(unsafe_load(point_array.children, 1))
        point_label_array = unsafe_load(unsafe_load(point_array.children, 2))

        @test _cstring(point_schema.format) == "+s"
        @test point_schema.n_children == 2
        @test _cstring(_child_schema(point_schema, 1).name) == "x"
        @test _cstring(_child_schema(point_schema, 2).name) == "label"
        @test maybe_schema.flags & CData.ARROW_FLAG_NULLABLE == CData.ARROW_FLAG_NULLABLE

        point = Tables.getcolumn(imported, :point)
        maybe = Tables.getcolumn(imported, :maybe)
        @test point isa CData.ImportedStructVector
        @test maybe isa CData.ImportedStructVector
        @test collect(point) == [(x=Int32(1), label="a"), (x=Int32(2), label="bb")]
        @test isequal(collect(maybe), maybe_type[(x=Int32(3), label="ccc"), missing])
        @test pointer(point.columns[1]) == Ptr{Int32}(unsafe_load(point_x_array.buffers, 2))
        @test pointer(point.columns[2].offsets) ==
              Ptr{Int32}(unsafe_load(point_label_array.buffers, 2))
        @test pointer(maybe.validity) == Ptr{UInt8}(unsafe_load(maybe_array.buffers, 1))

        CData.release!(imported)
        @test CData.isreleased(imported)
        @test CData.isreleased(exported)
    end

    @testset "exports and imports fixed-size columns" begin
        table = Arrow.Table(
            Arrow.tobuffer((
                bytes=NTuple{2,UInt8}[(0x01, 0x02), (0x03, 0x04)],
                fixed=NTuple{2,Int32}[(Int32(1), Int32(2)), (Int32(3), Int32(4))],
                maybe=Union{Missing,NTuple{2,Int32}}[(Int32(5), Int32(6)), missing],
            ),),
        )
        exported = CData.exporttable(table)
        imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

        schema = CData.schema(exported)
        array = CData.array(exported)
        bytes_schema = _child_schema(schema, 1)
        fixed_schema = _child_schema(schema, 2)
        bytes_array = _child_array(array, 1)
        fixed_array = _child_array(array, 2)
        maybe_array = _child_array(array, 3)
        fixed_child_array = unsafe_load(unsafe_load(fixed_array.children, 1))

        @test _cstring(bytes_schema.format) == "w:2"
        @test _cstring(fixed_schema.format) == "+w:2"
        @test fixed_schema.n_children == 1
        bytes = Tables.getcolumn(imported, :bytes)
        fixed = Tables.getcolumn(imported, :fixed)
        maybe = Tables.getcolumn(imported, :maybe)
        @test bytes isa CData.ImportedFixedSizeBinaryVector
        @test fixed isa CData.ImportedFixedSizeListVector
        @test maybe isa CData.ImportedFixedSizeListVector
        @test collect(bytes) == NTuple{2,UInt8}[(0x01, 0x02), (0x03, 0x04)]
        @test collect(fixed) == NTuple{2,Int32}[(Int32(1), Int32(2)), (Int32(3), Int32(4))]
        @test isequal(
            collect(maybe),
            Union{Missing,NTuple{2,Int32}}[(Int32(5), Int32(6)), missing],
        )
        @test pointer(bytes.data) == Ptr{UInt8}(unsafe_load(bytes_array.buffers, 2))
        @test pointer(fixed.values) == Ptr{Int32}(unsafe_load(fixed_child_array.buffers, 2))
        @test pointer(maybe.validity) == Ptr{UInt8}(unsafe_load(maybe_array.buffers, 1))

        CData.release!(imported)
        @test CData.isreleased(imported)
        @test CData.isreleased(exported)
    end

    @testset "exports and imports null and logical scalar columns" begin
        decimal128_type = Arrow.Decimal{Int32(4),Int32(2),Int128}
        decimal256_type = Arrow.Decimal{Int32(6),Int32(3),Arrow.Int256}
        interval_type = Arrow.Interval{Arrow.Meta.IntervalUnit.YEAR_MONTH,Int32}
        dates = Date[Date(2020, 1, 1), Date(2020, 1, 2)]
        times = Time[Time(1, 2, 3), Time(4, 5, 6)]
        datetimes = DateTime[DateTime(2020, 1, 1, 1, 2, 3), DateTime(2020, 1, 2, 4, 5, 6)]
        durations = Millisecond[Millisecond(5), Millisecond(10)]
        intervals = interval_type[interval_type(Int32(12)), interval_type(Int32(24))]
        decimals128 =
            decimal128_type[decimal128_type(Int128(1234)), decimal128_type(Int128(-50))]
        decimals256 = decimal256_type[
            decimal256_type(Arrow.Int256(123456)),
            decimal256_type(Arrow.Int256(-789)),
        ]
        maybe_dates = Union{Missing,Date}[Date(2021, 1, 1), missing]
        table = Arrow.Table(
            Arrow.tobuffer((
                nulls=[missing, missing],
                date=dates,
                time=times,
                datetime=datetimes,
                duration=durations,
                interval=intervals,
                decimal128=decimals128,
                decimal256=decimals256,
                maybe_date=maybe_dates,
            ),),
        )
        exported = CData.exporttable(table)
        imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

        schema = CData.schema(exported)
        array = CData.array(exported)
        nulls_schema = _child_schema(schema, 1)
        date_schema = _child_schema(schema, 2)
        time_schema = _child_schema(schema, 3)
        datetime_schema = _child_schema(schema, 4)
        duration_schema = _child_schema(schema, 5)
        interval_schema = _child_schema(schema, 6)
        decimal128_schema = _child_schema(schema, 7)
        decimal256_schema = _child_schema(schema, 8)
        maybe_date_schema = _child_schema(schema, 9)
        nulls_array = _child_array(array, 1)
        date_array = _child_array(array, 2)
        decimal128_array = _child_array(array, 7)
        maybe_date_array = _child_array(array, 9)

        @test _cstring(nulls_schema.format) == "n"
        @test _cstring(date_schema.format) == "tdD"
        @test _cstring(time_schema.format) == "ttn"
        @test _cstring(datetime_schema.format) == "tsm:"
        @test _cstring(duration_schema.format) == "tDm"
        @test _cstring(interval_schema.format) == "tiM"
        @test _cstring(decimal128_schema.format) == "d:4,2"
        @test _cstring(decimal256_schema.format) == "d:6,3,256"
        @test maybe_date_schema.flags & CData.ARROW_FLAG_NULLABLE ==
              CData.ARROW_FLAG_NULLABLE
        @test nulls_array.n_buffers == 0

        nulls = Tables.getcolumn(imported, :nulls)
        date = Tables.getcolumn(imported, :date)
        time = Tables.getcolumn(imported, :time)
        datetime = Tables.getcolumn(imported, :datetime)
        duration = Tables.getcolumn(imported, :duration)
        interval = Tables.getcolumn(imported, :interval)
        decimal128 = Tables.getcolumn(imported, :decimal128)
        decimal256 = Tables.getcolumn(imported, :decimal256)
        maybe_date = Tables.getcolumn(imported, :maybe_date)

        @test isequal(collect(nulls), [missing, missing])
        @test collect(date) == Arrow.ArrowTypes.toarrow.(dates)
        @test collect(time) == Arrow.ArrowTypes.toarrow.(times)
        @test collect(datetime) == Arrow.ArrowTypes.toarrow.(datetimes)
        @test collect(duration) == Arrow.ArrowTypes.toarrow.(durations)
        @test collect(interval) == intervals
        @test collect(decimal128) == decimals128
        @test collect(decimal256) == decimals256
        @test isequal(
            collect(maybe_date),
            Union{Missing,eltype(Arrow.ArrowTypes.toarrow.(dates))}[
                Arrow.ArrowTypes.toarrow(Date(2021, 1, 1)),
                missing,
            ],
        )
        @test pointer(date) == Ptr{eltype(date)}(unsafe_load(date_array.buffers, 2))
        @test pointer(decimal128) ==
              Ptr{decimal128_type}(unsafe_load(decimal128_array.buffers, 2))
        @test pointer(maybe_date.data) ==
              Ptr{eltype(maybe_date.data)}(unsafe_load(maybe_date_array.buffers, 2))
        @test pointer(maybe_date.validity) ==
              Ptr{UInt8}(unsafe_load(maybe_date_array.buffers, 1))

        CData.release!(imported)
        @test CData.isreleased(imported)
        @test CData.isreleased(exported)
    end

    @testset "exports and imports map columns" begin
        table = Arrow.Table(
            Arrow.tobuffer((
                map=Dict{String,Int32}[
                    Dict("a" => Int32(1), "b" => Int32(2)),
                    Dict("c" => Int32(3)),
                    Dict{String,Int32}(),
                ],
                maybe=Union{Missing,Dict{String,Int32}}[
                    Dict("z" => Int32(9)),
                    missing,
                    Dict{String,Int32}(),
                ],
            ),),
        )
        exported = CData.exporttable(table)
        imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

        schema = CData.schema(exported)
        array = CData.array(exported)
        map_schema = _child_schema(schema, 1)
        maybe_schema = _child_schema(schema, 2)
        map_array = _child_array(array, 1)
        maybe_array = _child_array(array, 2)
        entries_schema = _child_schema(map_schema, 1)
        entries_array = _child_array(map_array, 1)
        key_array = _child_array(entries_array, 1)
        value_array = _child_array(entries_array, 2)

        @test _cstring(map_schema.format) == "+m"
        @test _cstring(entries_schema.format) == "+s"
        @test _cstring(_child_schema(entries_schema, 1).name) == "key"
        @test _cstring(_child_schema(entries_schema, 2).name) == "value"
        @test maybe_schema.flags & CData.ARROW_FLAG_NULLABLE == CData.ARROW_FLAG_NULLABLE

        map = Tables.getcolumn(imported, :map)
        maybe = Tables.getcolumn(imported, :maybe)
        @test map isa CData.ImportedMapVector
        @test maybe isa CData.ImportedMapVector
        @test collect(map) == Dict{String,Int32}[
            Dict("a" => Int32(1), "b" => Int32(2)),
            Dict("c" => Int32(3)),
            Dict{String,Int32}(),
        ]
        @test isequal(
            collect(maybe),
            Union{Missing,Dict{String,Int32}}[
                Dict("z" => Int32(9)),
                missing,
                Dict{String,Int32}(),
            ],
        )
        @test pointer(map.offsets) == Ptr{Int32}(unsafe_load(map_array.buffers, 2))
        @test pointer(map.entries.columns[1].offsets) ==
              Ptr{Int32}(unsafe_load(key_array.buffers, 2))
        @test pointer(map.entries.columns[2]) ==
              Ptr{Int32}(unsafe_load(value_array.buffers, 2))
        @test pointer(maybe.validity) == Ptr{UInt8}(unsafe_load(maybe_array.buffers, 1))

        CData.release!(imported)
        @test CData.isreleased(imported)
        @test CData.isreleased(exported)

        large_offsets = Arrow.Table(
            Arrow.tobuffer(
                (map=Dict{String,Int32}[Dict("large" => Int32(1))],);
                largelists=true,
            ),
        )
        @test_throws ArgumentError CData.exporttable(large_offsets)
    end

    @testset "exports and imports union columns" begin
        source = Union{Int64,Float64,Missing}[1, 2.0, missing, 3]
        table = Arrow.Table(
            Arrow.tobuffer((
                dense=Arrow.DenseUnionVector(source),
                sparse=Arrow.SparseUnionVector(source),
            ),),
        )
        exported = CData.exporttable(table)
        imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

        schema = CData.schema(exported)
        array = CData.array(exported)
        dense_schema = _child_schema(schema, 1)
        sparse_schema = _child_schema(schema, 2)
        dense_array = _child_array(array, 1)
        sparse_array = _child_array(array, 2)
        dense_int_child_array = _child_array(dense_array, 3)
        sparse_float_child_array = _child_array(sparse_array, 2)

        @test _cstring(dense_schema.format) == "+ud:0,1,2"
        @test _cstring(sparse_schema.format) == "+us:0,1,2"
        @test dense_schema.n_children == 3
        @test sparse_schema.n_children == 3
        @test dense_array.n_buffers == 2
        @test sparse_array.n_buffers == 1

        dense = Tables.getcolumn(imported, :dense)
        sparse = Tables.getcolumn(imported, :sparse)
        expected = Union{Missing,Float64,Int64}[Int64(1), 2.0, missing, Int64(3)]
        @test dense isa CData.ImportedDenseUnionVector
        @test sparse isa CData.ImportedSparseUnionVector
        @test isequal(collect(dense), expected)
        @test isequal(collect(sparse), expected)
        @test pointer(dense.type_ids) == Ptr{UInt8}(unsafe_load(dense_array.buffers, 1))
        @test pointer(dense.offsets) == Ptr{Int32}(unsafe_load(dense_array.buffers, 2))
        @test pointer(sparse.type_ids) == Ptr{UInt8}(unsafe_load(sparse_array.buffers, 1))
        @test pointer(dense.children[3]) ==
              Ptr{Int64}(unsafe_load(dense_int_child_array.buffers, 2))
        @test pointer(sparse.children[2]) ==
              Ptr{Float64}(unsafe_load(sparse_float_child_array.buffers, 2))

        CData.release!(imported)
        @test CData.isreleased(imported)
        @test CData.isreleased(exported)
    end

    @testset "exports and imports run-end encoded columns" begin
        table =
            Arrow.Table(joinpath(@__DIR__, "run_end_encoded_small.arrow"); convert=false)
        exported = CData.exporttable(table)
        imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

        schema = CData.schema(exported)
        array = CData.array(exported)
        x_schema = _child_schema(schema, 1)
        x_array = _child_array(array, 1)
        run_ends_schema = _child_schema(x_schema, 1)
        values_schema = _child_schema(x_schema, 2)
        run_ends_array = _child_array(x_array, 1)
        values_array = _child_array(x_array, 2)

        @test _cstring(x_schema.format) == "+r"
        @test _cstring(run_ends_schema.name) == "run_ends"
        @test _cstring(values_schema.name) == "values"
        @test _cstring(run_ends_schema.format) == "s"
        @test _cstring(values_schema.format) == "u"
        @test x_array.length == 5
        @test x_array.n_buffers == 0
        @test x_array.n_children == 2

        x = Tables.getcolumn(imported, :x)
        values =
            x.values isa CData.ImportedNullableStringVector ? x.values.values : x.values
        @test x isa CData.ImportedRunEndEncodedVector
        @test isequal(collect(x), Union{Missing,String}["a", "a", "b", "b", "b"])
        @test pointer(x.run_ends) == Ptr{Int16}(unsafe_load(run_ends_array.buffers, 2))
        @test pointer(values.offsets) == Ptr{Int32}(unsafe_load(values_array.buffers, 2))
        @test pointer(values.data) == Ptr{UInt8}(unsafe_load(values_array.buffers, 3))

        CData.release!(imported)
        @test CData.isreleased(imported)
        @test CData.isreleased(exported)
    end

    @testset "exports and imports UTF-8 and binary view columns" begin
        table = Arrow.Table(joinpath(@__DIR__, "reject_reason_trimmed.arrow"))
        exported = CData.exporttable(table)
        imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

        schema = CData.schema(exported)
        array = CData.array(exported)
        reason_schema = _child_schema(schema, 1)
        reason_array = _child_array(array, 1)
        source_reason = Tables.getcolumn(table, :reject_reason)
        reason = Tables.getcolumn(imported, :reject_reason)

        @test _cstring(reason_schema.format) == "vu"
        @test reason_array.n_buffers == 4
        @test reason isa CData.ImportedStringViewVector
        @test isequal(collect(reason), collect(source_reason))
        @test pointer(reason.views) ==
              Ptr{Arrow.ViewElement}(unsafe_load(reason_array.buffers, 2))
        @test pointer(reason.buffers[1]) == Ptr{UInt8}(unsafe_load(reason_array.buffers, 3))
        @test pointer(reason.lengths) == Ptr{Int64}(unsafe_load(reason_array.buffers, 4))
        @test reason.lengths ==
              Int64[length(buffer) for buffer in getfield(source_reason, :buffers)]

        CData.release!(imported)
        @test CData.isreleased(imported)
        @test CData.isreleased(exported)

        binary_source = Arrow.View{Union{Missing,Base.CodeUnits}}(
            getfield(source_reason, :arrow),
            getfield(source_reason, :validity),
            getfield(source_reason, :data),
            getfield(source_reason, :inline),
            getfield(source_reason, :buffers),
            length(source_reason),
            getfield(source_reason, :metadata),
        )
        binary_table = Arrow.Table(
            [:blob],
            Type[eltype(binary_source)],
            AbstractVector[binary_source],
            Dict{Symbol,AbstractVector}(:blob => binary_source),
            Ref{Arrow.Meta.Schema}(),
        )
        binary_exported = CData.exporttable(binary_table)
        binary_imported = CData.importtable(
            CData.schema_ptr(binary_exported),
            CData.array_ptr(binary_exported),
        )

        blob_schema = _child_schema(CData.schema(binary_exported), 1)
        blob_array = _child_array(CData.array(binary_exported), 1)
        blob = Tables.getcolumn(binary_imported, :blob)
        @test _cstring(blob_schema.format) == "vz"
        @test blob_array.n_buffers == 4
        @test blob isa CData.ImportedBinaryViewVector
        @test isequal(
            [ismissing(value) ? missing : collect(value) for value in blob],
            [ismissing(value) ? missing : collect(value) for value in binary_source],
        )
        @test pointer(blob.views) ==
              Ptr{Arrow.ViewElement}(unsafe_load(blob_array.buffers, 2))
        @test pointer(blob.buffers[1]) == Ptr{UInt8}(unsafe_load(blob_array.buffers, 3))
        @test pointer(blob.lengths) == Ptr{Int64}(unsafe_load(blob_array.buffers, 4))

        CData.release!(binary_imported)
        @test CData.isreleased(binary_imported)
        @test CData.isreleased(binary_exported)
    end

    @testset "imports list-view C Data columns" begin
        function list_view_export(::Type{O}, format::AbstractString) where {O}
            values = Int32[12, -7, 25, 0, -127, 127, 50]
            offsets = O[0, 7, 3, 0]
            sizes = O[3, 0, 4, 0]
            validity = UInt8[0x0d]
            item_schema = CData._schema_export("i", "item")
            list_schema = CData._schema_export(
                format,
                "values";
                flags=CData.ARROW_FLAG_NULLABLE,
                children=CData.SchemaExport[item_schema],
            )
            top_schema =
                CData._schema_export("+s", ""; children=CData.SchemaExport[list_schema])
            item_array = CData._array_export(
                values,
                length(values),
                0,
                Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(values))],
            )
            list_array = CData._array_export(
                (validity, offsets, sizes, values),
                length(offsets),
                1,
                Ptr{Cvoid}[
                    Ptr{Cvoid}(pointer(validity)),
                    Ptr{Cvoid}(pointer(offsets)),
                    Ptr{Cvoid}(pointer(sizes)),
                ];
                children=CData.ArrayExport[item_array],
            )
            top_array = CData._array_export(
                (validity, offsets, sizes, values),
                length(offsets),
                0,
                Ptr{Cvoid}[C_NULL];
                children=CData.ArrayExport[list_array],
            )
            return CData.ExportedTable(top_schema, top_array), values
        end

        exported, values = list_view_export(Int32, "+vl")
        imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))
        schema = CData.schema(exported)
        array = CData.array(exported)
        values_schema = _child_schema(schema, 1)
        values_array = _child_array(array, 1)
        item_array = _child_array(values_array, 1)
        list_view = Tables.getcolumn(imported, :values)

        @test _cstring(values_schema.format) == "+vl"
        @test list_view isa CData.ImportedListViewVector
        @test eltype(list_view) == Union{Missing,AbstractVector{Int32}}
        @test isequal(
            [ismissing(value) ? missing : collect(value) for value in list_view],
            Union{Missing,Vector{Int32}}[
                Int32[12, -7, 25],
                missing,
                Int32[0, -127, 127, 50],
                Int32[],
            ],
        )
        @test pointer(list_view.offsets) == Ptr{Int32}(unsafe_load(values_array.buffers, 2))
        @test pointer(list_view.sizes) == Ptr{Int32}(unsafe_load(values_array.buffers, 3))
        @test pointer(list_view.values) == Ptr{Int32}(unsafe_load(item_array.buffers, 2))

        CData.release!(imported)
        @test CData.isreleased(imported)
        @test CData.isreleased(exported)

        large_exported, _ = list_view_export(Int64, "+vL")
        large_imported = CData.importtable(
            CData.schema_ptr(large_exported),
            CData.array_ptr(large_exported),
        )
        large_schema = _child_schema(CData.schema(large_exported), 1)
        large_array = _child_array(CData.array(large_exported), 1)
        large_values = Tables.getcolumn(large_imported, :values)
        @test _cstring(large_schema.format) == "+vL"
        @test large_values.offsets isa Vector{Int64}
        @test large_values.sizes isa Vector{Int64}
        @test isequal(
            map(value -> ismissing(value) ? missing : collect(value), large_values),
            Union{Missing,Vector{Int32}}[
                Int32[12, -7, 25],
                missing,
                Int32[0, -127, 127, 50],
                Int32[],
            ],
        )
        @test pointer(large_values.offsets) ==
              Ptr{Int64}(unsafe_load(large_array.buffers, 2))
        @test pointer(large_values.sizes) == Ptr{Int64}(unsafe_load(large_array.buffers, 3))

        CData.release!(large_imported)
        @test CData.isreleased(large_imported)
        @test CData.isreleased(large_exported)
        @test values == Int32[12, -7, 25, 0, -127, 127, 50]
    end

    @testset "aligns with C Data base-structure contract" begin
        table = Arrow.Table(Arrow.tobuffer((id=Int32[1, 2, 3], name=["a", "bb", "ccc"])))
        exported = CData.exporttable(table)
        schema = CData.schema(exported)
        array = CData.array(exported)

        @test _cstring(schema.format) == "+s"
        @test schema.metadata == C_NULL
        @test _child_schema(schema, 1).metadata == C_NULL
        @test array.offset == 0
        @test array.n_buffers == 1
        @test schema.release != C_NULL
        @test array.release != C_NULL

        bad_offset = Ref(
            CData.ArrowArray(
                array.length,
                array.null_count,
                1,
                array.n_buffers,
                array.n_children,
                array.buffers,
                array.children,
                array.dictionary,
                array.release,
                array.private_data,
            ),
        )
        GC.@preserve bad_offset begin
            @test_throws ArgumentError CData.importtable(
                CData.schema_ptr(exported),
                Base.unsafe_convert(Ptr{CData.ArrowArray}, bad_offset),
            )
        end

        bad_buffers = Ref(
            CData.ArrowArray(
                array.length,
                array.null_count,
                array.offset,
                0,
                array.n_children,
                array.buffers,
                array.children,
                array.dictionary,
                array.release,
                array.private_data,
            ),
        )
        GC.@preserve bad_buffers begin
            @test_throws ArgumentError CData.importtable(
                CData.schema_ptr(exported),
                Base.unsafe_convert(Ptr{CData.ArrowArray}, bad_buffers),
            )
        end

        ccall(array.release, Cvoid, (Ptr{CData.ArrowArray},), CData.array_ptr(exported))
        @test CData.array(exported).release == C_NULL

        CData.release!(exported)
        @test CData.isreleased(exported)
        CData.release!(exported)
        @test CData.isreleased(exported)
    end
end
