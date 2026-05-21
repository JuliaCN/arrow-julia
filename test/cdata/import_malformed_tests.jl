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

include("import_malformed/support.jl")

include("import_malformed/child_pointers.jl")
include("import_malformed/dictionary_peers.jl")

@testset "rejects malformed C Data metadata bytes" begin
    table = Arrow.Table(Arrow.tobuffer((id=Int32[1, 2], label=["a", "bb"])))

    negative_entry_count = CData.exporttable(table)
    metadata = _int32_metadata_bytes(Int32(-1))
    bad_schema = _schema_ref_with_metadata(CData.schema(negative_entry_count), metadata)
    GC.@preserve metadata bad_schema begin
        @test_throws ArgumentError("Arrow C Data metadata has negative entry count") CData.importtable(
            Base.unsafe_convert(Ptr{CData.ArrowSchema}, bad_schema),
            CData.array_ptr(negative_entry_count),
        )
    end
    CData.release!(negative_entry_count)
    @test CData.isreleased(negative_entry_count)

    negative_key_length = CData.exporttable(table)
    schema = CData.schema(negative_key_length)
    id_schema = _child_schema(schema, 1)
    field_metadata = _int32_metadata_bytes(Int32(1), Int32(-1))
    bad_id_schema = _schema_ref_with_metadata(id_schema, field_metadata)
    schema_children = Ptr{CData.ArrowSchema}[
        Base.unsafe_convert(Ptr{CData.ArrowSchema}, bad_id_schema),
        unsafe_load(schema.children, 2),
    ]
    bad_top_schema = _schema_ref_with_children(schema, schema_children)
    GC.@preserve field_metadata bad_id_schema schema_children bad_top_schema begin
        @test_throws ArgumentError("Arrow C Data metadata has negative byte length") CData.importtable(
            Base.unsafe_convert(Ptr{CData.ArrowSchema}, bad_top_schema),
            CData.array_ptr(negative_key_length),
        )
    end
    CData.release!(negative_key_length)
    @test CData.isreleased(negative_key_length)
end

@testset "rejects malformed C Data format strings" begin
    table = Arrow.Table(
        Arrow.tobuffer((
            id=Int32[1, 2],
            bytes=NTuple{2,UInt8}[(0x01, 0x02), (0x03, 0x04)],
            fixed=NTuple{2,Int32}[(Int32(1), Int32(2)), (Int32(3), Int32(4))],
        ),),
    )

    null_top_format = CData.exporttable(table)
    bad_top_schema =
        _schema_ref_with_format(CData.schema(null_top_format), Ptr{UInt8}(C_NULL))
    GC.@preserve bad_top_schema begin
        @test_throws ArgumentError("ArrowSchema format is C_NULL") CData.importtable(
            Base.unsafe_convert(Ptr{CData.ArrowSchema}, bad_top_schema),
            CData.array_ptr(null_top_format),
        )
    end
    CData.release!(null_top_format)
    @test CData.isreleased(null_top_format)

    null_child_format = CData.exporttable(table)
    schema = CData.schema(null_child_format)
    bad_id_schema = _schema_ref_with_format(_child_schema(schema, 1), Ptr{UInt8}(C_NULL))
    schema_children = Ptr{CData.ArrowSchema}[
        Base.unsafe_convert(Ptr{CData.ArrowSchema}, bad_id_schema),
        unsafe_load(schema.children, 2),
        unsafe_load(schema.children, 3),
    ]
    bad_schema = _schema_ref_with_children(schema, schema_children)
    GC.@preserve bad_id_schema schema_children bad_schema begin
        @test_throws ArgumentError("ArrowSchema format is C_NULL") CData.importtable(
            Base.unsafe_convert(Ptr{CData.ArrowSchema}, bad_schema),
            CData.array_ptr(null_child_format),
        )
    end
    CData.release!(null_child_format)
    @test CData.isreleased(null_child_format)

    invalid_fixed_binary = CData.exporttable(table)
    schema = CData.schema(invalid_fixed_binary)
    zero_width_format = _format_bytes("w:0")
    bad_bytes_schema =
        _schema_ref_with_format(_child_schema(schema, 2), pointer(zero_width_format))
    schema_children = Ptr{CData.ArrowSchema}[
        unsafe_load(schema.children, 1),
        Base.unsafe_convert(Ptr{CData.ArrowSchema}, bad_bytes_schema),
        unsafe_load(schema.children, 3),
    ]
    bad_schema = _schema_ref_with_children(schema, schema_children)
    GC.@preserve zero_width_format bad_bytes_schema schema_children bad_schema begin
        @test_throws ArgumentError("Arrow C Data import fixed-size format must be positive") CData.importtable(
            Base.unsafe_convert(Ptr{CData.ArrowSchema}, bad_schema),
            CData.array_ptr(invalid_fixed_binary),
        )
    end
    CData.release!(invalid_fixed_binary)
    @test CData.isreleased(invalid_fixed_binary)

    invalid_fixed_list = CData.exporttable(table)
    schema = CData.schema(invalid_fixed_list)
    invalid_list_format = _format_bytes("+w:not-an-int")
    bad_fixed_schema =
        _schema_ref_with_format(_child_schema(schema, 3), pointer(invalid_list_format))
    schema_children = Ptr{CData.ArrowSchema}[
        unsafe_load(schema.children, 1),
        unsafe_load(schema.children, 2),
        Base.unsafe_convert(Ptr{CData.ArrowSchema}, bad_fixed_schema),
    ]
    bad_schema = _schema_ref_with_children(schema, schema_children)
    GC.@preserve invalid_list_format bad_fixed_schema schema_children bad_schema begin
        @test_throws ArgumentError(
            "Arrow C Data import has invalid fixed-size format +w:not-an-int",
        ) CData.importtable(
            Base.unsafe_convert(Ptr{CData.ArrowSchema}, bad_schema),
            CData.array_ptr(invalid_fixed_list),
        )
    end
    CData.release!(invalid_fixed_list)
    @test CData.isreleased(invalid_fixed_list)
end

@testset "rejects released C Data import inputs" begin
    table = Arrow.Table(Arrow.tobuffer((id=Int32[1, 2], color=["red", "blue"])))

    released_top_schema = CData.exporttable(table)
    bad_schema =
        _schema_ref_with_release(CData.schema(released_top_schema), Ptr{Cvoid}(C_NULL))
    GC.@preserve bad_schema begin
        @test_throws ArgumentError("cannot import released ArrowSchema") CData.importtable(
            Base.unsafe_convert(Ptr{CData.ArrowSchema}, bad_schema),
            CData.array_ptr(released_top_schema),
        )
    end
    CData.release!(released_top_schema)
    @test CData.isreleased(released_top_schema)

    released_top_array = CData.exporttable(table)
    bad_array = _array_ref_with_release(CData.array(released_top_array), Ptr{Cvoid}(C_NULL))
    GC.@preserve bad_array begin
        @test_throws ArgumentError("cannot import released ArrowArray") CData.importtable(
            CData.schema_ptr(released_top_array),
            Base.unsafe_convert(Ptr{CData.ArrowArray}, bad_array),
        )
    end
    CData.release!(released_top_array)
    @test CData.isreleased(released_top_array)

    released_child_schema = CData.exporttable(table)
    schema = CData.schema(released_child_schema)
    bad_id_schema = _schema_ref_with_release(_child_schema(schema, 1), Ptr{Cvoid}(C_NULL))
    schema_children = Ptr{CData.ArrowSchema}[
        Base.unsafe_convert(Ptr{CData.ArrowSchema}, bad_id_schema),
        unsafe_load(schema.children, 2),
    ]
    bad_schema = _schema_ref_with_children(schema, schema_children)
    GC.@preserve bad_id_schema schema_children bad_schema begin
        @test_throws ArgumentError("cannot import released ArrowSchema") CData.importtable(
            Base.unsafe_convert(Ptr{CData.ArrowSchema}, bad_schema),
            CData.array_ptr(released_child_schema),
        )
    end
    CData.release!(released_child_schema)
    @test CData.isreleased(released_child_schema)

    released_child_array = CData.exporttable(table)
    array = CData.array(released_child_array)
    bad_id_array = _array_ref_with_release(_child_array(array, 1), Ptr{Cvoid}(C_NULL))
    array_children = Ptr{CData.ArrowArray}[
        Base.unsafe_convert(Ptr{CData.ArrowArray}, bad_id_array),
        unsafe_load(array.children, 2),
    ]
    bad_array = _array_ref_with_children(array, array_children)
    GC.@preserve bad_id_array array_children bad_array begin
        @test_throws ArgumentError("cannot import released ArrowArray") CData.importtable(
            CData.schema_ptr(released_child_array),
            Base.unsafe_convert(Ptr{CData.ArrowArray}, bad_array),
        )
    end
    CData.release!(released_child_array)
    @test CData.isreleased(released_child_array)

    dict_table =
        Arrow.Table(Arrow.tobuffer((color=["red", "blue", "red"],); dictencode=true))

    released_dictionary_schema = CData.exporttable(dict_table)
    schema = CData.schema(released_dictionary_schema)
    color_schema = _child_schema(schema, 1)
    bad_dictionary_schema =
        _schema_ref_with_release(unsafe_load(color_schema.dictionary), Ptr{Cvoid}(C_NULL))
    color_schema_with_bad_dictionary = Ref(
        CData.ArrowSchema(
            color_schema.format,
            color_schema.name,
            color_schema.metadata,
            color_schema.flags,
            color_schema.n_children,
            color_schema.children,
            Base.unsafe_convert(Ptr{CData.ArrowSchema}, bad_dictionary_schema),
            color_schema.release,
            color_schema.private_data,
        ),
    )
    schema_children = Ptr{CData.ArrowSchema}[Base.unsafe_convert(
        Ptr{CData.ArrowSchema},
        color_schema_with_bad_dictionary,
    ),]
    bad_schema = _schema_ref_with_children(schema, schema_children)
    GC.@preserve bad_dictionary_schema color_schema_with_bad_dictionary schema_children bad_schema begin
        @test_throws ArgumentError("cannot import released ArrowSchema") CData.importtable(
            Base.unsafe_convert(Ptr{CData.ArrowSchema}, bad_schema),
            CData.array_ptr(released_dictionary_schema),
        )
    end
    CData.release!(released_dictionary_schema)
    @test CData.isreleased(released_dictionary_schema)

    released_dictionary_array = CData.exporttable(dict_table)
    array = CData.array(released_dictionary_array)
    color_array = _child_array(array, 1)
    bad_dictionary_array =
        _array_ref_with_release(unsafe_load(color_array.dictionary), Ptr{Cvoid}(C_NULL))
    color_array_with_bad_dictionary = Ref(
        CData.ArrowArray(
            color_array.length,
            color_array.null_count,
            color_array.offset,
            color_array.n_buffers,
            color_array.n_children,
            color_array.buffers,
            color_array.children,
            Base.unsafe_convert(Ptr{CData.ArrowArray}, bad_dictionary_array),
            color_array.release,
            color_array.private_data,
        ),
    )
    array_children = Ptr{CData.ArrowArray}[Base.unsafe_convert(
        Ptr{CData.ArrowArray},
        color_array_with_bad_dictionary,
    ),]
    bad_array = _array_ref_with_children(array, array_children)
    GC.@preserve bad_dictionary_array color_array_with_bad_dictionary array_children bad_array begin
        @test_throws ArgumentError("cannot import released ArrowArray") CData.importtable(
            CData.schema_ptr(released_dictionary_array),
            Base.unsafe_convert(Ptr{CData.ArrowArray}, bad_array),
        )
    end
    CData.release!(released_dictionary_array)
    @test CData.isreleased(released_dictionary_array)
end
