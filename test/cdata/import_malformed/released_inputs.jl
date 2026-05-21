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
