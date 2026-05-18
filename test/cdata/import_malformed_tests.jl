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

function _schema_ref_with_children(schema::CData.ArrowSchema, children)
    return Ref(
        CData.ArrowSchema(
            schema.format,
            schema.name,
            schema.metadata,
            schema.flags,
            Int64(length(children)),
            pointer(children),
            schema.dictionary,
            schema.release,
            schema.private_data,
        ),
    )
end

function _array_ref_with_children(array::CData.ArrowArray, children)
    return Ref(
        CData.ArrowArray(
            array.length,
            array.null_count,
            array.offset,
            array.n_buffers,
            Int64(length(children)),
            array.buffers,
            pointer(children),
            array.dictionary,
            array.release,
            array.private_data,
        ),
    )
end

@testset "rejects malformed C Data child pointer entries" begin
    table = Arrow.Table(Arrow.tobuffer((id=Int32[1, 2], label=["a", "bb"])))
    exported = CData.exporttable(table)
    schema = CData.schema(exported)
    array = CData.array(exported)

    missing_schema_child = Ptr{CData.ArrowSchema}[
        Ptr{CData.ArrowSchema}(C_NULL),
        unsafe_load(schema.children, 2),
    ]
    bad_schema = _schema_ref_with_children(schema, missing_schema_child)
    GC.@preserve missing_schema_child bad_schema begin
        @test_throws ArgumentError("child ArrowSchema pointer is C_NULL") CData.importtable(
            Base.unsafe_convert(Ptr{CData.ArrowSchema}, bad_schema),
            CData.array_ptr(exported),
        )
    end

    missing_array_child =
        Ptr{CData.ArrowArray}[unsafe_load(array.children, 1), Ptr{CData.ArrowArray}(C_NULL)]
    bad_array = _array_ref_with_children(array, missing_array_child)
    GC.@preserve missing_array_child bad_array begin
        @test_throws ArgumentError("child ArrowArray pointer is C_NULL") CData.importtable(
            CData.schema_ptr(exported),
            Base.unsafe_convert(Ptr{CData.ArrowArray}, bad_array),
        )
    end

    CData.release!(exported)
    @test CData.isreleased(exported)
end

@testset "rejects malformed C Data dictionary peer pointers" begin
    table = Arrow.Table(Arrow.tobuffer((color=["red", "blue", "red"],); dictencode=true))

    missing_dictionary_schema = CData.exporttable(table)
    schema = CData.schema(missing_dictionary_schema)
    color_schema = _child_schema(schema, 1)
    color_schema_without_dictionary = Ref(
        CData.ArrowSchema(
            color_schema.format,
            color_schema.name,
            color_schema.metadata,
            color_schema.flags,
            color_schema.n_children,
            color_schema.children,
            Ptr{CData.ArrowSchema}(C_NULL),
            color_schema.release,
            color_schema.private_data,
        ),
    )
    schema_children = Ptr{CData.ArrowSchema}[Base.unsafe_convert(
        Ptr{CData.ArrowSchema},
        color_schema_without_dictionary,
    ),]
    bad_schema = _schema_ref_with_children(schema, schema_children)
    GC.@preserve color_schema_without_dictionary schema_children bad_schema begin
        @test_throws ArgumentError("dictionary column color has C_NULL dictionary schema") CData.importtable(
            Base.unsafe_convert(Ptr{CData.ArrowSchema}, bad_schema),
            CData.array_ptr(missing_dictionary_schema),
        )
    end
    CData.release!(missing_dictionary_schema)
    @test CData.isreleased(missing_dictionary_schema)

    missing_dictionary_array = CData.exporttable(table)
    array = CData.array(missing_dictionary_array)
    color_array = _child_array(array, 1)
    color_array_without_dictionary = Ref(
        CData.ArrowArray(
            color_array.length,
            color_array.null_count,
            color_array.offset,
            color_array.n_buffers,
            color_array.n_children,
            color_array.buffers,
            color_array.children,
            Ptr{CData.ArrowArray}(C_NULL),
            color_array.release,
            color_array.private_data,
        ),
    )
    array_children = Ptr{CData.ArrowArray}[Base.unsafe_convert(
        Ptr{CData.ArrowArray},
        color_array_without_dictionary,
    ),]
    bad_array = _array_ref_with_children(array, array_children)
    GC.@preserve color_array_without_dictionary array_children bad_array begin
        @test_throws ArgumentError("dictionary column color has C_NULL dictionary array") CData.importtable(
            CData.schema_ptr(missing_dictionary_array),
            Base.unsafe_convert(Ptr{CData.ArrowArray}, bad_array),
        )
    end
    CData.release!(missing_dictionary_array)
    @test CData.isreleased(missing_dictionary_array)
end
