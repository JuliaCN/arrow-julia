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
