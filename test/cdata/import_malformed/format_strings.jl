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
