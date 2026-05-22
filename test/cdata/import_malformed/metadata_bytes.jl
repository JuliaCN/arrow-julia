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
