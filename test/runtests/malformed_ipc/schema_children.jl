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

@testset "Malformed IPC schema child validation" begin
    function assert_argument_error(f::Function, needle::AbstractString)
        err = try
            f()
            nothing
        catch e
            e
        end
        @test err !== nothing
        @test occursin(needle, sprint(showerror, err))
        return
    end

    function first_schema(bytes)
        blob = Arrow.ArrowBlob(bytes, 1, nothing)
        for batch in Arrow.BatchIterator(blob)
            schema = batch.msg.header
            schema isa Arrow.Meta.Schema && return schema
        end
        error("schema not found")
    end

    function truncate_first_field_children!(bytes, declared_count::UInt32)
        field = first_schema(bytes).fields[1]
        raw = collect(reinterpret(UInt8, UInt32[declared_count]))
        copyto!(bytes, field.children.pos - 3, raw, 1, length(raw))
        return bytes
    end

    function patch_first_field_type_tag!(bytes, tag::UInt8)
        field = first_schema(bytes).fields[1]
        offset = Arrow.FlatBuffers.offset(field, 8)
        offset != 0 || error("field type tag not found")
        bytes[Arrow.FlatBuffers.pos(field) + offset + 1] = tag
        return bytes
    end

    valid_list = read(Arrow.tobuffer((values=[Int32[1, 2]],); ntasks=0))
    @test_throws ArgumentError(
        "field values is missing child 2; only 1 children are declared",
    ) Arrow._field_child(first_schema(valid_list).fields[1], 2)

    unsupported_field_type = patch_first_field_type_tag!(
        read(Arrow.tobuffer((values=Int32[1, 2],); ntasks=0)),
        UInt8(255),
    )
    assert_argument_error(
        () -> Arrow.validate(unsupported_field_type),
        "unsupported arrow field type tag 255",
    )
    assert_argument_error(
        () -> Arrow.validate(unsupported_field_type; stream=true),
        "unsupported arrow field type tag 255",
    )

    missing_list_child = truncate_first_field_children!(
        read(Arrow.tobuffer((values=[Int32[1, 2]],); ntasks=0)),
        UInt32(0),
    )
    assert_argument_error(
        () -> Arrow.validate(missing_list_child),
        "field values is missing child 1",
    )
    assert_argument_error(
        () -> Arrow.validate(missing_list_child; stream=true),
        "field values is missing child 1",
    )

    missing_struct_children = truncate_first_field_children!(
        read(Arrow.tobuffer((values=[(a=1, b=2)],); ntasks=0)),
        UInt32(0),
    )
    assert_argument_error(
        () -> Arrow.validate(missing_struct_children),
        "record batch declares 3 field nodes but schema consumed 1",
    )
    assert_argument_error(
        () -> Arrow.validate(missing_struct_children; stream=true),
        "record batch declares 3 field nodes but schema consumed 1",
    )
end
