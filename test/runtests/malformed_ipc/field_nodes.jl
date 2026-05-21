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

@testset "Malformed IPC field node validation" begin
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

    function first_record_batch_header(bytes)
        blob = Arrow.ArrowBlob(bytes, 1, nothing)
        for batch in Arrow.BatchIterator(blob)
            rb = batch.msg.header
            rb isa Arrow.Meta.RecordBatch && return rb
        end
        error("record batch not found")
    end

    function truncate_first_record_batch_nodes!(bytes, declared_count::UInt32)
        rb = first_record_batch_header(bytes)
        raw = collect(reinterpret(UInt8, UInt32[declared_count]))
        copyto!(bytes, rb.nodes.pos - 3, raw, 1, length(raw))
        return bytes
    end

    @test Arrow._assert_field_node_shape((length=0, null_count=0), :values) === nothing
    @test Arrow._assert_field_node_shape((length=3, null_count=2), :values) === nothing
    @test_throws ArgumentError("field values has negative length") Arrow._assert_field_node_shape(
        (length=-1, null_count=0),
        :values,
    )
    @test_throws ArgumentError("field values has negative null count") Arrow._assert_field_node_shape(
        (length=1, null_count=-1),
        :values,
    )
    @test_throws ArgumentError("field values null count exceeds length") Arrow._assert_field_node_shape(
        (length=1, null_count=2),
        :values,
    )
    @test_throws ArgumentError(
        "record batch is missing field node 2; only 1 field nodes are declared",
    ) Arrow._record_batch_node(
        first_record_batch_header(read(Arrow.tobuffer((values=Int32[1],); ntasks=0))),
        2,
    )

    missing_value_node = truncate_first_record_batch_nodes!(
        read(Arrow.tobuffer((values=Int32[1],); ntasks=0)),
        UInt32(0),
    )
    assert_argument_error(
        () -> Arrow.validate(missing_value_node),
        "record batch is missing field node 1",
    )
    assert_argument_error(
        () -> Arrow.validate(missing_value_node; stream=true),
        "record batch is missing field node 1",
    )
end
