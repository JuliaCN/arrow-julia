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

@testset "Malformed IPC buffer bounds fixtures" begin
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

    function patch_first_record_batch_buffer!(
        bytes,
        buffer_index;
        offset=nothing,
        new_length=nothing,
    )
        blob = Arrow.ArrowBlob(bytes, 1, nothing)
        for batch in Arrow.BatchIterator(blob)
            rb = batch.msg.header
            rb isa Arrow.Meta.RecordBatch || continue
            buffer = rb.buffers[buffer_index]
            if offset !== nothing
                raw = collect(reinterpret(UInt8, Int64[offset]))
                copyto!(bytes, Arrow.FlatBuffers.pos(buffer) + 1, raw, 1, length(raw))
            end
            if new_length !== nothing
                raw = collect(reinterpret(UInt8, Int64[new_length]))
                copyto!(bytes, Arrow.FlatBuffers.pos(buffer) + 9, raw, 1, length(raw))
            end
            return bytes
        end
        error("record batch not found")
    end

    function truncate_first_record_batch_buffers!(bytes, declared_count::UInt32)
        blob = Arrow.ArrowBlob(bytes, 1, nothing)
        for batch in Arrow.BatchIterator(blob)
            rb = batch.msg.header
            rb isa Arrow.Meta.RecordBatch || continue
            raw = collect(reinterpret(UInt8, UInt32[declared_count]))
            copyto!(bytes, rb.buffers.pos - 3, raw, 1, length(raw))
            return bytes
        end
        error("record batch not found")
    end

    function first_record_batch_header(bytes)
        blob = Arrow.ArrowBlob(bytes, 1, nothing)
        for batch in Arrow.BatchIterator(blob)
            rb = batch.msg.header
            rb isa Arrow.Meta.RecordBatch && return rb
        end
        error("record batch not found")
    end

    @test_throws ArgumentError(
        "record batch is missing buffer 3; only 2 buffers are declared",
    ) Arrow._record_batch_buffer(
        first_record_batch_header(read(Arrow.tobuffer((values=Int32[1],); ntasks=0))),
        3,
    )

    buffer_offset_beyond_body = patch_first_record_batch_buffer!(
        read(Arrow.tobuffer((values=Int32[1, 2],); ntasks=0)),
        2;
        offset=typemax(Int64),
    )
    assert_argument_error(
        () -> Arrow.validate(buffer_offset_beyond_body),
        "record batch buffer offset",
    )
    assert_argument_error(
        () -> Arrow.validate(buffer_offset_beyond_body; stream=true),
        "record batch buffer offset",
    )

    buffer_length_beyond_body = patch_first_record_batch_buffer!(
        read(Arrow.tobuffer((values=Int32[1, 2],); ntasks=0)),
        2;
        new_length=typemax(Int64),
    )
    assert_argument_error(
        () -> Arrow.validate(buffer_length_beyond_body),
        "record batch buffer length",
    )
    assert_argument_error(
        () -> Arrow.validate(buffer_length_beyond_body; stream=true),
        "record batch buffer length",
    )

    buffer_length_not_element_width = patch_first_record_batch_buffer!(
        read(Arrow.tobuffer((values=Int32[1],); ntasks=0)),
        2;
        new_length=Int64(6),
    )
    assert_argument_error(
        () -> Arrow.validate(buffer_length_not_element_width),
        "not a multiple",
    )
    assert_argument_error(
        () -> Arrow.validate(buffer_length_not_element_width; stream=true),
        "not a multiple",
    )

    missing_value_buffer = truncate_first_record_batch_buffers!(
        read(Arrow.tobuffer((values=Int32[1],); ntasks=0)),
        UInt32(1),
    )
    assert_argument_error(
        () -> Arrow.validate(missing_value_buffer),
        "record batch is missing buffer 2",
    )
    assert_argument_error(
        () -> Arrow.validate(missing_value_buffer; stream=true),
        "record batch is missing buffer 2",
    )
end
