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

@testset "Malformed IPC message framing fixtures" begin
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

    function patch_first_record_batch_body_length!(bytes, new_body_length::Int64)
        blob = Arrow.ArrowBlob(bytes, 1, nothing)
        for batch in Arrow.BatchIterator(blob)
            batch.msg.header isa Arrow.Meta.RecordBatch || continue
            body_length_offset = Arrow.FlatBuffers.offset(batch.msg, 10)
            @test body_length_offset != 0
            raw = collect(reinterpret(UInt8, Int64[new_body_length]))
            copyto!(
                bytes,
                Arrow.FlatBuffers.pos(batch.msg) + body_length_offset + 1,
                raw,
                1,
                length(raw),
            )
            return bytes
        end
        error("record batch not found")
    end

    function patch_first_message_version!(bytes, version::Int16)
        iterator = Arrow.BatchIterator(Arrow.ArrowBlob(bytes, 1, nothing))
        batch, _ = iterate(iterator, (iterator.startpos, 0))
        version_offset = Arrow.FlatBuffers.offset(batch.msg, 4)
        version_offset != 0 || error("message version field not found")
        raw = collect(reinterpret(UInt8, Int16[version]))
        copyto!(
            bytes,
            Arrow.FlatBuffers.pos(batch.msg) + version_offset + 1,
            raw,
            1,
            length(raw),
        )
        return bytes
    end

    invalid_message_version = patch_first_message_version!(
        read(Arrow.tobuffer((values=Int32[1, 2],); ntasks=0)),
        Int16(42),
    )
    assert_argument_error(
        () -> Arrow.validate(invalid_message_version),
        "unsupported arrow ipc message metadata version 42",
    )
    assert_argument_error(
        () -> Arrow.validate(invalid_message_version; stream=true),
        "unsupported arrow ipc message metadata version 42",
    )

    negative_body_length = patch_first_record_batch_body_length!(
        read(Arrow.tobuffer((values=Int32[1, 2],); ntasks=0)),
        Int64(-1),
    )
    assert_argument_error(
        () -> Arrow.validate(negative_body_length),
        "arrow ipc message body length must be non-negative",
    )
    assert_argument_error(
        () -> Arrow.validate(negative_body_length; stream=true),
        "arrow ipc message body length must be non-negative",
    )

    oversized_body_length = patch_first_record_batch_body_length!(
        read(Arrow.tobuffer((values=Int32[1, 2],); ntasks=0)),
        typemax(Int64),
    )
    assert_argument_error(
        () -> Arrow.validate(oversized_body_length),
        "truncated arrow ipc message body",
    )
    assert_argument_error(
        () -> Arrow.validate(oversized_body_length; stream=true),
        "truncated arrow ipc message body",
    )
end
