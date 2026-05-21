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
end
