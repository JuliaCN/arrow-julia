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

@testset "Malformed IPC dictionary batch validation" begin
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

    function first_dictionary_batch_header(bytes)
        blob = Arrow.ArrowBlob(bytes, 1, nothing)
        for batch in Arrow.BatchIterator(blob)
            dictionary = batch.msg.header
            dictionary isa Arrow.Meta.DictionaryBatch && return dictionary
        end
        error("dictionary batch not found")
    end

    function dictionary_batch_first_bytes(bytes)
        iterator = Arrow.BatchIterator(Arrow.ArrowBlob(bytes, 1, nothing))
        state = (iterator.startpos, 0)
        while true
            current_pos = state[1]
            next = iterate(iterator, state)
            next === nothing && break
            batch, state = next
            batch.msg.header isa Arrow.Meta.DictionaryBatch && return bytes[current_pos:end]
        end
        error("dictionary batch not found")
    end

    function patch_first_dictionary_batch_node_count!(bytes, declared_count::UInt32)
        dictionary = first_dictionary_batch_header(bytes)
        raw = collect(reinterpret(UInt8, UInt32[declared_count]))
        copyto!(bytes, dictionary.data.nodes.pos - 3, raw, 1, length(raw))
        return bytes
    end

    valid_dictionary =
        read(Arrow.tobuffer((values=PooledArray(["alpha", "beta", "alpha"]),); ntasks=0))
    @test isnothing(Arrow.validate(valid_dictionary))
    @test isnothing(Arrow.validate(valid_dictionary; stream=true))

    extra_dictionary_node = patch_first_dictionary_batch_node_count!(
        read(Arrow.tobuffer((values=PooledArray(["alpha", "beta", "alpha"]),); ntasks=0)),
        UInt32(2),
    )
    assert_argument_error(
        () -> Arrow.validate(extra_dictionary_node),
        "record batch declares 2 field nodes but schema consumed 1",
    )
    assert_argument_error(
        () -> Arrow.validate(extra_dictionary_node; stream=true),
        "record batch declares 2 field nodes but schema consumed 1",
    )

    schema_less_dictionary = dictionary_batch_first_bytes(
        read(Arrow.tobuffer((values=PooledArray(["alpha", "beta", "alpha"]),); ntasks=0)),
    )
    assert_argument_error(
        () -> Arrow.validate(schema_less_dictionary),
        "first arrow ipc message MUST be a schema message",
    )
    assert_argument_error(
        () -> Arrow.validate(schema_less_dictionary; stream=true),
        "first arrow ipc message MUST be a schema message",
    )

    @test_throws ArgumentError("dictionary batch id 42 has no schema dictionary field") Arrow._dictionary_encoded_field(
        Dict{Int64,Arrow.Meta.Field}(),
        Int64(42),
    )
end
