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

@testset "Sparse union IPC layout validation" begin
    source = (
        values=Arrow.SparseUnionVector(
            Union{Int64,Float64,Missing}[1, 2.0, 3, 4.0, missing],
        ),
    )
    dense_source = (
        values=Arrow.DenseUnionVector(
            Union{Int64,Float64,Missing}[1, 2.0, 3, 4.0, missing],
        ),
    )

    function assert_sparse_child_lengths(table)
        values = Tables.getcolumn(table, :values)
        @test values isa Arrow.SparseUnion
        @test map(length, values.data) == (length(values), length(values), length(values))
        return values
    end

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

    function patch_first_union_mode!(bytes, mode::Int16)
        union = first_schema(bytes).fields[1].type
        union isa Arrow.Meta.Union || error("union field not found")
        offset = Arrow.FlatBuffers.offset(union, 4)
        offset != 0 || error("union mode field not found")
        raw = collect(reinterpret(UInt8, Int16[mode]))
        copyto!(bytes, Arrow.FlatBuffers.pos(union) + offset + 1, raw, 1, length(raw))
        return bytes
    end

    table = Arrow.Table(Arrow.tobuffer(source))
    assert_sparse_child_lengths(table)

    invalid_union_mode =
        patch_first_union_mode!(read(Arrow.tobuffer(dense_source; ntasks=0)), Int16(7))
    assert_argument_error(
        () -> Arrow.validate(invalid_union_mode),
        "unsupported arrow union mode tag 7",
    )
    assert_argument_error(
        () -> Arrow.validate(invalid_union_mode; stream=true),
        "unsupported arrow union mode tag 7",
    )

    mktemp() do path, io
        write(io, read(Arrow.tobuffer(source)))
        close(io)

        Arrow.append(path, Arrow.Table(read(path)))
        for batch in Arrow.Stream(path)
            assert_sparse_child_lengths(batch)
        end
    end

    @test_throws ArgumentError(
        "sparse union column values child 2 length 2 must match row count 3",
    ) Arrow._assert_sparse_union_layout!(
        UInt8[0x00, 0x01, 0x00],
        (Int64[1, 3, 5], Float64[0.0, 2.0]),
        nothing,
        3,
        :values,
    )
end
