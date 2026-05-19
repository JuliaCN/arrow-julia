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

@testset "tensor message boundary" begin
    function patch_message_header_type(bytes, header_type::UInt8)
        patched = copy(bytes)
        msg = Arrow.FlatBuffers.getrootas(Arrow.Meta.Message, patched, 8)
        offset = Arrow.FlatBuffers.offset(msg, 6)
        @test offset != 0
        patched[Arrow.FlatBuffers.pos(msg) + offset + 1] = header_type
        return patched
    end

    base = take!(Arrow.tobuffer((x=[1, 2],)))

    tensor_bytes = patch_message_header_type(base, UInt8(4))
    @test_throws ArgumentError(Arrow.TENSOR_UNSUPPORTED) Arrow.Table(tensor_bytes)
    @test_throws ArgumentError(Arrow.TENSOR_UNSUPPORTED) collect(Arrow.Stream(tensor_bytes))

    sparse_tensor_bytes = patch_message_header_type(base, UInt8(5))
    @test_throws ArgumentError(Arrow.SPARSE_TENSOR_UNSUPPORTED) Arrow.Table(
        sparse_tensor_bytes,
    )
    @test_throws ArgumentError(Arrow.SPARSE_TENSOR_UNSUPPORTED) collect(
        Arrow.Stream(sparse_tensor_bytes),
    )
end

@testset "# 158" begin
    # arrow ipc stream generated from pyarrow with no record batches
    bytes = UInt8[
        0xff,
        0xff,
        0xff,
        0xff,
        0x78,
        0x00,
        0x00,
        0x00,
        0x10,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x0a,
        0x00,
        0x0c,
        0x00,
        0x06,
        0x00,
        0x05,
        0x00,
        0x08,
        0x00,
        0x0a,
        0x00,
        0x00,
        0x00,
        0x00,
        0x01,
        0x04,
        0x00,
        0x0c,
        0x00,
        0x00,
        0x00,
        0x08,
        0x00,
        0x08,
        0x00,
        0x00,
        0x00,
        0x04,
        0x00,
        0x08,
        0x00,
        0x00,
        0x00,
        0x04,
        0x00,
        0x00,
        0x00,
        0x01,
        0x00,
        0x00,
        0x00,
        0x14,
        0x00,
        0x00,
        0x00,
        0x10,
        0x00,
        0x14,
        0x00,
        0x08,
        0x00,
        0x06,
        0x00,
        0x07,
        0x00,
        0x0c,
        0x00,
        0x00,
        0x00,
        0x10,
        0x00,
        0x10,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x01,
        0x02,
        0x10,
        0x00,
        0x00,
        0x00,
        0x1c,
        0x00,
        0x00,
        0x00,
        0x04,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x01,
        0x00,
        0x00,
        0x00,
        0x61,
        0x00,
        0x00,
        0x00,
        0x08,
        0x00,
        0x0c,
        0x00,
        0x08,
        0x00,
        0x07,
        0x00,
        0x08,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x01,
        0x40,
        0x00,
        0x00,
        0x00,
        0xff,
        0xff,
        0xff,
        0xff,
        0x00,
        0x00,
        0x00,
        0x00,
    ]
    tbl = Arrow.Table(bytes)
    @test length(tbl.a) == 0
    @test eltype(tbl.a) == Union{Int64,Missing}
end

@testset "# 181" begin
    # XXX this test hangs on Julia 1.12 when using a deeper nesting
    d = Dict{Int,Int}()
    for i = 1:1
        d = Dict(i => d)
    end
    tbl = (x=[d],)
    msg = "reached nested serialization level (2) deeper than provided max depth argument (1); to increase allowed nesting level, pass `maxdepth=X`"
    @test_throws ErrorException(msg) Arrow.tobuffer(tbl; maxdepth=1)
    @test Arrow.Table(Arrow.tobuffer(tbl; maxdepth=5)).x == tbl.x
end

@testset "# 167" begin
    t = (col1=[["boop", "she"], ["boop", "she"], ["boo"]],)
    tbl = Arrow.Table(Arrow.tobuffer(t))
    @test eltype(tbl.col1) <: AbstractVector{String}

    toffset = (
        col1=OffsetArray([Int64[1, 2], Int64[3, 4], Int64[]], -1:1),
        col2=OffsetArray(
            Union{Missing,Vector{Int64}}[Int64[1], missing, Int64[2, 3]],
            -1:1,
        ),
    )
    tt = Arrow.Table(Arrow.tobuffer(toffset))
    @test eltype(tt.col1) <: AbstractVector{Int64}
    @test Base.nonmissingtype(eltype(tt.col2)) <: AbstractVector{Int64}
    @test collect(toffset.col1) == tt.col1
    @test isequal(collect(toffset.col2), tt.col2)
end

@testset "# 200 VersionNumber" begin
    t = (col1=[v"1"],)
    tbl = Arrow.Table(Arrow.tobuffer(t))
    @test eltype(tbl.col1) == VersionNumber
end

@testset "offset struct string write paths" begin
    rows = OffsetArray(
        Union{Missing,NamedTuple{(:s,),Tuple{String}}}[(s="a",), missing, (s="bc",)],
        -1:1,
    )
    tt = Arrow.Table(Arrow.tobuffer((rows=rows,)))
    @test Base.nonmissingtype(eltype(tt.rows)) == NamedTuple{(:s,),Tuple{String}}
    @test isequal(collect(rows), tt.rows)
end

@testset "Complex" begin
    t = (col1=Union{ComplexF64,Missing}[1 + 2im, missing, 3 + 4im],)
    tbl = Arrow.Table(Arrow.tobuffer(t))
    @test eltype(tbl.col1) == Union{ComplexF64,Missing}
    @test isequal(collect(tbl.col1), t.col1)
end

@testset "`show`" begin
    str = nothing
    table = (; a=1:5, b=fill(1.0, 5))
    arrow_table = Arrow.Table(Arrow.tobuffer(table))
    # 2 and 3-arg show with no metadata
    for outer str in
        (sprint(show, arrow_table), sprint(show, MIME"text/plain"(), arrow_table))
        @test length(str) < 100
        @test occursin("5 rows", str)
        @test occursin("2 columns", str)
        @test occursin("Int", str)
        @test occursin("Float64", str)
        @test !occursin("metadata entries", str)
    end

    # 2-arg show with metadata
    big_dict = Dict((randstring(rand(5:10)) => randstring(rand(1:3)) for _ = 1:100))
    arrow_table = Arrow.Table(Arrow.tobuffer(table; metadata=big_dict))
    str2 = sprint(show, arrow_table)
    @test length(str2) > length(str)
    @test length(str2) < 200
    @test occursin("metadata entries", str2)

    # 3-arg show with metadata
    str3 = sprint(
        show,
        MIME"text/plain"(),
        arrow_table;
        context=IOContext(IOBuffer(), :displaysize => (24, 100), :limit => true),
    )
    @test length(str3) < 1000
    # some but not too many `=>`'s for printing the metadata
    @test 5 < length(collect(eachmatch(r"=>", str3))) < 20
end

@testset "# 194" begin
    @test isempty(Arrow.Table(Arrow.tobuffer(Dict{Symbol,Vector}())))
end

@testset "# 229" begin
    struct Foo229{x}
        y::String
        z::Int
    end
    Arrow.ArrowTypes.arrowname(::Type{<:Foo229}) = Symbol("JuliaLang.Foo229")
    Arrow.ArrowTypes.ArrowType(::Type{Foo229{x}}) where {x} = Tuple{String,String,Int}
    Arrow.ArrowTypes.toarrow(row::Foo229{x}) where {x} = (String(x), row.y, row.z)
    Arrow.ArrowTypes.JuliaType(::Val{Symbol("JuliaLang.Foo229")}, ::Any) = Foo229
    Arrow.ArrowTypes.fromarrow(::Type{<:Foo229}, x, y, z) = Foo229{Symbol(x)}(y, z)
    cols = (
        k1=[Foo229{:a}("a", 1), Foo229{:b}("b", 2)],
        k2=[Foo229{:c}("c", 3), Foo229{:d}("d", 4)],
    )
    tbl = Arrow.Table(Arrow.tobuffer(cols))
    @test tbl.k1 == cols.k1
    @test tbl.k2 == cols.k2
end
