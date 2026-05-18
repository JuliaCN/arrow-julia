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

@testset "# PR 234" begin
    # bugfix parsing primitive arrays
    buf = [
        0x14,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x0e,
        0x00,
        0x14,
        0x00,
        0x00,
        0x00,
        0x10,
        0x00,
        0x0c,
        0x00,
        0x08,
        0x00,
        0x04,
        0x00,
        0x0e,
        0x00,
        0x00,
        0x00,
        0x2c,
        0x00,
        0x00,
        0x00,
        0x38,
        0x00,
        0x00,
        0x00,
        0x38,
        0x00,
        0x00,
        0x00,
        0x38,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x03,
        0x00,
        0x00,
        0x00,
        0x01,
        0x00,
        0x00,
        0x00,
        0x02,
        0x00,
        0x00,
        0x00,
        0x03,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
    ]

    struct TestData <: Arrow.FlatBuffers.Table
        bytes::Vector{UInt8}
        pos::Base.Int
    end

    function Base.getproperty(x::TestData, field::Symbol)
        if field === :DataInt32
            o = Arrow.FlatBuffers.offset(x, 12)
            o != 0 && return Arrow.FlatBuffers.Array{Int32}(x, o)
        else
            @warn "field $field not supported"
        end
    end

    d = Arrow.FlatBuffers.getrootas(TestData, buf, 0)
    @test d.DataInt32 == UInt32[1, 2, 3]
end

@testset "# test multiple inputs treated as one table" begin
    t = (col1=[1, 2, 3, 4, 5], col2=[1.2, 2.3, 3.4, 4.5, 5.6])
    tbl = Arrow.Table([Arrow.tobuffer(t), Arrow.tobuffer(t)])
    @test tbl.col1 == [1, 2, 3, 4, 5, 1, 2, 3, 4, 5]
    @test tbl.col2 == [1.2, 2.3, 3.4, 4.5, 5.6, 1.2, 2.3, 3.4, 4.5, 5.6]

    # schemas must match between multiple inputs
    t2 = (col1=[1.2, 2.3, 3.4, 4.5, 5.6],)
    @test_throws ArgumentError Arrow.Table([Arrow.tobuffer(t), Arrow.tobuffer(t2)])

    # test multiple inputs treated as one table
    tbls = collect(Arrow.Stream([Arrow.tobuffer(t), Arrow.tobuffer(t)]))
    @test tbls[1].col1 == tbls[2].col1
    @test tbls[1].col2 == tbls[2].col2

    # schemas must match between multiple inputs
    t2 = (col1=[1.2, 2.3, 3.4, 4.5, 5.6],)
    @test_throws ArgumentError collect(
        Arrow.Stream([Arrow.tobuffer(t), Arrow.tobuffer(t2)]),
    )
end

@testset "# 253" begin
    # https://github.com/apache/arrow-julia/issues/253
    @test Arrow.toidict(Pair{String,String}[]) == Base.ImmutableDict{String,String}()
end

@testset "# 232" begin
    # https://github.com/apache/arrow-julia/issues/232
    t = (; x=[Dict(true => 1.32, 1.2 => 0.53495216)])
    @test_throws ArgumentError(
        "`keytype(d)` must be concrete to serialize map-like `d`, but `keytype(d) == Real`",
    ) Arrow.tobuffer(t)
    t = (; x=[Dict(32.0 => true, 1.2 => 0.53495216)])
    @test_throws ArgumentError(
        "`valtype(d)` must be concrete to serialize map-like `d`, but `valtype(d) == Real`",
    ) Arrow.tobuffer(t)
    t = (; x=[Dict(true => 1.32, 1.2 => true)])
    @test_throws ArgumentError(
        "`keytype(d)` must be concrete to serialize map-like `d`, but `keytype(d) == Real`",
    ) Arrow.tobuffer(t)

    t = (
        x=OffsetArray([Dict("a" => 1, "b" => 2), Dict("c" => 3)], -1:0),
        xm=OffsetArray(Union{Missing,Dict{String,Int}}[Dict("a" => 1), missing], -1:0),
        xe=OffsetArray([Dict("a" => 1, "b" => 2, "c" => 3), Dict{String,Int}()], -1:0),
        xem=OffsetArray(Union{Missing,Dict{String,Int}}[Dict{String,Int}(), missing], -1:0),
        xa=OffsetArray(Any[Dict("a" => 1, "b" => 2), Dict("c" => 3)], -1:0),
        xam=OffsetArray(Any[Dict("a" => 1), missing], -1:0),
        xame=OffsetArray(Any[Dict{String,Int}(), missing], -1:0),
    )
    tt = Arrow.Table(Arrow.tobuffer(t))
    @test eltype(tt.x) == Dict{String,Int64}
    @test eltype(tt.xm) == Union{Missing,Dict{String,Int64}}
    @test eltype(tt.xe) == Dict{String,Int64}
    @test eltype(tt.xem) == Union{Missing,Dict{String,Int64}}
    @test eltype(tt.xa) == Dict{String,Int64}
    @test eltype(tt.xam) == Union{Missing,Dict{String,Int64}}
    @test eltype(tt.xame) == Union{Missing,Dict{String,Int64}}
    @test copy(tt.x) isa Vector{Dict{String,Int64}}
    @test copy(tt.xm) isa Vector{Union{Missing,Dict{String,Int64}}}
    @test copy(tt.xem) isa Vector{Union{Missing,Dict{String,Int64}}}
    @test copy(tt.xa) isa Vector{Dict{String,Int64}}
    @test copy(tt.xam) isa Vector{Union{Missing,Dict{String,Int64}}}
    @test copy(tt.xame) isa Vector{Union{Missing,Dict{String,Int64}}}
    @test collect(t.x) == tt.x
    @test isequal(collect(t.xm), tt.xm)
    @test collect(t.xe) == tt.xe
    @test isequal(collect(t.xem), tt.xem)
    @test collect(t.xa) == tt.xa
    @test isequal(collect(t.xam), tt.xam)
    @test isequal(collect(t.xame), tt.xame)

    mapio = IOBuffer()
    Arrow.write(mapio, (x=t.xm,))
    seekstart(mapio)
    @test read(Arrow.tobuffer((x=t.xm,))) == read(mapio)

    mapbuf = Arrow.tobuffer((x=t.xm,))
    seekend(mapbuf)
    mappos = position(mapbuf)
    Arrow.append(mapbuf, Arrow.Table(Arrow.tobuffer((x=t.xm,))))
    seekstart(mapbuf)
    mapbuf1 = read(mapbuf, mappos)
    mapbuf2 = read(mapbuf)
    mapt1 = Arrow.Table(mapbuf1)
    mapt2 = Arrow.Table(mapbuf2)
    @test isequal(collect(mapt1.x), collect(mapt2.x))

    emptymapbuf = Arrow.tobuffer((x=t.xe,))
    seekend(emptymapbuf)
    emptymappos = position(emptymapbuf)
    Arrow.append(emptymapbuf, Arrow.Table(Arrow.tobuffer((x=t.xe,))))
    seekstart(emptymapbuf)
    emptymapbuf1 = read(emptymapbuf, emptymappos)
    emptymapbuf2 = read(emptymapbuf)
    emptymapt1 = Arrow.Table(emptymapbuf1)
    emptymapt2 = Arrow.Table(emptymapbuf2)
    @test isequal(collect(emptymapt1.x), collect(emptymapt2.x))

    anymapbuf = Arrow.tobuffer((x=t.xam,))
    seekend(anymapbuf)
    anymappos = position(anymapbuf)
    Arrow.append(anymapbuf, Arrow.Table(Arrow.tobuffer((x=t.xam,))))
    seekstart(anymapbuf)
    anymapbuf1 = read(anymapbuf, anymappos)
    anymapbuf2 = read(anymapbuf)
    anymapt1 = Arrow.Table(anymapbuf1)
    anymapt2 = Arrow.Table(anymapbuf2)
    @test isequal(collect(anymapt1.x), collect(anymapt2.x))

    anyemptymapbuf = Arrow.tobuffer((x=t.xame,))
    seekend(anyemptymapbuf)
    anyemptymappos = position(anyemptymapbuf)
    Arrow.append(anyemptymapbuf, Arrow.Table(Arrow.tobuffer((x=t.xame,))))
    seekstart(anyemptymapbuf)
    anyemptymapbuf1 = read(anyemptymapbuf, anyemptymappos)
    anyemptymapbuf2 = read(anyemptymapbuf)
    anyemptymapt1 = Arrow.Table(anyemptymapbuf1)
    anyemptymapt2 = Arrow.Table(anyemptymapbuf2)
    @test isequal(collect(anyemptymapt1.x), collect(anyemptymapt2.x))
end

@testset "# 214" begin
    # https://github.com/apache/arrow-julia/issues/214
    t1 = (; x=[(Nanosecond(42),)])
    t2 = Arrow.Table(Arrow.tobuffer(t1))
    t3 = Arrow.Table(Arrow.tobuffer(t2))
    @test t3.x == t1.x

    t1 = (; x=[(; a=Nanosecond(i), b=Nanosecond(i + 1)) for i = 1:5])
    t2 = Arrow.Table(Arrow.tobuffer(t1))
    t3 = Arrow.Table(Arrow.tobuffer(t2))
    @test t3.x == t1.x
end

@testset "Writer" begin
    io = IOBuffer()
    writer = open(Arrow.Writer, io)
    a = 1:26
    b = 'A':'Z'
    partitionsize = 10
    iter_a = Iterators.partition(a, partitionsize)
    iter_b = Iterators.partition(b, partitionsize)
    for (part_a, part_b) in zip(iter_a, iter_b)
        Arrow.write(writer, (a=part_a, b=part_b))
    end
    close(writer)
    seekstart(io)
    table = Arrow.Table(io)
    @test table.a == collect(a)
    @test table.b == collect(b)
end

@testset "# Empty input" begin
    @test Arrow.Table(UInt8[]) isa Arrow.Table
    @test isempty(Tables.rows(Arrow.Table(UInt8[])))
    @test Arrow.Stream(UInt8[]) isa Arrow.Stream
    @test isempty(Tables.partitions(Arrow.Stream(UInt8[])))
end

@testset "# 324" begin
    # https://github.com/apache/arrow-julia/issues/324
    @test_throws ArgumentError filter!(x -> x > 1, Arrow.toarrowvector([1, 2, 3]))
end

@testset "# 327" begin
    # https://github.com/apache/arrow-julia/issues/327
    zdt = ZonedDateTime(DateTime(2020, 11, 1, 6), tz"America/New_York"; from_utc=true)
    arrow_zdt = ArrowTypes.toarrow(zdt)
    zdt_again = ArrowTypes.fromarrow(ZonedDateTime, arrow_zdt)
    @test zdt == zdt_again

    # Check that we still correctly read in old TimeZones
    original_table =
        (; col=[ZonedDateTime(DateTime(1, 2, 3, 4, 5, 6), tz"UTC+3") for _ = 1:5])
    table = Arrow.Table(joinpath(dirname(@__DIR__), "old_zdt.arrow"))
    @test original_table.col == table.col
end

@testset "# 243" begin
    table = (; col=[(; v=v"1"), (; v=v"2"), missing])
    @test isequal(Arrow.Table(Arrow.tobuffer(table)).col, table.col)
end

@testset "# 367" begin
    t = (; x=Union{ZonedDateTime,Missing}[missing])
    a = Arrow.Table(Arrow.tobuffer(t))
    @test Tables.schema(a) == Tables.schema(t)
    @test isequal(a.x, t.x)
end

# https://github.com/apache/arrow-julia/issues/414
df = DataFrame(("$i" => rand(1000) for i = 1:65536)...)
df_load = Arrow.Table(Arrow.tobuffer(df))
@test Tables.schema(df) == Tables.schema(df_load)
for (col1, col2) in zip(Tables.columns(df), Tables.columns(df_load))
    @test col1 == col2
end

@testset "# 411" begin
    # Vector{UInt8} are written as List{UInt8} in Arrow
    # Base.CodeUnits are written as Binary
    t = (
        a=[[0x00, 0x01], UInt8[], [0x03]],
        am=[[0x00, 0x01], [0x03], missing],
        b=[b"01", b"", b"3"],
        bm=[b"01", b"3", missing],
        c=["a", "b", "c"],
        cm=["a", "c", missing],
    )
    buf = Arrow.tobuffer(t)
    tt = Arrow.Table(buf)
    @test t.a == tt.a
    @test isequal(t.am, tt.am)
    @test t.b == tt.b
    @test isequal(t.bm, tt.bm)
    @test t.c == tt.c
    @test isequal(t.cm, tt.cm)
    @test Arrow.schema(tt)[].fields[1].type isa Arrow.Flatbuf.List
    @test Arrow.schema(tt)[].fields[3].type isa Arrow.Flatbuf.Binary
    pos = position(buf)
    Arrow.append(buf, tt)
    seekstart(buf)
    buf1 = read(buf, pos)
    buf2 = read(buf)
    t1 = Arrow.Table(buf1)
    t2 = Arrow.Table(buf2)
    @test isequal(t1.a, t2.a)
    @test isequal(t1.am, t2.am)
    @test isequal(t1.b, t2.b)
    @test isequal(t1.bm, t2.bm)
    @test isequal(t1.c, t2.c)
    @test isequal(t1.cm, t2.cm)

    toffset = (
        b=OffsetArray([b"01", b"", b"3"], -1:1),
        bm=OffsetArray(
            Union{Missing,Base.CodeUnits{UInt8,String}}[b"01", b"3", missing],
            -1:1,
        ),
        ba=OffsetArray(Any[b"01", b"", b"3"], -1:1),
        bam=OffsetArray(Any[b"01", missing, b"3"], -1:1),
        c=OffsetArray(["a", "b", "c"], -1:1),
        cm=OffsetArray(Union{Missing,String}["a", "c", missing], -1:1),
    )
    ttoffset = Arrow.Table(Arrow.tobuffer(toffset))
    @test eltype(ttoffset.b) <: Base.CodeUnits
    @test Base.nonmissingtype(eltype(ttoffset.bm)) <: Base.CodeUnits
    @test eltype(ttoffset.ba) <: Base.CodeUnits
    @test Base.nonmissingtype(eltype(ttoffset.bam)) <: Base.CodeUnits
    @test eltype(ttoffset.c) == String
    @test eltype(ttoffset.cm) == Union{Missing,String}
    @test collect(toffset.b) == ttoffset.b
    @test isequal(collect(toffset.bm), ttoffset.bm)
    @test collect(toffset.ba) == copy(ttoffset.ba)
    @test isequal(collect(toffset.bam), copy(ttoffset.bam))
    @test collect(toffset.c) == ttoffset.c
    @test isequal(collect(toffset.cm), ttoffset.cm)

    offsetbuf = Arrow.tobuffer(toffset)
    seekend(offsetbuf)
    offsetpos = position(offsetbuf)
    Arrow.append(offsetbuf, ttoffset)
    seekstart(offsetbuf)
    offsetbuf1 = read(offsetbuf, offsetpos)
    offsetbuf2 = read(offsetbuf)
    offsett1 = Arrow.Table(offsetbuf1)
    offsett2 = Arrow.Table(offsetbuf2)
    @test collect(offsett1.b) == collect(offsett2.b)
    @test isequal(collect(offsett1.bm), collect(offsett2.bm))
    @test collect(offsett1.c) == collect(offsett2.c)
    @test isequal(collect(offsett1.cm), collect(offsett2.cm))
end

@testset "# 435" begin
    t = Arrow.Table(
        joinpath(dirname(pathof(Arrow)), "../test/java_compress_len_neg_one.arrow"),
    )
    @test length(t) == 15
    @test length(t.isA) == 102
end

@testset "# 293" begin
    t = (a=[1, 2, 3], b=[1.0, 2.0, 3.0])
    buf = Arrow.tobuffer(t)
    tbl = Arrow.Table(buf)
    parts = Tables.partitioner((t, t))
    buf2 = Arrow.tobuffer(parts)
    tbl2 = Arrow.Table(buf2)
    for t in Tables.partitions(tbl2)
        @test t.a == tbl.a
        @test t.b == tbl.b
    end
end

@testset "# 437" begin
    t = Arrow.Table(
        joinpath(dirname(pathof(Arrow)), "../test/java_compressed_zero_length.arrow"),
    )
    @test length(t) == 2
    @test length(t.name) == 0
end

@testset "# 458" begin
    x = (; a=[[[[1]]]])
    buf = Arrow.tobuffer(x)
    t = Arrow.Table(buf)
    @test t.a[1][1][1][1] == 1
end

@testset "# 456" begin
    NT = @NamedTuple{x::Int, y::Union{Missing,Int}}
    data = NT[(x=1, y=2), (x=2, y=missing), (x=3, y=4), (x=4, y=5)]
    t = [(a=1, b=view(data, 1:2)), (a=2, b=view(data, 3:4)), missing]
    @test Arrow.toarrowvector(t) isa Arrow.Struct
end

# @testset "# 461" begin

# table = (; v=[v"1", v"2", missing])
# buf = Arrow.tobuffer(table)
# table2 = Arrow.Table(buf)
# @test isequal(table.v, table2.v)

# end
if isdefined(ArrowTypes, :StructElement)
    @testset "# 493" begin
        # This test stresses the existence of the mechanism
        # implemented in https://github.com/apache/arrow-julia/pull/493,
        # but doesn't stress the actual use case that motivates
        # that mechanism, simply because it'd be more annoying to
        # write that test; see the PR for details.
        struct Foo493
            x::Int
            y::Int
        end
        ArrowTypes.arrowname(::Type{Foo493}) = Symbol("JuliaLang.Foo493")
        ArrowTypes.JuliaType(::Val{Symbol("JuliaLang.Foo493")}, T) = Foo493
        function ArrowTypes.fromarrowstruct(
            ::Type{Foo493},
            ::Val{fnames},
            x...,
        ) where {fnames}
            nt = NamedTuple{fnames}(x)
            return Foo493(nt.x + 1, nt.y + 1)
        end
        t = (; f=[Foo493(1, 2), Foo493(3, 4)])
        buf = Arrow.tobuffer(t)
        tbl = Arrow.Table(buf)
        @test tbl.f[1] === Foo493(2, 3)
        @test tbl.f[2] === Foo493(4, 5)
    end
end

@testset "# 504" begin
    struct Foo504
        x::Int
    end

    struct Bar504
        a::Foo504
    end

    v = [Bar504(Foo504(i)) for i = 1:3]
    io = IOBuffer()
    Arrow.write(io, v; file=false)
    seekstart(io)
    Arrow.append(io, v) # testing the compatility between the schema of the arrow Table, and the "schema" of v (using the fallback mechanism of Tables.jl)
    seekstart(io)
    t = Arrow.Table(io)
    @test Arrow.Tables.rowcount(t) == 6
end

@testset "# 526: Arrow.Time" begin
    tt = testtables[4]
    # just to make sure we're grabbing the correct table
    @test first(tt) == "arrow date/time types"
    tbl = Arrow.Table(Arrow.tobuffer(tt[2]))
    @test tbl.col16[1] == Dates.Time(0, 0, 0)
end

@testset "#511: Bug in reading Utf8View data" begin
    t = Arrow.Table(joinpath(dirname(pathof(Arrow)), "../test/reject_reason_trimmed.arrow"))
    @test t.reject_reason[end] == "POST_ONLY"
end
