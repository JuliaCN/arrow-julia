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

@testset "View buffer count inference" begin
    inline_len = Int32(Arrow.VIEW_INLINE_BYTES)
    views = Arrow.ViewElement[
        Arrow.ViewElement(inline_len, Int32(0), Int32(0), Int32(0)),
        Arrow.ViewElement(inline_len + Int32(148), Int32(0), Int32(0), Int32(0)),
        Arrow.ViewElement(inline_len + Int32(207), Int32(0), Int32(1), Int32(160)),
    ]
    validity = Arrow.ValidityBitmap(UInt8[], 1, length(views), 0)
    @test Arrow._viewisinline(inline_len)
    @test !Arrow._viewisinline(inline_len + Int32(1))
    @test Arrow._viewbuffercount(validity, views, Int32(0)) == 2
    @test Arrow._viewbuffercount(validity, views, Int32(1)) == 2
    @test Arrow._viewbuffercount(validity, views, Int32(3)) == 3

    sparse_validity = Arrow.ValidityBitmap(UInt8[0x05], 1, 3, 1)
    sparse_views = Arrow.ViewElement[
        Arrow.ViewElement(inline_len + Int32(64), Int32(0), Int32(0), Int32(0)),
        Arrow.ViewElement(inline_len + Int32(64), Int32(0), Int32(99), Int32(0)),
        Arrow.ViewElement(inline_len, Int32(0), Int32(0), Int32(0)),
    ]
    @test !sparse_validity[2]
    @test Arrow._viewbuffercount(sparse_validity, sparse_views, Int32(0)) == 1
end

@testset "single-partition tobuffer byte equivalence" begin
    t = (col=OffsetArray(["a", "bc", "def"], 0:2),)
    io = IOBuffer()
    Arrow.write(io, t)
    seekstart(io)
    @test read(Arrow.tobuffer(t)) == read(io)

    tm = (col=OffsetArray(Union{Missing,String}["a", missing, "def"], 0:2),)
    io = IOBuffer()
    Arrow.write(io, tm)
    seekstart(io)
    @test read(Arrow.tobuffer(tm)) == read(io)

    bt = (col=OffsetArray([codeunits("a"), codeunits("bc"), codeunits("def")], 0:2),)
    io = IOBuffer()
    Arrow.write(io, bt)
    seekstart(io)
    @test read(Arrow.tobuffer(bt)) == read(io)

    btm = (
        col=OffsetArray(
            Union{Missing,Base.CodeUnits{UInt8,String}}[
                codeunits("a"),
                missing,
                codeunits("def"),
            ],
            0:2,
        ),
    )
    io = IOBuffer()
    Arrow.write(io, btm)
    seekstart(io)
    @test read(Arrow.tobuffer(btm)) == read(io)

    mapt = (col=OffsetArray([Dict("a" => 1, "b" => 2), Dict("a" => 3, "b" => 4)], 0:1),)
    io = IOBuffer()
    Arrow.write(io, mapt)
    seekstart(io)
    @test read(Arrow.tobuffer(mapt)) == read(io)

    nestedt = (col=OffsetArray([Int64[1, 2], Int64[3, 4], Int64[]], 0:2),)
    io = IOBuffer()
    Arrow.write(io, nestedt)
    seekstart(io)
    @test read(Arrow.tobuffer(nestedt)) == read(io)

    pooled = (col=PooledArray(["a", "b", "a", "c"]),)
    io = IOBuffer()
    Arrow.write(io, pooled; dictencode=true)
    seekstart(io)
    @test read(Arrow.tobuffer(pooled; dictencode=true)) == read(io)

    meta = Dict("key1" => "value1")
    colmeta = Dict(:col => Dict("colkey1" => "colvalue1"))
    io = IOBuffer()
    Arrow.write(io, t; metadata=meta, colmetadata=colmeta)
    seekstart(io)
    @test read(Arrow.tobuffer(t; metadata=meta, colmetadata=colmeta)) == read(io)

    parts = Tables.partitioner([t, t])
    io = IOBuffer()
    Arrow.write(io, parts)
    seekstart(io)
    @test read(Arrow.tobuffer(parts)) == read(io)

    string_missing_parts = Tables.partitioner([tm, tm])
    io = IOBuffer()
    Arrow.write(io, string_missing_parts)
    seekstart(io)
    @test read(Arrow.tobuffer(string_missing_parts)) == read(io)

    binary_parts = Tables.partitioner([bt, bt])
    io = IOBuffer()
    Arrow.write(io, binary_parts)
    seekstart(io)
    @test read(Arrow.tobuffer(binary_parts)) == read(io)

    binary_missing_parts = Tables.partitioner([btm, btm])
    io = IOBuffer()
    Arrow.write(io, binary_missing_parts)
    seekstart(io)
    @test read(Arrow.tobuffer(binary_missing_parts)) == read(io)

    map_parts = Tables.partitioner([mapt, mapt])
    io = IOBuffer()
    Arrow.write(io, map_parts)
    seekstart(io)
    @test read(Arrow.tobuffer(map_parts)) == read(io)
end

@testset "# 53" begin
    s = "a"^100
    t = (a=[SubString(s, 1:10), SubString(s, 11:20)],)
    tt = Arrow.Table(Arrow.tobuffer(t))
    @test tt.a == ["aaaaaaaaaa", "aaaaaaaaaa"]
end

@testset "# 49" begin
    @test_throws SystemError Arrow.Table("file_that_doesnt_exist")
    @test_throws SystemError Arrow.Table(p"file_that_doesnt_exist")
end

@testset "# 52" begin
    t = (a=Arrow.DictEncode(string.(1:129)),)
    tt = Arrow.Table(Arrow.tobuffer(t))
end

@testset "# 60: unequal column lengths" begin
    io = IOBuffer()
    @test_throws ArgumentError Arrow.write(io, (a=Int[], b=["asd"], c=collect(1:100)))
end

@testset "# nullability of custom extension types" begin
    t = (a=['a', missing],)
    tt = Arrow.Table(Arrow.tobuffer(t))
    @test isequal(tt.a, ['a', missing])
end

@testset "# offset bool write paths" begin
    t = (
        a=OffsetArray(Bool[true, false, true], -1:1),
        b=OffsetArray(Union{Missing,Bool}[true, missing, false], -1:1),
        c=OffsetArray(Any[true, false, true], -1:1),
        d=OffsetArray(Any[true, missing, false], -1:1),
    )
    tt = Arrow.Table(Arrow.tobuffer(t))
    @test eltype(tt.c) == Bool
    @test eltype(tt.d) == Union{Missing,Bool}
    @test tt.a == Bool[true, false, true]
    @test isequal(tt.b, Union{Missing,Bool}[true, missing, false])
    @test tt.c == Bool[true, false, true]
    @test isequal(tt.d, Union{Missing,Bool}[true, missing, false])
end

@testset "# offset primitive write paths" begin
    t = (
        a=OffsetArray(Int64[1, 2, 3], -1:1),
        b=OffsetArray(Union{Missing,Int64}[1, missing, 3], -1:1),
        c=OffsetArray(Any[1, 2, 3], -1:1),
        d=OffsetArray(Any[1, missing, 3], -1:1),
    )
    tt = Arrow.Table(Arrow.tobuffer(t))
    @test eltype(tt.c) == Int64
    @test eltype(tt.d) == Union{Missing,Int64}
    @test tt.a == Int64[1, 2, 3]
    @test isequal(tt.b, Union{Missing,Int64}[1, missing, 3])
    @test tt.c == Int64[1, 2, 3]
    @test isequal(tt.d, Union{Missing,Int64}[1, missing, 3])
end

@testset "# automatic custom struct serialization/deserialization" begin
    t = (col1=[CustomStruct(1, 2.3, "hey"), CustomStruct(4, 5.6, "there")],)

    Arrow.ArrowTypes.arrowname(::Type{CustomStruct}) = Symbol("JuliaLang.CustomStruct")
    Arrow.ArrowTypes.JuliaType(::Val{Symbol("JuliaLang.CustomStruct")}, S) = CustomStruct
    tt = Arrow.Table(Arrow.tobuffer(t))
    @test length(tt) == length(t)
    @test all(isequal.(values(t), values(tt)))
end

@testset "# Julia Enum extension logical type roundtrip" begin
    t = (
        col1=[EnumRoundtripModule.lexical, EnumRoundtripModule.hybrid],
        col2=Union{Missing,EnumRoundtripModule.RankingStrategy}[
            missing,
            EnumRoundtripModule.semantic,
        ],
    )

    bytes = read(Arrow.tobuffer(t))
    tt = Arrow.Table(IOBuffer(bytes))
    raw = Arrow.Table(IOBuffer(bytes); convert=false)

    @test length(tt) == length(t)
    @test eltype(tt.col1) == EnumRoundtripModule.RankingStrategy
    @test eltype(tt.col2) == Union{Missing,EnumRoundtripModule.RankingStrategy}
    @test tt.col1 == [EnumRoundtripModule.lexical, EnumRoundtripModule.hybrid]
    @test isequal(
        tt.col2,
        Union{Missing,EnumRoundtripModule.RankingStrategy}[
            missing,
            EnumRoundtripModule.semantic,
        ],
    )
    @test eltype(raw.col1) == Int32
    @test eltype(raw.col2) == Union{Missing,Int32}
    @test raw.col1 == Int32[1, 3]
    @test isequal(raw.col2, Union{Missing,Int32}[missing, 2])
    @test Arrow.getmetadata(tt.col1)["ARROW:extension:name"] == "JuliaLang.Enum"
    @test occursin(
        "Main.EnumRoundtripModule.RankingStrategy",
        Arrow.getmetadata(tt.col1)["ARROW:extension:metadata"],
    )
end

@testset "# Julia Enum extension contract edge cases" begin
    t = (
        col=[WideEnumRoundtripModule.tiny, WideEnumRoundtripModule.colossal],
        nullable=Union{Missing,WideEnumRoundtripModule.WideRanking}[
            missing,
            WideEnumRoundtripModule.colossal,
        ],
    )
    bytes = read(Arrow.tobuffer(t))
    tt = Arrow.Table(IOBuffer(bytes))
    raw = Arrow.Table(IOBuffer(bytes); convert=false)

    @test eltype(tt.col) == WideEnumRoundtripModule.WideRanking
    @test eltype(tt.nullable) == Union{Missing,WideEnumRoundtripModule.WideRanking}
    @test tt.col == [WideEnumRoundtripModule.tiny, WideEnumRoundtripModule.colossal]
    @test isequal(
        tt.nullable,
        Union{Missing,WideEnumRoundtripModule.WideRanking}[
            missing,
            WideEnumRoundtripModule.colossal,
        ],
    )
    @test eltype(raw.col) == UInt64
    @test eltype(raw.nullable) == Union{Missing,UInt64}
    @test raw.col == UInt64[1, typemax(UInt64)]
    @test isequal(raw.nullable, Union{Missing,UInt64}[missing, typemax(UInt64)])

    mismatch_metadata = "type=Main.WideEnumRoundtripModule.WideRanking;labels=tiny:1,colossal:2"
    @test_logs (:warn, r"unsupported ARROW:extension:name type: \"JuliaLang.Enum\"") begin
        mismatch_tt = Arrow.Table(
            Arrow.tobuffer(
                (col=UInt64[1, typemax(UInt64)],);
                colmetadata=Dict(
                    :col => Dict(
                        "ARROW:extension:name" => "JuliaLang.Enum",
                        "ARROW:extension:metadata" => mismatch_metadata,
                    ),
                ),
            ),
        )
        @test eltype(mismatch_tt.col) == UInt64
        @test copy(mismatch_tt.col) == UInt64[1, typemax(UInt64)]
    end
end
