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

@testset "# 76" begin
    t = (col1=NamedTuple{(:a,),Tuple{Union{Int,String}}}[(a=1,), (a="x",)],)
    tt = Arrow.Table(Arrow.tobuffer(t))
    @test length(tt) == length(t)
    @test all(isequal.(values(t), values(tt)))
end

@testset "# 89 etc. - UUID FixedSizeListKind overloads" begin
    @test Arrow.ArrowTypes.gettype(Arrow.ArrowTypes.ArrowKind(UUID)) == UInt8
    @test Arrow.ArrowTypes.getsize(Arrow.ArrowTypes.ArrowKind(UUID)) == 16
end

@testset "# 98" begin
    function assert_canonical_extension_error(f::Function, needle::AbstractString)
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

    t = (a=[Nanosecond(0), Nanosecond(1)], b=[uuid4(), uuid4()], c=[missing, Nanosecond(1)])
    tt = Arrow.Table(Arrow.tobuffer(t))
    @test copy(tt.a) isa Vector{Nanosecond}
    @test copy(tt.b) isa Vector{UUID}
    @test copy(tt.c) isa Vector{Union{Missing,Nanosecond}}
    @test Arrow.getmetadata(tt.b)["ARROW:extension:name"] == "arrow.uuid"

    legacy = (
        b=[
            Arrow.ArrowTypes.toarrow(UUID("550e8400-e29b-41d4-a716-446655440000")),
            Arrow.ArrowTypes.toarrow(UUID("550e8400-e29b-41d4-a716-446655440001")),
        ],
    )
    legacy_tt = Arrow.Table(
        Arrow.tobuffer(
            legacy;
            colmetadata=Dict(:b => Dict("ARROW:extension:name" => "JuliaLang.UUID")),
        ),
    )
    @test copy(legacy_tt.b) == [
        UUID("550e8400-e29b-41d4-a716-446655440000"),
        UUID("550e8400-e29b-41d4-a716-446655440001"),
    ]

    invalid_uuid_metadata = Arrow.tobuffer(
        legacy;
        colmetadata=Dict(
            :b => Dict(
                "ARROW:extension:name" => "arrow.uuid",
                "ARROW:extension:metadata" => "{}",
            ),
        ),
    )
    assert_canonical_extension_error(
        () -> Arrow.Table(invalid_uuid_metadata),
        "invalid canonical arrow.uuid extension",
    )

    invalid_uuid_storage = Arrow.tobuffer(
        (b=["550e8400-e29b-41d4-a716-446655440000"],);
        colmetadata=Dict(:b => Dict("ARROW:extension:name" => "arrow.uuid")),
    )
    assert_canonical_extension_error(
        () -> Arrow.Table(invalid_uuid_storage),
        "storage must be FixedSizeBinary(16)",
    )

    toffset = (
        b=OffsetArray(
            [
                UUID("550e8400-e29b-41d4-a716-446655440000"),
                UUID("550e8400-e29b-41d4-a716-446655440001"),
            ],
            -1:0,
        ),
        bm=OffsetArray(
            Union{Missing,UUID}[UUID("550e8400-e29b-41d4-a716-446655440000"), missing],
            -1:0,
        ),
        ba=OffsetArray(
            Any[
                UUID("550e8400-e29b-41d4-a716-446655440000"),
                UUID("550e8400-e29b-41d4-a716-446655440001"),
            ],
            -1:0,
        ),
        bam=OffsetArray(Any[UUID("550e8400-e29b-41d4-a716-446655440000"), missing], -1:0),
    )
    ttoffset = Arrow.Table(Arrow.tobuffer(toffset))
    @test collect(toffset.b) == ttoffset.b
    @test isequal(collect(toffset.bm), ttoffset.bm)
    @test eltype(ttoffset.ba) == NTuple{16,UInt8}
    @test eltype(ttoffset.bam) == Union{Missing,NTuple{16,UInt8}}
    @test map(Arrow.ArrowTypes.toarrow, collect(toffset.ba)) == copy(ttoffset.ba)
    @test isequal(
        map(
            x -> ismissing(x) ? missing : Arrow.ArrowTypes.toarrow(x),
            collect(toffset.bam),
        ),
        copy(ttoffset.bam),
    )
end

@testset "# copy on DictEncoding w/ missing values" begin
    x = PooledArray(["hey", missing])
    x2 = Arrow.toarrowvector(x)
    @test isequal(copy(x2), x)
end

@testset "# some dict encoding coverage" begin
    # signed indices for DictEncodedKind #112 #113 #114
    av = Arrow.toarrowvector(PooledArray(repeat(["a", "b"], inner=5)))
    @test isa(first(av.indices), Signed)

    av = Arrow.toarrowvector(CategoricalArray(repeat(["a", "b"], inner=5)))
    @test isa(first(av.indices), Signed)

    av = Arrow.toarrowvector(CategoricalArray(["a", "bb", missing]))
    @test isa(first(av.indices), Signed)
    @test length(av) == 3
    @test eltype(av) == Union{String,Missing}

    av = Arrow.toarrowvector(CategoricalArray(["a", "bb", "ccc"]))
    @test isa(first(av.indices), Signed)
    @test length(av) == 3
    @test eltype(av) == String

    x = CategoricalArray(Union{Missing,String}["a", missing, "ccc"])
    tt = Arrow.Table(Arrow.tobuffer((x=x,); dictencode=true))
    @test isequal(collect(tt.x), collect(x))
    @test isequal(collect(copy(tt.x)), collect(x))
    df = DataFrame(tt; copycols=true)
    @test isequal(collect(df.x), collect(x))
end

@testset "# 120" begin
    x = PooledArray(["hey", missing])
    x2 = Arrow.toarrowvector(x)
    @test eltype(DataAPI.refpool(x2)) == Union{Missing,String}
    @test eltype(DataAPI.levels(x2)) == String
    @test DataAPI.refarray(x2) == [1, 2]
end

@testset "# 121" begin
    a = PooledArray(repeat(string.('S', 1:130), inner=5), compress=true)
    @test eltype(a.refs) == UInt8
    av = Arrow.toarrowvector(a)
    @test eltype(av.indices) == Int16
end

@testset "# 123" begin
    t = (x=collect(zip(rand(10), rand(10))),)
    tt = Arrow.Table(Arrow.tobuffer(t))
    @test tt.x == t.x
end

@testset "# 144" begin
    t = Tables.partitioner((
        (a=Arrow.DictEncode([1, 2, 3]),),
        (a=Arrow.DictEncode(fill(1, 129)),),
    ))
    tt = Arrow.Table(Arrow.tobuffer(t))
    @test length(tt.a) == 132
end

@testset "# 126" begin
    # XXX This test also captures a race condition in multithreaded
    # writes of dictionary encoded arrays
    t = Tables.partitioner((
        (a=Arrow.toarrowvector(PooledArray([1, 2, 3])),),
        (a=Arrow.toarrowvector(PooledArray([1, 2, 3, 4])),),
        (a=Arrow.toarrowvector(PooledArray([1, 2, 3, 4, 5])),),
    ))
    tt = Arrow.Table(Arrow.tobuffer(t))
    @test length(tt.a) == 12
    @test tt.a == [1, 2, 3, 1, 2, 3, 4, 1, 2, 3, 4, 5]

    t = Tables.partitioner((
        (a=Arrow.toarrowvector(PooledArray([1, 2, 3], signed=true, compress=true)),),
        (a=Arrow.toarrowvector(PooledArray(collect(1:129))),),
    ))
    io = IOBuffer()
    @test_logs (:error, "error writing arrow data on partition = 2") begin
        @test_throws ErrorException Arrow.write(io, t)
    end
end

@testset "# 75" begin
    tbl = Arrow.Table(Arrow.tobuffer((sets=[Set([1, 2, 3]), Set([1, 2, 3])],)))
    @test eltype(tbl.sets) <: Set
end

@testset "# 85" begin
    tbl = Arrow.Table(Arrow.tobuffer((tups=[(1, 3.14, "hey"), (1, 3.14, "hey")],)))
    @test eltype(tbl.tups) <: Tuple
end

@testset "Nothing" begin
    tbl = Arrow.Table(Arrow.tobuffer((nothings=[nothing, nothing, nothing],)))
    @test tbl.nothings == [nothing, nothing, nothing]
end

@testset "arrowmetadata" begin
    # arrowmetadata
    t = (col1=[CustomStruct2{:hey}(1), CustomStruct2{:hey}(2)],)
    ArrowTypes.arrowname(::Type{<:CustomStruct2}) = Symbol("CustomStruct2")
    @test_logs (:warn, r"unsupported ARROW:extension:name type: \"CustomStruct2\"") begin
        tbl = Arrow.Table(Arrow.tobuffer(t))
    end
    @test eltype(tbl.col1) <: NamedTuple
    ArrowTypes.arrowmetadata(::Type{CustomStruct2{sym}}) where {sym} = sym
    ArrowTypes.JuliaType(::Val{:CustomStruct2}, S, meta) = CustomStruct2{Symbol(meta)}
    tbl = Arrow.Table(Arrow.tobuffer(t))
    @test eltype(tbl.col1) == CustomStruct2{:hey}
end

@testset "# 166" begin
    t = (col1=[zero(Arrow.Timestamp{Arrow.Meta.TimeUnit.NANOSECOND,nothing})],)
    tbl = Arrow.Table(Arrow.tobuffer(t))
    @test_logs (
        :warn,
        r"automatically converting Arrow.Timestamp with precision = NANOSECOND",
    ) begin
        @test tbl.col1[1] == Dates.DateTime(1970)
    end
end

@testset "# 95; Arrow.ToTimestamp" begin
    x = [ZonedDateTime(Dates.DateTime(2020), tz"Europe/Paris")]
    c = Arrow.ToTimestamp(x)
    @test eltype(c) ==
          Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.MILLISECOND,Symbol("Europe/Paris")}
    @test c[1] ==
          Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.MILLISECOND,Symbol("Europe/Paris")}(
        1577833200000,
    )
end

@testset "canonical timestamp_with_offset" begin
    function assert_canonical_extension_error(f::Function, needle::AbstractString)
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

    values = Union{Missing,Arrow.TimestampWithOffset{Arrow.Meta.TimeUnit.MILLISECOND}}[
        Arrow.TimestampWithOffset(
            Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,:UTC}(1577836800000),
            330,
        ),
        missing,
        Arrow.TimestampWithOffset(
            Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,:UTC}(1577923200000),
            -480,
        ),
    ]
    @test ArrowTypes.JuliaType(
        Val(Symbol("arrow.timestamp_with_offset")),
        NamedTuple{
            (:timestamp, :offset_minutes),
            Tuple{Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,:UTC},Int16},
        },
        "",
    ) == Arrow.TimestampWithOffset{Arrow.Meta.TimeUnit.MILLISECOND}
    tt = Arrow.Table(Arrow.tobuffer((col=values,)))
    @test eltype(tt.col) ==
          Union{Missing,Arrow.TimestampWithOffset{Arrow.Meta.TimeUnit.MILLISECOND}}
    @test isequal(copy(tt.col), values)
    @test Arrow.getmetadata(tt.col)["ARROW:extension:name"] == "arrow.timestamp_with_offset"

    raw_tt = Arrow.Table(Arrow.tobuffer((col=values,)); convert=false)
    @test eltype(raw_tt.col) == Union{
        Missing,
        NamedTuple{
            (:timestamp, :offset_minutes),
            Tuple{Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,:UTC},Int16},
        },
    }
    @test isequal(
        copy(raw_tt.col),
        Union{
            Missing,
            NamedTuple{
                (:timestamp, :offset_minutes),
                Tuple{Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,:UTC},Int16},
            },
        }[
            (
                timestamp=Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,:UTC}(
                    1577836800000,
                ),
                offset_minutes=Int16(330),
            ),
            missing,
            (
                timestamp=Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,:UTC}(
                    1577923200000,
                ),
                offset_minutes=Int16(-480),
            ),
        ],
    )

    timestamp_storage = [(
        timestamp=Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,:UTC}(1577836800000),
        offset_minutes=Int16(330),
    ),]
    invalid_timestamp_metadata = Arrow.tobuffer(
        (col=timestamp_storage,);
        colmetadata=Dict(
            :col => Dict(
                "ARROW:extension:name" => "arrow.timestamp_with_offset",
                "ARROW:extension:metadata" => "{}",
            ),
        ),
    )
    assert_canonical_extension_error(
        () -> Arrow.Table(invalid_timestamp_metadata),
        "invalid canonical arrow.timestamp_with_offset extension",
    )

    invalid_timestamp_storage = Arrow.tobuffer(
        (col=[(timestamp=Int64(0), offset_minutes=Int16(0))],);
        colmetadata=Dict(
            :col => Dict("ARROW:extension:name" => "arrow.timestamp_with_offset"),
        ),
    )
    assert_canonical_extension_error(
        () -> Arrow.Table(invalid_timestamp_storage),
        "\"timestamp\" field must use Timestamp storage",
    )
end
