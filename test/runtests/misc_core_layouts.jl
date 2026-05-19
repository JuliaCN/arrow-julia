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

@testset "# multiple record batches" begin
    t = Tables.partitioner((
        (col1=Union{Int64,Missing}[1, 2, 3, 4, 5, 6, 7, 8, 9, missing],),
        (col1=Union{Int64,Missing}[missing, 11],),
    ))
    io = Arrow.tobuffer(t)
    tt = Arrow.Table(io)
    @test length(tt) == 1
    @test isequal(tt.col1, vcat([1, 2, 3, 4, 5, 6, 7, 8, 9, missing], [missing, 11]))
    @test eltype(tt.col1) === Union{Int64,Missing}

    # Arrow.Stream
    seekstart(io)
    str = Arrow.Stream(io)
    @test eltype(str) == Arrow.Table
    @test !Base.isdone(str)
    state = iterate(str)
    @test state !== nothing
    tt, st = state
    @test length(tt) == 1
    @test isequal(tt.col1, [1, 2, 3, 4, 5, 6, 7, 8, 9, missing])

    state = iterate(str, st)
    @test state !== nothing
    tt, st = state
    @test length(tt) == 1
    @test isequal(tt.col1, [missing, 11])

    @test iterate(str, st) === nothing

    @test isequal(collect(str)[1].col1, [1, 2, 3, 4, 5, 6, 7, 8, 9, missing])
    @test isequal(collect(str)[2].col1, [missing, 11])
end

@testset "# dictionary batch isDelta" begin
    t = (
        col1=Int64[1, 2, 3, 4],
        col2=Union{String,Missing}["hey", "there", "sailor", missing],
        col3=NamedTuple{
            (:a, :b),
            Tuple{Int64,Union{Missing,NamedTuple{(:c,),Tuple{String}}}},
        }[
            (a=Int64(1), b=missing),
            (a=Int64(1), b=missing),
            (a=Int64(3), b=(c="sailor",)),
            (a=Int64(4), b=(c="jo-bob",)),
        ],
    )
    t2 = (
        col1=Int64[1, 2, 5, 6],
        col2=Union{String,Missing}["hey", "there", "sailor2", missing],
        col3=NamedTuple{
            (:a, :b),
            Tuple{Int64,Union{Missing,NamedTuple{(:c,),Tuple{String}}}},
        }[
            (a=Int64(1), b=missing),
            (a=Int64(1), b=missing),
            (a=Int64(5), b=(c="sailor2",)),
            (a=Int64(4), b=(c="jo-bob",)),
        ],
    )
    tt = Tables.partitioner((t, t2))
    tt = Arrow.Table(Arrow.tobuffer(tt; dictencode=true, dictencodenested=true))
    @test tt.col1 == [1, 2, 3, 4, 1, 2, 5, 6]
    @test isequal(
        tt.col2,
        ["hey", "there", "sailor", missing, "hey", "there", "sailor2", missing],
    )
    @test isequal(
        tt.col3,
        vcat(
            NamedTuple{(:a, :b),Tuple{Int64,Union{Missing,NamedTuple{(:c,),Tuple{String}}}}}[
                (a=Int64(1), b=missing),
                (a=Int64(1), b=missing),
                (a=Int64(3), b=(c="sailor",)),
                (a=Int64(4), b=(c="jo-bob",)),
            ],
            NamedTuple{(:a, :b),Tuple{Int64,Union{Missing,NamedTuple{(:c,),Tuple{String}}}}}[
                (a=Int64(1), b=missing),
                (a=Int64(1), b=missing),
                (a=Int64(5), b=(c="sailor2",)),
                (a=Int64(4), b=(c="jo-bob",)),
            ],
        ),
    )
end

@testset "metadata" begin
    t = (col1=Int64[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],)
    meta = Dict("key1" => "value1", "key2" => "value2")
    meta2 = Dict("colkey1" => "colvalue1", "colkey2" => "colvalue2")
    tt = Arrow.Table(Arrow.tobuffer(t; colmetadata=Dict(:col1 => meta2), metadata=meta))
    @test length(tt) == length(t)
    @test tt.col1 == t.col1
    @test eltype(tt.col1) === Int64
    @test Arrow.getmetadata(tt) == Arrow.toidict(meta)
    @test Arrow.getmetadata(tt.col1) == Arrow.toidict(meta2)

    t = (col1=collect(1:10), col2=collect('a':'j'), col3=collect(1:10))
    meta = ("key1" => :value1, :key2 => "value2")
    meta2 = ("colkey1" => :colvalue1, :colkey2 => "colvalue2")
    meta3 = ("colkey3" => :colvalue3,)
    tt = Arrow.Table(
        Arrow.tobuffer(t; colmetadata=Dict(:col2 => meta2, :col3 => meta3), metadata=meta),
    )
    @test Arrow.getmetadata(tt) == Arrow.toidict(String(k) => String(v) for (k, v) in meta)
    @test Arrow.getmetadata(tt.col1) === nothing
    @test Arrow.getmetadata(tt.col2)["colkey1"] == "colvalue1"
    @test Arrow.getmetadata(tt.col2)["colkey2"] == "colvalue2"
    @test Arrow.getmetadata(tt.col3)["colkey3"] == "colvalue3"

    source = Arrow.withmetadata(
        (col1=collect(1:3), col2=["a", "b", "c"]);
        metadata=["source" => "base"],
        colmetadata=Dict(:col1 => ["semantic.role" => "left"]),
    )
    overlay = Arrow.withmetadata(
        source;
        metadata=["overlay" => "yes"],
        colmetadata=Dict(
            :col1 => ["unit" => "count"],
            :col2 => ["semantic.role" => "right"],
        ),
    )
    overlay_tt = Arrow.Table(Arrow.tobuffer(overlay))
    @test Arrow.getmetadata(overlay_tt)["source"] == "base"
    @test Arrow.getmetadata(overlay_tt)["overlay"] == "yes"
    @test Arrow.getmetadata(overlay_tt.col1)["semantic.role"] == "left"
    @test Arrow.getmetadata(overlay_tt.col1)["unit"] == "count"
    @test Arrow.getmetadata(overlay_tt.col2)["semantic.role"] == "right"
end

@testset "# custom compressors" begin
    lz4 = Arrow.CodecLz4.LZ4FrameCompressor(; compressionlevel=8)
    Arrow.CodecLz4.TranscodingStreams.initialize(lz4)
    t = (col1=Int64[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],)
    tt = Arrow.Table(Arrow.tobuffer(t; compress=lz4))
    @test length(tt) == length(t)
    @test all(isequal.(values(t), values(tt)))

    zstd = Arrow.CodecZstd.ZstdCompressor(; level=8)
    Arrow.CodecZstd.TranscodingStreams.initialize(zstd)
    t = (col1=Int64[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],)
    tt = Arrow.Table(Arrow.tobuffer(t; compress=zstd))
    @test length(tt) == length(t)
    @test all(isequal.(values(t), values(tt)))
end

@testset "# custom alignment" begin
    t = (col1=Int64[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],)
    tt = Arrow.Table(Arrow.tobuffer(t; alignment=64))
    @test length(tt) == length(t)
    @test all(isequal.(values(t), values(tt)))
end

@testset "misaligned stream roundtrips core physical layouts" begin
    source = (
        primitive=Int64[1, 2, 3],
        utf8=["a", "bb", "ccc"],
        fixed=[
            UUID("550e8400-e29b-41d4-a716-446655440000"),
            UUID("550e8400-e29b-41d4-a716-446655440001"),
            UUID("550e8400-e29b-41d4-a716-446655440002"),
        ],
    )
    bytes = read(Arrow.tobuffer(source))
    padded = vcat(UInt8[0x00], bytes)
    tt = Arrow.Table(padded, 2, nothing)
    @test length(tt) == length(source)
    columns = Tables.columntable(tt)
    @test columns.primitive == source.primitive
    @test columns.utf8 == source.utf8
    @test columns.fixed == source.fixed
end

@testset "metadata physical layout mapping follows Arrow spec" begin
    source = (
        primitive=Int64[1, 2, 3],
        utf8=["a", "bb", "ccc"],
        list=[[1, 2], [3], Int[]],
        uuid=[
            UUID("550e8400-e29b-41d4-a716-446655440000"),
            UUID("550e8400-e29b-41d4-a716-446655440001"),
            UUID("550e8400-e29b-41d4-a716-446655440002"),
        ],
    )
    bytes = read(Arrow.tobuffer(source))
    stream = Arrow.Stream(bytes)
    Tables.schema(stream)
    schema = getfield(stream, :schema)
    fields = Dict(String(field.name) => field for field in schema.fields)

    @test ArrowTypes.physicallayout(fields["primitive"]) ==
          ArrowTypes.PrimitiveLayout{Int64}()
    null_bool_stream = Arrow.Stream(
        read(
            Arrow.tobuffer((
                null_col=Missing[missing, missing, missing],
                bool_col=Bool[true, false, true],
            )),
        ),
    )
    Tables.schema(null_bool_stream)
    null_bool_fields = Dict(
        String(field.name) => field for field in getfield(null_bool_stream, :schema).fields
    )
    @test ArrowTypes.physicallayout(null_bool_fields["null_col"]) == ArrowTypes.NullLayout()
    @test ArrowTypes.physicallayout(null_bool_fields["bool_col"]) ==
          ArrowTypes.BooleanLayout()
    @test ArrowTypes.physicallayout(fields["utf8"]) ==
          ArrowTypes.VariableBinaryLayout{Int32}()
    @test ArrowTypes.physicallayout(fields["list"]) ==
          ArrowTypes.VariableListLayout{Int32}()
    @test ArrowTypes.physicallayout(fields["uuid"]) ==
          ArrowTypes.FixedSizeBinaryLayout{16}()

    pooled_bytes = read(
        Arrow.tobuffer((pooled=PooledArray(["alpha", "alpha", "beta"]),); dictencode=true),
    )
    pooled_stream = Arrow.Stream(pooled_bytes)
    Tables.schema(pooled_stream)
    pooled_schema = getfield(pooled_stream, :schema)
    pooled_field = only(pooled_schema.fields)
    pooled_layout = ArrowTypes.physicallayout(pooled_field)
    @test pooled_layout isa ArrowTypes.DictionaryEncodedLayout
    @test ArrowTypes.indextype(pooled_layout) <: Integer

    extended = (
        large_utf8=["a", "bb", "ccc"],
        large_list=[[1, 2], [3], Int[]],
        dense_union=Arrow.DenseUnionVector(Union{Int64,Float64,Missing}[1, 2.0, missing]),
        sparse_union=Arrow.SparseUnionVector(Union{Int64,Float64,Missing}[1, 2.0, missing]),
    )
    extended_bytes = read(Arrow.tobuffer(extended; largelists=true))
    extended_stream = Arrow.Stream(extended_bytes)
    Tables.schema(extended_stream)
    extended_schema = getfield(extended_stream, :schema)
    extended_fields = Dict(String(field.name) => field for field in extended_schema.fields)

    @test ArrowTypes.physicallayout(extended_fields["large_utf8"]) ==
          ArrowTypes.VariableBinaryLayout{Int64}()
    @test ArrowTypes.physicallayout(extended_fields["large_list"]) ==
          ArrowTypes.VariableListLayout{Int64}()
    @test ArrowTypes.physicallayout(extended_fields["dense_union"]) ==
          ArrowTypes.UnionLayout{:dense}()
    @test ArrowTypes.physicallayout(extended_fields["sparse_union"]) ==
          ArrowTypes.UnionLayout{:sparse}()

    nested = (
        fixed_list=[(Int32(1), Int32(2)), (Int32(3), Int32(4)), (Int32(5), Int32(6))],
        struct_col=[(a=1, b="x"), (a=2, b="y"), (a=3, b="z")],
        map_col=[Dict("a" => 1, "b" => 2), Dict("c" => 3), Dict{String,Int}()],
    )
    nested_stream = Arrow.Stream(read(Arrow.tobuffer(nested)))
    Tables.schema(nested_stream)
    nested_fields = Dict(
        String(field.name) => field for field in getfield(nested_stream, :schema).fields
    )

    @test ArrowTypes.physicallayout(nested_fields["fixed_list"]) ==
          ArrowTypes.FixedSizeListLayout{2}()
    @test ArrowTypes.physicallayout(nested_fields["struct_col"]) ==
          ArrowTypes.StructLayout()
    @test ArrowTypes.physicallayout(nested_fields["map_col"]) ==
          ArrowTypes.VariableListLayout{Int32}()

    view_bytes = read(joinpath(dirname(@__DIR__), "reject_reason_trimmed.arrow"))
    view_stream = Arrow.Stream(view_bytes)
    Tables.schema(view_stream)
    view_schema = getfield(view_stream, :schema)
    @test ArrowTypes.physicallayout(only(view_schema.fields)) ==
          ArrowTypes.VariableBinaryViewLayout()

    placeholder_stream = Arrow.Stream(read(Arrow.tobuffer((primitive=Int64[1, 2, 3],))))
    Tables.schema(placeholder_stream)
    placeholder_field = only(getfield(placeholder_stream, :schema).fields)
    @test ArrowTypes.physicallayout(placeholder_field, Arrow.Meta.ListView(UInt8[], 0)) ==
          ArrowTypes.VariableListViewLayout{Int32}()
    @test ArrowTypes.physicallayout(
        placeholder_field,
        Arrow.Meta.LargeListView(UInt8[], 0),
    ) == ArrowTypes.VariableListViewLayout{Int64}()

    ree_bytes = read(joinpath(dirname(@__DIR__), "run_end_encoded_small.arrow"))
    ree_stream = Arrow.Stream(ree_bytes)
    Tables.schema(ree_stream)
    ree_schema = getfield(ree_stream, :schema)
    @test ArrowTypes.physicallayout(only(ree_schema.fields)) ==
          ArrowTypes.RunEndEncodedLayout{Int16}()
end

@testset "Run-End Encoded IPC roundtrip" begin
    function native_run_end_encoded_table(column)
        return Arrow.Table(
            Symbol[:encoded],
            Type[eltype(column)],
            AbstractVector[column],
            Dict{Symbol,AbstractVector}(:encoded => column),
            Ref{Arrow.Meta.Schema}(),
        )
    end

    logical_values = Union{Missing,Int32}[10, 10, missing, missing, missing, -5]
    encoded = Arrow.RunEndEncoded(
        Int16[2, 5, 6],
        Union{Missing,Int32}[10, missing, -5],
        length(logical_values),
        nothing,
    )

    stream = Arrow.Stream(
        Arrow.tobuffer(native_run_end_encoded_table(encoded); ntasks=0);
        convert=false,
    )
    Tables.schema(stream)
    field = only(getfield(stream, :schema).fields)
    @test field.type isa Arrow.Meta.RunEndEncoded
    @test String(field.children[1].name) == "run_ends"
    @test String(field.children[2].name) == "values"
    @test ArrowTypes.physicallayout(field) == ArrowTypes.RunEndEncodedLayout{Int16}()

    table = first(stream)
    values = Tables.getcolumn(table, :encoded)
    @test values isa Arrow.RunEndEncoded
    @test eltype(values.run_ends) === Int16
    @test collect(values.run_ends) == Int16[2, 5, 6]
    @test isequal(collect(values), logical_values)

    compressed_table = Arrow.Table(
        Arrow.tobuffer(native_run_end_encoded_table(encoded); ntasks=0, compress=:lz4);
        convert=false,
    )
    compressed_values = Tables.getcolumn(compressed_table, :encoded)
    @test compressed_values isa Arrow.RunEndEncoded
    @test isequal(collect(compressed_values), logical_values)

    @test_throws ArgumentError Arrow.RunEndEncoded(UInt8[1], Int32[10], 1, nothing)
    @test_throws ArgumentError Arrow.RunEndEncoded(
        Union{Missing,Int16}[missing],
        Int32[10],
        1,
        nothing,
    )
end

@testset "ListView IPC roundtrip" begin
    function native_list_view_table(column)
        return Arrow.Table(
            Symbol[:values],
            Type[eltype(column)],
            AbstractVector[column],
            Dict{Symbol,AbstractVector}(:values => column),
            Ref{Arrow.Meta.Schema}(),
        )
    end

    materialize_lists(column) =
        [ismissing(value) ? missing : collect(value) for value in column]

    item_table = Arrow.Table(Arrow.tobuffer((item=Int32[12, -7, 25, 0, -127, 127, 50],)))
    item = Tables.getcolumn(item_table, :item)
    validity = Arrow.ValidityBitmap(Union{Missing,Int8}[1, missing, 1, 1])
    list_view = Arrow.ListView(Int32[0, 7, 3, 0], Int32[3, 0, 4, 0], item; validity)

    stream = Arrow.Stream(
        Arrow.tobuffer(native_list_view_table(list_view); ntasks=0);
        convert=false,
    )
    Tables.schema(stream)
    field = only(getfield(stream, :schema).fields)
    @test field.type isa Arrow.Meta.ListView
    @test ArrowTypes.physicallayout(field) == ArrowTypes.VariableListViewLayout{Int32}()

    table = first(stream)
    values = Tables.getcolumn(table, :values)
    @test values isa Arrow.ListView
    @test typeof(values).parameters[2] === Int32
    @test isequal(
        materialize_lists(values),
        Union{Missing,Vector{Int32}}[
            Int32[12, -7, 25],
            missing,
            Int32[0, -127, 127, 50],
            Int32[],
        ],
    )

    large_list_view = Arrow.ListView(Int64[0, 3], Int64[3, 2], item)
    large_stream = Arrow.Stream(
        Arrow.tobuffer(native_list_view_table(large_list_view); ntasks=0);
        convert=false,
    )
    Tables.schema(large_stream)
    large_field = only(getfield(large_stream, :schema).fields)
    @test large_field.type isa Arrow.Meta.LargeListView
    @test ArrowTypes.physicallayout(large_field) ==
          ArrowTypes.VariableListViewLayout{Int64}()

    large_table = first(large_stream)
    large_values = Tables.getcolumn(large_table, :values)
    @test large_values isa Arrow.ListView
    @test typeof(large_values).parameters[2] === Int64
    @test [collect(value) for value in large_values] == [Int32[12, -7, 25], Int32[0, -127]]

    compressed_large_table = Arrow.Table(
        Arrow.tobuffer(native_list_view_table(large_list_view); ntasks=0, compress=:lz4);
        convert=false,
    )
    compressed_large_values = Tables.getcolumn(compressed_large_table, :values)
    @test compressed_large_values isa Arrow.ListView
    @test [collect(value) for value in compressed_large_values] == [Int32[12, -7, 25], Int32[0, -127]]
end
