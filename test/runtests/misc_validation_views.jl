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

@testset "Malformed IPC offset validation" begin
    function native_arrow_vector_table(name::Symbol, column)
        return Arrow.Table(
            Symbol[name],
            Type[eltype(column)],
            AbstractVector[column],
            Dict{Symbol,AbstractVector}(name => column),
            Ref{Arrow.Meta.Schema}(),
        )
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

    function native_dictionary_values()
        table = Arrow.Table(Arrow.tobuffer((values=["red", "blue"],)))
        return Tables.getcolumn(table, :values)
    end

    function native_dictencoded_column(
        ::Type{T},
        indices,
        validity=Arrow.ValidityBitmap(fill(true, length(indices))),
    ) where {T}
        values = native_dictionary_values()
        encoding = Arrow.DictEncoding{T,eltype(indices),typeof(values)}(
            Int64(1),
            values,
            false,
            nothing,
        )
        return Arrow.DictEncoded(UInt8[], validity, indices, encoding, nothing)
    end
    native_dictencoded_column(
        indices,
        validity=Arrow.ValidityBitmap(fill(true, length(indices))),
    ) = native_dictencoded_column(String, indices, validity)

    function patched_record_batch_bytes(column, patcher)
        bytes = read(Arrow.tobuffer(native_arrow_vector_table(:values, column); ntasks=0))
        blob = Arrow.ArrowBlob(bytes, 1, nothing)
        for batch in Arrow.BatchIterator(blob)
            rb = batch.msg.header
            rb isa Arrow.Meta.RecordBatch || continue
            patcher(bytes, batch, rb)
            return bytes
        end
        error("record batch not found")
    end

    function patch_record_batch_buffer!(bytes, batch, rb, buffer_index, values)
        start = batch.pos + rb.buffers[buffer_index].offset
        raw = collect(reinterpret(UInt8, values))
        copyto!(bytes, start, raw, 1, length(raw))
        return bytes
    end

    item = Tables.getcolumn(Arrow.Table(Arrow.tobuffer((item=Int32[1, 2, 3],))), :item)
    validity = Arrow.ValidityBitmap(fill(true, 2))

    descending = Arrow.List{Vector{Int32},Int32,typeof(item)}(
        UInt8[],
        validity,
        Arrow.Offsets(UInt8[], Int32[0, 3, 2]),
        item,
        2,
        nothing,
    )
    assert_argument_error(
        () -> Arrow.Table(
            Arrow.tobuffer(native_arrow_vector_table(:values, descending); ntasks=0);
            convert=false,
        ),
        "offsets must be monotonically increasing",
    )

    @test_throws ArgumentError(
        "variable-size array offsets length 2 does not match logical length 2",
    ) Arrow._assert_offsets_spans(Int32[0, 2], 2, length(item), "variable-size array")

    overflowing_utf8 = Arrow.List{String,Int32,Vector{UInt8}}(
        UInt8[],
        validity,
        Arrow.Offsets(UInt8[], Int32[0, 1, 3]),
        UInt8[0x61],
        2,
        nothing,
    )
    assert_argument_error(
        () -> Arrow.Table(
            Arrow.tobuffer(native_arrow_vector_table(:values, overflowing_utf8); ntasks=0);
            convert=false,
        ),
        "offset 3 exceeds child/data length 1",
    )

    invalid_utf8 = Arrow.List{String,Int32,Vector{UInt8}}(
        UInt8[],
        Arrow.ValidityBitmap(fill(true, 1)),
        Arrow.Offsets(UInt8[], Int32[0, 1]),
        UInt8[0xff],
        1,
        nothing,
    )
    assert_argument_error(
        () -> Arrow.Table(
            Arrow.tobuffer(native_arrow_vector_table(:values, invalid_utf8); ntasks=0);
            convert=false,
        ),
        "UTF-8 value at index 1 is not valid UTF-8",
    )

    invalid_large_utf8 = Arrow.List{String,Int64,Vector{UInt8}}(
        UInt8[],
        Arrow.ValidityBitmap(fill(true, 1)),
        Arrow.Offsets(UInt8[], Int64[0, 1]),
        UInt8[0xff],
        1,
        nothing,
    )
    assert_argument_error(
        () -> Arrow.Table(
            Arrow.tobuffer(
                native_arrow_vector_table(:values, invalid_large_utf8);
                ntasks=0,
            );
            convert=false,
        ),
        "UTF-8 value at index 1 is not valid UTF-8",
    )

    null_invalid_utf8 = Arrow.List{Union{Missing,String},Int32,Vector{UInt8}}(
        UInt8[],
        Arrow.ValidityBitmap(Union{Missing,String}[missing]),
        Arrow.Offsets(UInt8[], Int32[0, 1]),
        UInt8[0xff],
        1,
        nothing,
    )
    null_invalid_table = Arrow.Table(
        Arrow.tobuffer(native_arrow_vector_table(:values, null_invalid_utf8); ntasks=0);
        convert=false,
    )
    @test ismissing(Tables.getcolumn(null_invalid_table, :values)[1])

    invalid_binary = Arrow.List{Base.CodeUnits{UInt8,String},Int32,Vector{UInt8}}(
        UInt8[],
        Arrow.ValidityBitmap(fill(true, 1)),
        Arrow.Offsets(UInt8[], Int32[0, 1]),
        UInt8[0xff],
        1,
        nothing,
    )
    binary_table = Arrow.Table(
        Arrow.tobuffer(native_arrow_vector_table(:values, invalid_binary); ntasks=0);
        convert=false,
    )
    @test collect(Tables.getcolumn(binary_table, :values)[1]) == UInt8[0xff]

    invalid_inline_view =
        Arrow.ViewElement[Arrow.ViewElement(Int32(1), Int32(0xff), Int32(0), Int32(0)),]
    inline_bytes = collect(reinterpret(UInt8, invalid_inline_view))
    @test_throws ArgumentError("UTF-8 view value at index 1 is not valid UTF-8") Arrow._assert_utf8_view_spans(
        invalid_inline_view,
        inline_bytes,
        Vector{UInt8}[],
        Arrow.ValidityBitmap(fill(true, 1)),
        1,
        "UTF-8 view",
    )
    @test Arrow._assert_utf8_view_spans(
        invalid_inline_view,
        inline_bytes,
        Vector{UInt8}[],
        Arrow.ValidityBitmap(Union{Missing,String}[missing]),
        1,
        "UTF-8 view",
    ) === nothing

    invalid_external_view = Arrow.ViewElement[Arrow.ViewElement(
        Int32(Arrow.VIEW_INLINE_BYTES + 1),
        Int32(0),
        Int32(0),
        Int32(0),
    ),]
    @test_throws ArgumentError("binary view references missing variadic buffer") Arrow._assert_view_spans(
        invalid_external_view,
        UInt8[],
        Vector{UInt8}[],
        Arrow.ValidityBitmap(fill(true, 1)),
        1,
        "binary view",
    )
    @test_throws ArgumentError("UTF-8 view value at index 1 is not valid UTF-8") Arrow._assert_utf8_view_spans(
        invalid_external_view,
        UInt8[],
        [vcat(UInt8[0xff], fill(UInt8(0x61), Arrow.VIEW_INLINE_BYTES))],
        Arrow.ValidityBitmap(fill(true, 1)),
        1,
        "UTF-8 view",
    )

    list_view = Arrow.ListView{AbstractVector{Int32},Int32,typeof(item)}(
        UInt8[],
        UInt8[],
        validity,
        Int32[0, 2],
        Int32[2, 2],
        item,
        2,
        nothing,
    )
    assert_argument_error(
        () -> Arrow.Table(
            Arrow.tobuffer(native_arrow_vector_table(:values, list_view); ntasks=0);
            convert=false,
        ),
        "list-view span exceeds child value length",
    )

    source_map = Tables.getcolumn(
        Arrow.Table(
            Arrow.tobuffer((
                values=Dict{String,Int32}[Dict("a" => Int32(1)), Dict("b" => Int32(2))],
            )),
        ),
        :values,
    )
    map_entries = getfield(source_map, :data)
    map_validity = Arrow.ValidityBitmap(fill(true, 2))
    descending_map = Arrow.Map{Dict{String,Int32},Int32,typeof(map_entries)}(
        map_validity,
        Arrow.Offsets(UInt8[], Int32[0, 2, 1]),
        map_entries,
        2,
        nothing,
    )
    assert_argument_error(
        () -> Arrow.Table(
            Arrow.tobuffer(native_arrow_vector_table(:values, descending_map); ntasks=0);
            convert=false,
        ),
        "map offsets must be monotonically increasing",
    )

    @test_throws ArgumentError("map offsets length 2 does not match logical length 2") Arrow._assert_offsets_spans(
        Int32[0, 1],
        2,
        length(map_entries),
        "map",
    )

    overflowing_map = Arrow.Map{Dict{String,Int32},Int32,typeof(map_entries)}(
        map_validity,
        Arrow.Offsets(UInt8[], Int32[0, 1, 3]),
        map_entries,
        2,
        nothing,
    )
    assert_argument_error(
        () -> Arrow.Table(
            Arrow.tobuffer(native_arrow_vector_table(:values, overflowing_map); ntasks=0);
            convert=false,
        ),
        "map offset 3 exceeds child/data length 2",
    )

    assert_argument_error(
        () -> Arrow.Table(
            Arrow.tobuffer(
                (values=Dict{String,Int32}[Dict("large" => Int32(1))],);
                largelists=true,
            );
            convert=false,
        ),
        "map offsets length 4 does not match logical length 1",
    )

    negative_dictionary_index = native_dictencoded_column(Int8[0, -1])
    assert_argument_error(
        () -> Arrow.Table(
            Arrow.tobuffer(
                native_arrow_vector_table(:values, negative_dictionary_index);
                ntasks=0,
            );
            convert=false,
        ),
        "dictionary column values has negative dictionary index",
    )

    out_of_bounds_dictionary_index = native_dictencoded_column(Int8[0, 2])
    assert_argument_error(
        () -> Arrow.Table(
            Arrow.tobuffer(
                native_arrow_vector_table(:values, out_of_bounds_dictionary_index);
                ntasks=0,
            );
            convert=false,
        ),
        "dictionary column values has dictionary index out of bounds",
    )

    null_slot_dictionary_index = native_dictencoded_column(
        Union{Missing,String},
        Int8[0, 2],
        Arrow.ValidityBitmap(Union{Missing,String}["red", missing]),
    )
    null_slot_table = Arrow.Table(
        Arrow.tobuffer(
            native_arrow_vector_table(:values, null_slot_dictionary_index);
            ntasks=0,
        );
        convert=false,
    )
    @test Tables.getcolumn(null_slot_table, :values)[1] == "red"
    @test ismissing(Tables.getcolumn(null_slot_table, :values)[2])

    short_fixed_size_list_child = Arrow.Primitive(
        Int32,
        UInt8[],
        Arrow.ValidityBitmap(fill(true, 2)),
        Int32[1, 2, 3],
        3,
        nothing,
    )
    short_fixed_size_list =
        Arrow.FixedSizeList{NTuple{2,Int32},typeof(short_fixed_size_list_child)}(
            UInt8[],
            Arrow.ValidityBitmap(fill(true, 2)),
            short_fixed_size_list_child,
            2,
            nothing,
        )
    assert_argument_error(
        () -> Arrow.Table(
            Arrow.tobuffer(
                native_arrow_vector_table(:values, short_fixed_size_list);
                ntasks=0,
            );
            convert=false,
        ),
        "fixed-size-list column values child length 3 is shorter than required length 4",
    )

    union_source = Union{Int64,Float64,Missing}[1, 2.0, missing]
    dense_union = Arrow.DenseUnionVector(union_source)
    sparse_union = Arrow.SparseUnionVector(union_source)

    dense_union_bad_type_id = patched_record_batch_bytes(
        dense_union,
        (bytes, batch, rb) ->
            patch_record_batch_buffer!(bytes, batch, rb, 1, UInt8[0xff, 0x01, 0x02]),
    )
    assert_argument_error(
        () -> Arrow.Table(IOBuffer(dense_union_bad_type_id); convert=false),
        "union column values references undeclared type id 255",
    )

    dense_union_negative_offset = patched_record_batch_bytes(
        dense_union,
        (bytes, batch, rb) ->
            patch_record_batch_buffer!(bytes, batch, rb, 2, Int32[0, -1, 0]),
    )
    assert_argument_error(
        () -> Arrow.Table(IOBuffer(dense_union_negative_offset); convert=false),
        "dense union column values has negative child offset",
    )

    dense_union_bad_offset = patched_record_batch_bytes(
        dense_union,
        (bytes, batch, rb) ->
            patch_record_batch_buffer!(bytes, batch, rb, 2, Int32[0, 1, 0]),
    )
    assert_argument_error(
        () -> Arrow.Table(IOBuffer(dense_union_bad_offset); convert=false),
        "dense union column values has child offset out of bounds",
    )

    sparse_union_bad_type_id = patched_record_batch_bytes(
        sparse_union,
        (bytes, batch, rb) ->
            patch_record_batch_buffer!(bytes, batch, rb, 1, UInt8[0x00, 0xff, 0x02]),
    )
    assert_argument_error(
        () -> Arrow.Table(IOBuffer(sparse_union_bad_type_id); convert=false),
        "union column values references undeclared type id 255",
    )

    @test Arrow._assert_sparse_union_layout!(
        UInt8[0x00, 0x01, 0x00],
        (Int64[1, 3, 5], Float64[0, 2, 0]),
        nothing,
        3,
        :values,
    ) === nothing

    run_end_encoded = Arrow.RunEndEncoded(Int16[2, 5], ["a", "b"], 5, nothing)
    short_final_run_end = patched_record_batch_bytes(
        run_end_encoded,
        (bytes, batch, rb) ->
            patch_record_batch_buffer!(bytes, batch, rb, 2, Int16[2, 4]),
    )
    assert_argument_error(
        () -> Arrow.Table(IOBuffer(short_final_run_end); convert=false),
        "invalid Run-End Encoded array: final run end 4 does not match logical length 5",
    )
end

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
