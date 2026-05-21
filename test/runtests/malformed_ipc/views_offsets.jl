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

    function patched_record_batch_bytes(column, patcher; writekw...)
        bytes = read(
            Arrow.tobuffer(
                native_arrow_vector_table(:values, column);
                ntasks=0,
                writekw...,
            ),
        )
        return patch_record_batch_bytes!(bytes, patcher)
    end

    function patched_table_bytes(table, patcher; writekw...)
        bytes = read(Arrow.tobuffer(table; ntasks=0, writekw...))
        return patch_record_batch_bytes!(bytes, patcher)
    end

    function patch_record_batch_bytes!(bytes, patcher)
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

    function patch_record_batch_buffer_length!(bytes, rb, buffer_index, new_length::Int64)
        buffer = rb.buffers[buffer_index]
        start = Arrow.FlatBuffers.pos(buffer) + 9
        raw = collect(reinterpret(UInt8, Int64[new_length]))
        copyto!(bytes, start, raw, 1, length(raw))
        return bytes
    end

    function patch_record_batch_compression_codec!(bytes, rb, codec::Int8)
        compression = rb.compression
        offset = Arrow.FlatBuffers.offset(compression, 4)
        offset != 0 || error("record batch compression codec field not found")
        bytes[Arrow.FlatBuffers.pos(compression) + offset + 1] = reinterpret(UInt8, codec)
        return bytes
    end

    function ipc_prefix(message_length::Int32)
        return vcat(
            collect(reinterpret(UInt8, UInt32[Arrow.CONTINUATION_INDICATOR_BYTES])),
            collect(reinterpret(UInt8, Int32[message_length])),
        )
    end

    item = Tables.getcolumn(Arrow.Table(Arrow.tobuffer((item=Int32[1, 2, 3],))), :item)
    validity = Arrow.ValidityBitmap(fill(true, 2))

    valid_bytes = read(Arrow.tobuffer((id=Int32[1, 2], label=["a", "b"])))
    @test isnothing(Arrow.validate(valid_bytes))
    @test isnothing(Arrow.validate(IOBuffer(valid_bytes)))
    @test isnothing(Arrow.validate([valid_bytes]))
    @test isnothing(Arrow.validate(valid_bytes; stream=true))

    for malformed in (UInt8[], UInt8[1, 2, 3, 4, 5, 6, 7, 8])
        assert_argument_error(
            () -> Arrow.validate(malformed),
            "arrow ipc validation requires a schema message",
        )
        assert_argument_error(
            () -> Arrow.validate(malformed; stream=true),
            "arrow ipc validation requires a schema message",
        )
    end

    assert_argument_error(
        () -> Arrow.validate(UInt8[0xff, 0xff, 0xff, 0xff, 0x08]),
        "truncated arrow ipc message length",
    )
    assert_argument_error(
        () -> Arrow.validate(ipc_prefix(Int32(-1))),
        "arrow ipc message length must be non-negative",
    )
    assert_argument_error(
        () -> Arrow.validate(vcat(ipc_prefix(Int32(64)), zeros(UInt8, 4))),
        "truncated arrow ipc message metadata",
    )
    assert_argument_error(
        () -> Arrow.validate(UInt8[0xff, 0xff, 0xff, 0xff, 0x08]; stream=true),
        "truncated arrow ipc message length",
    )

    short_primitive_values = patched_record_batch_bytes(
        Tables.getcolumn(Arrow.Table(Arrow.tobuffer((values=Int32[1, 2],))), :values),
        (bytes, batch, rb) -> patch_record_batch_buffer_length!(bytes, rb, 2, Int64(4)),
    )
    assert_argument_error(
        () -> Arrow.validate(short_primitive_values),
        "primitive column values value buffer length 1 is shorter than logical length 2",
    )

    short_bool_values = patched_record_batch_bytes(
        Tables.getcolumn(
            Arrow.Table(Arrow.tobuffer((values=fill(true, 9),)); convert=false),
            :values,
        ),
        (bytes, batch, rb) -> patch_record_batch_buffer_length!(bytes, rb, 2, Int64(1)),
    )
    assert_argument_error(
        () -> Arrow.validate(short_bool_values),
        "bool column values value buffer length 1 is shorter than required byte length 2",
    )

    compressed_short_header = patched_table_bytes(
        (values=Int32[1, 2],),
        (bytes, batch, rb) -> patch_record_batch_buffer_length!(bytes, rb, 2, Int64(4));
        compress=:lz4,
    )
    assert_argument_error(
        () -> Arrow.validate(compressed_short_header),
        "compressed arrow buffer length 4 is shorter than the 8-byte uncompressed length header",
    )

    compressed_negative_length = patched_table_bytes(
        (values=Int32[1, 2],),
        (bytes, batch, rb) ->
            patch_record_batch_buffer!(bytes, batch, rb, 2, Int64[-2]);
        compress=:lz4,
    )
    assert_argument_error(
        () -> Arrow.validate(compressed_negative_length),
        "compressed arrow buffer has invalid uncompressed length -2",
    )

    compressed_unknown_codec = patched_table_bytes(
        (values=Int32[1, 2],),
        (bytes, batch, rb) -> patch_record_batch_compression_codec!(bytes, rb, Int8(7));
        compress=:zstd,
    )
    assert_argument_error(
        () -> Arrow.validate(compressed_unknown_codec),
        "unsupported compression codec when reading arrow buffers",
    )
    assert_argument_error(
        () -> Arrow.validate(compressed_unknown_codec; stream=true),
        "unsupported compression codec when reading arrow buffers",
    )

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
    assert_argument_error(
        () -> Arrow.validate(
            Arrow.tobuffer(native_arrow_vector_table(:values, descending); ntasks=0),
        ),
        "offsets must be monotonically increasing",
    )
    assert_argument_error(
        () -> Arrow.validate(
            Arrow.tobuffer(native_arrow_vector_table(:values, descending); ntasks=0);
            stream=true,
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

    external_utf8_views = Arrow.ViewElement[Arrow.ViewElement(
        Int32(Arrow.VIEW_INLINE_BYTES + 1),
        Int32(0),
        Int32(0),
        Int32(0),
    ),]
    external_utf8 = Arrow.View{String}(
        UInt8[],
        Arrow.ValidityBitmap(fill(true, 1)),
        external_utf8_views,
        collect(reinterpret(UInt8, external_utf8_views)),
        [fill(UInt8(0x61), Arrow.VIEW_INLINE_BYTES + 1)],
        1,
        nothing,
    )
    external_utf8_rb = first(
        batch.msg.header for batch in Arrow.BatchIterator(
            Arrow.ArrowBlob(
                read(
                    Arrow.tobuffer(
                        native_arrow_vector_table(:values, external_utf8);
                        ntasks=0,
                    ),
                ),
                1,
                nothing,
            ),
        ) if batch.msg.header isa Arrow.Meta.RecordBatch
    )
    @test_throws ArgumentError(
        "record batch is missing variadic buffer count 2; only 1 counts are declared",
    ) Arrow._record_batch_variadic_count(external_utf8_rb, 2)
    missing_variadic_count = patched_record_batch_bytes(
        external_utf8,
        (bytes, batch, rb) -> begin
            raw = collect(reinterpret(UInt8, UInt32[0]))
            copyto!(bytes, rb.variadicBufferCounts.pos - 3, raw, 1, length(raw))
        end,
    )
    assert_argument_error(
        () -> Arrow.validate(missing_variadic_count),
        "record batch is missing variadic buffer count 1",
    )
    assert_argument_error(
        () -> Arrow.validate(missing_variadic_count; stream=true),
        "record batch is missing variadic buffer count 1",
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
