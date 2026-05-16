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

@testset "exports and imports nested struct columns" begin
    maybe_type = Union{Missing,NamedTuple{(:x, :label),Tuple{Int32,String}}}
    table = Arrow.Table(
        Arrow.tobuffer((
            point=[(x=Int32(1), label="a"), (x=Int32(2), label="bb")],
            maybe=maybe_type[(x=Int32(3), label="ccc"), missing],
        ),),
    )
    exported = CData.exporttable(table)
    imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

    schema = CData.schema(exported)
    array = CData.array(exported)
    point_schema = _child_schema(schema, 1)
    maybe_schema = _child_schema(schema, 2)
    point_array = _child_array(array, 1)
    maybe_array = _child_array(array, 2)
    point_x_array = unsafe_load(unsafe_load(point_array.children, 1))
    point_label_array = unsafe_load(unsafe_load(point_array.children, 2))

    @test _cstring(point_schema.format) == "+s"
    @test point_schema.n_children == 2
    @test _cstring(_child_schema(point_schema, 1).name) == "x"
    @test _cstring(_child_schema(point_schema, 2).name) == "label"
    @test maybe_schema.flags & CData.ARROW_FLAG_NULLABLE == CData.ARROW_FLAG_NULLABLE

    point = Tables.getcolumn(imported, :point)
    maybe = Tables.getcolumn(imported, :maybe)
    @test point isa CData.ImportedStructVector
    @test maybe isa CData.ImportedStructVector
    @test collect(point) == [(x=Int32(1), label="a"), (x=Int32(2), label="bb")]
    @test isequal(collect(maybe), maybe_type[(x=Int32(3), label="ccc"), missing])
    @test pointer(point.columns[1]) == Ptr{Int32}(unsafe_load(point_x_array.buffers, 2))
    @test pointer(point.columns[2].offsets) ==
          Ptr{Int32}(unsafe_load(point_label_array.buffers, 2))
    @test pointer(maybe.validity) == Ptr{UInt8}(unsafe_load(maybe_array.buffers, 1))

    CData.release!(imported)
    @test CData.isreleased(imported)
    @test CData.isreleased(exported)
end

@testset "imports nullable top-level struct rows" begin
    table = Arrow.Table(
        Arrow.tobuffer((id=Int32[1, 2, 3, 4], name=["a", "bb", "ccc", "dddd"]),),
    )
    exported = CData.exporttable(table)
    top_validity = UInt8[0x0b]
    top_buffers = Ptr{Cvoid}[Ptr{Cvoid}(pointer(top_validity))]
    _set_array_layout!(CData.array_ptr(exported); null_count=1, buffers=top_buffers)

    GC.@preserve top_validity top_buffers begin
        imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))
        id = Tables.getcolumn(imported, :id)
        name = Tables.getcolumn(imported, :name)
        id_array = _child_array(CData.array(exported), 1)
        name_array = _child_array(CData.array(exported), 2)

        @test id isa CData.ImportedRowValidityVector
        @test name isa CData.ImportedRowValidityVector
        @test Tables.schema(imported).types == (Union{Missing,Int32}, Union{Missing,String})
        @test isequal(collect(id), Union{Missing,Int32}[1, 2, missing, 4])
        @test isequal(collect(name), Union{Missing,String}["a", "bb", missing, "dddd"])
        @test pointer(id) == Ptr{Int32}(unsafe_load(id_array.buffers, 2))
        @test pointer(name.offsets) == Ptr{Int32}(unsafe_load(name_array.buffers, 2))
        @test pointer(name.data) == Ptr{UInt8}(unsafe_load(name_array.buffers, 3))
        @test pointer(id.validity) ==
              Ptr{UInt8}(unsafe_load(CData.array(exported).buffers, 1))

        CData.release!(imported)
        @test CData.isreleased(imported)
        @test CData.isreleased(exported)
    end
end

@testset "exports and imports fixed-size columns" begin
    table = Arrow.Table(
        Arrow.tobuffer((
            bytes=NTuple{2,UInt8}[(0x01, 0x02), (0x03, 0x04)],
            fixed=NTuple{2,Int32}[(Int32(1), Int32(2)), (Int32(3), Int32(4))],
            maybe=Union{Missing,NTuple{2,Int32}}[(Int32(5), Int32(6)), missing],
        ),),
    )
    exported = CData.exporttable(table)
    imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

    schema = CData.schema(exported)
    array = CData.array(exported)
    bytes_schema = _child_schema(schema, 1)
    fixed_schema = _child_schema(schema, 2)
    bytes_array = _child_array(array, 1)
    fixed_array = _child_array(array, 2)
    maybe_array = _child_array(array, 3)
    fixed_child_array = unsafe_load(unsafe_load(fixed_array.children, 1))

    @test _cstring(bytes_schema.format) == "w:2"
    @test _cstring(fixed_schema.format) == "+w:2"
    @test fixed_schema.n_children == 1
    bytes = Tables.getcolumn(imported, :bytes)
    fixed = Tables.getcolumn(imported, :fixed)
    maybe = Tables.getcolumn(imported, :maybe)
    @test bytes isa CData.ImportedFixedSizeBinaryVector
    @test fixed isa CData.ImportedFixedSizeListVector
    @test maybe isa CData.ImportedFixedSizeListVector
    @test collect(bytes) == NTuple{2,UInt8}[(0x01, 0x02), (0x03, 0x04)]
    @test collect(fixed) == NTuple{2,Int32}[(Int32(1), Int32(2)), (Int32(3), Int32(4))]
    @test isequal(
        collect(maybe),
        Union{Missing,NTuple{2,Int32}}[(Int32(5), Int32(6)), missing],
    )
    @test pointer(bytes.data) == Ptr{UInt8}(unsafe_load(bytes_array.buffers, 2))
    @test pointer(fixed.values) == Ptr{Int32}(unsafe_load(fixed_child_array.buffers, 2))
    @test pointer(maybe.validity) == Ptr{UInt8}(unsafe_load(maybe_array.buffers, 1))

    CData.release!(imported)
    @test CData.isreleased(imported)
    @test CData.isreleased(exported)
end

@testset "exports and imports null and logical scalar columns" begin
    decimal128_type = Arrow.Decimal{Int32(4),Int32(2),Int128}
    decimal256_type = Arrow.Decimal{Int32(6),Int32(3),Arrow.Int256}
    interval_type = Arrow.Interval{Arrow.Meta.IntervalUnit.YEAR_MONTH,Int32}
    month_day_nano_type =
        Arrow.Interval{Arrow.Meta.IntervalUnit.MONTH_DAY_NANO,Arrow.MonthDayNanoInterval}
    dates = Date[Date(2020, 1, 1), Date(2020, 1, 2)]
    times = Time[Time(1, 2, 3), Time(4, 5, 6)]
    datetimes = DateTime[DateTime(2020, 1, 1, 1, 2, 3), DateTime(2020, 1, 2, 4, 5, 6)]
    durations = Millisecond[Millisecond(5), Millisecond(10)]
    intervals = interval_type[interval_type(Int32(12)), interval_type(Int32(24))]
    month_day_nanos = month_day_nano_type[
        month_day_nano_type(Arrow.MonthDayNanoInterval(1, 2, 3)),
        month_day_nano_type(Arrow.MonthDayNanoInterval(4, 5, 6)),
    ]
    decimals128 =
        decimal128_type[decimal128_type(Int128(1234)), decimal128_type(Int128(-50))]
    decimals256 = decimal256_type[
        decimal256_type(Arrow.Int256(123456)),
        decimal256_type(Arrow.Int256(-789)),
    ]
    maybe_dates = Union{Missing,Date}[Date(2021, 1, 1), missing]
    table = Arrow.Table(
        Arrow.tobuffer((
            nulls=[missing, missing],
            date=dates,
            time=times,
            datetime=datetimes,
            duration=durations,
            interval=intervals,
            month_day_nano=month_day_nanos,
            decimal128=decimals128,
            decimal256=decimals256,
            maybe_date=maybe_dates,
        ),),
    )
    exported = CData.exporttable(table)
    imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

    schema = CData.schema(exported)
    array = CData.array(exported)
    nulls_schema = _child_schema(schema, 1)
    date_schema = _child_schema(schema, 2)
    time_schema = _child_schema(schema, 3)
    datetime_schema = _child_schema(schema, 4)
    duration_schema = _child_schema(schema, 5)
    interval_schema = _child_schema(schema, 6)
    month_day_nano_schema = _child_schema(schema, 7)
    decimal128_schema = _child_schema(schema, 8)
    decimal256_schema = _child_schema(schema, 9)
    maybe_date_schema = _child_schema(schema, 10)
    nulls_array = _child_array(array, 1)
    date_array = _child_array(array, 2)
    month_day_nano_array = _child_array(array, 7)
    decimal128_array = _child_array(array, 8)
    maybe_date_array = _child_array(array, 10)

    @test _cstring(nulls_schema.format) == "n"
    @test _cstring(date_schema.format) == "tdD"
    @test _cstring(time_schema.format) == "ttn"
    @test _cstring(datetime_schema.format) == "tsm:"
    @test _cstring(duration_schema.format) == "tDm"
    @test _cstring(interval_schema.format) == "tiM"
    @test _cstring(month_day_nano_schema.format) == "tin"
    @test _cstring(decimal128_schema.format) == "d:4,2"
    @test _cstring(decimal256_schema.format) == "d:6,3,256"
    @test maybe_date_schema.flags & CData.ARROW_FLAG_NULLABLE == CData.ARROW_FLAG_NULLABLE
    @test nulls_array.n_buffers == 0

    nulls = Tables.getcolumn(imported, :nulls)
    date = Tables.getcolumn(imported, :date)
    time = Tables.getcolumn(imported, :time)
    datetime = Tables.getcolumn(imported, :datetime)
    duration = Tables.getcolumn(imported, :duration)
    interval = Tables.getcolumn(imported, :interval)
    month_day_nano = Tables.getcolumn(imported, :month_day_nano)
    decimal128 = Tables.getcolumn(imported, :decimal128)
    decimal256 = Tables.getcolumn(imported, :decimal256)
    maybe_date = Tables.getcolumn(imported, :maybe_date)

    @test isequal(collect(nulls), [missing, missing])
    @test collect(date) == Arrow.ArrowTypes.toarrow.(dates)
    @test collect(time) == Arrow.ArrowTypes.toarrow.(times)
    @test collect(datetime) == Arrow.ArrowTypes.toarrow.(datetimes)
    @test collect(duration) == Arrow.ArrowTypes.toarrow.(durations)
    @test collect(interval) == intervals
    @test collect(month_day_nano) == month_day_nanos
    @test collect(decimal128) == decimals128
    @test collect(decimal256) == decimals256
    @test isequal(
        collect(maybe_date),
        Union{Missing,eltype(Arrow.ArrowTypes.toarrow.(dates))}[
            Arrow.ArrowTypes.toarrow(Date(2021, 1, 1)),
            missing,
        ],
    )
    @test pointer(date) == Ptr{eltype(date)}(unsafe_load(date_array.buffers, 2))
    @test pointer(decimal128) ==
          Ptr{decimal128_type}(unsafe_load(decimal128_array.buffers, 2))
    @test pointer(month_day_nano) ==
          Ptr{month_day_nano_type}(unsafe_load(month_day_nano_array.buffers, 2))
    @test pointer(maybe_date.data) ==
          Ptr{eltype(maybe_date.data)}(unsafe_load(maybe_date_array.buffers, 2))
    @test pointer(maybe_date.validity) ==
          Ptr{UInt8}(unsafe_load(maybe_date_array.buffers, 1))

    CData.release!(imported)
    @test CData.isreleased(imported)
    @test CData.isreleased(exported)
end

@testset "exports and imports map columns" begin
    table = Arrow.Table(
        Arrow.tobuffer((
            map=Dict{String,Int32}[
                Dict("a" => Int32(1), "b" => Int32(2)),
                Dict("c" => Int32(3)),
                Dict{String,Int32}(),
            ],
            maybe=Union{Missing,Dict{String,Int32}}[
                Dict("z" => Int32(9)),
                missing,
                Dict{String,Int32}(),
            ],
        ),),
    )
    exported = CData.exporttable(table)
    imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

    schema = CData.schema(exported)
    array = CData.array(exported)
    map_schema = _child_schema(schema, 1)
    maybe_schema = _child_schema(schema, 2)
    map_array = _child_array(array, 1)
    maybe_array = _child_array(array, 2)
    entries_schema = _child_schema(map_schema, 1)
    entries_array = _child_array(map_array, 1)
    key_array = _child_array(entries_array, 1)
    value_array = _child_array(entries_array, 2)

    @test _cstring(map_schema.format) == "+m"
    @test _cstring(entries_schema.format) == "+s"
    @test _cstring(_child_schema(entries_schema, 1).name) == "key"
    @test _cstring(_child_schema(entries_schema, 2).name) == "value"
    @test maybe_schema.flags & CData.ARROW_FLAG_NULLABLE == CData.ARROW_FLAG_NULLABLE

    map = Tables.getcolumn(imported, :map)
    maybe = Tables.getcolumn(imported, :maybe)
    @test map isa CData.ImportedMapVector
    @test maybe isa CData.ImportedMapVector
    @test collect(map) == Dict{String,Int32}[
        Dict("a" => Int32(1), "b" => Int32(2)),
        Dict("c" => Int32(3)),
        Dict{String,Int32}(),
    ]
    @test isequal(
        collect(maybe),
        Union{Missing,Dict{String,Int32}}[
            Dict("z" => Int32(9)),
            missing,
            Dict{String,Int32}(),
        ],
    )
    @test pointer(map.offsets) == Ptr{Int32}(unsafe_load(map_array.buffers, 2))
    @test pointer(map.entries.columns[1].offsets) ==
          Ptr{Int32}(unsafe_load(key_array.buffers, 2))
    @test pointer(map.entries.columns[2]) == Ptr{Int32}(unsafe_load(value_array.buffers, 2))
    @test pointer(maybe.validity) == Ptr{UInt8}(unsafe_load(maybe_array.buffers, 1))

    CData.release!(imported)
    @test CData.isreleased(imported)
    @test CData.isreleased(exported)

    large_offsets = Arrow.Table(
        Arrow.tobuffer(
            (map=Dict{String,Int32}[Dict("large" => Int32(1))],);
            largelists=true,
        ),
    )
    @test_throws ArgumentError CData.exporttable(large_offsets)
end

@testset "exports and imports union columns" begin
    source = Union{Int64,Float64,Missing}[1, 2.0, missing, 3]
    table = Arrow.Table(
        Arrow.tobuffer((
            dense=Arrow.DenseUnionVector(source),
            sparse=Arrow.SparseUnionVector(source),
        ),),
    )
    exported = CData.exporttable(table)
    imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

    schema = CData.schema(exported)
    array = CData.array(exported)
    dense_schema = _child_schema(schema, 1)
    sparse_schema = _child_schema(schema, 2)
    dense_array = _child_array(array, 1)
    sparse_array = _child_array(array, 2)
    dense_int_child_array = _child_array(dense_array, 3)
    sparse_float_child_array = _child_array(sparse_array, 2)

    @test _cstring(dense_schema.format) == "+ud:0,1,2"
    @test _cstring(sparse_schema.format) == "+us:0,1,2"
    @test dense_schema.n_children == 3
    @test sparse_schema.n_children == 3
    @test dense_array.n_buffers == 2
    @test sparse_array.n_buffers == 1

    dense = Tables.getcolumn(imported, :dense)
    sparse = Tables.getcolumn(imported, :sparse)
    expected = Union{Missing,Float64,Int64}[Int64(1), 2.0, missing, Int64(3)]
    @test dense isa CData.ImportedDenseUnionVector
    @test sparse isa CData.ImportedSparseUnionVector
    @test isequal(collect(dense), expected)
    @test isequal(collect(sparse), expected)
    @test pointer(dense.type_ids) == Ptr{UInt8}(unsafe_load(dense_array.buffers, 1))
    @test pointer(dense.offsets) == Ptr{Int32}(unsafe_load(dense_array.buffers, 2))
    @test pointer(sparse.type_ids) == Ptr{UInt8}(unsafe_load(sparse_array.buffers, 1))
    @test pointer(dense.children[3]) ==
          Ptr{Int64}(unsafe_load(dense_int_child_array.buffers, 2))
    @test pointer(sparse.children[2]) ==
          Ptr{Float64}(unsafe_load(sparse_float_child_array.buffers, 2))

    CData.release!(imported)
    @test CData.isreleased(imported)
    @test CData.isreleased(exported)
end

@testset "exports and imports run-end encoded columns" begin
    table = Arrow.Table(_test_fixture("run_end_encoded_small.arrow"); convert=false)
    exported = CData.exporttable(table)
    imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

    schema = CData.schema(exported)
    array = CData.array(exported)
    x_schema = _child_schema(schema, 1)
    x_array = _child_array(array, 1)
    run_ends_schema = _child_schema(x_schema, 1)
    values_schema = _child_schema(x_schema, 2)
    run_ends_array = _child_array(x_array, 1)
    values_array = _child_array(x_array, 2)

    @test _cstring(x_schema.format) == "+r"
    @test _cstring(run_ends_schema.name) == "run_ends"
    @test _cstring(values_schema.name) == "values"
    @test _cstring(run_ends_schema.format) == "s"
    @test _cstring(values_schema.format) == "u"
    @test x_array.length == 5
    @test x_array.n_buffers == 0
    @test x_array.n_children == 2

    x = Tables.getcolumn(imported, :x)
    values = x.values isa CData.ImportedNullableStringVector ? x.values.values : x.values
    @test x isa CData.ImportedRunEndEncodedVector
    @test isequal(collect(x), Union{Missing,String}["a", "a", "b", "b", "b"])
    @test pointer(x.run_ends) == Ptr{Int16}(unsafe_load(run_ends_array.buffers, 2))
    @test pointer(values.offsets) == Ptr{Int32}(unsafe_load(values_array.buffers, 2))
    @test pointer(values.data) == Ptr{UInt8}(unsafe_load(values_array.buffers, 3))

    CData.release!(imported)
    @test CData.isreleased(imported)
    @test CData.isreleased(exported)
end

@testset "exports and imports UTF-8 and binary view columns" begin
    table = Arrow.Table(_test_fixture("reject_reason_trimmed.arrow"))
    exported = CData.exporttable(table)
    imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

    schema = CData.schema(exported)
    array = CData.array(exported)
    reason_schema = _child_schema(schema, 1)
    reason_array = _child_array(array, 1)
    source_reason = Tables.getcolumn(table, :reject_reason)
    reason = Tables.getcolumn(imported, :reject_reason)

    @test _cstring(reason_schema.format) == "vu"
    @test reason_array.n_buffers == 4
    @test reason isa CData.ImportedStringViewVector
    @test isequal(collect(reason), collect(source_reason))
    @test pointer(reason.views) ==
          Ptr{Arrow.ViewElement}(unsafe_load(reason_array.buffers, 2))
    @test pointer(reason.buffers[1]) == Ptr{UInt8}(unsafe_load(reason_array.buffers, 3))
    @test pointer(reason.lengths) == Ptr{Int64}(unsafe_load(reason_array.buffers, 4))
    @test reason.lengths ==
          Int64[length(buffer) for buffer in getfield(source_reason, :buffers)]

    CData.release!(imported)
    @test CData.isreleased(imported)
    @test CData.isreleased(exported)

    binary_source = Arrow.View{Union{Missing,Base.CodeUnits}}(
        getfield(source_reason, :arrow),
        getfield(source_reason, :validity),
        getfield(source_reason, :data),
        getfield(source_reason, :inline),
        getfield(source_reason, :buffers),
        length(source_reason),
        getfield(source_reason, :metadata),
    )
    binary_table = Arrow.Table(
        [:blob],
        Type[eltype(binary_source)],
        AbstractVector[binary_source],
        Dict{Symbol,AbstractVector}(:blob => binary_source),
        Ref{Arrow.Meta.Schema}(),
    )
    binary_exported = CData.exporttable(binary_table)
    binary_imported = CData.importtable(
        CData.schema_ptr(binary_exported),
        CData.array_ptr(binary_exported),
    )

    blob_schema = _child_schema(CData.schema(binary_exported), 1)
    blob_array = _child_array(CData.array(binary_exported), 1)
    blob = Tables.getcolumn(binary_imported, :blob)
    @test _cstring(blob_schema.format) == "vz"
    @test blob_array.n_buffers == 4
    @test blob isa CData.ImportedBinaryViewVector
    @test isequal(
        [ismissing(value) ? missing : collect(value) for value in blob],
        [ismissing(value) ? missing : collect(value) for value in binary_source],
    )
    @test pointer(blob.views) == Ptr{Arrow.ViewElement}(unsafe_load(blob_array.buffers, 2))
    @test pointer(blob.buffers[1]) == Ptr{UInt8}(unsafe_load(blob_array.buffers, 3))
    @test pointer(blob.lengths) == Ptr{Int64}(unsafe_load(blob_array.buffers, 4))

    CData.release!(binary_imported)
    @test CData.isreleased(binary_imported)
    @test CData.isreleased(binary_exported)
end

@testset "imports list-view C Data columns" begin
    function list_view_export(::Type{O}, format::AbstractString) where {O}
        values = Int32[12, -7, 25, 0, -127, 127, 50]
        offsets = O[0, 7, 3, 0]
        sizes = O[3, 0, 4, 0]
        validity = UInt8[0x0d]
        item_schema = CData._schema_export("i", "item")
        list_schema = CData._schema_export(
            format,
            "values";
            flags=CData.ARROW_FLAG_NULLABLE,
            children=CData.SchemaExport[item_schema],
        )
        top_schema =
            CData._schema_export("+s", ""; children=CData.SchemaExport[list_schema])
        item_array = CData._array_export(
            values,
            length(values),
            0,
            Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(values))],
        )
        list_array = CData._array_export(
            (validity, offsets, sizes, values),
            length(offsets),
            1,
            Ptr{Cvoid}[
                Ptr{Cvoid}(pointer(validity)),
                Ptr{Cvoid}(pointer(offsets)),
                Ptr{Cvoid}(pointer(sizes)),
            ];
            children=CData.ArrayExport[item_array],
        )
        top_array = CData._array_export(
            (validity, offsets, sizes, values),
            length(offsets),
            0,
            Ptr{Cvoid}[C_NULL];
            children=CData.ArrayExport[list_array],
        )
        return CData.ExportedTable(top_schema, top_array), values
    end

    exported, values = list_view_export(Int32, "+vl")
    imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))
    schema = CData.schema(exported)
    array = CData.array(exported)
    values_schema = _child_schema(schema, 1)
    values_array = _child_array(array, 1)
    item_array = _child_array(values_array, 1)
    list_view = Tables.getcolumn(imported, :values)

    @test _cstring(values_schema.format) == "+vl"
    @test list_view isa CData.ImportedListViewVector
    @test eltype(list_view) == Union{Missing,AbstractVector{Int32}}
    @test isequal(
        [ismissing(value) ? missing : collect(value) for value in list_view],
        Union{Missing,Vector{Int32}}[
            Int32[12, -7, 25],
            missing,
            Int32[0, -127, 127, 50],
            Int32[],
        ],
    )
    @test pointer(list_view.offsets) == Ptr{Int32}(unsafe_load(values_array.buffers, 2))
    @test pointer(list_view.sizes) == Ptr{Int32}(unsafe_load(values_array.buffers, 3))
    @test pointer(list_view.values) == Ptr{Int32}(unsafe_load(item_array.buffers, 2))

    CData.release!(imported)
    @test CData.isreleased(imported)
    @test CData.isreleased(exported)

    large_exported, _ = list_view_export(Int64, "+vL")
    large_imported =
        CData.importtable(CData.schema_ptr(large_exported), CData.array_ptr(large_exported))
    large_schema = _child_schema(CData.schema(large_exported), 1)
    large_array = _child_array(CData.array(large_exported), 1)
    large_values = Tables.getcolumn(large_imported, :values)
    @test _cstring(large_schema.format) == "+vL"
    @test large_values.offsets isa Vector{Int64}
    @test large_values.sizes isa Vector{Int64}
    @test isequal(
        map(value -> ismissing(value) ? missing : collect(value), large_values),
        Union{Missing,Vector{Int32}}[
            Int32[12, -7, 25],
            missing,
            Int32[0, -127, 127, 50],
            Int32[],
        ],
    )
    @test pointer(large_values.offsets) == Ptr{Int64}(unsafe_load(large_array.buffers, 2))
    @test pointer(large_values.sizes) == Ptr{Int64}(unsafe_load(large_array.buffers, 3))

    CData.release!(large_imported)
    @test CData.isreleased(large_imported)
    @test CData.isreleased(large_exported)
    @test values == Int32[12, -7, 25, 0, -127, 127, 50]
end
