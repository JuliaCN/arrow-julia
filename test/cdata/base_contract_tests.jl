@testset "aligns with C Data base-structure contract" begin
    table = Arrow.Table(Arrow.tobuffer((id=Int32[1, 2, 3], name=["a", "bb", "ccc"])))
    exported = CData.exporttable(table)
    schema = CData.schema(exported)
    array = CData.array(exported)

    @test _cstring(schema.format) == "+s"
    @test schema.metadata == C_NULL
    @test _child_schema(schema, 1).metadata == C_NULL
    @test array.offset == 0
    @test array.n_buffers == 1
    @test schema.release != C_NULL
    @test array.release != C_NULL

    bad_offset = Ref(
        CData.ArrowArray(
            array.length,
            array.null_count,
            -1,
            array.n_buffers,
            array.n_children,
            array.buffers,
            array.children,
            array.dictionary,
            array.release,
            array.private_data,
        ),
    )
    GC.@preserve bad_offset begin
        @test_throws ArgumentError CData.importtable(
            CData.schema_ptr(exported),
            Base.unsafe_convert(Ptr{CData.ArrowArray}, bad_offset),
        )
    end

    bad_buffers = Ref(
        CData.ArrowArray(
            array.length,
            array.null_count,
            array.offset,
            0,
            array.n_children,
            array.buffers,
            array.children,
            array.dictionary,
            array.release,
            array.private_data,
        ),
    )
    GC.@preserve bad_buffers begin
        @test_throws ArgumentError CData.importtable(
            CData.schema_ptr(exported),
            Base.unsafe_convert(Ptr{CData.ArrowArray}, bad_buffers),
        )
    end

    ccall(array.release, Cvoid, (Ptr{CData.ArrowArray},), CData.array_ptr(exported))
    @test CData.array(exported).release == C_NULL

    CData.release!(exported)
    @test CData.isreleased(exported)
    CData.release!(exported)
    @test CData.isreleased(exported)
end

@testset "guards fixed-width and logical C Data format mappings" begin
    mappings = [
        (Int8, "c"),
        (UInt8, "C"),
        (Int16, "s"),
        (UInt16, "S"),
        (Int32, "i"),
        (UInt32, "I"),
        (Int64, "l"),
        (UInt64, "L"),
        (Float16, "e"),
        (Float32, "f"),
        (Float64, "g"),
        (Arrow.Decimal{Int32(10),Int32(2),Int128}, "d:10,2"),
        (Arrow.Decimal{Int32(76),Int32(5),Arrow.Int256}, "d:76,5,256"),
        (Arrow.Date{Arrow.Meta.DateUnit.DAY,Int32}, "tdD"),
        (Arrow.Date{Arrow.Meta.DateUnit.MILLISECOND,Int64}, "tdm"),
        (Arrow.Time{Arrow.Meta.TimeUnit.SECOND,Int32}, "tts"),
        (Arrow.Time{Arrow.Meta.TimeUnit.MILLISECOND,Int32}, "ttm"),
        (Arrow.Time{Arrow.Meta.TimeUnit.MICROSECOND,Int64}, "ttu"),
        (Arrow.Time{Arrow.Meta.TimeUnit.NANOSECOND,Int64}, "ttn"),
        (Arrow.Timestamp{Arrow.Meta.TimeUnit.SECOND,nothing}, "tss:"),
        (Arrow.Timestamp{Arrow.Meta.TimeUnit.NANOSECOND,:UTC}, "tsn:UTC"),
        (Arrow.Duration{Arrow.Meta.TimeUnit.SECOND}, "tDs"),
        (Arrow.Duration{Arrow.Meta.TimeUnit.MILLISECOND}, "tDm"),
        (Arrow.Duration{Arrow.Meta.TimeUnit.MICROSECOND}, "tDu"),
        (Arrow.Duration{Arrow.Meta.TimeUnit.NANOSECOND}, "tDn"),
        (Arrow.Interval{Arrow.Meta.IntervalUnit.YEAR_MONTH,Int32}, "tiM"),
        (Arrow.Interval{Arrow.Meta.IntervalUnit.DAY_TIME,Int64}, "tiD"),
        (
            Arrow.Interval{
                Arrow.Meta.IntervalUnit.MONTH_DAY_NANO,
                Arrow.MonthDayNanoInterval,
            },
            "tin",
        ),
    ]

    @testset "$format" for (storage_type, format) in mappings
        @test CData._format_for_storage_type(storage_type) == format
        @test CData._storage_type_for_format(format) == storage_type
    end
end
