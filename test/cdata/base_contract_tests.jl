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
