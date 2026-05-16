@testset "C smoke consumer validates and releases caller-owned structs" begin
    smoke_lib = _compile_cdata_smoke()
    before = CData._retained_handle_count()
    table = Arrow.Table(Arrow.tobuffer((id=Int32[1, 2], name=["a", "b"])))
    schema_ref = Ref{CData.ArrowSchema}()
    array_ref = Ref{CData.ArrowArray}()
    schema_out = Base.unsafe_convert(Ptr{CData.ArrowSchema}, schema_ref)
    array_out = Base.unsafe_convert(Ptr{CData.ArrowArray}, array_ref)
    exported = CData.exporttable!(schema_out, array_out, table)
    smoke_handle = Libdl.dlopen(smoke_lib)
    validate = Libdl.dlsym(smoke_handle, :arrow_julia_cdata_smoke_validate)
    release_array = Libdl.dlsym(smoke_handle, :arrow_julia_cdata_smoke_release_array)
    release_schema = Libdl.dlsym(smoke_handle, :arrow_julia_cdata_smoke_release_schema)

    @test ccall(
        validate,
        Cint,
        (Ptr{CData.ArrowSchema}, Ptr{CData.ArrowArray}),
        schema_out,
        array_out,
    ) == 0
    @test ccall(release_array, Cint, (Ptr{CData.ArrowArray},), array_out) == 0
    @test CData.array(exported).release == C_NULL
    @test !CData.isreleased(exported)
    @test ccall(release_schema, Cint, (Ptr{CData.ArrowSchema},), schema_out) == 0
    @test CData.isreleased(exported)
    @test CData._retained_handle_count() == before
end

@testset "embedded C process exports and releases C Data structs" begin
    executable = _compile_cdata_embed_smoke()
    @test _run_cdata_embed_smoke(executable)
end

@testset "release callbacks release child structs and registry handles" begin
    before = CData._retained_handle_count()
    table = Arrow.Table(Arrow.tobuffer((id=Int32[1, 2], flag=Bool[true, false])))
    exported = CData.exporttable(table)
    @test CData._retained_handle_count() >= before + 6

    schema = CData.schema(exported)
    array = CData.array(exported)
    schema_child_ptr = unsafe_load(schema.children, 1)
    array_child_ptr = unsafe_load(array.children, 1)

    CData.release!(exported)

    @test CData.schema(exported).release == C_NULL
    @test CData.array(exported).release == C_NULL
    @test unsafe_load(schema_child_ptr).release == C_NULL
    @test unsafe_load(array_child_ptr).release == C_NULL
    @test CData._retained_handle_count() == before
end
