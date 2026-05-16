@testset "exports primitive and boolean table columns" begin
    table = Arrow.Table(
        Arrow.tobuffer((
            id=Int32[1, 2, 3],
            score=Float64[1.5, 2.5, 3.5],
            flag=Bool[true, false, true],
        )),
    )
    exported = CData.exporttable(table)

    schema = CData.schema(exported)
    array = CData.array(exported)

    @test _cstring(schema.format) == "+s"
    @test _cstring(schema.name) == ""
    @test schema.n_children == 3
    @test array.length == 3
    @test array.null_count == 0
    @test array.n_buffers == 1
    @test array.n_children == 3
    @test unsafe_load(array.buffers, 1) == C_NULL

    id_schema = _child_schema(schema, 1)
    score_schema = _child_schema(schema, 2)
    flag_schema = _child_schema(schema, 3)
    @test _cstring(id_schema.format) == "i"
    @test _cstring(id_schema.name) == "id"
    @test _cstring(score_schema.format) == "g"
    @test _cstring(score_schema.name) == "score"
    @test _cstring(flag_schema.format) == "b"
    @test _cstring(flag_schema.name) == "flag"

    id_column = Tables.getcolumn(table, :id)
    score_column = Tables.getcolumn(table, :score)
    flag_column = Tables.getcolumn(table, :flag)
    id_array = _child_array(array, 1)
    score_array = _child_array(array, 2)
    flag_array = _child_array(array, 3)

    @test id_array.n_buffers == 2
    @test score_array.n_buffers == 2
    @test flag_array.n_buffers == 2
    @test unsafe_load(id_array.buffers, 1) == C_NULL
    @test unsafe_load(score_array.buffers, 1) == C_NULL
    @test unsafe_load(flag_array.buffers, 1) == C_NULL
    @test unsafe_load(id_array.buffers, 2) ==
          Ptr{Cvoid}(pointer(getfield(id_column, :data)))
    @test unsafe_load(score_array.buffers, 2) ==
          Ptr{Cvoid}(pointer(getfield(score_column, :data)))
    @test unsafe_load(flag_array.buffers, 2) ==
          Ptr{Cvoid}(pointer(getfield(flag_column, :arrow), getfield(flag_column, :pos)))

    CData.release!(exported)
    @test CData.isreleased(exported)
end

@testset "exports nullable primitive validity bitmap" begin
    table = Arrow.Table(Arrow.tobuffer((maybe=Union{Missing,Int64}[1, missing, 3],)))
    exported = CData.exporttable(table)
    schema = CData.schema(exported)
    array = CData.array(exported)
    maybe_schema = _child_schema(schema, 1)
    maybe_array = _child_array(array, 1)

    @test _cstring(maybe_schema.format) == "l"
    @test maybe_schema.flags & CData.ARROW_FLAG_NULLABLE == CData.ARROW_FLAG_NULLABLE
    @test maybe_array.length == 3
    @test maybe_array.null_count == 1
    @test unsafe_load(maybe_array.buffers, 1) != C_NULL

    CData.release!(exported)
    @test CData.isreleased(exported)
end

@testset "exports string columns and schema metadata" begin
    source = Arrow.withmetadata(
        (name=["alpha", "beta", ""], maybe=Union{Missing,String}["x", missing, "yz"]);
        metadata=Dict("dataset" => "strings"),
        colmetadata=Dict(
            :name => Dict("role" => "label"),
            :maybe => Dict("role" => "optional"),
        ),
    )
    table = Arrow.Table(Arrow.tobuffer(source))
    exported = CData.exporttable(table)

    schema = CData.schema(exported)
    array = CData.array(exported)
    name_schema = _child_schema(schema, 1)
    maybe_schema = _child_schema(schema, 2)
    name_array = _child_array(array, 1)
    maybe_array = _child_array(array, 2)
    name_column = Tables.getcolumn(table, :name)
    maybe_column = Tables.getcolumn(table, :maybe)

    @test _metadata_dict(schema.metadata) == Dict("dataset" => "strings")
    @test _cstring(name_schema.format) == "u"
    @test _cstring(maybe_schema.format) == "u"
    @test _metadata_dict(name_schema.metadata) == Dict("role" => "label")
    @test _metadata_dict(maybe_schema.metadata) == Dict("role" => "optional")
    @test maybe_schema.flags & CData.ARROW_FLAG_NULLABLE == CData.ARROW_FLAG_NULLABLE
    @test name_array.n_buffers == 3
    @test maybe_array.n_buffers == 3
    @test unsafe_load(name_array.buffers, 1) == C_NULL
    @test unsafe_load(name_array.buffers, 2) ==
          Ptr{Cvoid}(pointer(getfield(getfield(name_column, :offsets), :offsets)))
    @test unsafe_load(name_array.buffers, 3) ==
          Ptr{Cvoid}(pointer(getfield(name_column, :data)))
    @test maybe_array.null_count == 1
    @test unsafe_load(maybe_array.buffers, 1) != C_NULL
    @test unsafe_load(maybe_array.buffers, 2) ==
          Ptr{Cvoid}(pointer(getfield(getfield(maybe_column, :offsets), :offsets)))
    @test unsafe_load(maybe_array.buffers, 3) ==
          Ptr{Cvoid}(pointer(getfield(maybe_column, :data)))

    CData.release!(exported)
    @test CData.isreleased(exported)
end

@testset "exports list columns with child arrays" begin
    table = Arrow.Table(Arrow.tobuffer((values=[[Int64(1), 2], [Int64(3)], Int64[]],)))
    exported = CData.exporttable(table)

    schema = CData.schema(exported)
    array = CData.array(exported)
    values_schema = _child_schema(schema, 1)
    values_array = _child_array(array, 1)
    item_schema = _child_schema(values_schema, 1)
    item_array = _child_array(values_array, 1)
    values_column = Tables.getcolumn(table, :values)
    item_column = getfield(values_column, :data)

    @test _cstring(values_schema.format) == "+l"
    @test values_schema.n_children == 1
    @test _cstring(item_schema.format) == "l"
    @test _cstring(item_schema.name) == "item"
    @test values_array.n_buffers == 2
    @test values_array.n_children == 1
    @test unsafe_load(values_array.buffers, 1) == C_NULL
    @test unsafe_load(values_array.buffers, 2) ==
          Ptr{Cvoid}(pointer(getfield(getfield(values_column, :offsets), :offsets)))
    @test item_array.length == 3
    @test unsafe_load(item_array.buffers, 2) ==
          Ptr{Cvoid}(pointer(getfield(item_column, :data)))

    CData.release!(exported)
    @test CData.isreleased(exported)
end

@testset "exports dictionary encoded columns" begin
    table = Arrow.Table(Arrow.tobuffer((color=["red", "blue", "red"],); dictencode=true))
    exported = CData.exporttable(table)

    schema = CData.schema(exported)
    array = CData.array(exported)
    color_schema = _child_schema(schema, 1)
    color_array = _child_array(array, 1)
    color_column = Tables.getcolumn(table, :color)
    dictionary_schema_ptr = color_schema.dictionary
    dictionary_array_ptr = color_array.dictionary
    dictionary_schema = unsafe_load(dictionary_schema_ptr)
    dictionary_array = unsafe_load(dictionary_array_ptr)

    @test _cstring(color_schema.format) == "c"
    @test dictionary_schema_ptr != C_NULL
    @test dictionary_array_ptr != C_NULL
    @test _cstring(dictionary_schema.format) == "u"
    @test dictionary_array.length ==
          length(getfield(getfield(color_column, :encoding), :data))
    @test color_array.n_buffers == 2
    @test unsafe_load(color_array.buffers, 1) == C_NULL
    @test unsafe_load(color_array.buffers, 2) ==
          Ptr{Cvoid}(pointer(getfield(color_column, :indices)))
    @test dictionary_array.n_buffers == 3

    CData.release!(exported)
    @test CData.isreleased(exported)
    @test unsafe_load(dictionary_schema_ptr).release == C_NULL
    @test unsafe_load(dictionary_array_ptr).release == C_NULL
end

function _single_column_table(name::Symbol, column::AbstractVector)
    return Arrow.Table(
        Symbol[name],
        Type[eltype(column)],
        AbstractVector[column],
        Dict{Symbol,AbstractVector}(name => column),
        Ref{Arrow.Meta.Schema}(),
    )
end

@testset "exports list-view columns" begin
    item_table = Arrow.Table(Arrow.tobuffer((item=Int32[12, -7, 25, 0, -127, 127, 50],)))
    item = Tables.getcolumn(item_table, :item)
    offsets = Int32[0, 7, 3, 0]
    sizes = Int32[3, 0, 4, 0]
    validity = Arrow.ValidityBitmap(Union{Missing,Int8}[1, missing, 1, 1])
    list_view = Arrow.ListView(offsets, sizes, item; validity=validity)
    table = _single_column_table(:values, list_view)
    exported = CData.exporttable(table)

    schema = CData.schema(exported)
    array = CData.array(exported)
    values_schema = _child_schema(schema, 1)
    values_array = _child_array(array, 1)
    item_array = _child_array(values_array, 1)

    @test _cstring(values_schema.format) == "+vl"
    @test values_schema.flags & CData.ARROW_FLAG_NULLABLE == CData.ARROW_FLAG_NULLABLE
    @test values_array.n_buffers == 3
    @test values_array.n_children == 1
    @test unsafe_load(values_array.buffers, 1) ==
          Ptr{Cvoid}(pointer(validity.bytes, validity.pos))
    @test unsafe_load(values_array.buffers, 2) == Ptr{Cvoid}(pointer(offsets))
    @test unsafe_load(values_array.buffers, 3) == Ptr{Cvoid}(pointer(sizes))
    @test unsafe_load(item_array.buffers, 2) == Ptr{Cvoid}(pointer(getfield(item, :data)))

    imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))
    values = Tables.getcolumn(imported, :values)
    @test values isa CData.ImportedListViewVector
    @test isequal(
        [ismissing(value) ? missing : collect(value) for value in values],
        Union{Missing,Vector{Int32}}[
            Int32[12, -7, 25],
            missing,
            Int32[0, -127, 127, 50],
            Int32[],
        ],
    )

    CData.release!(imported)
    @test CData.isreleased(imported)
    CData.release!(exported)
    @test CData.isreleased(exported)

    large = Arrow.ListView(Int64[0, 3], Int64[3, 2], item)
    large_exported = CData.exporttable(_single_column_table(:values, large))
    large_schema = _child_schema(CData.schema(large_exported), 1)
    large_array = _child_array(CData.array(large_exported), 1)
    @test _cstring(large_schema.format) == "+vL"
    @test unsafe_load(large_array.buffers, 1) == C_NULL
    @test unsafe_load(large_array.buffers, 2) == Ptr{Cvoid}(pointer(large.offsets))
    @test unsafe_load(large_array.buffers, 3) == Ptr{Cvoid}(pointer(large.sizes))

    CData.release!(large_exported)
    @test CData.isreleased(large_exported)
end

@testset "exports into caller-owned base structs" begin
    before = CData._retained_handle_count()
    table = Arrow.Table(Arrow.tobuffer((id=Int32[1, 2], name=["a", "b"])))
    schema_ref = Ref{CData.ArrowSchema}()
    array_ref = Ref{CData.ArrowArray}()
    schema_out = Base.unsafe_convert(Ptr{CData.ArrowSchema}, schema_ref)
    array_out = Base.unsafe_convert(Ptr{CData.ArrowArray}, array_ref)

    exported = CData.exporttable!(schema_out, array_out, table)
    schema = unsafe_load(schema_out)
    array = unsafe_load(array_out)

    @test CData.schema_ptr(exported) == schema_out
    @test CData.array_ptr(exported) == array_out
    @test _cstring(schema.format) == "+s"
    @test array.length == 2
    @test schema.release != C_NULL
    @test array.release != C_NULL
    @test schema.private_data != C_NULL
    @test array.private_data != C_NULL
    @test CData._retained_handle_count() >= before + 6

    ccall(array.release, Cvoid, (Ptr{CData.ArrowArray},), array_out)
    @test CData.array(exported).release == C_NULL
    @test !CData.isreleased(exported)
    ccall(schema.release, Cvoid, (Ptr{CData.ArrowSchema},), schema_out)
    @test CData.isreleased(exported)
    CData.release!(exported)
    @test CData.isreleased(exported)
    @test CData._retained_handle_count() == before

    @test_throws ArgumentError CData.exporttable!(
        Ptr{CData.ArrowSchema}(C_NULL),
        array_out,
        table,
    )
    @test_throws ArgumentError CData.exporttable!(
        schema_out,
        Ptr{CData.ArrowArray}(C_NULL),
        table,
    )
end
