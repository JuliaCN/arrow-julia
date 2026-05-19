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

@testset "Run-End Encoded read support" begin
    path = joinpath(dirname(@__DIR__), "run_end_encoded_small.arrow")
    expected = ["a", "a", "b", "b", "b"]

    tt = Arrow.Table(path)
    @test tt isa Arrow.Table
    @test eltype(tt.x) == Union{Missing,String}
    @test collect(tt.x) == expected
    @test copy(tt.x) == expected

    batches = collect(Arrow.Stream(path))
    @test length(batches) == 1
    @test collect(batches[1].x) == expected

    roundtrip = Arrow.Table(Arrow.tobuffer(tt); convert=false)
    @test roundtrip.x isa Arrow.RunEndEncoded
    @test collect(roundtrip.x) == expected

    column_roundtrip = Arrow.Table(Arrow.tobuffer((x=tt.x,)); convert=false)
    @test column_roundtrip.x isa Arrow.RunEndEncoded
    @test collect(column_roundtrip.x) == expected
end

@testset "canonical bool8/json/opaque" begin
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

    bools = Union{Missing,Arrow.Bool8}[Arrow.Bool8(true), missing, Arrow.Bool8(false)]
    @test ArrowTypes.JuliaType(Val(Symbol("arrow.bool8")), Int8, "") == Arrow.Bool8
    tt = Arrow.Table(Arrow.tobuffer((col=bools,)))
    @test eltype(tt.col) == Union{Missing,Arrow.Bool8}
    @test isequal(copy(tt.col), bools)
    @test Arrow.getmetadata(tt.col)["ARROW:extension:name"] == "arrow.bool8"

    raw_tt = Arrow.Table(Arrow.tobuffer((col=bools,)); convert=false)
    @test eltype(raw_tt.col) == Union{Missing,Int8}
    @test isequal(copy(raw_tt.col), Union{Missing,Int8}[1, missing, 0])

    jsons = Union{Missing,Arrow.JSONText{String}}[
        Arrow.JSONText("{\"a\":1}"),
        missing,
        Arrow.JSONText("[1,2,3]"),
    ]
    @test ArrowTypes.JuliaType(Val(Symbol("arrow.json")), String, "") ==
          Arrow.JSONText{String}
    json_tt = Arrow.Table(Arrow.tobuffer((col=jsons,)))
    @test eltype(json_tt.col) == Union{Missing,Arrow.JSONText{String}}
    @test isequal(copy(json_tt.col), jsons)
    @test Arrow.getmetadata(json_tt.col)["ARROW:extension:name"] == "arrow.json"

    raw_json_tt = Arrow.Table(Arrow.tobuffer((col=jsons,)); convert=false)
    @test eltype(raw_json_tt.col) == Union{Missing,String}
    @test isequal(
        copy(raw_json_tt.col),
        Union{Missing,String}["{\"a\":1}", missing, "[1,2,3]"],
    )

    json_object_metadata = Arrow.Table(
        Arrow.tobuffer(
            (col=["{\"ok\":true}"],);
            colmetadata=Dict(
                :col => Dict(
                    "ARROW:extension:name" => "arrow.json",
                    "ARROW:extension:metadata" => "{}",
                ),
            ),
        ),
    )
    @test eltype(json_object_metadata.col) == Arrow.JSONText{String}
    @test Arrow.getmetadata(json_object_metadata.col)["ARROW:extension:metadata"] == "{}"

    opaque_meta = Arrow.opaquemetadata("pkg.Type", "vendor.example")
    @test ArrowTypes.JuliaType(Val(Symbol("arrow.opaque")), String, opaque_meta) == String
    opaque_tt = Arrow.Table(
        Arrow.tobuffer(
            (col=["a", "b"],);
            colmetadata=Dict(
                :col => Dict(
                    "ARROW:extension:name" => "arrow.opaque",
                    "ARROW:extension:metadata" => opaque_meta,
                ),
            ),
        ),
    )
    @test eltype(opaque_tt.col) == String
    @test copy(opaque_tt.col) == ["a", "b"]
    @test Arrow.getmetadata(opaque_tt.col)["ARROW:extension:name"] == "arrow.opaque"
    @test Arrow.getmetadata(opaque_tt.col)["ARROW:extension:metadata"] == opaque_meta

    opaque_extra_metadata = Arrow.Table(
        Arrow.tobuffer(
            (col=["a"],);
            colmetadata=Dict(
                :col => Dict(
                    "ARROW:extension:name" => "arrow.opaque",
                    "ARROW:extension:metadata" => "{\"type_name\":\"pkg.Type\",\"vendor_name\":\"vendor.example\",\"extra\":true}",
                ),
            ),
        ),
    )
    @test copy(opaque_extra_metadata.col) == ["a"]

    invalid_bool8_metadata = Arrow.tobuffer(
        (col=Int8[1, 0],);
        colmetadata=Dict(
            :col => Dict(
                "ARROW:extension:name" => "arrow.bool8",
                "ARROW:extension:metadata" => "{}",
            ),
        ),
    )
    assert_canonical_extension_error(
        () -> Arrow.Table(invalid_bool8_metadata),
        "invalid canonical arrow.bool8 extension",
    )

    invalid_bool8_storage = Arrow.tobuffer(
        (col=Int16[1, 0],);
        colmetadata=Dict(:col => Dict("ARROW:extension:name" => "arrow.bool8")),
    )
    assert_canonical_extension_error(
        () -> Arrow.Table(invalid_bool8_storage),
        "storage must be signed Int8",
    )

    invalid_json_metadata = Arrow.tobuffer(
        (col=["{}"],);
        colmetadata=Dict(
            :col => Dict(
                "ARROW:extension:name" => "arrow.json",
                "ARROW:extension:metadata" => "[]",
            ),
        ),
    )
    assert_canonical_extension_error(
        () -> Arrow.Table(invalid_json_metadata),
        "metadata must be a JSON object",
    )

    invalid_json_storage = Arrow.tobuffer(
        (col=Int32[1],);
        colmetadata=Dict(:col => Dict("ARROW:extension:name" => "arrow.json")),
    )
    assert_canonical_extension_error(
        () -> Arrow.Table(invalid_json_storage),
        "storage must be Utf8, LargeUtf8, or Utf8View",
    )

    invalid_opaque_metadata = Arrow.tobuffer(
        (col=["a"],);
        colmetadata=Dict(
            :col => Dict(
                "ARROW:extension:name" => "arrow.opaque",
                "ARROW:extension:metadata" => "{\"type_name\":\"pkg.Type\"}",
            ),
        ),
    )
    assert_canonical_extension_error(
        () -> Arrow.Table(invalid_opaque_metadata),
        "\"vendor_name\" is required",
    )
end

@testset "canonical advanced passthrough" begin
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

    @test Arrow.variantmetadata() == ""

    fixed_metadata =
        Arrow.fixedshapetensormetadata([2, 2]; dim_names=["x", "y"], permutation=[1, 0])
    @test JSON3.read(fixed_metadata)["shape"] == [2, 2]
    @test JSON3.read(fixed_metadata)["dim_names"] == ["x", "y"]
    @test JSON3.read(fixed_metadata)["permutation"] == [1, 0]

    variable_metadata = Arrow.variableshapetensormetadata(
        uniform_shape=Union{Nothing,Int}[2],
        dim_names=["axis0"],
        permutation=[0],
    )
    @test ArrowTypes.JuliaType(Val(Symbol("arrow.parquet.variant")), String, "") == String
    @test ArrowTypes.JuliaType(
        Val(Symbol("arrow.fixed_shape_tensor")),
        NTuple{4,Int32},
        fixed_metadata,
    ) == NTuple{4,Int32}
    @test ArrowTypes.JuliaType(
        Val(Symbol("arrow.variable_shape_tensor")),
        NamedTuple{(:data, :shape),Tuple{Vector{Int32},NTuple{1,Int32}}},
        variable_metadata,
    ) == NamedTuple{(:data, :shape),Tuple{Vector{Int32},NTuple{1,Int32}}}
    @test JSON3.read(variable_metadata)["uniform_shape"] == [2]
    @test JSON3.read(variable_metadata)["dim_names"] == ["axis0"]
    @test JSON3.read(variable_metadata)["permutation"] == [0]
    @test Arrow.variableshapetensormetadata() == ""

    @test_throws ArgumentError Arrow.fixedshapetensormetadata([2, 2]; dim_names=["x"])
    @test_throws ArgumentError Arrow.variableshapetensormetadata(
        uniform_shape=Union{Nothing,Int}[2, nothing];
        permutation=[0],
    )

    variant_values = Union{Missing,NamedTuple{(:metadata, :value),Tuple{String,String}}}[
        (metadata="json", value="{\"a\":1}"),
        missing,
        (metadata="str", value="abc"),
    ]
    @test_logs min_level = Base.CoreLogging.Warn begin
        variant_tt = Arrow.Table(
            Arrow.tobuffer(
                (col=variant_values,);
                colmetadata=Dict(
                    :col => Dict(
                        "ARROW:extension:name" => "arrow.parquet.variant",
                        "ARROW:extension:metadata" => Arrow.variantmetadata(),
                    ),
                ),
            ),
        )
        @test eltype(variant_tt.col) == eltype(variant_values)
        @test isequal(copy(variant_tt.col), variant_values)
        @test Arrow.getmetadata(variant_tt.col)["ARROW:extension:name"] ==
              "arrow.parquet.variant"
    end

    fixed_tensor_values = Union{Missing,NTuple{4,Int32}}[
        (Int32(1), Int32(2), Int32(3), Int32(4)),
        missing,
        (Int32(5), Int32(6), Int32(7), Int32(8)),
    ]
    @test_logs min_level = Base.CoreLogging.Warn begin
        fixed_tensor_tt = Arrow.Table(
            Arrow.tobuffer(
                (col=fixed_tensor_values,);
                colmetadata=Dict(
                    :col => Dict(
                        "ARROW:extension:name" => "arrow.fixed_shape_tensor",
                        "ARROW:extension:metadata" => fixed_metadata,
                    ),
                ),
            ),
        )
        @test eltype(fixed_tensor_tt.col) == eltype(fixed_tensor_values)
        @test isequal(copy(fixed_tensor_tt.col), fixed_tensor_values)
        @test Arrow.getmetadata(fixed_tensor_tt.col)["ARROW:extension:name"] ==
              "arrow.fixed_shape_tensor"
    end

    variable_tensor_values =
        Union{Missing,NamedTuple{(:data, :shape),Tuple{Vector{Int32},NTuple{1,Int32}}}}[
            (data=Int32[1, 2, 3, 4], shape=(Int32(2),)),
            missing,
            (data=Int32[5, 6], shape=(Int32(1),)),
        ]
    @test_logs min_level = Base.CoreLogging.Warn begin
        variable_tensor_tt = Arrow.Table(
            Arrow.tobuffer(
                (col=variable_tensor_values,);
                colmetadata=Dict(
                    :col => Dict(
                        "ARROW:extension:name" => "arrow.variable_shape_tensor",
                        "ARROW:extension:metadata" => variable_metadata,
                    ),
                ),
            ),
        )
        @test eltype(variable_tensor_tt.col) == eltype(variable_tensor_values)
        @test isequal(
            map(
                x -> x === missing ? missing : (data=copy(x.data), shape=x.shape),
                copy(variable_tensor_tt.col),
            ),
            variable_tensor_values,
        )
        @test Arrow.getmetadata(variable_tensor_tt.col)["ARROW:extension:name"] ==
              "arrow.variable_shape_tensor"
    end

    invalid_variant_bytes = Arrow.tobuffer(
        (col=variant_values,);
        colmetadata=Dict(
            :col => Dict(
                "ARROW:extension:name" => "arrow.parquet.variant",
                "ARROW:extension:metadata" => "{\"unexpected\":true}",
            ),
        ),
    )
    assert_canonical_extension_error(
        () -> Arrow.Table(invalid_variant_bytes),
        "invalid canonical arrow.parquet.variant extension",
    )

    invalid_fixed_bytes = Arrow.tobuffer(
        (col=fixed_tensor_values,);
        colmetadata=Dict(
            :col => Dict(
                "ARROW:extension:name" => "arrow.fixed_shape_tensor",
                "ARROW:extension:metadata" => Arrow.fixedshapetensormetadata([3, 2]),
            ),
        ),
    )
    assert_canonical_extension_error(
        () -> Arrow.Table(invalid_fixed_bytes),
        "invalid canonical arrow.fixed_shape_tensor extension",
    )

    invalid_variable_bytes = Arrow.tobuffer(
        (col=["a", "b"],);
        colmetadata=Dict(
            :col => Dict(
                "ARROW:extension:name" => "arrow.variable_shape_tensor",
                "ARROW:extension:metadata" => Arrow.variableshapetensormetadata(
                    uniform_shape=Union{Nothing,Int}[1],
                ),
            ),
        ),
    )
    assert_canonical_extension_error(
        () -> Arrow.Table(invalid_variable_bytes),
        "invalid canonical arrow.variable_shape_tensor extension",
    )
end

@testset "logical extension runtime contract" begin
    uuid = UUID("550e8400-e29b-41d4-a716-446655440000")
    @test Arrow._builtinarrowtype(UUID) == NTuple{16,UInt8}
    @test Arrow._builtintoarrow(uuid) == ArrowTypes._cast(NTuple{16,UInt8}, uuid.value)
    @test Arrow._builtinarrowname(UUID) == Symbol("arrow.uuid")
    @test ArrowTypes.ArrowType(UUID) == Arrow._builtinarrowtype(UUID)
    @test ArrowTypes.toarrow(uuid) == Arrow._builtintoarrow(uuid)
    @test ArrowTypes.arrowname(UUID) == Arrow._builtinarrowname(UUID)
    @test ArrowTypes.JuliaType(Val(Symbol("arrow.uuid"))) == UUID
    @test ArrowTypes.JuliaType(Val(Symbol("JuliaLang.UUID"))) == UUID
    @test Arrow.LEGACY_UUID_EXTENSION_SYMBOL == Symbol("JuliaLang.UUID")
    uuid_spec = Arrow._extensionspec(UUID)
    @test uuid_spec isa Arrow.ExtensionTypeSpec
    @test uuid_spec.name == Arrow.ArrowTypes.UUIDSYMBOL
    @test uuid_spec.metadata == ""
    @test Arrow._resolveextensionjuliatype(
        Arrow.ExtensionTypeSpec(Arrow.LEGACY_UUID_EXTENSION_SYMBOL, ""),
        NTuple{16,UInt8},
    ) == UUID

    bool8_spec = Arrow._extensionspec(Arrow.Bool8)
    @test bool8_spec isa Arrow.ExtensionTypeSpec
    @test bool8_spec.name == Symbol("arrow.bool8")
    @test Arrow._builtinarrowtype(Arrow.Bool8) == Int8
    @test Arrow._builtintoarrow(Arrow.Bool8(true)) == Int8(1)
    @test Arrow._builtinarrowname(Arrow.Bool8) == Symbol("arrow.bool8")
    @test Arrow._builtinfromarrow(Arrow.Bool8, Int8(1)) == Arrow.Bool8(true)
    @test Arrow._builtindefault(Arrow.Bool8) == Arrow.Bool8(false)
    @test Arrow._resolveextensionjuliatype(bool8_spec, Int8) == Arrow.Bool8

    @test Arrow._builtinarrowtype(Arrow.JSONText{String}) == String
    @test Arrow._builtintoarrow(Arrow.JSONText("abc")) == "abc"
    @test Arrow._builtinarrowname(Arrow.JSONText{String}) == Symbol("arrow.json")
    @test Arrow._builtinfromarrow(Arrow.JSONText{String}, pointer("abc"), 3) ==
          Arrow.JSONText("abc")
    @test Arrow._builtinfromarrow(Arrow.JSONText{String}, "xyz") == Arrow.JSONText("xyz")
    @test Arrow._builtindefault(Arrow.JSONText{String}) == Arrow.JSONText("")

    timestamp_storage = NamedTuple{
        (:timestamp, :offset_minutes),
        Tuple{Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,:UTC},Int16},
    }
    zdt = ZonedDateTime(Dates.DateTime(2020), tz"Europe/Paris")
    @test Arrow._builtinarrowtype(ZonedDateTime) == Arrow.Timestamp
    @test Arrow._builtintoarrow(zdt) == convert(
        Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,Symbol("Europe/Paris")},
        zdt,
    )
    @test Arrow._builtinarrowname(ZonedDateTime) == Symbol("JuliaLang.ZonedDateTime-UTC")
    paris_timestamp =
        Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,Symbol("Europe/Paris")}(0)
    @test Arrow._builtinfromarrow(ZonedDateTime, paris_timestamp) ==
          convert(ZonedDateTime, paris_timestamp)
    @test Arrow._builtindefault(ZonedDateTime) == ZonedDateTime(1, 1, 1, 1, 1, 1, tz"UTC")
    @test Arrow._builtinarrowname(
        Arrow.TimestampWithOffset{Arrow.Meta.TimeUnit.MILLISECOND},
    ) == Symbol("arrow.timestamp_with_offset")
    @test Arrow._builtinarrowtype(
        Arrow.TimestampWithOffset{Arrow.Meta.TimeUnit.MILLISECOND},
    ) == NamedTuple{
        (:timestamp, :offset_minutes),
        Tuple{Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,:UTC},Int16},
    }
    ts_with_offset = Arrow.TimestampWithOffset(
        Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,:UTC}(123),
        Int16(-480),
    )
    @test Arrow._builtintoarrow(ts_with_offset) == (
        timestamp=Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,:UTC}(123),
        offset_minutes=Int16(-480),
    )
    @test ArrowTypes.ArrowType(
        Arrow.TimestampWithOffset{Arrow.Meta.TimeUnit.MILLISECOND},
    ) == Arrow._builtinarrowtype(
        Arrow.TimestampWithOffset{Arrow.Meta.TimeUnit.MILLISECOND},
    )
    @test ArrowTypes.toarrow(ts_with_offset) == Arrow._builtintoarrow(ts_with_offset)
    @test Arrow._builtindefault(
        Arrow.TimestampWithOffset{Arrow.Meta.TimeUnit.MILLISECOND},
    ) == zero(Arrow.TimestampWithOffset{Arrow.Meta.TimeUnit.MILLISECOND})
    @test Arrow._builtinfromarrowstruct(
        Arrow.TimestampWithOffset{Arrow.Meta.TimeUnit.MILLISECOND},
        Val((:timestamp, :offset_minutes)),
        Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,:UTC}(123),
        Int16(-480),
    ) == ts_with_offset
    @test Arrow._resolveextensionjuliatype(
        Arrow.ExtensionTypeSpec(Symbol("arrow.timestamp_with_offset"), ""),
        timestamp_storage,
    ) == Arrow.TimestampWithOffset{Arrow.Meta.TimeUnit.MILLISECOND}

    opaque_spec = Arrow.ExtensionTypeSpec(
        Symbol("arrow.opaque"),
        Arrow.opaquemetadata("demo.type", "demo.vendor"),
    )
    @test Arrow.opaquemetadata("demo.type", "demo.vendor") ==
          Arrow._builtinopaquemetadata("demo.type", "demo.vendor")
    @test Arrow._resolveextensionjuliatype(opaque_spec, Vector{UInt8}) == Vector{UInt8}
    @test Arrow.variantmetadata() == Arrow._builtinvariantmetadata()
    @test Arrow.fixedshapetensormetadata(
        [2, 2];
        dim_names=["row", "col"],
        permutation=[1, 0],
    ) == Arrow._builtinfixedshapetensormetadata(
        [2, 2];
        dim_names=["row", "col"],
        permutation=[1, 0],
    )
    @test Arrow.variableshapetensormetadata(
        uniform_shape=[2, nothing];
        dim_names=["row", "col"],
        permutation=[1, 0],
    ) == Arrow._builtinvariableshapetensormetadata(
        uniform_shape=[2, nothing];
        dim_names=["row", "col"],
        permutation=[1, 0],
    )
    @test Arrow._builtinextensionjuliatype(
        Val(Symbol("JuliaLang.ZonedDateTime-UTC")),
        Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,:UTC},
    ) == ZonedDateTime
    @test ArrowTypes.JuliaType(
        Val(Symbol("JuliaLang.ZonedDateTime-UTC")),
        Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,:UTC},
    ) == ZonedDateTime
    @test Arrow._builtinextensionjuliatype(
        Val(Symbol("JuliaLang.ZonedDateTime")),
        Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,:UTC},
    ) == Arrow.LocalZonedDateTime
    @test ArrowTypes.JuliaType(
        Val(Symbol("JuliaLang.ZonedDateTime")),
        Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,:UTC},
    ) == Arrow.LocalZonedDateTime
    local_zdt_timestamp =
        Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,Symbol("Europe/Paris")}(0)
    @test Arrow._builtinfromarrow(Arrow.LocalZonedDateTime, local_zdt_timestamp) ==
          ArrowTypes.fromarrow(Arrow.LocalZonedDateTime, local_zdt_timestamp)

    @test Arrow._resolveextensionjuliatype(
        Arrow.ExtensionTypeSpec(Symbol("JuliaLang.ZonedDateTime"), ""),
        Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,:UTC},
    ) == Arrow.LocalZonedDateTime
end
