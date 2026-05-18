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

    bad_null_count = Ref(
        CData.ArrowArray(
            array.length,
            array.length + 1,
            array.offset,
            array.n_buffers,
            array.n_children,
            array.buffers,
            array.children,
            array.dictionary,
            array.release,
            array.private_data,
        ),
    )
    GC.@preserve bad_null_count begin
        @test_throws ArgumentError(
            "column top-level struct has null count greater than length",
        ) CData.importtable(
            CData.schema_ptr(exported),
            Base.unsafe_convert(Ptr{CData.ArrowArray}, bad_null_count),
        )
    end

    bad_child_count = Ref(
        CData.ArrowArray(
            array.length,
            array.null_count,
            array.offset,
            array.n_buffers,
            -1,
            array.buffers,
            array.children,
            array.dictionary,
            array.release,
            array.private_data,
        ),
    )
    GC.@preserve bad_child_count begin
        @test_throws ArgumentError("column top-level struct has negative child count") CData.importtable(
            CData.schema_ptr(exported),
            Base.unsafe_convert(Ptr{CData.ArrowArray}, bad_child_count),
        )
    end

    bad_schema_child_count = Ref(
        CData.ArrowSchema(
            schema.format,
            schema.name,
            schema.metadata,
            schema.flags,
            -1,
            schema.children,
            schema.dictionary,
            schema.release,
            schema.private_data,
        ),
    )
    GC.@preserve bad_schema_child_count begin
        @test_throws ArgumentError(
            "schema for column top-level struct has negative child count",
        ) CData.importtable(
            Base.unsafe_convert(Ptr{CData.ArrowSchema}, bad_schema_child_count),
            CData.array_ptr(exported),
        )
    end

    child_bad_null_count = CData.exporttable(table)
    child_array_ptr = _child_array_ptr(CData.array(child_bad_null_count), 1)
    child_array = unsafe_load(child_array_ptr)
    _set_array_layout!(child_array_ptr; null_count=(child_array.length + 1))
    @test_throws ArgumentError("column id has null count greater than length") CData.importtable(
        CData.schema_ptr(child_bad_null_count),
        CData.array_ptr(child_bad_null_count),
    )
    CData.release!(child_bad_null_count)
    @test CData.isreleased(child_bad_null_count)

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
