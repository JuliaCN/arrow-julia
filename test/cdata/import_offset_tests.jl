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

@testset "imports non-zero offset C Data arrays" begin
    table = Arrow.Table(
        Arrow.tobuffer((
            maybe=Union{Missing,Int32}[
                Int32(10),
                missing,
                Int32(30),
                Int32(40),
                missing,
                Int32(60),
            ],
            flag=Union{Missing,Bool}[true, missing, false, true, false, missing],
            label=Union{Missing,String}["skip", "a", missing, "ccc", "dddd", "tail"],
            values=[Int64[99], Int64[1, 2], Int64[], Int64[3, 4, 5], Int64[6], Int64[88]],
        ),),
    )
    exported = CData.exporttable(table)
    _set_array_layout!(CData.array_ptr(exported); length=4)
    top = CData.array(exported)
    for index = 1:top.n_children
        _set_array_layout!(_child_array_ptr(top, index); length=4, offset=1)
    end
    imported = CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported))

    maybe = Tables.getcolumn(imported, :maybe)
    flag = Tables.getcolumn(imported, :flag)
    label = Tables.getcolumn(imported, :label)
    values = Tables.getcolumn(imported, :values)
    maybe_array = _child_array(CData.array(exported), 1)
    flag_array = _child_array(CData.array(exported), 2)
    label_array = _child_array(CData.array(exported), 3)
    values_array = _child_array(CData.array(exported), 4)
    @test isequal(
        collect(maybe),
        Union{Missing,Int32}[missing, Int32(30), Int32(40), missing],
    )
    @test isequal(collect(flag), Union{Missing,Bool}[missing, false, true, false])
    @test isequal(collect(label), Union{Missing,String}["a", missing, "ccc", "dddd"])
    @test map(collect, collect(values)) == [Int64[1, 2], Int64[], Int64[3, 4, 5], Int64[6]]
    @test pointer(maybe.data) ==
          Ptr{Int32}(unsafe_load(maybe_array.buffers, 2)) + sizeof(Int32)
    @test pointer(maybe.validity) == Ptr{UInt8}(unsafe_load(maybe_array.buffers, 1))
    @test pointer(flag.data) == Ptr{UInt8}(unsafe_load(flag_array.buffers, 2))
    @test pointer(label.values.offsets) ==
          Ptr{Int32}(unsafe_load(label_array.buffers, 2)) + sizeof(Int32)
    @test pointer(values.offsets) ==
          Ptr{Int32}(unsafe_load(values_array.buffers, 2)) + sizeof(Int32)

    CData.release!(imported)
    @test CData.isreleased(imported)
    @test CData.isreleased(exported)

    dict_table = Arrow.Table(
        Arrow.tobuffer(
            (color=["skip", "red", "blue", "red", "green", "tail"],);
            dictencode=true,
        ),
    )
    dict_exported = CData.exporttable(dict_table)
    _set_array_layout!(CData.array_ptr(dict_exported); length=4)
    dict_top = CData.array(dict_exported)
    _set_array_layout!(_child_array_ptr(dict_top, 1); length=4, offset=1)
    dict_imported =
        CData.importtable(CData.schema_ptr(dict_exported), CData.array_ptr(dict_exported))
    color = Tables.getcolumn(dict_imported, :color)
    color_array = _child_array(CData.array(dict_exported), 1)
    @test collect(color) == ["red", "blue", "red", "green"]
    @test Ptr{Cvoid}(pointer(color.indices)) ==
          unsafe_load(color_array.buffers, 2) + sizeof(eltype(color.indices))

    CData.release!(dict_imported)
    @test CData.isreleased(dict_imported)
    @test CData.isreleased(dict_exported)

    top_table = Arrow.Table(
        Arrow.tobuffer((id=Int32[10, 20, 30, 40], name=["a", "bb", "ccc", "dddd"])),
    )
    top_exported = CData.exporttable(top_table)
    _set_array_layout!(CData.array_ptr(top_exported); length=2, offset=1)
    top_imported =
        CData.importtable(CData.schema_ptr(top_exported), CData.array_ptr(top_exported))
    @test collect(Tables.getcolumn(top_imported, :id)) == Int32[20, 30]
    @test collect(Tables.getcolumn(top_imported, :name)) == ["bb", "ccc"]

    CData.release!(top_imported)
    @test CData.isreleased(top_imported)
    @test CData.isreleased(top_exported)

    fixed_table = Arrow.Table(
        Arrow.tobuffer((
            fixed=NTuple{2,Int32}[
                (Int32(1), Int32(2)),
                (Int32(3), Int32(4)),
                (Int32(5), Int32(6)),
            ],
        ),),
    )
    fixed_exported = CData.exporttable(fixed_table)
    _set_array_layout!(CData.array_ptr(fixed_exported); length=1)
    fixed_top = CData.array(fixed_exported)
    _set_array_layout!(_child_array_ptr(fixed_top, 1); length=1, offset=1)
    fixed_imported =
        CData.importtable(CData.schema_ptr(fixed_exported), CData.array_ptr(fixed_exported))
    @test collect(Tables.getcolumn(fixed_imported, :fixed)) ==
          NTuple{2,Int32}[(Int32(3), Int32(4))]

    CData.release!(fixed_imported)
    @test CData.isreleased(fixed_imported)
    @test CData.isreleased(fixed_exported)

    view_table = Arrow.Table(_test_fixture("reject_reason_trimmed.arrow"))
    source_reason = Tables.getcolumn(view_table, :reject_reason)
    view_exported = CData.exporttable(view_table)
    view_len = length(source_reason) - 1
    _set_array_layout!(CData.array_ptr(view_exported); length=view_len)
    view_top = CData.array(view_exported)
    _set_array_layout!(_child_array_ptr(view_top, 1); length=view_len, offset=1)
    view_imported =
        CData.importtable(CData.schema_ptr(view_exported), CData.array_ptr(view_exported))
    @test isequal(
        collect(Tables.getcolumn(view_imported, :reject_reason)),
        collect(source_reason)[2:end],
    )

    CData.release!(view_imported)
    @test CData.isreleased(view_imported)
    @test CData.isreleased(view_exported)

    ree_table = Arrow.Table(_test_fixture("run_end_encoded_small.arrow"); convert=false)
    ree_source = Tables.getcolumn(ree_table, :x)
    ree_exported = CData.exporttable(ree_table)
    _set_array_layout!(CData.array_ptr(ree_exported); length=3)
    ree_top = CData.array(ree_exported)
    _set_array_layout!(_child_array_ptr(ree_top, 1); length=3, offset=2)
    ree_imported =
        CData.importtable(CData.schema_ptr(ree_exported), CData.array_ptr(ree_exported))
    @test isequal(collect(Tables.getcolumn(ree_imported, :x)), collect(ree_source)[3:5])

    CData.release!(ree_imported)
    @test CData.isreleased(ree_imported)
    @test CData.isreleased(ree_exported)
end
