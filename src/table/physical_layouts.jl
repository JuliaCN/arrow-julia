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

function ArrowTypes.physicallayout(field::Meta.Field, convert::Bool=true)
    dictionary = field.dictionary
    if dictionary !== nothing
        index_storage =
            dictionary.indexType === nothing ? Int32 :
            juliaeltype(field, dictionary.indexType, false)
        return ArrowTypes.DictionaryEncodedLayout{index_storage}()
    end
    return ArrowTypes.physicallayout(field, field.type, convert)
end

ArrowTypes.physicallayout(field::Meta.Field, ::Meta.Null, convert::Bool=true) =
    ArrowTypes.NullLayout()

ArrowTypes.physicallayout(field::Meta.Field, ::Meta.Bool, convert::Bool=true) =
    ArrowTypes.BooleanLayout()

ArrowTypes.physicallayout(
    field::Meta.Field,
    ::Union{
        Meta.Int,
        Meta.FloatingPoint,
        Meta.Decimal,
        Meta.Date,
        Meta.Time,
        Meta.Timestamp,
        Meta.Interval,
        Meta.Duration,
    },
    convert::Bool=true,
) = ArrowTypes.PrimitiveLayout{juliaeltype(field, field.type, false)}()

ArrowTypes.physicallayout(
    field::Meta.Field,
    ::Union{Meta.Binary,Meta.Utf8},
    convert::Bool=true,
) = ArrowTypes.VariableBinaryLayout{Int32}()

ArrowTypes.physicallayout(
    field::Meta.Field,
    ::Union{Meta.LargeBinary,Meta.LargeUtf8},
    convert::Bool=true,
) = ArrowTypes.VariableBinaryLayout{Int64}()

ArrowTypes.physicallayout(
    field::Meta.Field,
    ::Union{Meta.BinaryView,Meta.Utf8View},
    convert::Bool=true,
) = ArrowTypes.VariableBinaryViewLayout()

ArrowTypes.physicallayout(
    field::Meta.Field,
    layout::Meta.FixedSizeBinary,
    convert::Bool=true,
) = ArrowTypes.FixedSizeBinaryLayout{Int(layout.byteWidth)}()

ArrowTypes.physicallayout(
    field::Meta.Field,
    ::Union{Meta.List,Meta.Map},
    convert::Bool=true,
) = ArrowTypes.VariableListLayout{Int32}()

ArrowTypes.physicallayout(field::Meta.Field, ::Meta.LargeList, convert::Bool=true) =
    ArrowTypes.VariableListLayout{Int64}()

ArrowTypes.physicallayout(field::Meta.Field, ::Meta.ListView, convert::Bool=true) =
    ArrowTypes.VariableListViewLayout{Int32}()

ArrowTypes.physicallayout(field::Meta.Field, ::Meta.LargeListView, convert::Bool=true) =
    ArrowTypes.VariableListViewLayout{Int64}()

ArrowTypes.physicallayout(
    field::Meta.Field,
    layout::Meta.FixedSizeList,
    convert::Bool=true,
) = ArrowTypes.FixedSizeListLayout{Int(layout.listSize)}()

ArrowTypes.physicallayout(field::Meta.Field, ::Meta.Struct, convert::Bool=true) =
    ArrowTypes.StructLayout()

function ArrowTypes.physicallayout(
    field::Meta.Field,
    layout::Meta.Union,
    convert::Bool=true,
)
    mode = layout.mode == Meta.UnionMode.Dense ? :dense : :sparse
    return ArrowTypes.UnionLayout{mode}()
end

function ArrowTypes.physicallayout(
    field::Meta.Field,
    ::Meta.RunEndEncoded,
    convert::Bool=true,
)
    children = field.children
    run_ends = children === nothing ? nothing : first(children)
    run_end_type =
        run_ends === nothing ? Int32 : juliaeltype(run_ends, buildmetadata(run_ends), false)
    return ArrowTypes.RunEndEncodedLayout{run_end_type}()
end

function materializecolumns(x::VectorIterator)
    cols = Vector{AbstractVector}(undef, length(x))
    i = 1
    state = (Int64(1), Int64(1), Int64(1), Int64(1))
    while true
        next = iterate(x, state)
        next === nothing && break
        col, state = next
        cols[i] = col
        i += 1
    end
    _, nodeidx, bufferidx, varbufferidx = state
    _assert_record_batch_fully_consumed(
        x.batch.msg.header,
        nodeidx,
        bufferidx,
        varbufferidx,
    )
    return cols
end

function Base.iterate(
    x::VectorIterator,
    (columnidx, nodeidx, bufferidx, varbufferidx)=(Int64(1), Int64(1), Int64(1), Int64(1)),
)
    columnidx > length(x.schema.fields) && return nothing
    field = x.schema.fields[columnidx]
    @debug "building top-level column: field = $(field), columnidx = $columnidx, nodeidx = $nodeidx, bufferidx = $bufferidx, varbufferidx = $varbufferidx"
    A, nodeidx, bufferidx, varbufferidx = build(
        field,
        x.batch,
        x.batch.msg.header,
        x.dictencodings,
        nodeidx,
        bufferidx,
        varbufferidx,
        x.convert,
    )
    @debug "built top-level column: A = $(typeof(A)), columnidx = $columnidx, nodeidx = $nodeidx, bufferidx = $bufferidx, varbufferidx = $varbufferidx"
    @debug A
    return A, (columnidx + 1, nodeidx, bufferidx, varbufferidx)
end

Base.length(x::VectorIterator) = length(x.schema.fields)
