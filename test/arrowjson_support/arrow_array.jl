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

_mapentrykey(entry) = getfield(entry, 1)
_mapentryvalue(entry) = getfield(entry, 2)

function Base.getindex(x::ArrowArray{T}, i::Base.Int) where {T}
    @boundscheck checkbounds(x, i)
    S = Base.nonmissingtype(T)
    if x.field.dictionary !== nothing
        fielddata =
            x.dictionaries[findfirst(y -> y.id == x.field.dictionary.id, x.dictionaries)].data.columns[1]
        field = copy(x.field)
        field.dictionary = nothing
        idx = x.fielddata.DATA[i] + 1
        return ArrowArray(field, fielddata, x.dictionaries)[idx]
    end
    if T === Missing
        return missing
    elseif x.field.type isa RunEndEncoded
        run_ends = ArrowArray(x.field.children[1], x.fielddata.children[1], x.dictionaries)
        values = ArrowArray(x.field.children[2], x.fielddata.children[2], x.dictionaries)
        physical = searchsortedfirst(run_ends, i)
        return values[physical]
    elseif S <: UnionT
        U = eltype(S)
        tids = Arrow.typeids(S) === nothing ? (0:fieldcount(U)) : Arrow.typeids(S)
        typeid = tids[x.fielddata.TYPE_ID[i]]
        if Arrow.unionmode(S) == Arrow.Meta.UnionMode.DENSE
            off = x.fielddata.OFFSET[i]
            return ArrowArray(
                x.field.children[typeid + 1],
                x.fielddata.children[typeid + 1],
                x.dictionaries,
            )[off]
        else
            return ArrowArray(
                x.field.children[typeid + 1],
                x.fielddata.children[typeid + 1],
                x.dictionaries,
            )[i]
        end
    end
    x.fielddata.VALIDITY[i] == 0 && return missing
    if S <: Vector{UInt8}
        x.field.type isa BinaryView &&
            return _binaryviewvalue(x.fielddata.VIEWS[i], x.fielddata.VARIADIC_DATA_BUFFERS)
        return _hexbytes(x.fielddata.DATA[i])
    elseif S <: String
        x.field.type isa Utf8View &&
            return _utf8viewvalue(x.fielddata.VIEWS[i], x.fielddata.VARIADIC_DATA_BUFFERS)
        return x.fielddata.DATA[i]
    elseif S <: Vector
        offs = x.fielddata.OFFSET
        if x.field.type isa Union{ListView,LargeListView}
            A = ArrowArray{eltype(S)}(
                x.field.children[1],
                x.fielddata.children[1],
                x.dictionaries,
            )
            first = _offsetvalue(offs[i]) + 1
            last = first + _offsetvalue(x.fielddata.SIZE[i]) - 1
            return A[first:last]
        end
        A = ArrowArray{eltype(S)}(
            x.field.children[1],
            x.fielddata.children[1],
            x.dictionaries,
        )
        return A[(_offsetvalue(offs[i]) + 1):_offsetvalue(offs[i + 1])]
    elseif S <: Dict
        offs = x.fielddata.OFFSET
        A = ArrowArray(x.field.children[1], x.fielddata.children[1], x.dictionaries)
        return Dict(
            _mapentrykey(y) => _mapentryvalue(y) for
            y in A[(_offsetvalue(offs[i]) + 1):_offsetvalue(offs[i + 1])]
        )
    elseif S <: Tuple
        if Arrow.ArrowTypes.gettype(Arrow.ArrowTypes.ArrowKind(S)) == UInt8
            A = x.fielddata.DATA
            return Tuple(_hexbytes(A[i])[1:(x.field.type.byteWidth)])
        else
            sz = x.field.type.listSize
            A = ArrowArray{Arrow.ArrowTypes.gettype(Arrow.ArrowTypes.ArrowKind(S))}(
                x.field.children[1],
                x.fielddata.children[1],
                x.dictionaries,
            )
            off = (i - 1) * sz + 1
            return Tuple(A[off:(off + sz - 1)])
        end
    elseif S <: NamedTuple
        data = (
            ArrowArray(x.field.children[j], x.fielddata.children[j], x.dictionaries)[i] for
            j = 1:length(x.field.children)
        )
        return NamedTuple{fieldnames(S)}(Tuple(data))
    elseif S == Int64 || S == UInt64
        return parse(S, x.fielddata.DATA[i])
    elseif S <: Arrow.Decimal
        str = x.fielddata.DATA[i]
        return S(parse(Int128, str))
    elseif S <: Arrow.Date || S <: Arrow.Time
        val = x.fielddata.DATA[i]
        return Arrow.storagetype(S) == Int32 ? S(val) : S(parse(Int64, val))
    elseif S <: Arrow.Timestamp
        return S(parse(Int64, x.fielddata.DATA[i]))
    elseif S <: Arrow.Interval
        return _intervalvalue(S, x.fielddata.DATA[i])
    else
        return S(x.fielddata.DATA[i])
    end
end
