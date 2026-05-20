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

# nested types; call juliaeltype recursively on nested children
function juliaeltype(
    f::Meta.Field,
    list::Union{Meta.List,Meta.LargeList,Meta.ListView,Meta.LargeListView},
    convert,
)
    return Vector{juliaeltype(f.children[1], buildmetadata(f.children[1]), convert)}
end

# arrowtype will call fieldoffset recursively for children
function arrowtype(b, x::List{T,O,A}) where {T,O,A}
    if liststringtype(x)
        if T <: AbstractString || T <: Union{AbstractString,Missing}
            if O == Int32
                Meta.utf8Start(b)
                return Meta.Utf8, Meta.utf8End(b), nothing
            else # if O == Int64
                Meta.largUtf8Start(b)
                return Meta.LargeUtf8, Meta.largUtf8End(b), nothing
            end
        else # if Base.CodeUnits
            if O == Int32
                Meta.binaryStart(b)
                return Meta.Binary, Meta.binaryEnd(b), nothing
            else # if O == Int64
                Meta.largeBinaryStart(b)
                return Meta.LargeBinary, Meta.largeBinaryEnd(b), nothing
            end
        end
    else
        children = [fieldoffset(b, "", x.data)]
        if O == Int32
            Meta.listStart(b)
            return Meta.List, Meta.listEnd(b), children
        else
            Meta.largeListStart(b)
            return Meta.LargeList, Meta.largeListEnd(b), children
        end
    end
end

function arrowtype(b, x::ListView{T,O,A}) where {T,O,A}
    children = [fieldoffset(b, "", x.data)]
    if O == Int32
        Meta.listViewStart(b)
        return Meta.ListView, Meta.listViewEnd(b), children
    else
        Meta.largeListViewStart(b)
        return Meta.LargeListView, Meta.largeListViewEnd(b), children
    end
end

function juliaeltype(f::Meta.Field, list::Meta.FixedSizeList, convert)
    type = juliaeltype(f.children[1], buildmetadata(f.children[1]), convert)
    return NTuple{Int(list.listSize),type}
end

function arrowtype(b, x::FixedSizeList{T,A}) where {T,A}
    N = ArrowTypes.getsize(
        ArrowTypes.ArrowKind(ArrowTypes.ArrowType(Base.nonmissingtype(T))),
    )
    if eltype(A) == UInt8
        Meta.fixedSizeBinaryStart(b)
        Meta.fixedSizeBinaryAddByteWidth(b, Int32(N))
        return Meta.FixedSizeBinary, Meta.fixedSizeBinaryEnd(b), nothing
    else
        children = [fieldoffset(b, "", x.data)]
        Meta.fixedSizeListStart(b)
        Meta.fixedSizeListAddListSize(b, Int32(N))
        return Meta.FixedSizeList, Meta.fixedSizeListEnd(b), children
    end
end

function juliaeltype(f::Meta.Field, map::Meta.Map, convert)
    K = juliaeltype(
        f.children[1].children[1],
        buildmetadata(f.children[1].children[1]),
        convert,
    )
    V = juliaeltype(
        f.children[1].children[2],
        buildmetadata(f.children[1].children[2]),
        convert,
    )
    return Dict{K,V}
end

function arrowtype(b, x::Map)
    children = [fieldoffset(b, "entries", x.data)]
    Meta.mapStart(b)
    return Meta.Map, Meta.mapEnd(b), children
end

struct KeyValue{K,V}
    key::K
    value::V
end
keyvalueK(::Type{KeyValue{K,V}}) where {K,V} = K
keyvalueV(::Type{KeyValue{K,V}}) where {K,V} = V
Base.length(kv::KeyValue) = 1
Base.iterate(kv::KeyValue, st=1) = st === nothing ? nothing : (kv, nothing)
ArrowTypes.default(::Type{KeyValue{K,V}}) where {K,V} = KeyValue(default(K), default(V))

function arrowtype(b, ::Type{KeyValue{K,V}}) where {K,V}
    children = [fieldoffset(b, "key", K), fieldoffset(b, "value", V)]
    Meta.structStart(b)
    return Meta.Struct, Meta.structEnd(b), children
end

function juliaeltype(f::Meta.Field, list::Meta.Struct, convert)
    names = Tuple(Symbol(x.name) for x in f.children)
    types = Tuple(juliaeltype(x, buildmetadata(x), convert) for x in f.children)
    return allunique(names) ? NamedTuple{names,Tuple{types...}} : Tuple{types...}
end

function arrowtype(b, x::Struct{T,S,fnames}) where {T,S,fnames}
    names = fnames
    children = [fieldoffset(b, names[i], x.data[i]) for i = 1:length(names)]
    Meta.structStart(b)
    return Meta.Struct, Meta.structEnd(b), children
end

# Unions
function UnionT(f::Meta.Field, convert)
    typeids = f.type.typeIds === nothing ? nothing : Tuple(Int(x) for x in f.type.typeIds)
    UT = UnionT{
        f.type.mode,
        typeids,
        Tuple{(juliaeltype(x, buildmetadata(x), convert) for x in f.children)...},
    }
    return UT
end

juliaeltype(f::Meta.Field, u::Meta.Union, convert) =
    Union{(juliaeltype(x, buildmetadata(x), convert) for x in f.children)...}

function arrowtype(
    b,
    x::Union{DenseUnion{S,UnionT{T,typeIds,U}},SparseUnion{S,UnionT{T,typeIds,U}}},
) where {S,T,typeIds,U}
    if typeIds !== nothing
        Meta.unionStartTypeIdsVector(b, length(typeIds))
        for id in Iterators.reverse(typeIds)
            FlatBuffers.prepend!(b, Int32(id))
        end
        TI = FlatBuffers.endvector!(b, length(typeIds))
    end
    children = [fieldoffset(b, "", x.data[i]) for i = 1:fieldcount(U)]
    Meta.unionStart(b)
    Meta.unionAddMode(b, T)
    if typeIds !== nothing
        Meta.unionAddTypeIds(b, TI)
    end
    return Meta.Union, Meta.unionEnd(b), children
end
