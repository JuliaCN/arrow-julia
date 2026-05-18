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

const Metadata = Union{Nothing,Vector{NamedTuple{(:key, :value),Tuple{String,String}}}}
Metadata() = nothing

function _metadata_pairs(meta)
    meta === nothing && return nothing
    entries = NamedTuple{(:key, :value),Tuple{String,String}}[]
    for (key, value) in pairs(meta)
        push!(entries, (key=String(key), value=String(value)))
    end
    return isempty(entries) ? nothing : entries
end

function _metadata_pairs(meta::AbstractVector{<:NamedTuple})
    isempty(meta) && return nothing
    return NamedTuple{(:key, :value),Tuple{String,String}}[
        (key=String(entry.key), value=String(entry.value)) for entry in meta
    ]
end

function _metadata_dict(meta::Metadata)
    meta === nothing && return nothing
    isempty(meta) && return nothing
    dict = Dict{String,String}()
    for entry in meta
        dict[String(entry.key)] = String(entry.value)
    end
    return dict
end

function _metadata_dict(meta)
    meta === nothing && return nothing
    dict = Dict{String,String}()
    for (key, value) in pairs(meta)
        dict[String(key)] = String(value)
    end
    return isempty(dict) ? nothing : dict
end

function _metadata_equal(expected::Metadata, actual)
    return _metadata_dict(expected) == _metadata_dict(actual)
end

function _validity_bitmap(fielddata)
    fielddata.VALIDITY === nothing &&
        return Arrow.ValidityBitmap(fill(true, fielddata.count))
    validity = Union{Missing,Base.Bool}[
        value == 0 ? missing : true for value in fielddata.VALIDITY
    ]
    return Arrow.ValidityBitmap(validity)
end

mutable struct DictEncoding
    id::Int64
    indexType::Type
    isOrdered::Base.Bool
end

DictEncoding() = DictEncoding(0, Type(), false)
StructTypes.StructType(::Base.Type{DictEncoding}) = StructTypes.Mutable()

mutable struct Field
    name::String
    nullable::Base.Bool
    type::Type
    children::Vector{Field}
    dictionary::Union{DictEncoding,Nothing}
    metadata::Metadata
end

Field() = Field("", true, Type(), Field[], nothing, Metadata())
StructTypes.StructType(::Base.Type{Field}) = StructTypes.Mutable()
Base.copy(f::Field) =
    Field(f.name, f.nullable, f.type, f.children, f.dictionary, f.metadata)

function juliatype(f::Field)
    T = juliatype(f, f.type)
    return f.nullable ? Union{T,Missing} : T
end

function Field(nm, ::Base.Type{T}, dictencodings) where {T}
    S = Arrow.maybemissing(T)
    type = Type(S)
    ch = children(S)
    if dictencodings !== nothing && haskey(dictencodings, nm)
        dict = dictencodings[nm]
    else
        dict = nothing
    end
    return Field(nm, T !== S, type, ch, dict, nothing)
end

mutable struct Schema
    fields::Vector{Field}
    metadata::Metadata
end

Schema() = Schema(Field[], Metadata())
StructTypes.StructType(::Base.Type{Schema}) = StructTypes.Mutable()

struct Offsets{T} <: AbstractVector{T}
    data::Vector{T}
end

Base.size(x::Offsets) = size(x.data)
Base.getindex(x::Offsets, i::Base.Int) = getindex(x.data, i)

mutable struct ViewData
    SIZE::Int64
    INLINED::Union{Nothing,String}
    PREFIX_HEX::Union{Nothing,String}
    BUFFER_INDEX::Union{Nothing,Int64}
    OFFSET::Union{Nothing,Int64}
end

ViewData() = ViewData(0, nothing, nothing, nothing, nothing)
StructTypes.StructType(::Base.Type{ViewData}) = StructTypes.Mutable()

mutable struct FieldData
    name::String
    count::Int64
    VALIDITY::Union{Nothing,Vector{Int8}}
    OFFSET::Union{Nothing,Offsets}
    TYPE_ID::Union{Nothing,Vector{Int8}}
    SIZE::Union{Nothing,Offsets}
    VIEWS::Union{Nothing,Vector{ViewData}}
    VARIADIC_DATA_BUFFERS::Union{Nothing,Vector{String}}
    DATA::Union{Nothing,Vector{Any}}
    children::Vector{FieldData}
end

FieldData() = FieldData(
    "",
    0,
    nothing,
    nothing,
    nothing,
    nothing,
    nothing,
    nothing,
    nothing,
    FieldData[],
)
StructTypes.StructType(::Base.Type{FieldData}) = StructTypes.Mutable()

function FieldData(nm, ::Base.Type{T}, col, dictencodings) where {T}
    if dictencodings !== nothing && haskey(dictencodings, nm)
        refvals = DataAPI.refarray(col.data)
        if refvals !== col.data
            IT = eltype(refvals)
            col = (x - one(T) for x in refvals)
        else
            _, de = dictencodings[nm]
            IT = de.indexType
            vals = unique(col)
            col = Arrow.DictEncoder(col, vals, Arrow.encodingtype(length(vals)))
        end
        return FieldData(nm, IT, col, nothing)
    end
    S = Arrow.maybemissing(T)
    len = length(col)
    VALIDITY = OFFSET = TYPE_ID = SIZE = VIEWS = VARIADIC_DATA_BUFFERS = DATA = nothing
    children = FieldData[]
    if S <: Pair
        return FieldData(
            nm,
            Vector{Arrow.KeyValue{Arrow._keytype(S),Arrow._valtype(S)}},
            (Arrow.KeyValue(k, v) for (k, v) in pairs(col)),
        )
    elseif S !== Missing
        # VALIDITY
        VALIDITY = Int8[!ismissing(x) for x in col]
        # OFFSET
        if S <: AbstractString
            DATA = [ismissing(x) ? "" : String(x) for x in col]
        elseif S <: AbstractVector{UInt8}
            DATA = [ismissing(x) ? "" : _hexstring(x) for x in col]
        elseif S <: Vector
            lenfun = x -> ismissing(x) ? 0 : length(x)
            tot = sum(lenfun, col)
            if tot > 2147483647
                OFFSET = String[String(lenfun(x)) for x in col]
                pushfirst!(OFFSET, "0")
            else
                OFFSET = Int32[ismissing(x) ? 0 : lenfun(x) for x in col]
                pushfirst!(OFFSET, 0)
            end
            OFFSET = Offsets(OFFSET)
            push!(
                children,
                FieldData(
                    "item",
                    eltype(S),
                    Arrow.flatten(skipmissing(col)),
                    dictencodings,
                ),
            )
        elseif S <: NTuple
            if Arrow.ArrowTypes.gettype(Arrow.ArrowTypes.ArrowKind(S)) == UInt8
                DATA = [
                    ismissing(x) ? _hexstring(Arrow.ArrowTypes.default(S)) : _hexstring(x) for x in col
                ]
            else
                push!(
                    children,
                    FieldData(
                        "item",
                        Arrow.ArrowTypes.gettype(Arrow.ArrowTypes.ArrowKind(S)),
                        Arrow.flatten(
                            coalesce(x, Arrow.ArrowTypes.default(S)) for x in col
                        ),
                        dictencodings,
                    ),
                )
            end
        elseif S <: NamedTuple
            for (nm, typ) in zip(fieldnames(S), fieldtypes(S))
                push!(
                    children,
                    FieldData(
                        String(nm),
                        typ,
                        (getfield(x, nm) for x in col),
                        dictencodings,
                    ),
                )
            end
        elseif S <: Arrow.UnionT
            U = eltype(S)
            tids = Arrow.typeids(S) === nothing ? (0:fieldcount(U)) : Arrow.typeids(S)
            TYPE_ID = [x === missing ? 0 : tids[Arrow.isatypeid(x, U)] for x in col]
            if Arrow.unionmode(S) == Arrow.Meta.UnionMode.Dense
                offs = zeros(Int32, fieldcount(U))
                OFFSET = Int32[]
                for x in col
                    idx = x === missing ? 1 : Arrow.isatypeid(x, U)
                    push!(OFFSET, offs[idx])
                    offs[idx] += 1
                end
                for i = 1:fieldcount(U)
                    SS = fieldtype(U, i)
                    push!(
                        children,
                        FieldData(
                            "$i",
                            SS,
                            Arrow.filtered(
                                i == 1 ? Union{SS,Missing} : Arrow.maybemissing(SS),
                                col,
                            ),
                            dictencodings,
                        ),
                    )
                end
            else
                for i = 1:fieldcount(U)
                    SS = fieldtype(U, i)
                    push!(
                        children,
                        FieldData("$i", SS, Arrow.replaced(SS, col), dictencodings),
                    )
                end
            end
        elseif S <: Arrow.KeyValue
            push!(
                children,
                FieldData("key", Arrow.keyvalueK(S), (x.key for x in col), dictencodings),
            )
            push!(
                children,
                FieldData(
                    "value",
                    Arrow.keyvalueV(S),
                    (x.value for x in col),
                    dictencodings,
                ),
            )
        end
    end
    return FieldData(
        nm,
        len,
        VALIDITY,
        OFFSET,
        TYPE_ID,
        SIZE,
        VIEWS,
        VARIADIC_DATA_BUFFERS,
        DATA,
        children,
    )
end
