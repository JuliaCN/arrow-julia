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

struct Bool8
    value::Bool
end

Bool8(x::Integer) = Bool8(!iszero(x))

Base.Bool(x::Bool8) = getfield(x, :value)
Base.convert(::Type{Bool}, x::Bool8) = Bool(x)
Base.convert(::Type{Int8}, x::Bool8) = Int8(Bool(x))
Base.zero(::Type{Bool8}) = Bool8(false)
Base.:(==)(x::Bool8, y::Bool8) = Bool(x) == Bool(y)
Base.isequal(x::Bool8, y::Bool8) = isequal(Bool(x), Bool(y))

ArrowTypes.ArrowType(::Type{Bool8}) = _builtinarrowtype(Bool8)
ArrowTypes.toarrow(x::Bool8) = _builtintoarrow(x)
ArrowTypes.arrowname(::Type{Bool8}) = _builtinarrowname(Bool8)
ArrowTypes.JuliaType(::Val{BOOL8_SYMBOL}, ::Type{Int8}, metadata::String) =
    _builtinextensionjuliatype(Val(BOOL8_SYMBOL), Int8, metadata)
ArrowTypes.fromarrow(::Type{Bool8}, x::Int8) = _builtinfromarrow(Bool8, x)
ArrowTypes.default(::Type{Bool8}) = _builtindefault(Bool8)

function writearray(
    io::IO,
    ::Type{Int8},
    col::ArrowTypes.ToArrow{Int8,A},
) where {A<:AbstractVector{Bool8}}
    data = ArrowTypes._sourcedata(col)
    strides(data) == (1,) || return _writearrayfallback(io, Int8, col)
    return Base.write(io, reinterpret(Int8, data))
end

struct JSONText{S<:AbstractString}
    value::S
end

Base.String(x::JSONText) = String(getfield(x, :value))
Base.convert(::Type{String}, x::JSONText) = String(x)
Base.:(==)(x::JSONText, y::JSONText) = getfield(x, :value) == getfield(y, :value)
Base.isequal(x::JSONText, y::JSONText) = isequal(getfield(x, :value), getfield(y, :value))

ArrowTypes.ArrowType(::Type{JSONText{S}}) where {S<:AbstractString} =
    _builtinarrowtype(JSONText{S})
ArrowTypes.toarrow(x::JSONText) = _builtintoarrow(x)
ArrowTypes.arrowname(::Type{JSONText{S}}) where {S<:AbstractString} =
    _builtinarrowname(JSONText{S})
ArrowTypes.JuliaType(
    ::Val{JSON_SYMBOL},
    ::Type{S},
    metadata::String,
) where {S<:AbstractString} = _builtinextensionjuliatype(Val(JSON_SYMBOL), S, metadata)
ArrowTypes.fromarrow(::Type{JSONText{String}}, ptr::Ptr{UInt8}, len::Int) =
    _builtinfromarrow(JSONText{String}, ptr, len)
ArrowTypes.fromarrow(::Type{JSONText{S}}, x::S) where {S<:AbstractString} =
    _builtinfromarrow(JSONText{S}, x)
ArrowTypes.default(::Type{JSONText{S}}) where {S<:AbstractString} =
    _builtindefault(JSONText{S})

ArrowTypes.JuliaType(::Val{OPAQUE_SYMBOL}, S, metadata::String) =
    _builtinextensionjuliatype(Val(OPAQUE_SYMBOL), S, metadata)
ArrowTypes.JuliaType(::Val{PARQUET_VARIANT_SYMBOL}, S, metadata::String) =
    _builtinextensionjuliatype(Val(PARQUET_VARIANT_SYMBOL), S, metadata)
ArrowTypes.JuliaType(::Val{FIXED_SHAPE_TENSOR_SYMBOL}, S, metadata::String) =
    _builtinextensionjuliatype(Val(FIXED_SHAPE_TENSOR_SYMBOL), S, metadata)
ArrowTypes.JuliaType(::Val{VARIABLE_SHAPE_TENSOR_SYMBOL}, S, metadata::String) =
    _builtinextensionjuliatype(Val(VARIABLE_SHAPE_TENSOR_SYMBOL), S, metadata)

@inline function _jsonstringliteral(x::AbstractString)
    return '"' * escape_string(x) * '"'
end

opaquemetadata(type_name::AbstractString, vendor_name::AbstractString) =
    _builtinopaquemetadata(type_name, vendor_name)

variantmetadata() = _builtinvariantmetadata()

function fixedshapetensormetadata(
    shape::AbstractVector{<:Integer};
    dim_names::Union{Nothing,AbstractVector{<:AbstractString}}=nothing,
    permutation::Union{Nothing,AbstractVector{<:Integer}}=nothing,
)
    return _builtinfixedshapetensormetadata(
        shape;
        dim_names=dim_names,
        permutation=permutation,
    )
end

function variableshapetensormetadata(;
    uniform_shape::Union{Nothing,AbstractVector}=nothing,
    dim_names::Union{Nothing,AbstractVector{<:AbstractString}}=nothing,
    permutation::Union{Nothing,AbstractVector{<:Integer}}=nothing,
)
    return _builtinvariableshapetensormetadata(;
        uniform_shape=uniform_shape,
        dim_names=dim_names,
        permutation=permutation,
    )
end
