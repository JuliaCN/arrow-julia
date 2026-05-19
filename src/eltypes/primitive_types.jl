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

# primitive types
function juliaeltype(f::Meta.Field, fp::Meta.FloatingPoint, convert)
    if fp.precision == Meta.Precision.HALF
        Float16
    elseif fp.precision == Meta.Precision.SINGLE
        Float32
    elseif fp.precision == Meta.Precision.DOUBLE
        Float64
    end
end

function arrowtype(b, ::Type{T}) where {T<:AbstractFloat}
    Meta.floatingPointStart(b)
    Meta.floatingPointAddPrecision(
        b,
        T === Float16 ? Meta.Precision.HALF :
        T === Float32 ? Meta.Precision.SINGLE : Meta.Precision.DOUBLE,
    )
    return Meta.FloatingPoint, Meta.floatingPointEnd(b), nothing
end

juliaeltype(f::Meta.Field, b::Union{Meta.Utf8,Meta.LargeUtf8,Meta.Utf8View}, convert) =
    String

datasizeof(x) = sizeof(x)
datasizeof(x::AbstractVector) = sum(datasizeof, x)

juliaeltype(
    f::Meta.Field,
    b::Union{Meta.Binary,Meta.LargeBinary,Meta.BinaryView},
    convert,
) = Base.CodeUnits

function arrowtype(b, x::View{T}) where {T}
    S = Base.nonmissingtype(T)
    if S <: Base.CodeUnits
        Meta.binaryViewStart(b)
        return Meta.BinaryView, Meta.binaryViewEnd(b), nothing
    else
        Meta.utf8ViewStart(b)
        return Meta.Utf8View, Meta.utf8ViewEnd(b), nothing
    end
end

juliaeltype(f::Meta.Field, x::Meta.FixedSizeBinary, convert) =
    NTuple{Int(x.byteWidth),UInt8}

# arggh!
Base.write(io::IO, x::NTuple{N,T}) where {N,T} = sum(y -> Base.write(io, y), x)

juliaeltype(f::Meta.Field, x::Meta.Bool, convert) = Bool

function arrowtype(b, ::Type{Bool})
    Meta.boolStart(b)
    return Meta.Bool, Meta.boolEnd(b), nothing
end

struct Decimal{P,S,T}
    value::T # only Int128 or Int256
end

Base.zero(::Type{Decimal{P,S,T}}) where {P,S,T} = Decimal{P,S,T}(T(0))
==(a::Decimal{P,S,T}, b::Decimal{P,S,T}) where {P,S,T} = ==(a.value, b.value)
Base.isequal(a::Decimal{P,S,T}, b::Decimal{P,S,T}) where {P,S,T} = isequal(a.value, b.value)

function juliaeltype(f::Meta.Field, x::Meta.Decimal, convert)
    return Decimal{x.precision,x.scale,x.bitWidth == 256 ? Int256 : Int128}
end

ArrowTypes.ArrowKind(::Type{<:Decimal}) = PrimitiveKind()

function arrowtype(b, ::Type{Decimal{P,S,T}}) where {P,S,T}
    Meta.decimalStart(b)
    Meta.decimalAddPrecision(b, Int32(P))
    Meta.decimalAddScale(b, Int32(S))
    Meta.decimalAddBitWidth(b, Int32(T == Int256 ? 256 : 128))
    return Meta.Decimal, Meta.decimalEnd(b), nothing
end

Base.write(io::IO, x::Decimal) = Base.write(io, x.value)
