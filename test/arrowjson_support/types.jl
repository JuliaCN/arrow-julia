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

abstract type Type end
Type() = Null("null")
StructTypes.StructType(::Base.Type{Type}) = StructTypes.AbstractType()

children(::Base.Type{T}) where {T} = Field[]

mutable struct Int <: Type
    name::String
    bitWidth::Int64
    isSigned::Base.Bool
end

Int() = Int("", 0, true)
Type(::Base.Type{T}) where {T<:Integer} = Int("int", 8 * sizeof(T), T <: Signed)
StructTypes.StructType(::Base.Type{Int}) = StructTypes.Mutable()
function juliatype(f, x::Int)
    T =
        x.bitWidth == 8 ? Int8 :
        x.bitWidth == 16 ? Int16 :
        x.bitWidth == 32 ? Int32 : x.bitWidth == 64 ? Int64 : Int128
    return x.isSigned ? T : unsigned(T)
end

struct FloatingPoint <: Type
    name::String
    precision::String
end

Type(::Base.Type{T}) where {T<:AbstractFloat} = FloatingPoint(
    "floatingpoint",
    T == Float16 ? "HALF" : T == Float32 ? "SINGLE" : "DOUBLE",
)
StructTypes.StructType(::Base.Type{FloatingPoint}) = StructTypes.Struct()
juliatype(f, x::FloatingPoint) =
    x.precision == "HALF" ? Float16 : x.precision == "SINGLE" ? Float32 : Float64

struct FixedSizeBinary <: Type
    name::String
    byteWidth::Int64
end

Type(::Base.Type{NTuple{N,UInt8}}) where {N} = FixedSizeBinary("fixedsizebinary", N)
children(::Base.Type{NTuple{N,UInt8}}) where {N} = Field[]
StructTypes.StructType(::Base.Type{FixedSizeBinary}) = StructTypes.Struct()
juliatype(f, x::FixedSizeBinary) = NTuple{x.byteWidth,UInt8}

struct Decimal <: Type
    name::String
    precision::Int32
    scale::Int32
    bitWidth::Int32
end

Decimal(name::String, precision::Integer, scale::Integer) =
    Decimal(name, Int32(precision), Int32(scale), Int32(128))

Type(::Base.Type{Arrow.Decimal{P,S,T}}) where {P,S,T} =
    Decimal("decimal", P, S, T === Arrow.Int256 ? Int32(256) : Int32(128))
StructTypes.StructType(::Base.Type{Decimal}) = StructTypes.Struct()
juliatype(f, x::Decimal) =
    Arrow.Decimal{x.precision,x.scale,x.bitWidth == 256 ? Arrow.Int256 : Int128}

mutable struct Timestamp <: Type
    name::String
    unit::String
    timezone::Union{Nothing,String}
end

Timestamp() = Timestamp("", "", nothing)
unit(U) =
    U == Arrow.Meta.TimeUnit.SECOND ? "SECOND" :
    U == Arrow.Meta.TimeUnit.MILLISECOND ? "MILLISECOND" :
    U == Arrow.Meta.TimeUnit.MICROSECOND ? "MICROSECOND" : "NANOSECOND"
Type(::Base.Type{Arrow.Timestamp{U,TZ}}) where {U,TZ} =
    Timestamp("timestamp", unit(U), TZ === nothing ? nothing : String(TZ))
StructTypes.StructType(::Base.Type{Timestamp}) = StructTypes.Mutable()
unitT(u) =
    u == "SECOND" ? Arrow.Meta.TimeUnit.SECOND :
    u == "MILLISECOND" ? Arrow.Meta.TimeUnit.MILLISECOND :
    u == "MICROSECOND" ? Arrow.Meta.TimeUnit.MICROSECOND : Arrow.Meta.TimeUnit.NANOSECOND
juliatype(f, x::Timestamp) =
    Arrow.Timestamp{unitT(x.unit),x.timezone === nothing ? nothing : Symbol(x.timezone)}

struct Duration <: Type
    name::String
    unit::String
end

Type(::Base.Type{Arrow.Duration{U}}) where {U} = Duration("duration", unit(U))
StructTypes.StructType(::Base.Type{Duration}) = StructTypes.Struct()
juliatype(f, x::Duration) = Arrow.Duration{unit % (x.unit)}

struct Date <: Type
    name::String
    unit::String
end

Type(::Base.Type{Arrow.Date{U,T}}) where {U,T} =
    Date("date", U == Arrow.Meta.DateUnit.DAY ? "DAY" : "MILLISECOND")
StructTypes.StructType(::Base.Type{Date}) = StructTypes.Struct()
juliatype(f, x::Date) = Arrow.Date{
    x.unit == "DAY" ? Arrow.Meta.DateUnit.DAY : Arrow.Meta.DateUnit.MILLISECOND,
    x.unit == "DAY" ? Int32 : Int64,
}

struct Time <: Type
    name::String
    unit::String
    bitWidth::Int64
end

Type(::Base.Type{Arrow.Time{U,T}}) where {U,T} = Time("time", unit(U), 8 * sizeof(T))
StructTypes.StructType(::Base.Type{Time}) = StructTypes.Struct()
juliatype(f, x::Time) =
    Arrow.Time{unitT(x.unit),x.unit == "SECOND" || x.unit == "MILLISECOND" ? Int32 : Int64}

struct Interval <: Type
    name::String
    unit::String
end

Type(::Base.Type{Arrow.Interval{U,T}}) where {U,T} = Interval(
    "interval",
    U == Arrow.Meta.IntervalUnit.YEAR_MONTH ? "YEAR_MONTH" :
    U == Arrow.Meta.IntervalUnit.DAY_TIME ? "DAY_TIME" : "MONTH_DAY_NANO",
)
StructTypes.StructType(::Base.Type{Interval}) = StructTypes.Struct()
juliatype(f, x::Interval) = Arrow.Interval{
    x.unit == "YEAR_MONTH" ? Arrow.Meta.IntervalUnit.YEAR_MONTH :
    x.unit == "DAY_TIME" ? Arrow.Meta.IntervalUnit.DAY_TIME :
    Arrow.Meta.IntervalUnit.MONTH_DAY_NANO,
    x.unit == "YEAR_MONTH" ? Int32 :
    x.unit == "DAY_TIME" ? Int64 : Arrow.MonthDayNanoInterval,
}

struct UnionT <: Type
    name::String
    mode::String
    typeIds::Vector{Int64}
end

Type(::Base.Type{Arrow.UnionT{T,typeIds,U}}) where {T,typeIds,U} =
    UnionT("union", T == Arrow.Meta.UnionMode.Dense ? "DENSE" : "SPARSE", collect(typeIds))
children(::Base.Type{Arrow.UnionT{T,typeIds,U}}) where {T,typeIds,U} =
    Field[Field("", fieldtype(U, i), nothing) for i = 1:fieldcount(U)]
StructTypes.StructType(::Base.Type{UnionT}) = StructTypes.Struct()
juliatype(f, x::UnionT) = Union{(juliatype(y) for y in f.children)...}

struct List <: Type
    name::String
end

Type(::Base.Type{Vector{T}}) where {T} = List("list")
children(::Base.Type{Vector{T}}) where {T} = [Field("item", T, nothing)]
StructTypes.StructType(::Base.Type{List}) = StructTypes.Struct()
juliatype(f, x::List) = Vector{juliatype(f.children[1])}

struct LargeList <: Type
    name::String
end

StructTypes.StructType(::Base.Type{LargeList}) = StructTypes.Struct()
juliatype(f, x::LargeList) = Vector{juliatype(f.children[1])}

struct ListView <: Type
    name::String
end

StructTypes.StructType(::Base.Type{ListView}) = StructTypes.Struct()
juliatype(f, x::ListView) = Vector{juliatype(f.children[1])}

struct LargeListView <: Type
    name::String
end

StructTypes.StructType(::Base.Type{LargeListView}) = StructTypes.Struct()
juliatype(f, x::LargeListView) = Vector{juliatype(f.children[1])}

struct FixedSizeList <: Type
    name::String
    listSize::Int64
end

Type(::Base.Type{NTuple{N,T}}) where {N,T} = FixedSizeList("fixedsizelist", N)
children(::Base.Type{NTuple{N,T}}) where {N,T} = [Field("item", T, nothing)]
StructTypes.StructType(::Base.Type{FixedSizeList}) = StructTypes.Struct()
juliatype(f, x::FixedSizeList) = NTuple{x.listSize,juliatype(f.children[1])}

struct Struct <: Type
    name::String
end

Type(::Base.Type{NamedTuple{names,types}}) where {names,types} = Struct("struct")
children(::Base.Type{NamedTuple{names,types}}) where {names,types} =
    [Field(String(names[i]), fieldtype(types, i), nothing) for i = 1:length(names)]
StructTypes.StructType(::Base.Type{Struct}) = StructTypes.Struct()
juliatype(f, x::Struct) = NamedTuple{
    Tuple(Symbol(x.name) for x in f.children),
    Tuple{(juliatype(y) for y in f.children)...},
}

struct Map <: Type
    name::String
    keysSorted::Base.Bool
end

Type(::Base.Type{Dict{K,V}}) where {K,V} = Map("map", false)
children(::Base.Type{Dict{K,V}}) where {K,V} =
    [Field("entries", Arrow.KeyValue{K,V}, nothing)]
StructTypes.StructType(::Base.Type{Map}) = StructTypes.Struct()
juliatype(f, x::Map) =
    Dict{juliatype(f.children[1].children[1]),juliatype(f.children[1].children[2])}

Type(::Base.Type{Arrow.KeyValue{K,V}}) where {K,V} = Struct("struct")
children(::Base.Type{Arrow.KeyValue{K,V}}) where {K,V} =
    [Field("key", K, nothing), Field("value", V, nothing)]

struct Null <: Type
    name::String
end

Type(::Base.Type{Missing}) = Null("null")
StructTypes.StructType(::Base.Type{Null}) = StructTypes.Struct()
juliatype(f, x::Null) = Missing

struct Utf8 <: Type
    name::String
end

Type(::Base.Type{<:String}) = Utf8("utf8")
StructTypes.StructType(::Base.Type{Utf8}) = StructTypes.Struct()
juliatype(f, x::Utf8) = String

struct LargeUtf8 <: Type
    name::String
end

StructTypes.StructType(::Base.Type{LargeUtf8}) = StructTypes.Struct()
juliatype(f, x::LargeUtf8) = String

struct Utf8View <: Type
    name::String
end

StructTypes.StructType(::Base.Type{Utf8View}) = StructTypes.Struct()
juliatype(f, x::Utf8View) = String

struct Binary <: Type
    name::String
end

Type(::Base.Type{<:AbstractVector{UInt8}}) = Binary("binary")
children(::Base.Type{<:AbstractVector{UInt8}}) = Field[]
StructTypes.StructType(::Base.Type{Binary}) = StructTypes.Struct()
juliatype(f, x::Binary) = Vector{UInt8}

struct LargeBinary <: Type
    name::String
end

StructTypes.StructType(::Base.Type{LargeBinary}) = StructTypes.Struct()
juliatype(f, x::LargeBinary) = Vector{UInt8}

struct BinaryView <: Type
    name::String
end

StructTypes.StructType(::Base.Type{BinaryView}) = StructTypes.Struct()
juliatype(f, x::BinaryView) = Vector{UInt8}

struct Bool <: Type
    name::String
end

Type(::Base.Type{Base.Bool}) = Bool("bool")
StructTypes.StructType(::Base.Type{Bool}) = StructTypes.Struct()
juliatype(f, x::Bool) = Base.Bool

struct RunEndEncoded <: Type
    name::String
end

StructTypes.StructType(::Base.Type{RunEndEncoded}) = StructTypes.Struct()
juliatype(f, x::RunEndEncoded) = juliatype(f.children[2])

StructTypes.subtypekey(::Base.Type{Type}) = :name

const SUBTYPES = @eval (
    int=Int,
    floatingpoint=FloatingPoint,
    fixedsizebinary=FixedSizeBinary,
    decimal=Decimal,
    timestamp=Timestamp,
    duration=Duration,
    date=Date,
    time=Time,
    interval=Interval,
    union=UnionT,
    list=List,
    largelist=LargeList,
    listview=ListView,
    largelistview=LargeListView,
    fixedsizelist=FixedSizeList,
    ($(Symbol("struct")))=Struct,
    map=Map,
    null=Null,
    utf8=Utf8,
    largeutf8=LargeUtf8,
    utf8view=Utf8View,
    binary=Binary,
    largebinary=LargeBinary,
    binaryview=BinaryView,
    bool=Bool,
    runendencoded=RunEndEncoded,
)

StructTypes.subtypes(::Base.Type{Type}) = SUBTYPES
