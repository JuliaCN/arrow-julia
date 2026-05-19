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

abstract type ArrowTimeType end
Base.write(io::IO, x::ArrowTimeType) = Base.write(io, x.x)
ArrowTypes.ArrowKind(::Type{<:ArrowTimeType}) = PrimitiveKind()

struct Date{U,T} <: ArrowTimeType
    x::T
end

const DATE = Date{Meta.DateUnit.DAY,Int32}
Base.zero(::Type{Date{U,T}}) where {U,T} = Date{U,T}(T(0))
storagetype(::Type{Date{U,T}}) where {U,T} = T
bitwidth(x::Meta.DateUnit.T) = x == Meta.DateUnit.DAY ? Int32 : Int64
Date{Meta.DateUnit.DAY}(days) = DATE(Int32(days))
Date{Meta.DateUnit.MILLISECOND}(ms) = Date{Meta.DateUnit.MILLISECOND,Int64}(Int64(ms))

juliaeltype(f::Meta.Field, x::Meta.Date, convert) = Date{x.unit,bitwidth(x.unit)}
finaljuliatype(::Type{DATE}) = Dates.Date
Base.convert(::Type{Dates.Date}, x::DATE) =
    Dates.Date(Dates.UTD(Int64(x.x + UNIX_EPOCH_DATE)))
finaljuliatype(::Type{Date{Meta.DateUnit.MILLISECOND,Int64}}) = Dates.DateTime
Base.convert(::Type{Dates.DateTime}, x::Date{Meta.DateUnit.MILLISECOND,Int64}) =
    Dates.DateTime(Dates.UTM(Int64(x.x + UNIX_EPOCH_DATETIME)))

function arrowtype(b, ::Type{Date{U,T}}) where {U,T}
    Meta.dateStart(b)
    Meta.dateAddUnit(b, U)
    return Meta.Date, Meta.dateEnd(b), nothing
end

const UNIX_EPOCH_DATE = Dates.value(Dates.Date(1970))
Base.convert(::Type{DATE}, x::Dates.Date) = DATE(Int32(Dates.value(x) - UNIX_EPOCH_DATE))

const UNIX_EPOCH_DATETIME = Dates.value(Dates.DateTime(1970))
Base.convert(::Type{Date{Meta.DateUnit.MILLISECOND,Int64}}, x::Dates.DateTime) =
    Date{Meta.DateUnit.MILLISECOND,Int64}(Int64(Dates.value(x) - UNIX_EPOCH_DATETIME))

ArrowTypes.ArrowType(::Type{Dates.Date}) = DATE
ArrowTypes.toarrow(x::Dates.Date) = convert(DATE, x)
const DATE_SYMBOL = Symbol("JuliaLang.Date")
ArrowTypes.arrowname(::Type{Dates.Date}) = DATE_SYMBOL
ArrowTypes.JuliaType(::Val{DATE_SYMBOL}, S) = Dates.Date
ArrowTypes.fromarrow(::Type{Dates.Date}, x::DATE) = convert(Dates.Date, x)
ArrowTypes.default(::Type{Dates.Date}) = Dates.Date(1, 1, 1)

struct Time{U,T} <: ArrowTimeType
    x::T
end

Base.zero(::Type{Time{U,T}}) where {U,T} = Time{U,T}(T(0))
const TIME = Time{Meta.TimeUnit.NANOSECOND,Int64}

bitwidth(x::Meta.TimeUnit.T) =
    x == Meta.TimeUnit.SECOND || x == Meta.TimeUnit.MILLISECOND ? Int32 : Int64
Time{U}(x) where {U<:Meta.TimeUnit.T} = Time{U,bitwidth(U)}(bitwidth(U)(x))
storagetype(::Type{Time{U,T}}) where {U,T} = T
juliaeltype(f::Meta.Field, x::Meta.Time, convert) = Time{x.unit,bitwidth(x.unit)}
finaljuliatype(::Type{<:Time}) = Dates.Time
periodtype(U::Meta.TimeUnit.T) =
    U === Meta.TimeUnit.SECOND ? Dates.Second :
    U === Meta.TimeUnit.MILLISECOND ? Dates.Millisecond :
    U === Meta.TimeUnit.MICROSECOND ? Dates.Microsecond : Dates.Nanosecond
Base.convert(::Type{Dates.Time}, x::Time{U,T}) where {U,T} =
    Dates.Time(Dates.Nanosecond(Dates.tons(periodtype(U)(x.x))))

function arrowtype(b, ::Type{Time{U,T}}) where {U,T}
    Meta.timeStart(b)
    Meta.timeAddUnit(b, U)
    Meta.timeAddBitWidth(b, Int32(8 * sizeof(T)))
    return Meta.Time, Meta.timeEnd(b), nothing
end

Base.convert(::Type{TIME}, x::Dates.Time) = TIME(Dates.value(x))

ArrowTypes.ArrowType(::Type{Dates.Time}) = TIME
ArrowTypes.toarrow(x::Dates.Time) = convert(TIME, x)
const TIME_SYMBOL = Symbol("JuliaLang.Time")
ArrowTypes.arrowname(::Type{Dates.Time}) = TIME_SYMBOL
ArrowTypes.JuliaType(::Val{TIME_SYMBOL}, S) = Dates.Time
ArrowTypes.fromarrow(::Type{Dates.Time}, x::Arrow.Time) = convert(Dates.Time, x)
ArrowTypes.default(::Type{Dates.Time}) = Dates.Time(1, 1, 1)

struct Timestamp{U,TZ} <: ArrowTimeType
    x::Int64
end

Base.zero(::Type{Timestamp{U,T}}) where {U,T} = Timestamp{U,T}(Int64(0))

struct TimestampWithOffset{U}
    timestamp::Timestamp{U,:UTC}
    offset_minutes::Int16
end

TimestampWithOffset(timestamp::Timestamp{U,:UTC}, offset_minutes::Integer) where {U} =
    TimestampWithOffset{U}(timestamp, Int16(offset_minutes))

Base.zero(::Type{TimestampWithOffset{U}}) where {U} =
    TimestampWithOffset{U}(zero(Timestamp{U,:UTC}), Int16(0))

function juliaeltype(f::Meta.Field, x::Meta.Timestamp, convert)
    return Timestamp{x.unit,x.timezone === nothing ? nothing : Symbol(x.timezone)}
end

const DATETIME = Timestamp{Meta.TimeUnit.MILLISECOND,nothing}

finaljuliatype(::Type{Timestamp{U,TZ}}) where {U,TZ} = ZonedDateTime
finaljuliatype(::Type{Timestamp{U,nothing}}) where {U} = DateTime

@noinline warntimestamp(U, T) =
    @warn "automatically converting Arrow.Timestamp with precision = $U to `$T` which only supports millisecond precision; conversion may be lossy; to avoid converting, pass `Arrow.Table(source; convert=false)" maxlog =
        1 _id = hash((:warntimestamp, U, T))

function Base.convert(::Type{ZonedDateTime}, x::Timestamp{U,TZ}) where {U,TZ}
    (U === Meta.TimeUnit.MICROSECOND || U == Meta.TimeUnit.NANOSECOND) &&
        warntimestamp(U, ZonedDateTime)
    return ZonedDateTime(
        Dates.DateTime(
            Dates.UTM(Int64(Dates.toms(periodtype(U)(x.x)) + UNIX_EPOCH_DATETIME)),
        ),
        TimeZone(String(TZ));
        from_utc=true,
    )
end

function Base.convert(::Type{DateTime}, x::Timestamp{U,nothing}) where {U}
    (U === Meta.TimeUnit.MICROSECOND || U == Meta.TimeUnit.NANOSECOND) &&
        warntimestamp(U, DateTime)
    return Dates.DateTime(
        Dates.UTM(Int64(Dates.toms(periodtype(U)(x.x)) + UNIX_EPOCH_DATETIME)),
    )
end

Base.convert(::Type{Timestamp{Meta.TimeUnit.MILLISECOND,TZ}}, x::ZonedDateTime) where {TZ} =
    Timestamp{Meta.TimeUnit.MILLISECOND,TZ}(
        Int64(Dates.value(DateTime(x, UTC)) - UNIX_EPOCH_DATETIME),
    )
Base.convert(::Type{Timestamp{Meta.TimeUnit.MILLISECOND,nothing}}, x::DateTime) =
    Timestamp{Meta.TimeUnit.MILLISECOND,nothing}(
        Int64(Dates.value(x) - UNIX_EPOCH_DATETIME),
    )

function arrowtype(b, ::Type{Timestamp{U,TZ}}) where {U,TZ}
    tz = TZ !== nothing ? FlatBuffers.createstring!(b, String(TZ)) : FlatBuffers.UOffsetT(0)
    Meta.timestampStart(b)
    Meta.timestampAddUnit(b, U)
    Meta.timestampAddTimezone(b, tz)
    return Meta.Timestamp, Meta.timestampEnd(b), nothing
end

ArrowTypes.ArrowType(::Type{Dates.DateTime}) = DATETIME
ArrowTypes.toarrow(x::Dates.DateTime) = convert(DATETIME, x)
const DATETIME_SYMBOL = Symbol("JuliaLang.DateTime")
ArrowTypes.arrowname(::Type{Dates.DateTime}) = DATETIME_SYMBOL
ArrowTypes.JuliaType(::Val{DATETIME_SYMBOL}, S) = Dates.DateTime
ArrowTypes.fromarrow(::Type{Dates.DateTime}, x::Timestamp) = convert(Dates.DateTime, x)
ArrowTypes.fromarrow(::Type{Dates.DateTime}, x::Date{Meta.DateUnit.MILLISECOND,Int64}) =
    convert(Dates.DateTime, x)
ArrowTypes.default(::Type{Dates.DateTime}) = Dates.DateTime(1, 1, 1, 1, 1, 1)

ArrowTypes.ArrowType(::Type{ZonedDateTime}) = _builtinarrowtype(ZonedDateTime)
ArrowTypes.toarrow(x::ZonedDateTime) = _builtintoarrow(x)
const ZONEDDATETIME_SYMBOL = Symbol("JuliaLang.ZonedDateTime-UTC")
ArrowTypes.arrowname(::Type{ZonedDateTime}) = _builtinarrowname(ZonedDateTime)
ArrowTypes.JuliaType(::Val{ZONEDDATETIME_SYMBOL}, S) =
    _builtinextensionjuliatype(Val(ZONEDDATETIME_SYMBOL), S)
ArrowTypes.fromarrow(::Type{ZonedDateTime}, x::Timestamp) =
    _builtinfromarrow(ZonedDateTime, x)
ArrowTypes.default(::Type{TimeZones.ZonedDateTime}) = _builtindefault(ZonedDateTime)

const TIMESTAMP_WITH_OFFSET_SYMBOL = Symbol("arrow.timestamp_with_offset")
ArrowTypes.ArrowType(::Type{TimestampWithOffset{U}}) where {U} =
    _builtinarrowtype(TimestampWithOffset{U})
ArrowTypes.toarrow(x::TimestampWithOffset{U}) where {U} = _builtintoarrow(x)
ArrowTypes.arrowname(::Type{TimestampWithOffset{U}}) where {U} =
    _builtinarrowname(TimestampWithOffset{U})
ArrowTypes.JuliaType(
    ::Val{TIMESTAMP_WITH_OFFSET_SYMBOL},
    ::Type{NamedTuple{(:timestamp, :offset_minutes),Tuple{Timestamp{U,:UTC},Int16}}},
    metadata::String,
) where {U} = _builtinextensionjuliatype(
    Val(TIMESTAMP_WITH_OFFSET_SYMBOL),
    NamedTuple{(:timestamp, :offset_minutes),Tuple{Timestamp{U,:UTC},Int16}},
    metadata,
)
ArrowTypes.default(::Type{TimestampWithOffset{U}}) where {U} =
    _builtindefault(TimestampWithOffset{U})
ArrowTypes.fromarrowstruct(
    ::Type{TimestampWithOffset{U}},
    ::Val{(:timestamp, :offset_minutes)},
    timestamp::Timestamp{U,:UTC},
    offset_minutes::Int16,
) where {U} = _builtinfromarrowstruct(
    TimestampWithOffset{U},
    Val((:timestamp, :offset_minutes)),
    timestamp,
    offset_minutes,
)
ArrowTypes.fromarrowstruct(
    ::Type{TimestampWithOffset{U}},
    ::Val{(:offset_minutes, :timestamp)},
    offset_minutes::Int16,
    timestamp::Timestamp{U,:UTC},
) where {U} = _builtinfromarrowstruct(
    TimestampWithOffset{U},
    Val((:offset_minutes, :timestamp)),
    offset_minutes,
    timestamp,
)

# Backwards compatibility: older versions of Arrow saved ZonedDateTime's with this metdata:
const OLD_ZONEDDATETIME_SYMBOL = Symbol("JuliaLang.ZonedDateTime")
# and stored the local time instead of the UTC time.
struct LocalZonedDateTime end
ArrowTypes.JuliaType(::Val{OLD_ZONEDDATETIME_SYMBOL}, S) =
    _builtinextensionjuliatype(Val(OLD_ZONEDDATETIME_SYMBOL), S)
ArrowTypes.fromarrow(::Type{LocalZonedDateTime}, x::Timestamp{U,TZ}) where {U,TZ} =
    _builtinfromarrow(LocalZonedDateTime, x)

"""
    Arrow.ToTimestamp(x::AbstractVector{ZonedDateTime})

Wrapper array that provides a more efficient encoding of `ZonedDateTime` elements to the arrow format. In the arrow format,
timestamp columns with timezone information are encoded as the arrow equivalent of a Julia type parameter, meaning an entire column
_should_ have elements all with the same timezone. If a `ZonedDateTime` column is passed to `Arrow.write`, for correctness, it must
scan each element to check each timezone. `Arrow.ToTimestamp` provides a "bypass" of this process by encoding the timezone of the
first element of the `AbstractVector{ZonedDateTime}`, which in turn allows `Arrow.write` to avoid costly checking/conversion and
can encode the `ZonedDateTime` as `Arrow.Timestamp` directly.
"""
struct ToTimestamp{A,TZ} <: AbstractVector{Timestamp{Meta.TimeUnit.MILLISECOND,TZ}}
    data::A # AbstractVector{ZonedDateTime}
end

ToTimestamp(x::A) where {A<:AbstractVector{ZonedDateTime}} =
    ToTimestamp{A,Symbol(x[1].timezone)}(x)
Base.IndexStyle(::Type{<:ToTimestamp}) = Base.IndexLinear()
Base.size(x::ToTimestamp) = (length(x.data),)
Base.eltype(::Type{ToTimestamp{A,TZ}}) where {A,TZ} =
    Timestamp{Meta.TimeUnit.MILLISECOND,TZ}
Base.getindex(x::ToTimestamp{A,TZ}, i::Integer) where {A,TZ} =
    convert(Timestamp{Meta.TimeUnit.MILLISECOND,TZ}, getindex(x.data, i))

struct MonthDayNanoInterval
    months::Int32
    days::Int32
    nanoseconds::Int64
end

MonthDayNanoInterval(months::Integer, days::Integer, nanoseconds::Integer) =
    MonthDayNanoInterval(Int32(months), Int32(days), Int64(nanoseconds))

Base.zero(::Type{MonthDayNanoInterval}) = MonthDayNanoInterval(0, 0, 0)
==(a::MonthDayNanoInterval, b::MonthDayNanoInterval) =
    a.months == b.months && a.days == b.days && a.nanoseconds == b.nanoseconds
Base.isequal(a::MonthDayNanoInterval, b::MonthDayNanoInterval) =
    isequal(a.months, b.months) &&
    isequal(a.days, b.days) &&
    isequal(a.nanoseconds, b.nanoseconds)

function Base.write(io::IO, x::MonthDayNanoInterval)
    written = Base.write(io, x.months)
    written += Base.write(io, x.days)
    written += Base.write(io, x.nanoseconds)
    return written
end

struct Interval{U,T} <: ArrowTimeType
    x::T
end

Base.zero(::Type{Interval{U,T}}) where {U,T} = Interval{U,T}(zero(T))

function bitwidth(x::Meta.IntervalUnit.T)
    x == Meta.IntervalUnit.YEAR_MONTH && return Int32
    x == Meta.IntervalUnit.DAY_TIME && return Int64
    x == Meta.IntervalUnit.MONTH_DAY_NANO && return MonthDayNanoInterval
    throw(ArgumentError("Arrow interval unit $x is not supported"))
end

Interval{Meta.IntervalUnit.YEAR_MONTH}(x) =
    Interval{Meta.IntervalUnit.YEAR_MONTH,Int32}(Int32(x))
Interval{Meta.IntervalUnit.DAY_TIME}(x) =
    Interval{Meta.IntervalUnit.DAY_TIME,Int64}(Int64(x))
Interval{Meta.IntervalUnit.MONTH_DAY_NANO}(x::MonthDayNanoInterval) =
    Interval{Meta.IntervalUnit.MONTH_DAY_NANO,MonthDayNanoInterval}(x)
Interval{Meta.IntervalUnit.MONTH_DAY_NANO}(months, days, nanoseconds) =
    Interval{Meta.IntervalUnit.MONTH_DAY_NANO}(
        MonthDayNanoInterval(months, days, nanoseconds),
    )

function juliaeltype(f::Meta.Field, x::Meta.Interval, convert)
    return Interval{x.unit,bitwidth(x.unit)}
end

function juliaeltype(f::Meta.Field, x::Meta.RunEndEncoded, convert)
    return juliaeltype(f.children[2], buildmetadata(f.children[2]), convert)
end

function arrowtype(b, x::RunEndEncoded)
    children = [fieldoffset(b, "run_ends", x.run_ends), fieldoffset(b, "values", x.values)]
    Meta.runEndEncodedStart(b)
    return Meta.RunEndEncoded, Meta.runEndEncodedEnd(b), children
end

function arrowtype(b, ::Type{Interval{U,T}}) where {U,T}
    Meta.intervalStart(b)
    Meta.intervalAddUnit(b, U)
    return Meta.Interval, Meta.intervalEnd(b), nothing
end

struct Duration{U} <: ArrowTimeType
    x::Int64
end

Base.zero(::Type{Duration{U}}) where {U} = Duration{U}(Int64(0))

function juliaeltype(f::Meta.Field, x::Meta.Duration, convert)
    return Duration{x.unit}
end

finaljuliatype(::Type{Duration{U}}) where {U} = periodtype(U)
Base.convert(::Type{P}, x::Duration{U}) where {P<:Dates.Period,U} = P(periodtype(U)(x.x))

function arrowtype(b, ::Type{Duration{U}}) where {U}
    Meta.durationStart(b)
    Meta.durationAddUnit(b, U)
    return Meta.Duration, Meta.durationEnd(b), nothing
end

arrowtype(b, ::Type{P}) where {P<:Dates.Period} = arrowtype(b, Duration{arrowperiodtype(P)})

arrowperiodtype(P) = Meta.TimeUnit.SECOND
arrowperiodtype(::Type{Dates.Millisecond}) = Meta.TimeUnit.MILLISECOND
arrowperiodtype(::Type{Dates.Microsecond}) = Meta.TimeUnit.MICROSECOND
arrowperiodtype(::Type{Dates.Nanosecond}) = Meta.TimeUnit.NANOSECOND

Base.convert(::Type{Duration{U}}, x::Dates.Period) where {U} =
    Duration{U}(Dates.value(periodtype(U)(x)))

ArrowTypes.ArrowType(::Type{P}) where {P<:Dates.Period} = Duration{arrowperiodtype(P)}
ArrowTypes.toarrow(x::P) where {P<:Dates.Period} = convert(Duration{arrowperiodtype(P)}, x)
const PERIOD_SYMBOL = Symbol("JuliaLang.Dates.Period")
ArrowTypes.arrowname(::Type{P}) where {P<:Dates.Period} = PERIOD_SYMBOL
ArrowTypes.JuliaType(::Val{PERIOD_SYMBOL}, ::Type{Duration{U}}) where {U} = periodtype(U)
ArrowTypes.fromarrow(::Type{P}, x::Duration{U}) where {P<:Dates.Period,U} = convert(P, x)
