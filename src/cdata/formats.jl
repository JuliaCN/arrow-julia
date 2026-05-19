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

function _nullable_flags(::Type{T}) where {T}
    return T >: Missing ? ARROW_FLAG_NULLABLE : Int64(0)
end

function _format_for_storage_type(::Type{T}) where {T}
    S = Base.nonmissingtype(T)
    S === Int8 && return "c"
    S === UInt8 && return "C"
    S === Int16 && return "s"
    S === UInt16 && return "S"
    S === Int32 && return "i"
    S === UInt32 && return "I"
    S === Int64 && return "l"
    S === UInt64 && return "L"
    S === Float16 && return "e"
    S === Float32 && return "f"
    S === Float64 && return "g"
    throw(ArgumentError("Arrow C Data export does not support storage type $S"))
end

function _time_unit_code(unit)
    unit == Meta.TimeUnit.SECOND && return "s"
    unit == Meta.TimeUnit.MILLISECOND && return "m"
    unit == Meta.TimeUnit.MICROSECOND && return "u"
    unit == Meta.TimeUnit.NANOSECOND && return "n"
    throw(ArgumentError("Arrow C Data does not support time unit $unit"))
end

function _time_unit_for_code(code::AbstractString)
    code == "s" && return Meta.TimeUnit.SECOND
    code == "m" && return Meta.TimeUnit.MILLISECOND
    code == "u" && return Meta.TimeUnit.MICROSECOND
    code == "n" && return Meta.TimeUnit.NANOSECOND
    throw(ArgumentError("Arrow C Data import does not support time unit code $code"))
end

function _format_for_storage_type(::Type{Decimal{P,S,T}}) where {P,S,T}
    T === Int128 && return "d:$(Int(P)),$(Int(S))"
    T === Int256 && return "d:$(Int(P)),$(Int(S)),256"
    throw(ArgumentError("Arrow C Data export does not support decimal storage type $T"))
end

function _format_for_storage_type(::Type{Date{U,T}}) where {U,T}
    U == Meta.DateUnit.DAY && T === Int32 && return "tdD"
    U == Meta.DateUnit.MILLISECOND && T === Int64 && return "tdm"
    throw(
        ArgumentError("Arrow C Data export does not support date storage type Date{$U,$T}"),
    )
end

function _format_for_storage_type(::Type{Time{U,T}}) where {U,T}
    code = _time_unit_code(U)
    expected = (U == Meta.TimeUnit.SECOND || U == Meta.TimeUnit.MILLISECOND) ? Int32 : Int64
    T === expected || throw(
        ArgumentError("Arrow C Data export does not support time storage type Time{$U,$T}"),
    )
    return "tt$code"
end

function _format_for_storage_type(::Type{Timestamp{U,TZ}}) where {U,TZ}
    return "ts$(_time_unit_code(U)):$(TZ === nothing ? "" : String(TZ))"
end

function _format_for_storage_type(::Type{Duration{U}}) where {U}
    return "tD$(_time_unit_code(U))"
end

function _format_for_storage_type(::Type{Interval{U,T}}) where {U,T}
    U == Meta.IntervalUnit.YEAR_MONTH && T === Int32 && return "tiM"
    U == Meta.IntervalUnit.DAY_TIME && T === Int64 && return "tiD"
    if U == Meta.IntervalUnit.MONTH_DAY_NANO && T === MonthDayNanoInterval
        return "tin"
    end
    throw(
        ArgumentError(
            "Arrow C Data export does not support interval storage type Interval{$U,$T}",
        ),
    )
end

function _decimal_type_for_format(format::AbstractString)
    parts = split(format[3:end], ",")
    length(parts) == 2 ||
        length(parts) == 3 ||
        throw(ArgumentError("Arrow C Data import has invalid decimal format $format"))
    precision = tryparse(Int32, parts[1])
    scale = tryparse(Int32, parts[2])
    precision === nothing &&
        throw(ArgumentError("Arrow C Data import has invalid decimal precision in $format"))
    scale === nothing &&
        throw(ArgumentError("Arrow C Data import has invalid decimal scale in $format"))
    bitwidth = if length(parts) == 2
        128
    else
        parsed = tryparse(Int, parts[3])
        parsed === nothing && throw(
            ArgumentError("Arrow C Data import has invalid decimal bitwidth in $format"),
        )
        parsed
    end
    bitwidth == 128 && return Decimal{precision,scale,Int128}
    bitwidth == 256 && return Decimal{precision,scale,Int256}
    throw(ArgumentError("Arrow C Data import does not support decimal bitwidth $bitwidth"))
end

function _timestamp_type_for_format(format::AbstractString)
    length(format) >= 4 && format[1:2] == "ts" && format[4:4] == ":" ||
        throw(ArgumentError("Arrow C Data import has invalid timestamp format $format"))
    unit = _time_unit_for_code(format[3:3])
    timezone = format[5:end]
    TZ = isempty(timezone) ? nothing : Symbol(timezone)
    return Timestamp{unit,TZ}
end

function _storage_type_for_format(format::AbstractString)
    format == "c" && return Int8
    format == "C" && return UInt8
    format == "s" && return Int16
    format == "S" && return UInt16
    format == "i" && return Int32
    format == "I" && return UInt32
    format == "l" && return Int64
    format == "L" && return UInt64
    format == "e" && return Float16
    format == "f" && return Float32
    format == "g" && return Float64
    format == "tdD" && return Date{Meta.DateUnit.DAY,Int32}
    format == "tdm" && return Date{Meta.DateUnit.MILLISECOND,Int64}
    format == "tts" && return Time{Meta.TimeUnit.SECOND,Int32}
    format == "ttm" && return Time{Meta.TimeUnit.MILLISECOND,Int32}
    format == "ttu" && return Time{Meta.TimeUnit.MICROSECOND,Int64}
    format == "ttn" && return Time{Meta.TimeUnit.NANOSECOND,Int64}
    startswith(format, "ts") && return _timestamp_type_for_format(format)
    format == "tDs" && return Duration{Meta.TimeUnit.SECOND}
    format == "tDm" && return Duration{Meta.TimeUnit.MILLISECOND}
    format == "tDu" && return Duration{Meta.TimeUnit.MICROSECOND}
    format == "tDn" && return Duration{Meta.TimeUnit.NANOSECOND}
    format == "tiM" && return Interval{Meta.IntervalUnit.YEAR_MONTH,Int32}
    format == "tiD" && return Interval{Meta.IntervalUnit.DAY_TIME,Int64}
    if format == "tin"
        return Interval{Meta.IntervalUnit.MONTH_DAY_NANO,MonthDayNanoInterval}
    end
    startswith(format, "d:") && return _decimal_type_for_format(format)
    throw(ArgumentError("Arrow C Data import does not support format $format"))
end

function _list_offsets_type(::List{T,O,A}) where {T,O,A}
    return O
end

function _list_view_offsets_type(::ListView{T,O,A}) where {T,O,A}
    return O
end

function _map_offsets_type(::Map{T,O,A}) where {T,O,A}
    return O
end

function _offset_width_format(column::List, normal::AbstractString, large::AbstractString)
    O = _list_offsets_type(column)
    O === Int32 && return normal
    O === Int64 && return large
    throw(ArgumentError("Arrow C Data export does not support list offset type $O"))
end

function _list_view_format(column::ListView)
    O = _list_view_offsets_type(column)
    O === Int32 && return "+vl"
    O === Int64 && return "+vL"
    throw(ArgumentError("Arrow C Data export does not support list-view offset type $O"))
end

function _assert_standard_map_offsets(column::Map)
    O = _map_offsets_type(column)
    O === Int32 && return nothing
    throw(ArgumentError("Arrow C Data export does not support map offset type $O"))
end

function _assert_map_offsets_shape(column::Map)
    offsets = getfield(getfield(column, :offsets), :offsets)
    length(offsets) == length(column) + 1 && return nothing
    throw(
        ArgumentError("Arrow C Data export expected map offsets length to match row count"),
    )
end

function _union_type_ids_for_parameters(::Type{UnionT{M,typeIds,U}}) where {M,typeIds,U}
    ids = typeIds === nothing ? collect(0:(fieldcount(U) - 1)) : collect(typeIds)
    all(id -> 0 <= id <= typemax(UInt8), ids) ||
        throw(ArgumentError("Arrow C Data export only supports UInt8 union type ids"))
    length(unique(ids)) == length(ids) ||
        throw(ArgumentError("Arrow C Data export requires unique union type ids"))
    return Int.(ids)
end

_union_type_ids(::DenseUnion{T,U,S}) where {T,U<:UnionT,S} =
    _union_type_ids_for_parameters(U)
_union_type_ids(::SparseUnion{T,U,S}) where {T,U<:UnionT,S} =
    _union_type_ids_for_parameters(U)
_union_format(column::DenseUnion) = "+ud:" * join(_union_type_ids(column), ",")
_union_format(column::SparseUnion) = "+us:" * join(_union_type_ids(column), ",")

function _view_format(column::View)
    S = Base.nonmissingtype(eltype(column))
    S <: Base.CodeUnits && return "vz"
    S <: AbstractString && return "vu"
    throw(ArgumentError("Arrow C Data export does not yet support list-view columns"))
end

function _is_binary_list(::List{T,O,A}) where {T,O,A}
    return Base.nonmissingtype(T) <: Base.CodeUnits
end

_struct_field_names(::Struct{T,S,field_names}) where {T,S,field_names} = field_names

function _fixed_size_list_size(::FixedSizeList{T}) where {T}
    kind = ArrowTypes.ArrowKind(ArrowTypes.ArrowType(Base.nonmissingtype(T)))
    return ArrowTypes.getsize(kind)
end

function _list_format(column::List)
    if liststringtype(column)
        return _is_binary_list(column) ? _offset_width_format(column, "z", "Z") :
               _offset_width_format(column, "u", "U")
    end
    return _offset_width_format(column, "+l", "+L")
end
