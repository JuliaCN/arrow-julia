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

function _hexbytes(hex::AbstractString)
    bytes = Vector{UInt8}(undef, length(hex) ÷ 2)
    for (out, idx) in enumerate(1:2:length(hex))
        bytes[out] = parse(UInt8, hex[idx:(idx + 1)]; base=16)
    end
    return bytes
end

_hexstring(bytes) = uppercase(bytes2hex(collect(UInt8, bytes)))

function _int32_from_prefix(prefix::Union{Nothing,String})
    bytes = zeros(UInt8, 4)
    if prefix !== nothing
        prefix_bytes = _hexbytes(prefix)
        copyto!(bytes, 1, prefix_bytes, 1, min(4, length(prefix_bytes)))
    end
    return reinterpret(Int32, bytes)[1]
end

_inline_view_bytes(::Utf8View, view::ViewData) = collect(UInt8, codeunits(view.INLINED))
_inline_view_bytes(::BinaryView, view::ViewData) = _hexbytes(view.INLINED)

function _view_elements(type::Union{Utf8View,BinaryView}, fielddata::FieldData)
    views = Arrow.ViewElement[]
    for view in fielddata.VIEWS
        prefix = view.INLINED === nothing ? _int32_from_prefix(view.PREFIX_HEX) : Int32(0)
        push!(
            views,
            Arrow.ViewElement(
                Int32(view.SIZE),
                prefix,
                Int32(something(view.BUFFER_INDEX, 0)),
                Int32(something(view.OFFSET, 0)),
            ),
        )
    end
    inline = reinterpret(UInt8, views)
    for (index, view) in enumerate(fielddata.VIEWS)
        view.INLINED === nothing && continue
        bytes = _inline_view_bytes(type, view)
        first = ((index - 1) * Arrow.VIEW_ELEMENT_BYTES) + Arrow.VIEW_LENGTH_BYTES + 1
        copyto!(inline, first, bytes, 1, length(bytes))
    end
    return views, collect(inline)
end

function _view_buffers(fielddata::FieldData)
    fielddata.VARIADIC_DATA_BUFFERS === nothing && return Vector{UInt8}[]
    return [_hexbytes(buffer) for buffer in fielddata.VARIADIC_DATA_BUFFERS]
end

function _native_view(field::Field, fielddata::FieldData)
    views, inline = _view_elements(field.type, fielddata)
    buffers = _view_buffers(fielddata)
    validity = _validity_bitmap(fielddata)
    if field.type isa Utf8View
        Arrow._assert_utf8_view_spans(
            views,
            inline,
            buffers,
            validity,
            fielddata.count,
            "UTF-8 view",
        )
        T = field.nullable ? Union{Missing,String} : String
    else
        Arrow._assert_view_spans(
            views,
            inline,
            buffers,
            validity,
            fielddata.count,
            "binary view",
        )
        T =
            field.nullable ? Union{Missing,Base.CodeUnits{UInt8,String}} :
            Base.CodeUnits{UInt8,String}
    end
    return Arrow.View{T}(
        UInt8[],
        validity,
        views,
        inline,
        buffers,
        fielddata.count,
        _metadata_dict(field.metadata),
    )
end

function _native_arrowvector(field::Field, fielddata::FieldData, dictionaries)
    return Arrow.toarrowvector(ArrowArray(field, fielddata, dictionaries))
end

function _native_list_view(field::Field, fielddata::FieldData, dictionaries)
    O = field.type isa LargeListView ? Int64 : Int32
    offsets = O[_offsetvalue(value) for value in fielddata.OFFSET]
    sizes = O[_offsetvalue(value) for value in fielddata.SIZE]
    data = _native_arrowvector(field.children[1], fielddata.children[1], dictionaries)
    return Arrow.ListView(
        offsets,
        sizes,
        data;
        validity=_validity_bitmap(fielddata),
        metadata=_metadata_dict(field.metadata),
    )
end

function _native_run_end_encoded(field::Field, fielddata::FieldData, dictionaries)
    run_ends = _native_arrowvector(field.children[1], fielddata.children[1], dictionaries)
    values = _native_arrowvector(field.children[2], fielddata.children[2], dictionaries)
    return Arrow.RunEndEncoded(
        run_ends,
        values,
        fielddata.count,
        _metadata_dict(field.metadata),
    )
end

function _physical_column(field::Field, fielddata::FieldData, dictionaries)
    field.type isa Union{Utf8View,BinaryView} && return _native_view(field, fielddata)
    field.type isa Union{ListView,LargeListView} &&
        return _native_list_view(field, fielddata, dictionaries)
    field.type isa RunEndEncoded &&
        return _native_run_end_encoded(field, fielddata, dictionaries)
    return nothing
end

function _utf8viewvalue(view::ViewData, buffers::Union{Nothing,Vector{String}})
    view.INLINED !== nothing && return view.INLINED
    buffer = _hexbytes(buffers[view.BUFFER_INDEX + 1])
    first = view.OFFSET + 1
    return String(buffer[first:(first + view.SIZE - 1)])
end

function _binaryviewvalue(view::ViewData, buffers::Union{Nothing,Vector{String}})
    if view.INLINED !== nothing
        return _hexbytes(view.INLINED)
    end
    buffer = _hexbytes(buffers[view.BUFFER_INDEX + 1])
    first = view.OFFSET + 1
    return buffer[first:(first + view.SIZE - 1)]
end

function _intervalvalue(::Base.Type{S}, value) where {S}
    if S <: Arrow.Interval{Arrow.Meta.IntervalUnit.MONTH_DAY_NANO}
        return S(
            Arrow.MonthDayNanoInterval(
                _jsonint(_jsonfield(value, :months)),
                _jsonint(_jsonfield(value, :days)),
                _jsonint(_jsonfield(value, :nanoseconds)),
            ),
        )
    elseif S <: Arrow.Interval{Arrow.Meta.IntervalUnit.DAY_TIME} &&
           hasproperty(value, :days)
        days = Int32(_jsonint(_jsonfield(value, :days)))
        milliseconds = Int32(_jsonint(_jsonfield(value, :milliseconds)))
        return S(reinterpret(Int64, [days, milliseconds])[1])
    else
        return S(_jsonint(value))
    end
end
