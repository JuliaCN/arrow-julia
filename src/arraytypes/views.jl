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

struct ViewElement
    length::Int32
    prefix::Int32
    bufindex::Int32
    offset::Int32
end

const VIEW_ELEMENT_BYTES = sizeof(ViewElement)
const VIEW_LENGTH_BYTES = sizeof(Int32)
const VIEW_INLINE_BYTES = VIEW_ELEMENT_BYTES - VIEW_LENGTH_BYTES

@inline _viewisinline(length::Integer) = length <= VIEW_INLINE_BYTES
@inline _viewinlinestart(i::Integer) =
    ((i - 1) * VIEW_ELEMENT_BYTES) + VIEW_LENGTH_BYTES + 1
@inline _viewinlineend(i::Integer, length::Integer) = _viewinlinestart(i) + length - 1
@inline _viewinlineslice(inline::Vector{UInt8}, i::Integer, length::Integer) =
    @view inline[_viewinlinestart(i):_viewinlineend(i, length)]

function _assert_view_spans(views, inline, buffers, validity, len, label)
    logical_len = Int(len)
    length(views) == logical_len || throw(
        ArgumentError(
            "$label views length $(length(views)) does not match logical length $logical_len",
        ),
    )
    for i = 1:logical_len
        @inbounds validity[i] || continue
        view = @inbounds views[i]
        value_len = Int(view.length)
        value_len >= 0 || throw(ArgumentError("$label has negative value length"))
        if _viewisinline(value_len)
            _viewinlineend(i, value_len) <= length(inline) ||
                throw(ArgumentError("$label inline value exceeds views buffer length"))
            continue
        end
        buffer_index = Int(view.bufindex) + 1
        1 <= buffer_index <= length(buffers) ||
            throw(ArgumentError("$label references missing variadic buffer"))
        offset = Int(view.offset)
        offset >= 0 || throw(ArgumentError("$label has negative data offset"))
        buffer = @inbounds buffers[buffer_index]
        offset + value_len <= length(buffer) ||
            throw(ArgumentError("$label references bytes outside variadic buffer"))
    end
    return nothing
end

function _assert_utf8_view_spans(views, inline, buffers, validity, len, label)
    _assert_view_spans(views, inline, buffers, validity, len, label)
    logical_len = Int(len)
    for i = 1:logical_len
        @inbounds validity[i] || continue
        view = @inbounds views[i]
        value_len = Int(view.length)
        value_len == 0 && continue
        valid = if _viewisinline(value_len)
            isvalid(String, @view inline[_viewinlinestart(i):_viewinlineend(i, value_len)])
        else
            buffer = @inbounds buffers[Int(view.bufindex) + 1]
            start = Int(view.offset) + 1
            stop = start + value_len - 1
            isvalid(String, @view buffer[start:stop])
        end
        valid || throw(ArgumentError("$label value at index $i is not valid UTF-8"))
    end
    return nothing
end

"""
    Arrow.View

An `ArrowVector` where each element is a variable sized list of some kind, like an `AbstractVector` or `AbstractString`.
"""
struct View{T} <: ArrowVector{T}
    arrow::Vector{UInt8} # need to hold a reference to arrow memory blob
    validity::ValidityBitmap
    data::Vector{ViewElement}
    inline::Vector{UInt8} # `data` field reinterpreted as a byte array
    buffers::Vector{Vector{UInt8}} # holds non-inlined data
    ℓ::Int
    metadata::Union{Nothing,Base.ImmutableDict{String,String}}
end

Base.size(l::View) = (l.ℓ,)

@propagate_inbounds function Base.getindex(l::View{T}, i::Integer) where {T}
    @boundscheck checkbounds(l, i)
    @inbounds v = l.data[i]
    S = Base.nonmissingtype(T)
    if S <: Base.CodeUnits
        # BinaryView
        return !l.validity[i] ? missing :
               _viewisinline(v.length) ?
               Base.CodeUnits(StringView(_viewinlineslice(l.inline, i, v.length))) :
               Base.CodeUnits(
            StringView(
                @view l.buffers[v.bufindex + 1][(v.offset + 1):(v.offset + v.length)]
            ),
        )
    else
        # Utf8View
        return !l.validity[i] ? missing :
               _viewisinline(v.length) ?
               ArrowTypes.fromarrow(
            T,
            StringView(_viewinlineslice(l.inline, i, v.length)),
        ) :
               ArrowTypes.fromarrow(
            T,
            StringView(
                @view l.buffers[v.bufindex + 1][(v.offset + 1):(v.offset + v.length)]
            ),
        )
    end
end

# @propagate_inbounds function Base.setindex!(l::List{T}, v, i::Integer) where {T}

# end
