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

"""
Given a flatbuffers metadata type definition (a Field instance from Schema.fbs),
translate to the appropriate Julia storage eltype
"""
function juliaeltype end

finaljuliatype(T) = T
finaljuliatype(::Type{Missing}) = Missing
finaljuliatype(::Type{Union{T,Missing}}) where {T} = Union{Missing,finaljuliatype(T)}

const BOOL8_SYMBOL = Symbol("arrow.bool8")
const JSON_SYMBOL = Symbol("arrow.json")
const OPAQUE_SYMBOL = Symbol("arrow.opaque")
const PARQUET_VARIANT_SYMBOL = Symbol("arrow.parquet.variant")
const FIXED_SHAPE_TENSOR_SYMBOL = Symbol("arrow.fixed_shape_tensor")
const VARIABLE_SHAPE_TENSOR_SYMBOL = Symbol("arrow.variable_shape_tensor")

@inline _canonicalextensionerror(sym::Symbol, msg::AbstractString) =
    throw(ArgumentError("invalid canonical $(String(sym)) extension: $msg"))

@inline _fieldchildren(field::Meta.Field) =
    field.children === nothing ? Meta.Field[] : field.children

@inline _jsonhaskey(x, key::AbstractString) = haskey(x, key)
@inline _jsonget(x, key::AbstractString) = x[key]

function _parsecanonicalmetadata(sym::Symbol, metadata::String; required::Bool=false)
    isempty(metadata) &&
        return required ? _canonicalextensionerror(sym, "metadata is required") : nothing
    value = try
        JSON3.read(metadata)
    catch
        _canonicalextensionerror(sym, "metadata must be valid JSON")
    end
    value isa JSON3.Object ||
        _canonicalextensionerror(sym, "metadata must be a JSON object")
    return value
end

function _parseintvector(sym::Symbol, value, label::AbstractString; allow_null::Bool=false)
    value isa AbstractVector ||
        _canonicalextensionerror(sym, "\"$label\" must be a JSON array")
    parsed = Vector{allow_null ? Union{Nothing,Int} : Int}()
    for item in value
        if allow_null && isnothing(item)
            push!(parsed, nothing)
        elseif item isa Integer
            item >= 0 ||
                _canonicalextensionerror(sym, "\"$label\" values must be non-negative")
            push!(parsed, Int(item))
        else
            suffix = allow_null ? "integers or null" : "integers"
            _canonicalextensionerror(sym, "\"$label\" must contain only $suffix")
        end
    end
    return parsed
end

function _parsestringvector(sym::Symbol, value, label::AbstractString)
    value isa AbstractVector ||
        _canonicalextensionerror(sym, "\"$label\" must be a JSON array")
    parsed = String[]
    for item in value
        item isa AbstractString ||
            _canonicalextensionerror(sym, "\"$label\" must contain only strings")
        push!(parsed, String(item))
    end
    return parsed
end

function _validatepermutation(sym::Symbol, permutation::Vector{Int}, ndim::Int)
    length(permutation) == ndim ||
        _canonicalextensionerror(sym, "\"permutation\" must have length $ndim")
    length(unique(permutation)) == ndim ||
        _canonicalextensionerror(sym, "\"permutation\" must not contain duplicates")
    return permutation
end

function _extractdimensionalmetadata(
    sym::Symbol,
    metadata;
    ndim::Union{Nothing,Int}=nothing,
)
    metadata === nothing && return (nothing, nothing, nothing)
    dim_names =
        _jsonhaskey(metadata, "dim_names") ?
        _parsestringvector(sym, _jsonget(metadata, "dim_names"), "dim_names") : nothing
    permutation =
        _jsonhaskey(metadata, "permutation") ?
        _parseintvector(sym, _jsonget(metadata, "permutation"), "permutation") : nothing
    uniform_shape =
        _jsonhaskey(metadata, "uniform_shape") ?
        _parseintvector(
            sym,
            _jsonget(metadata, "uniform_shape"),
            "uniform_shape";
            allow_null=true,
        ) : nothing
    if ndim !== nothing
        dim_names !== nothing && length(dim_names) == ndim ||
            isnothing(dim_names) ||
            _canonicalextensionerror(sym, "\"dim_names\" must have length $ndim")
        permutation !== nothing && _validatepermutation(sym, permutation, ndim)
        uniform_shape !== nothing && length(uniform_shape) == ndim ||
            isnothing(uniform_shape) ||
            _canonicalextensionerror(sym, "\"uniform_shape\" must have length $ndim")
    end
    return dim_names, permutation, uniform_shape
end

@inline _isliststoragetype(x) =
    x isa Union{Meta.List,Meta.LargeList,Meta.ListView,Meta.LargeListView}

@inline _isbinarystoragetype(x) =
    x isa Union{Meta.Binary,Meta.LargeBinary,Meta.BinaryView,Meta.FixedSizeBinary}

@inline _isutf8storagetype(x) = x isa Union{Meta.Utf8,Meta.LargeUtf8,Meta.Utf8View}

@inline _isintstoragetype(x, bitwidth::Integer, signed::Bool) =
    x isa Meta.Int && x.bitWidth == bitwidth && x.is_signed == signed

@inline _isfixedsizebinary(x, bytewidth::Integer) =
    x isa Meta.FixedSizeBinary && x.byteWidth == bytewidth

function _validateemptymetadata(sym::Symbol, metadata::String)
    isempty(metadata) || _canonicalextensionerror(sym, "metadata must be the empty string")
    return nothing
end

function _validateuuid(field::Meta.Field, metadata::String)
    _validateemptymetadata(ArrowTypes.UUIDSYMBOL, metadata)
    _isfixedsizebinary(field.type, 16) || _canonicalextensionerror(
        ArrowTypes.UUIDSYMBOL,
        "storage must be FixedSizeBinary(16)",
    )
    return nothing
end

function _validatejsonextension(field::Meta.Field, metadata::String)
    if !isempty(metadata)
        _parsecanonicalmetadata(JSON_SYMBOL, metadata)
    end
    _isutf8storagetype(field.type) || _canonicalextensionerror(
        JSON_SYMBOL,
        "storage must be Utf8, LargeUtf8, or Utf8View",
    )
    return nothing
end

function _validatebool8(field::Meta.Field, metadata::String)
    _validateemptymetadata(BOOL8_SYMBOL, metadata)
    _isintstoragetype(field.type, 8, true) ||
        _canonicalextensionerror(BOOL8_SYMBOL, "storage must be signed Int8")
    return nothing
end

function _validateopaque(field::Meta.Field, metadata::String)
    meta = _parsecanonicalmetadata(OPAQUE_SYMBOL, metadata; required=true)
    _jsonhaskey(meta, "type_name") ||
        _canonicalextensionerror(OPAQUE_SYMBOL, "\"type_name\" is required")
    _jsonhaskey(meta, "vendor_name") ||
        _canonicalextensionerror(OPAQUE_SYMBOL, "\"vendor_name\" is required")
    _jsonget(meta, "type_name") isa AbstractString ||
        _canonicalextensionerror(OPAQUE_SYMBOL, "\"type_name\" must be a string")
    _jsonget(meta, "vendor_name") isa AbstractString ||
        _canonicalextensionerror(OPAQUE_SYMBOL, "\"vendor_name\" must be a string")
    field
    return nothing
end

function _validatetimestampwithoffset(field::Meta.Field, metadata::String)
    _validateemptymetadata(TIMESTAMP_WITH_OFFSET_SYMBOL, metadata)
    field.type isa Meta.Struct ||
        _canonicalextensionerror(TIMESTAMP_WITH_OFFSET_SYMBOL, "storage must be a Struct")
    children = collect(_fieldchildren(field))
    length(children) == 2 || _canonicalextensionerror(
        TIMESTAMP_WITH_OFFSET_SYMBOL,
        "storage must contain exactly \"timestamp\" and \"offset_minutes\" fields",
    )
    timestamp_field, offset_field = children
    timestamp_field.name == "timestamp" || _canonicalextensionerror(
        TIMESTAMP_WITH_OFFSET_SYMBOL,
        "first child field must be named \"timestamp\"",
    )
    offset_field.name == "offset_minutes" || _canonicalextensionerror(
        TIMESTAMP_WITH_OFFSET_SYMBOL,
        "second child field must be named \"offset_minutes\"",
    )
    !timestamp_field.nullable && !offset_field.nullable || _canonicalextensionerror(
        TIMESTAMP_WITH_OFFSET_SYMBOL,
        "child fields must be non-nullable",
    )
    timestamp_type = timestamp_field.type
    timestamp_type isa Meta.Timestamp || _canonicalextensionerror(
        TIMESTAMP_WITH_OFFSET_SYMBOL,
        "\"timestamp\" field must use Timestamp storage",
    )
    timestamp_type.timezone == "UTC" || _canonicalextensionerror(
        TIMESTAMP_WITH_OFFSET_SYMBOL,
        "\"timestamp\" field timezone must be UTC",
    )
    _isintstoragetype(offset_field.type, 16, true) || _canonicalextensionerror(
        TIMESTAMP_WITH_OFFSET_SYMBOL,
        "\"offset_minutes\" field must use signed Int16 storage",
    )
    return nothing
end

function _validateparquetvariant(field::Meta.Field, metadata::String)
    isempty(metadata) || _canonicalextensionerror(
        PARQUET_VARIANT_SYMBOL,
        "metadata must be the empty string",
    )
    field
    return
end

function _validatefixedshapetensor(field::Meta.Field, metadata::String)
    meta = _parsecanonicalmetadata(FIXED_SHAPE_TENSOR_SYMBOL, metadata; required=true)
    _jsonhaskey(meta, "shape") ||
        _canonicalextensionerror(FIXED_SHAPE_TENSOR_SYMBOL, "\"shape\" is required")
    shape = _parseintvector(FIXED_SHAPE_TENSOR_SYMBOL, _jsonget(meta, "shape"), "shape")
    dim_names, permutation, _ =
        _extractdimensionalmetadata(FIXED_SHAPE_TENSOR_SYMBOL, meta; ndim=length(shape))
    field.type isa Meta.FixedSizeList || _canonicalextensionerror(
        FIXED_SHAPE_TENSOR_SYMBOL,
        "storage must be a FixedSizeList",
    )
    length(collect(_fieldchildren(field))) == 1 || _canonicalextensionerror(
        FIXED_SHAPE_TENSOR_SYMBOL,
        "storage must contain exactly one child field",
    )
    expected = isempty(shape) ? 1 : prod(shape)
    Int(field.type.listSize) == expected || _canonicalextensionerror(
        FIXED_SHAPE_TENSOR_SYMBOL,
        "\"shape\" product $expected does not match FixedSizeList size $(field.type.listSize)",
    )
    dim_names
    permutation
    return
end

function _validatevariableshapetensor(field::Meta.Field, metadata::String)
    field.type isa Meta.Struct ||
        _canonicalextensionerror(VARIABLE_SHAPE_TENSOR_SYMBOL, "storage must be a Struct")
    children = Dict(String(child.name) => child for child in collect(_fieldchildren(field)))
    keys(children) == Set(("data", "shape")) || _canonicalextensionerror(
        VARIABLE_SHAPE_TENSOR_SYMBOL,
        "storage must contain exactly \"data\" and \"shape\" fields",
    )
    data_field = children["data"]
    shape_field = children["shape"]
    _isliststoragetype(data_field.type) || _canonicalextensionerror(
        VARIABLE_SHAPE_TENSOR_SYMBOL,
        "\"data\" field must use list storage",
    )
    length(collect(_fieldchildren(data_field))) == 1 || _canonicalextensionerror(
        VARIABLE_SHAPE_TENSOR_SYMBOL,
        "\"data\" field must contain exactly one child field",
    )
    shape_field.type isa Meta.FixedSizeList || _canonicalextensionerror(
        VARIABLE_SHAPE_TENSOR_SYMBOL,
        "\"shape\" field must use FixedSizeList storage",
    )
    shape_children = collect(_fieldchildren(shape_field))
    length(shape_children) == 1 || _canonicalextensionerror(
        VARIABLE_SHAPE_TENSOR_SYMBOL,
        "\"shape\" field must contain exactly one child field",
    )
    shape_value = only(shape_children)
    shape_value.type isa Meta.Int || _canonicalextensionerror(
        VARIABLE_SHAPE_TENSOR_SYMBOL,
        "\"shape\" values must use Int32 storage",
    )
    (shape_value.type.bitWidth == 32 && shape_value.type.is_signed) ||
        _canonicalextensionerror(
            VARIABLE_SHAPE_TENSOR_SYMBOL,
            "\"shape\" values must use signed Int32 storage",
        )
    ndim = Int(shape_field.type.listSize)
    meta = _parsecanonicalmetadata(VARIABLE_SHAPE_TENSOR_SYMBOL, metadata)
    _extractdimensionalmetadata(VARIABLE_SHAPE_TENSOR_SYMBOL, meta; ndim=ndim)
    return
end

"""
Given a FlatBuffers.Builder and a Julia column or column eltype,
Write the field.type flatbuffer definition of the eltype
"""
