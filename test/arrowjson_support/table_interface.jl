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

mutable struct RecordBatch
    count::Int64
    columns::Vector{FieldData}
end

RecordBatch() = RecordBatch(0, FieldData[])
StructTypes.StructType(::Base.Type{RecordBatch}) = StructTypes.Mutable()

mutable struct DictionaryBatch
    id::Int64
    data::RecordBatch
end

DictionaryBatch() = DictionaryBatch(0, RecordBatch())
StructTypes.StructType(::Base.Type{DictionaryBatch}) = StructTypes.Mutable()

mutable struct DataFile <: Tables.AbstractColumns
    schema::Schema
    batches::Vector{RecordBatch}
    dictionaries::Vector{DictionaryBatch}
end

Base.propertynames(x::DataFile) = (:schema, :batches, :dictionaries)

function Base.getproperty(df::DataFile, nm::Symbol)
    if nm === :schema
        return getfield(df, :schema)
    elseif nm === :batches
        return getfield(df, :batches)
    elseif nm === :dictionaries
        return getfield(df, :dictionaries)
    end
    return Tables.getcolumn(df, nm)
end

DataFile() = DataFile(Schema(), RecordBatch[], DictionaryBatch[])
StructTypes.StructType(::Base.Type{DataFile}) = StructTypes.Mutable()

parsefile(file) = JSON3.read(Mmap.mmap(file), DataFile)

Arrow.getmetadata(x::DataFile) = _metadata_dict(x.schema.metadata)

# make DataFile satisfy Tables.jl interface
function Tables.partitions(x::DataFile)
    if isempty(x.batches)
        # special case empty batches by producing a single DataFile w/ schema
        return (DataFile(x.schema, RecordBatch[], x.dictionaries),)
    else
        return (
            DataFile(x.schema, [x.batches[i]], x.dictionaries) for i = 1:length(x.batches)
        )
    end
end

Tables.columns(x::DataFile) = x

function Tables.schema(x::DataFile)
    names = map(x -> x.name, x.schema.fields)
    types = map(x -> juliatype(x), x.schema.fields)
    return Tables.Schema(names, types)
end

Tables.columnnames(x::DataFile) = map(x -> Symbol(x.name), x.schema.fields)

function Tables.getcolumn(x::DataFile, i::Base.Int)
    field = x.schema.fields[i]
    if length(x.batches) == 1
        physical = _physical_column(field, x.batches[1].columns[i], x.dictionaries)
        physical === nothing || return physical
    end
    type = juliatype(field)
    column = ChainedVector(
        ArrowArray{type}[
            ArrowArray{type}(
                field,
                length(x.batches) > 0 ? x.batches[j].columns[i] : FieldData(),
                x.dictionaries,
            ) for j = 1:length(x.batches)
        ],
    )
    return Arrow._wrapmetadata(column, _metadata_dict(field.metadata))
end

function Tables.getcolumn(x::DataFile, nm::Symbol)
    i = findfirst(x -> x.name == String(nm), x.schema.fields)
    return Tables.getcolumn(x, i)
end

struct ArrowArray{T} <: AbstractVector{T}
    field::Field
    fielddata::FieldData
    dictionaries::Vector{DictionaryBatch}
end
ArrowArray(f::Field, fd::FieldData, d) = ArrowArray{juliatype(f)}(f, fd, d)
Base.size(x::ArrowArray) = (x.fielddata.count,)

_jsonint(x::Integer) = Base.Int(x)
_jsonint(x::AbstractString) = parse(Base.Int, x)
_offsetvalue(x) = _jsonint(x)
_jsonfield(x::AbstractDict, key::Symbol) = haskey(x, String(key)) ? x[String(key)] : x[key]
_jsonfield(x, key::Symbol) = getproperty(x, key)
