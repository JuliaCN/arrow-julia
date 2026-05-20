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
    Arrow.Table(io::IO; convert::Bool=true)
    Arrow.Table(file::String; convert::Bool=true)
    Arrow.Table(bytes::Vector{UInt8}, pos=1, len=nothing; convert::Bool=true)
    Arrow.Table(inputs::Vector; convert::Bool=true)

Read an arrow formatted table, from:
 * `io`, bytes will be read all at once via `read(io)`
 * `file`, bytes will be read via `Mmap.mmap(file)`
 * `bytes`, a byte vector directly, optionally allowing specifying the starting byte position `pos` and `len`
 * A `Vector` of any of the above, in which each input should be an IPC or arrow file and must match schema

Returns a `Arrow.Table` object that allows column access via `table.col1`, `table[:col1]`, or `table[1]`.

NOTE: the columns in an `Arrow.Table` are views into the original arrow memory, and hence are not easily
modifiable (with e.g. `push!`, `append!`, etc.). To mutate arrow columns, call `copy(x)` to materialize
the arrow data as a normal Julia array.

`Arrow.Table` also satisfies the [Tables.jl](https://github.com/JuliaData/Tables.jl) interface, and so can easily be materialied via any supporting
sink function: e.g. `DataFrame(Arrow.Table(file))`, `SQLite.load!(db, "table", Arrow.Table(file))`, etc.

Supports the `convert` keyword argument which controls whether certain arrow primitive types will be
lazily converted to more friendly Julia defaults; by default, `convert=true`.
"""
struct Table <: Tables.AbstractColumns
    names::Vector{Symbol}
    types::Vector{Type}
    columns::Vector{AbstractVector}
    lookup::Dict{Symbol,AbstractVector}
    schema::Ref{Meta.Schema}
    metadata::Ref{Union{Nothing,Base.ImmutableDict{String,String}}}
end

Table() = Table(
    Symbol[],
    Type[],
    AbstractVector[],
    Dict{Symbol,AbstractVector}(),
    Ref{Meta.Schema}(),
    Ref{Union{Nothing,Base.ImmutableDict{String,String}}}(nothing),
)

function Table(names, types, columns, lookup, schema)
    m = isassigned(schema) ? buildmetadata(schema[]) : nothing
    return Table(
        names,
        types,
        columns,
        lookup,
        schema,
        Ref{Union{Nothing,Base.ImmutableDict{String,String}}}(m),
    )
end

names(t::Table) = getfield(t, :names)
types(t::Table) = getfield(t, :types)
columns(t::Table) = getfield(t, :columns)
lookup(t::Table) = getfield(t, :lookup)
schema(t::Table) = getfield(t, :schema)
metadata(t::Table) = getfield(t, :metadata)

"""
    Arrow.getmetadata(x)

If `x isa Arrow.Table` return a `Base.ImmutableDict{String,String}` representation of `x`'s
`Schema` `custom_metadata`, or `nothing` if no such metadata exists.

If `x isa Arrow.ArrowVector`, return a `Base.ImmutableDict{String,String}` representation of `x`'s
`Field` `custom_metadata`, or `nothing` if no such metadata exists.

Otherwise, return `nothing`.

See [the official Arrow documentation for more details on custom application metadata](https://arrow.apache.org/docs/format/Columnar.html#custom-application-metadata).
"""
getmetadata(t::Table) = getfield(t, :metadata)[]
getmetadata(::Any) = nothing

DataAPI.metadatasupport(::Type{Table}) = (read=true, write=false)
DataAPI.colmetadatasupport(::Type{Table}) = (read=true, write=false)

function DataAPI.metadata(t::Table, key::AbstractString; style::Bool=false)
    meta = getmetadata(t)[key]
    return style ? (meta, :default) : meta
end

function DataAPI.metadata(t::Table, key::AbstractString, default; style::Bool=false)
    meta = getmetadata(t)
    if meta !== nothing
        haskey(meta, key) && return style ? meta[key] : (meta[key], :default)
    end
    return style ? (default, :default) : default
end

function DataAPI.metadatakeys(t::Table)
    meta = getmetadata(t)
    meta === nothing && return ()
    return keys(meta)
end

function DataAPI.colmetadata(t::Table, col, key::AbstractString; style::Bool=false)
    meta = getmetadata(t[col])[key]
    return style ? (meta, :default) : meta
end

function DataAPI.colmetadata(t::Table, col, key::AbstractString, default; style::Bool=false)
    meta = getmetadata(t[col])
    if meta !== nothing
        haskey(meta, key) && return style ? (meta[key], :default) : meta[key]
    end
    return style ? (default, :default) : default
end

function DataAPI.colmetadatakeys(t::Table, col)
    meta = getmetadata(t[col])
    meta === nothing && return ()
    return keys(meta)
end

function DataAPI.colmetadatakeys(t::Table)
    return (
        col => DataAPI.colmetadatakeys(t, col) for
        col in Tables.columnnames(t) if getmetadata(t[col]) !== nothing
    )
end

Tables.istable(::Table) = true
Tables.columnaccess(::Table) = true
Tables.columns(t::Table) = Tables.CopiedColumns(t)
Tables.schema(t::Table) = Tables.Schema(names(t), types(t))
Tables.columnnames(t::Table) = names(t)
Tables.getcolumn(t::Table, i::Int) = columns(t)[i]
Tables.getcolumn(t::Table, nm::Symbol) = lookup(t)[nm]

struct MetadataVector{T,A<:AbstractVector{T},M} <: AbstractVector{T}
    data::A
    metadata::M
end

Base.IndexStyle(::Type{<:MetadataVector}) = Base.IndexLinear()
Base.size(x::MetadataVector) = size(x.data)
Base.axes(x::MetadataVector) = axes(x.data)
Base.length(x::MetadataVector) = length(x.data)
Base.getindex(x::MetadataVector, i::Int) = getindex(x.data, i)
Base.iterate(x::MetadataVector) = iterate(x.data)
Base.iterate(x::MetadataVector, state) = iterate(x.data, state)
getmetadata(x::MetadataVector) = x.metadata

_metadatavectordata(x::MetadataVector) = x.data
_metadatavectordata(x) = x
_wrapmetadata(data, metadata) = metadata === nothing ? data : MetadataVector(data, metadata)

struct TablePartitions
    table::Table
    npartitions::Int
end

Base.IteratorSize(::Type{TablePartitions}) = Base.HasLength()
Base.length(tp::TablePartitions) = tp.npartitions

function _partitionarrays(col::MetadataVector)
    data = getfield(col, :data)
    return data isa ChainedVector ? data.arrays : nothing
end

_partitionarrays(col) = col isa ChainedVector ? col.arrays : _wrappedpartitionarrays(col)

function _wrappedpartitionarrays(col)
    if hasfield(typeof(col), :data)
        data = getfield(col, :data)
        data isa ChainedVector && return data.arrays
    end
    return nothing
end

_partitioncolumn(col::MetadataVector, i::Int) =
    MetadataVector(getfield(col, :data).arrays[i], getfield(col, :metadata))

_partitioncolumn(col, i::Int) =
    col isa ChainedVector ? col.arrays[i] : _wrappedpartitioncolumn(col, i)

function _wrappedpartitioncolumn(col, i::Int)
    if hasfield(typeof(col), :data) && hasfield(typeof(col), :metadata)
        data = getfield(col, :data)
        if data isa ChainedVector
            wrapper = getfield(parentmodule(typeof(col)), nameof(typeof(col)))
            return wrapper(data.arrays[i], getfield(col, :metadata))
        end
    end
    return col
end

function TablePartitions(table::Table)
    cols = columns(table)
    npartitions = if length(cols) == 0
        0
    elseif (arrays = _partitionarrays(cols[1])) !== nothing
        isempty(arrays) ? 1 : length(arrays)
    else
        1
    end
    return TablePartitions(table, npartitions)
end

function Base.iterate(tp::TablePartitions, i=1)
    i > tp.npartitions && return nothing
    tp.npartitions == 1 && return tp.table, i + 1
    cols = columns(tp.table)
    newcols = AbstractVector[_partitioncolumn(cols[j], i) for j = 1:length(cols)]
    nms = names(tp.table)
    tbl = Table(
        nms,
        types(tp.table),
        newcols,
        Dict{Symbol,AbstractVector}(nms[i] => newcols[i] for i = 1:length(nms)),
        schema(tp.table),
    )
    return tbl, i + 1
end

Tables.partitions(t::Table) = TablePartitions(t)
