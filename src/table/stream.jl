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

struct ArrowBlob
    bytes::Vector{UInt8}
    pos::Int
    len::Int
end

ArrowBlob(bytes::Vector{UInt8}, pos::Int, len::Nothing) =
    ArrowBlob(bytes, pos, length(bytes))

tobytes(bytes::Vector{UInt8}) = bytes
tobytes(io::IO) = Base.read(io)
tobytes(io::IOStream) = Mmap.mmap(io)
tobytes(file_path) = open(tobytes, file_path, "r")

rejectunsupported(field::Meta.Field) =
    (rejectunsupported(field.type); foreach(rejectunsupported, field.children))
rejectunsupported(x) = nothing

struct BatchIterator
    bytes::Vector{UInt8}
    startpos::Int
    function BatchIterator(blob::ArrowBlob)
        bytes, pos, len = blob.bytes, blob.pos, blob.len
        if len > 24 && _startswith(bytes, pos, FILE_FORMAT_MAGIC_BYTES)
            pos += 8 # skip past magic bytes + padding
        end
        new(bytes, pos)
    end
end

"""
    Arrow.Stream(io::IO; convert::Bool=true)
    Arrow.Stream(file::String; convert::Bool=true)
    Arrow.Stream(bytes::Vector{UInt8}, pos=1, len=nothing; convert::Bool=true)
    Arrow.Stream(inputs::Vector; convert::Bool=true)

Start reading an arrow formatted table, from:
 * `io`, bytes will be read all at once via `read(io)`
 * `file`, bytes will be read via `Mmap.mmap(file)`
 * `bytes`, a byte vector directly, optionally allowing specifying the starting byte position `pos` and `len`
 * A `Vector` of any of the above, in which each input should be an IPC or arrow file and must match schema

Reads the initial schema message from the arrow stream/file, then returns an `Arrow.Stream` object
which will iterate over record batch messages, producing an [`Arrow.Table`](@ref) on each iteration.

By iterating [`Arrow.Table`](@ref), `Arrow.Stream` satisfies the `Tables.partitions` interface, and as such can
be passed to Tables.jl-compatible sink functions.

This allows iterating over extremely large "arrow tables" in chunks represented as record batches.

Supports the `convert` keyword argument which controls whether certain arrow primitive types will be
lazily converted to more friendly Julia defaults; by default, `convert=true`.
"""
mutable struct Stream
    inputs::Vector{ArrowBlob}
    inputindex::Int
    batchiterator::Union{Nothing,BatchIterator}
    names::Vector{Symbol}
    types::Vector{Type}
    schema::Union{Nothing,Meta.Schema}
    dictencodings::Lockable{Dict{Int64,DictEncoding}} # dictionary id => DictEncoding
    dictencoded::Dict{Int64,Meta.Field} # dictionary id => field
    convert::Bool
    compression::Ref{Union{Symbol,Nothing}}
end

function Stream(inputs::Vector{ArrowBlob}; convert::Bool=true)
    inputindex = 1
    batchiterator = nothing
    names = Symbol[]
    types = Type[]
    schema = nothing
    dictencodings = Lockable(Dict{Int64,DictEncoding}())
    dictencoded = Dict{Int64,Meta.Field}()
    compression = Ref{Union{Symbol,Nothing}}(nothing)
    Stream(
        inputs,
        inputindex,
        batchiterator,
        names,
        types,
        schema,
        dictencodings,
        dictencoded,
        convert,
        compression,
    )
end

function Stream(input, pos::Integer=1, len=nothing; kw...)
    b = tobytes(input)
    isempty(b) ? Stream(ArrowBlob[]; kw...) : Stream([ArrowBlob(b, pos, len)]; kw...)
end

function Stream(input::Vector{UInt8}, pos::Integer=1, len=nothing; kw...)
    b = tobytes(input)
    isempty(b) ? Stream(ArrowBlob[]; kw...) : Stream([ArrowBlob(b, pos, len)]; kw...)
end

function Stream(inputs::AbstractVector; kw...)
    blobs = ArrowBlob[]
    for x in inputs
        b = tobytes(x)
        isempty(b) && continue
        push!(blobs, ArrowBlob(b, 1, nothing))
    end
    Stream(blobs; kw...)
end

function initialize!(x::Stream)
    isempty(getfield(x, :names)) || return
    # Initialize member fields using iteration and reset state
    lastinputindex = x.inputindex
    lastbatchiterator = x.batchiterator
    iterate(x)
    x.inputindex = lastinputindex
    x.batchiterator = lastbatchiterator
    nothing
end

Tables.partitions(x::Stream) = x

function Tables.columnnames(x::Stream)
    initialize!(x)
    getfield(x, :names)
end

function Tables.schema(x::Stream)
    initialize!(x)
    Tables.Schema(Tables.columnnames(x), getfield(x, :types))
end

Base.IteratorSize(::Type{Stream}) = Base.SizeUnknown()
Base.eltype(::Type{Stream}) = Table
Base.isdone(x::Stream) = x.inputindex > length(x.inputs)

function Base.iterate(x::Stream, (pos, id)=(1, 0))
    if Base.isdone(x)
        x.inputindex = 1
        x.batchiterator = nothing
        return nothing
    end
    if isnothing(x.batchiterator)
        blob = x.inputs[x.inputindex]
        x.batchiterator = BatchIterator(blob)
        pos = x.batchiterator.startpos
    end

    columns = AbstractVector[]
    compression = nothing

    while true
        state = iterate(x.batchiterator, (pos, id))
        # check for additional inputs
        while state === nothing
            x.inputindex += 1
            if Base.isdone(x)
                x.inputindex = 1
                x.batchiterator = nothing
                return nothing
            end
            blob = x.inputs[x.inputindex]
            x.batchiterator = BatchIterator(blob)
            pos = x.batchiterator.startpos
            state = iterate(x.batchiterator, (pos, id))
        end
        batch, (pos, id) = state
        header = batch.msg.header
        if header isa Meta.Tensor
            throw(ArgumentError(TENSOR_UNSUPPORTED))
        elseif header isa Meta.SparseTensor
            throw(ArgumentError(SPARSE_TENSOR_UNSUPPORTED))
        elseif isnothing(x.schema) && !isa(header, Meta.Schema)
            throw(ArgumentError("first arrow ipc message MUST be a schema message"))
        elseif header isa Meta.Schema
            if isnothing(x.schema)
                x.schema = header
                # assert endianness?
                # store custom_metadata?
                for (i, field) in enumerate(x.schema.fields)
                    rejectunsupported(field)
                    push!(x.names, Symbol(field.name))
                    push!(
                        x.types,
                        juliaeltype(field, buildmetadata(field.custom_metadata), x.convert),
                    )
                    # recursively find any dictionaries for any fields
                    getdictionaries!(x.dictencoded, field)
                    @debug "parsed column from schema: field = $field"
                end
            elseif header != x.schema
                throw(
                    ArgumentError(
                        "mismatched schemas between different arrow batches: $(x.schema) != $header",
                    ),
                )
            end
        elseif header isa Meta.DictionaryBatch
            id = header.id
            recordbatch = header.data
            @debug "parsing dictionary batch message: id = $id, compression = $(recordbatch.compression)"
            if recordbatch.compression !== nothing
                compression = recordbatch.compression
            end
            @lock x.dictencodings begin
                dictencodings = x.dictencodings[]
                if haskey(dictencodings, id) && header.isDelta
                    # delta
                    field = _dictionary_encoded_field(x.dictencoded, id)
                    values = _build_dictionary_values(
                        field,
                        batch,
                        recordbatch,
                        x.dictencodings,
                        x.convert,
                    )
                    dictencoding = dictencodings[id]
                    append!(dictencoding.data, values)
                    continue
                end
                # new dictencoding or replace
                field = _dictionary_encoded_field(x.dictencoded, id)
                values = _build_dictionary_values(
                    field,
                    batch,
                    recordbatch,
                    x.dictencodings,
                    x.convert,
                )
                A = ChainedVector([values])
                S =
                    field.dictionary.indexType === nothing ? Int32 :
                    juliaeltype(field, field.dictionary.indexType, false)
                dictencodings[id] = DictEncoding{eltype(A),S,typeof(A)}(
                    id,
                    A,
                    field.dictionary.isOrdered,
                    values.metadata,
                )
            end # lock
            @debug "parsed dictionary batch message: id=$id, data=$values\n"
        elseif header isa Meta.RecordBatch
            @debug "parsing record batch message: compression = $(header.compression)"
            if header.compression !== nothing
                compression = header.compression
            end
            append!(
                columns,
                materializecolumns(
                    VectorIterator(x.schema, batch, x.dictencodings, x.convert),
                ),
            )
            break
        elseif header isa Meta.Tensor
            throw(ArgumentError(TENSOR_UNSUPPORTED))
        elseif header isa Meta.SparseTensor
            throw(ArgumentError(SPARSE_TENSOR_UNSUPPORTED))
        else
            throw(ArgumentError("unsupported arrow message type: $(typeof(header))"))
        end
    end

    if compression !== nothing
        if compression.codec == Flatbuf.CompressionType.ZSTD
            x.compression[] = :zstd
        elseif compression.codec == Flatbuf.CompressionType.LZ4_FRAME
            x.compression[] = :lz4
        else
            throw(ArgumentError("unsupported compression codec: $(compression.codec)"))
        end
    end

    lookup = Dict{Symbol,AbstractVector}()
    types = Type[]
    for (nm, col) in zip(x.names, columns)
        lookup[nm] = col
        push!(types, eltype(col))
    end
    return Table(x.names, types, columns, lookup, Ref(x.schema)), (pos, id)
end
