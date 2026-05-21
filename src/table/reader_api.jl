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

# high-level user API functions
Table(input, pos::Integer=1, len=nothing; kw...) =
    Table([ArrowBlob(tobytes(input), pos, len)]; kw...)
Table(input::Vector{UInt8}, pos::Integer=1, len=nothing; kw...) =
    Table([ArrowBlob(tobytes(input), pos, len)]; kw...)
Table(inputs::Vector; kw...) =
    Table([ArrowBlob(tobytes(x), 1, nothing) for x in inputs]; kw...)

# will detect whether we're reading a Table from a file or stream
function Table(blobs::Vector{ArrowBlob}; convert::Bool=true)
    t = Table()
    sch = nothing
    dictencodingslockable = Lockable(Dict{Int64,DictEncoding}()) # dictionary id => DictEncoding
    dictencoded = Dict{Int64,Meta.Field}() # dictionary id => field
    # we'll grow/add a record batch set of columns as they're constructed
    # must be holding the lock while growing/adding
    # starts at 0-length because we don't know how many record batches there will be
    rb_cols = []
    rb_cols_lock = ReentrantLock()
    rbi = 1
    tasks = Task[]
    for blob in blobs
        for batch in BatchIterator(blob)
            # store custom_metadata of batch.msg?
            header = batch.msg.header
            if header isa Meta.Schema
                @debug "parsing schema message"
                # assert endianness?
                # store custom_metadata?
                if sch === nothing
                    for (i, field) in enumerate(header.fields)
                        rejectunsupported(field)
                        push!(names(t), Symbol(field.name))
                        # recursively find any dictionaries for any fields
                        getdictionaries!(dictencoded, field)
                        @debug "parsed column from schema: field = $field"
                    end
                    sch = header
                    schema(t)[] = sch
                elseif sch != header
                    throw(
                        ArgumentError(
                            "mismatched schemas between different arrow batches: $sch != $header",
                        ),
                    )
                end
            elseif header isa Meta.DictionaryBatch
                id = header.id
                recordbatch = header.data
                @debug "parsing dictionary batch message: id = $id, compression = $(recordbatch.compression)"
                @lock dictencodingslockable begin
                    dictencodings = dictencodingslockable[]
                    if haskey(dictencodings, id) && header.isDelta
                        # delta
                        field = dictencoded[id]
                        values = _build_dictionary_values(
                            field,
                            batch,
                            recordbatch,
                            dictencodingslockable,
                            convert,
                        )
                        dictencoding = dictencodings[id]
                        if typeof(dictencoding.data) <: ChainedVector
                            append!(dictencoding.data, values)
                        else
                            A = ChainedVector([dictencoding.data, values])
                            S =
                                field.dictionary.indexType === nothing ? Int32 :
                                juliaeltype(field, field.dictionary.indexType, false)
                            dictencodings[id] = DictEncoding{eltype(A),S,typeof(A)}(
                                id,
                                A,
                                field.dictionary.isOrdered,
                                values.metadata,
                            )
                        end
                        continue
                    end
                    # new dictencoding or replace
                    field = dictencoded[id]
                    values = _build_dictionary_values(
                        field,
                        batch,
                        recordbatch,
                        dictencodingslockable,
                        convert,
                    )
                    A = values
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
                push!(
                    tasks,
                    collect_cols!(
                        rbi,
                        rb_cols_lock,
                        rb_cols,
                        sch,
                        batch,
                        dictencodingslockable,
                        convert,
                    ),
                )
                rbi += 1
            elseif header isa Meta.Tensor
                throw(ArgumentError(TENSOR_UNSUPPORTED))
            elseif header isa Meta.SparseTensor
                throw(ArgumentError(SPARSE_TENSOR_UNSUPPORTED))
            else
                throw(ArgumentError("unsupported arrow message type: $(typeof(header))"))
            end
        end
    end
    _waitall(tasks)
    lu = lookup(t)
    ty = types(t)
    # 158; some implementations may send 0 record batches
    # no more multithreading, so no need to take the lock now
    if length(rb_cols) == 0 && !isnothing(sch)
        for field in sch.fields
            T = juliaeltype(field, buildmetadata(field), convert)
            push!(columns(t), T[])
        end
    end
    if length(rb_cols) > 0
        foreach(x -> push!(columns(t), x), rb_cols[1])
    end
    if length(rb_cols) > 1
        foreach(enumerate(rb_cols[2])) do (i, x)
            columns(t)[i] = ChainedVector([columns(t)[i], x])
        end
        foreach(3:length(rb_cols)) do j
            foreach(enumerate(rb_cols[j])) do (i, x)
                append!(columns(t)[i], x)
            end
        end
    end
    if sch !== nothing
        foreach(enumerate(sch.fields)) do (i, field)
            columns(t)[i] = _wrapfieldmetadata(columns(t)[i], field)
        end
    end
    for (nm, col) in zip(names(t), columns(t))
        lu[nm] = col
        push!(ty, eltype(col))
    end
    getfield(t, :metadata)[] = buildmetadata(sch)
    return t
end

function collect_cols!(
    rbi,
    rb_cols_lock,
    rb_cols,
    sch,
    batch,
    dictencodingslockable,
    convert,
)
    @wkspawn begin
        cols =
            materializecolumns(VectorIterator(sch, batch, dictencodingslockable, convert))
        @lock rb_cols_lock begin
            if length(rb_cols) < rbi
                resize!(rb_cols, rbi)
            end
            rb_cols[rbi] = cols
        end
    end
end

function getdictionaries!(dictencoded, field)
    d = field.dictionary
    if d !== nothing
        dictencoded[d.id] = field
    end
    if field.children !== nothing
        for child in field.children
            getdictionaries!(dictencoded, child)
        end
    end
    return
end

struct Batch
    msg::Meta.Message
    bytes::Vector{UInt8}
    pos::Int
    id::Int
end

function Base.iterate(x::BatchIterator, (pos, id)=(x.startpos, 0))
    @debug "checking for next arrow message: pos = $pos"
    if pos + 3 > length(x.bytes)
        @debug "not enough bytes left for another batch message"
        return nothing
    end
    if readbuffer(x.bytes, pos, UInt32) != CONTINUATION_INDICATOR_BYTES
        @debug "didn't find continuation byte to keep parsing messages: $(readbuffer(x.bytes, pos, UInt32))"
        return nothing
    end
    pos += 4
    if pos + 3 > length(x.bytes)
        throw(ArgumentError("truncated arrow ipc message length"))
    end
    msglen = readbuffer(x.bytes, pos, Int32)
    msglen < 0 && throw(ArgumentError("arrow ipc message length must be non-negative"))
    if msglen == 0
        @debug "message has 0 length; terminating message parsing"
        return nothing
    end
    pos += 4
    if pos + msglen - 1 > length(x.bytes)
        throw(
            ArgumentError(
                "truncated arrow ipc message metadata: declared length $msglen exceeds remaining bytes $(length(x.bytes) - pos + 1)",
            ),
        )
    end
    msg = FlatBuffers.getrootas(Meta.Message, x.bytes, pos - 1)
    pos += msglen
    # pos now points to message body
    @debug "parsing message: pos = $pos, msglen = $msglen, bodyLength = $(msg.bodyLength)"
    msg.bodyLength < 0 &&
        throw(ArgumentError("arrow ipc message body length must be non-negative"))
    remaining_body_bytes = length(x.bytes) - pos + 1
    if msg.bodyLength > remaining_body_bytes
        throw(
            ArgumentError(
                "truncated arrow ipc message body: declared length $(msg.bodyLength) exceeds remaining bytes $remaining_body_bytes",
            ),
        )
    end
    return Batch(msg, x.bytes, pos, id), (pos + msg.bodyLength, id + 1)
end

struct VectorIterator
    schema::Meta.Schema
    batch::Batch # batch.msg.header MUST BE RecordBatch
    dictencodings::Lockable{Dict{Int64,DictEncoding}}
    convert::Bool
end

buildmetadata(f::Union{Meta.Field,Meta.Schema}) = buildmetadata(f.custom_metadata)
buildmetadata(meta) = toidict(String(kv.key) => String(kv.value) for kv in meta)
buildmetadata(::Nothing) = nothing
buildmetadata(x::AbstractDict) = x

function _wrapfieldmetadata(col, field::Meta.Field)
    metadata = buildmetadata(field.custom_metadata)
    metadata === nothing && return col
    getmetadata(col) == metadata && return col
    return _wrapmetadata(_metadatavectordata(col), metadata)
end
