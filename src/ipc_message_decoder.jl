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
Incremental decoder for transports, such as Arrow Flight, that already split
IPC metadata from its body. Messages are admitted one at a time while schema,
dictionary snapshots, compression contexts, semantic validation, and the
cumulative allocation budget remain owned by the Arrow IPC layer.

`pushmessage!` may make zero or more record batches ready. `takebatch!`
preserves record-message order, including the IPC exception that permits an
all-null dictionary column to precede its first DictionaryBatch.
"""
mutable struct IPCMessageDecoder
    schema::Schema
    corefields::AC.FrozenVector{Field}
    dictids::Dict{Int64,Meta.Field}
    fielddictids::IdDict{Field,Int64}
    dictvaluefields::Dict{Int64,Field}
    dictionaries::Dict{Int64,ArrayData}
    validated_dictionaries::AC._ValidatedDictionaries
    batchslots::Vector{Union{Nothing,AC.RecordBatch}}
    pending::Vector{PendingRecord}
    nextindex::Int
    records_seen::Int
    records_yielded::Int
    state::DecodeState
    limits::Limits
    budget::AllocationBudget
    schemaversion::Int16
    messages_seen::Int
    finished::Bool
    closed::Bool
end

function IPCMessageDecoder(
    fm::FramedMessage,
    limits::Limits,
    budget::AllocationBudget=AllocationBudget(limits.max_total_allocated_bytes),
)
    fm.header_type == 1 || throw(ValidationError("first IPC message must be a schema"))
    fm.msg.header isa Meta.Schema ||
        throw(ValidationError("first IPC message must be a schema"))
    fm.body.len == 0 || throw(ValidationError("schema message must have an empty body"))
    metaschema = fm.msg.header
    endian = something(metaschema.endianness, Meta.Endianness.Little)
    endian == Meta.Endianness.Little || throw(
        ValidationError("big-endian IPC is not supported (no endianness normalization)"),
    )
    dictids = Dict{Int64,Meta.Field}()
    fielddictids = IdDict{Field,Int64}()
    fields = Field[
        corefield(field, dictids, fielddictids) for
        field in something(metaschema.fields, Meta.Field[])
    ]
    foreach(validateschemafield, fields)
    dictvaluefields = validatedictionaryids(fields, fielddictids)
    schema = Schema(
        fields;
        metadata=coremetadata(metaschema.custom_metadata),
        endianness=AC.LittleEndian,
    )
    decoder = IPCMessageDecoder(
        schema,
        AC.FrozenVector{Field}(fields),
        dictids,
        fielddictids,
        dictvaluefields,
        Dict{Int64,ArrayData}(),
        AC._ValidatedDictionaries(),
        Union{Nothing,AC.RecordBatch}[],
        PendingRecord[],
        1,
        0,
        0,
        DecodeState(budget),
        limits,
        budget,
        fm.version,
        1,
        false,
        false,
    )
    finalizer(close, decoder)
    return decoder
end

AC.schema(decoder::IPCMessageDecoder) = decoder.schema

function Base.close(decoder::IPCMessageDecoder)
    decoder.closed && return nothing
    close(decoder.state)
    decoder.closed = true
    return nothing
end

function pushmessage!(decoder::IPCMessageDecoder, fm::FramedMessage)
    decoder.closed && throw(InvalidStateException("the IPC decoder was closed", :closed))
    decoder.finished &&
        throw(InvalidStateException("the IPC decoder was already finished", :closed))
    decoder.messages_seen < decoder.limits.max_messages ||
        throw(ValidationError("message count exceeds limit"))
    decoder.messages_seen += 1
    fm.version == decoder.schemaversion ||
        throw(ValidationError("IPC metadata version changes within the stream"))
    rejectexperimentalcompression(fm)
    header = fm.msg.header
    if header isa Meta.DictionaryBatch
        rb = header.data
        codec = _batchcodec(rb.compression, fm.version)
        haskey(decoder.dictids, header.id) ||
            throw(ValidationError("dictionary batch has unknown id $(header.id)"))
        replacement = haskey(decoder.dictionaries, header.id)
        _dictionarytransition(header.id, header.isDelta, replacement)
        haskey(decoder.dictvaluefields, header.id) ||
            throw(ValidationError("dictionary batch has unknown id $(header.id)"))
        vf = decoder.dictvaluefields[header.id]
        rblen = something(rb.length, Int64(0))
        0 <= rblen <= decoder.limits.max_array_length ||
            throw(ValidationError("dictionary batch length $rblen exceeds limit"))
        cursor = DecodeCursor(
            rb.nodes,
            rb.buffers,
            fm.body,
            decoder.limits;
            codec=codec,
            state=decoder.state,
            variadics=variadiccounts(rb),
        )
        decoded = decodefield(vf, cursor, decoder.dictionaries, decoder.fielddictids)
        finishcursor!(cursor)
        decoded.len == rblen || throw(
            ValidationError("dictionary RecordBatch length does not match its field node"),
        )
        decoded = _updatedictionary!(
            decoder.dictionaries,
            decoder.validated_dictionaries,
            header.id,
            header.isDelta,
            vf,
            decoded,
            decoder.limits,
            decoder.budget,
        )
        if !replacement
            stillpending = PendingRecord[]
            for pending in decoder.pending
                if header.id in pending.missing
                    pending.dictionaries[header.id] = decoded
                    delete!(pending.missing, header.id)
                end
                if isempty(pending.missing)
                    decoder.batchslots[pending.slot] = decoderecord(
                        pending.fm,
                        decoder.corefields,
                        decoder.schema,
                        pending.dictionaries,
                        decoder.fielddictids,
                        decoder.limits,
                        decoder.validated_dictionaries,
                        decoder.state,
                    )
                else
                    push!(stillpending, pending)
                end
            end
            decoder.pending = stillpending
        end
    elseif header isa Meta.RecordBatch
        decoder.records_seen += 1
        missing = missingdicts(
            decoder.corefields,
            header.nodes,
            decoder.dictionaries,
            decoder.fielddictids,
        )
        push!(decoder.batchslots, nothing)
        slot = length(decoder.batchslots)
        if isempty(missing)
            decoder.batchslots[slot] = decoderecord(
                fm,
                decoder.corefields,
                decoder.schema,
                decoder.dictionaries,
                decoder.fielddictids,
                decoder.limits,
                decoder.validated_dictionaries,
                decoder.state,
            )
        else
            push!(
                decoder.pending,
                PendingRecord(fm, copy(decoder.dictionaries), missing, slot),
            )
        end
    elseif header isa Meta.Schema
        throw(ValidationError("schema message may appear only once at stream start"))
    else
        throw(ValidationError("unsupported IPC message header $(typeof(header))"))
    end
    return nothing
end

function takebatch!(decoder::IPCMessageDecoder)
    decoder.nextindex <= length(decoder.batchslots) || return nothing
    batch = decoder.batchslots[decoder.nextindex]
    batch === nothing && return nothing
    decoder.batchslots[decoder.nextindex] = nothing
    decoder.nextindex += 1
    decoder.records_yielded += 1
    record_id = decoder.records_yielded
    # Bound retained queue storage for long-running transports. Pending slots
    # are local indexes, so shift them together after removing the consumed
    # prefix. Eager IPC reads never pull here and still retain every batch.
    if decoder.nextindex > 64
        consumed = decoder.nextindex - 1
        deleteat!(decoder.batchslots, 1:consumed)
        for pending in decoder.pending
            pending.slot -= consumed
        end
        decoder.nextindex = 1
    end
    return batch, record_id
end

function finish!(decoder::IPCMessageDecoder)
    decoder.finished && return nothing
    isempty(decoder.pending) ||
        throw(ValidationError("stream ended before required dictionary batches arrived"))
    decoder.finished = true
    close(decoder)
    return nothing
end
