# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

streambytes(message::Protocol.FlightData; kwargs...) =
    streambytes(Protocol.FlightData[message]; kwargs...)

function _has_schema_message(messages)
    return any(messages) do message
        isempty(message.data_header) && return false
        _flight_message_header(message).header isa ArrowParent.Meta.Schema
    end
end

function _missing_schema_message()
    return join(
        [
            "cannot derive Arrow Flight schema from a response stream without a schema message",
            "the server may have terminated the stream before emitting the first schema-bearing FlightData message",
            "or the underlying transport did not surface the corresponding gRPC status",
        ],
        "; ",
    )
end

function streambytes(
    messages;
    schema=nothing,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    end_marker::Bool=true,
)
    collected = _collect_messages(messages)
    has_schema = _has_schema_message(collected)
    has_schema || schema !== nothing || throw(ArgumentError(_missing_schema_message()))
    io = IOBuffer()
    !has_schema && Base.write(io, schemaipc(schema; alignment=alignment))
    for message in collected
        if isempty(message.data_header)
            isempty(message.data_body) || throw(
                ArgumentError("FlightData message has a body but no Arrow IPC header"),
            )
            continue
        end
        _write_framed_message(io, message.data_header, message.data_body, alignment)
    end
    end_marker && _write_end_marker(io)
    return take!(io)
end

function _record_app_metadata(messages)
    metadata = Vector{Vector{UInt8}}()
    for message in messages
        isempty(message.data_header) && continue
        header = _flight_message_header(message).header
        header isa ArrowParent.Meta.RecordBatch || continue
        push!(metadata, Vector{UInt8}(message.app_metadata))
    end
    return metadata
end

function _stream_schema(stream::ArrowParent.Stream)
    fields = ArrowParent._batchfields(getfield(stream, :src))
    names = Symbol[Symbol(field.name) for field in fields]
    types = Type[ArrowParent._declaredeltype(field) for field in fields]
    return Tables.Schema(
        names,
        types;
        stored=length(names) > ArrowParent._MAX_TYPED_SCHEMA_FIELDS,
    )
end

struct FlightStream{S,M}
    stream::S
    schema::Tables.Schema
    app_metadata::M
end

struct FlightStreamWithAppMetadata{S}
    stream::S
end

Base.IteratorSize(::Type{<:FlightStream}) = Base.HasLength()
Base.IteratorSize(::Type{<:FlightStreamWithAppMetadata}) = Base.HasLength()
Base.eltype(::Type{<:FlightStream}) = ArrowParent.Table
Base.eltype(::Type{<:FlightStreamWithAppMetadata}) = NamedTuple
Base.length(x::FlightStream) = length(x.stream)
Base.length(x::FlightStreamWithAppMetadata) = length(x.stream)
Base.isdone(x::FlightStream, state...) = Base.isdone(x.stream, state...)
Tables.partitions(x::FlightStream) = x
Tables.partitions(x::FlightStreamWithAppMetadata) = x
Tables.schema(x::FlightStream) = x.schema
Tables.schema(x::FlightStreamWithAppMetadata) = Tables.schema(x.stream)
Tables.columnnames(x::FlightStream) = x.schema.names
Tables.columnnames(x::FlightStreamWithAppMetadata) = Tables.columnnames(x.stream)

Base.iterate(x::FlightStream) = iterate(x.stream)
Base.iterate(x::FlightStream, state) = iterate(x.stream, state)

function Base.iterate(x::FlightStreamWithAppMetadata)
    item = iterate(x.stream.stream)
    item === nothing && return nothing
    table, state = item
    return (table=table, app_metadata=x.stream.app_metadata[1]), (state, 2)
end

function Base.iterate(x::FlightStreamWithAppMetadata, state)
    stream_state, index = state
    item = iterate(x.stream.stream, stream_state)
    item === nothing && return nothing
    table, next_state = item
    return (table=table, app_metadata=x.stream.app_metadata[index]),
    (next_state, index + 1)
end

function _flight_stream(
    messages;
    schema=nothing,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    end_marker::Bool=true,
)
    collected = _collect_messages(messages)
    bytes = streambytes(
        collected;
        schema=schema,
        alignment=alignment,
        end_marker=end_marker,
    )
    stream = ArrowParent.Stream(bytes; mmap=false)
    table_schema = _stream_schema(stream)
    metadata = _record_app_metadata(collected)
    length(metadata) == length(stream) || throw(
        ArgumentError(
            "Flight record-batch metadata count does not match decoded Arrow batch count",
        ),
    )
    return FlightStream(stream, table_schema, metadata)
end

"""
    Arrow.Flight.stream(messages; schema=nothing, convert=true, include_app_metadata=false)

Decode Flight `FlightData` through Arrow 3's validated IPC stream reader. The
Flight layer owns only framing and application metadata; schema, dictionary,
compression, ownership, and materialization semantics come from Arrow 3.
"""
function stream(
    messages;
    schema=nothing,
    convert::Bool=true,
    include_app_metadata::Bool=false,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    end_marker::Bool=true,
)
    convert || @warn "Arrow 3 Flight always returns public-domain materialized values" maxlog = 1
    value = _flight_stream(
        messages;
        schema=schema,
        alignment=alignment,
        end_marker=end_marker,
    )
    return include_app_metadata ? FlightStreamWithAppMetadata(value) : value
end

"""
    Arrow.Flight.table(messages; schema=nothing, convert=true, include_app_metadata=false)

Materialize Flight `FlightData` through Arrow 3's validated `Arrow.Table`
facade. Optional Flight application metadata is returned batch-for-batch.
"""
function table(
    messages;
    schema=nothing,
    convert::Bool=true,
    include_app_metadata::Bool=false,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    end_marker::Bool=true,
)
    convert || @warn "Arrow 3 Flight always returns public-domain materialized values" maxlog = 1
    collected = _collect_messages(messages)
    bytes = streambytes(
        collected;
        schema=schema,
        alignment=alignment,
        end_marker=end_marker,
    )
    value = ArrowParent.Table(bytes; mmap=false)
    return include_app_metadata ?
           (table=value, app_metadata=_record_app_metadata(collected)) : value
end
