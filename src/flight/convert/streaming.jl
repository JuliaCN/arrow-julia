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

streambytes(message::Protocol.FlightData; kwargs...) = streambytes((message,); kwargs...)

function _validate_flight_message_limits(message::Protocol.FlightData, limits)
    header_bytes = length(message.data_header)
    header_bytes <= limits.max_metadata_bytes || throw(
        ArrowParent.ValidationError(
            "FlightData IPC header length $(header_bytes) exceeds limit $(limits.max_metadata_bytes)",
        ),
    )
    body_bytes = length(message.data_body)
    body_bytes <= limits.max_body_bytes || throw(
        ArrowParent.ValidationError(
            "FlightData body length $(body_bytes) exceeds limit $(limits.max_body_bytes)",
        ),
    )
    app_metadata_bytes = length(message.app_metadata)
    app_metadata_bytes <= limits.max_metadata_bytes || throw(
        ArrowParent.ValidationError(
            "FlightData application metadata length $(app_metadata_bytes) exceeds limit $(limits.max_metadata_bytes)",
        ),
    )
    return nothing
end

function _charge_rebuilt_stream!(budget, amount::Integer, what::AbstractString)
    ArrowParent._charge!(budget, Int64(amount), what)
    return nothing
end

function _write_budgeted_schema!(io::IO, schema, alignment, budget)
    bytes = schemaipc(schema; alignment=alignment)
    _charge_rebuilt_stream!(budget, length(bytes), "rebuilt Flight schema")
    Base.write(io, bytes)
    return nothing
end

function _write_budgeted_flight_message!(
    io::IO,
    header,
    body,
    alignment,
    ipc_message,
    budget,
)
    padded_header_bytes = length(header) + _padding_length(length(header), alignment)
    _charge_rebuilt_stream!(budget, 8, "rebuilt Flight IPC prefix")
    _charge_rebuilt_stream!(budget, padded_header_bytes, "rebuilt Flight IPC metadata")
    _charge_rebuilt_stream!(budget, length(body), "rebuilt Flight IPC body")
    _write_framed_message(io, header, body, alignment, ipc_message)
    return nothing
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

function _rebuild_stream(
    messages;
    schema=nothing,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    end_marker::Bool=true,
    capture_metadata::Bool=false,
    limits=ArrowParent.Limits(),
)
    ArrowParent._validatelimits(limits)
    io = IOBuffer()
    metadata = Vector{Vector{UInt8}}()
    budget = ArrowParent.AllocationBudget(limits.max_total_allocated_bytes)
    message_count = 0
    has_schema = false
    injected_schema = false
    for message in messages
        message_count < limits.max_messages || throw(
            ArrowParent.ValidationError(
                "Flight message count exceeds limit $(limits.max_messages)",
            ),
        )
        message_count += 1
        _validate_flight_message_limits(message, limits)
        if isempty(message.data_header)
            isempty(message.data_body) || throw(
                ArgumentError("FlightData message has a body but no Arrow IPC header"),
            )
            continue
        end
        header = message.data_header
        ipc_message = _flight_message_header(header)
        kind = ipc_message.header
        if kind isa ArrowParent.Meta.Schema
            has_schema = true
        elseif !has_schema && !injected_schema
            schema === nothing && throw(ArgumentError(_missing_schema_message()))
            _write_budgeted_schema!(io, schema, alignment, budget)
            injected_schema = true
        end
        _write_budgeted_flight_message!(
            io,
            header,
            message.data_body,
            alignment,
            ipc_message,
            budget,
        )
        if capture_metadata && kind isa ArrowParent.Meta.RecordBatch
            ArrowParent._chargevector!(
                budget,
                UInt8,
                length(message.app_metadata),
                "retained Flight application metadata",
            )
            push!(metadata, Vector{UInt8}(message.app_metadata))
        end
    end
    if !has_schema && !injected_schema
        schema === nothing && throw(ArgumentError(_missing_schema_message()))
        _write_budgeted_schema!(io, schema, alignment, budget)
    end
    if end_marker
        _charge_rebuilt_stream!(budget, 8, "rebuilt Flight IPC end marker")
        _write_end_marker(io)
    end
    return take!(io), metadata, budget
end

function streambytes(
    messages;
    schema=nothing,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    end_marker::Bool=true,
    limits=ArrowParent.Limits(),
)
    bytes, _, _ = _rebuild_stream(
        messages;
        schema=schema,
        alignment=alignment,
        end_marker=end_marker,
        limits=limits,
    )
    return bytes
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
    return (table=table, app_metadata=x.stream.app_metadata[index]), (next_state, index + 1)
end

function _flight_stream(
    messages;
    schema=nothing,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    end_marker::Bool=true,
    limits=ArrowParent.Limits(),
)
    bytes, metadata, budget = _rebuild_stream(
        messages;
        schema=schema,
        alignment=alignment,
        end_marker=end_marker,
        capture_metadata=true,
        limits=limits,
    )
    opened = ArrowParent._openbytes(bytes; limits=limits, budget=budget)
    stream = ArrowParent.Stream(opened; mmap=false, limits=limits)
    table_schema = _stream_schema(stream)
    length(metadata) == length(stream) || throw(
        ArgumentError(
            "Flight record-batch metadata count does not match decoded Arrow batch count",
        ),
    )
    return FlightStream(stream, table_schema, metadata)
end

"""
    Arrow.Flight.stream(messages; schema=nothing, convert=true,
                        include_app_metadata=false, limits=Arrow.Limits())

Decode Flight `FlightData` through Arrow 3's validated IPC stream reader. The
Flight layer owns only framing and application metadata; schema, dictionary,
compression, ownership, and materialization semantics come from Arrow 3.
`limits` governs both Flight framing and the delegated Arrow IPC decode with
one cumulative allocation budget.
"""
function stream(
    messages;
    schema=nothing,
    convert::Bool=true,
    include_app_metadata::Bool=false,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    end_marker::Bool=true,
    limits=ArrowParent.Limits(),
)
    convert ||
        @warn "Arrow 3 Flight always returns public-domain materialized values" maxlog = 1
    value = _flight_stream(
        messages;
        schema=schema,
        alignment=alignment,
        end_marker=end_marker,
        limits=limits,
    )
    return include_app_metadata ? FlightStreamWithAppMetadata(value) : value
end

"""
    Arrow.Flight.table(messages; schema=nothing, convert=true,
                       include_app_metadata=false, limits=Arrow.Limits())

Materialize Flight `FlightData` through Arrow 3's validated `Arrow.Table`
facade. Optional Flight application metadata is returned batch-for-batch.
`limits` governs both Flight framing and the delegated Arrow IPC decode with
one cumulative allocation budget.
"""
function table(
    messages;
    schema=nothing,
    convert::Bool=true,
    include_app_metadata::Bool=false,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    end_marker::Bool=true,
    limits=ArrowParent.Limits(),
)
    convert ||
        @warn "Arrow 3 Flight always returns public-domain materialized values" maxlog = 1
    bytes, metadata, budget = _rebuild_stream(
        messages;
        schema=schema,
        alignment=alignment,
        end_marker=end_marker,
        capture_metadata=include_app_metadata,
        limits=limits,
    )
    opened = ArrowParent._openbytes(bytes; limits=limits, budget=budget)
    value = ArrowParent.Table(opened; mmap=false, limits=limits)
    return include_app_metadata ? (table=value, app_metadata=metadata) : value
end
