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

function _flight_framed_message(message::Protocol.FlightData, limits, budget)
    _validate_flight_message_limits(message, limits)
    isempty(message.data_header) && begin
        isempty(message.data_body) || throw(
            ArgumentError("FlightData message has a body but no Arrow IPC header"),
        )
        return nothing
    end
    ArrowParent._chargevector!(
        budget,
        UInt8,
        length(message.data_header),
        "Flight IPC metadata",
    )
    header = Vector{UInt8}(message.data_header)
    version, header_type, features, _ =
        ArrowParent._verify_ipc_metadata_budgeted(header, limits, budget)
    ipc_message = ArrowParent.FB.getrootas(ArrowParent.Meta.Message, header, 0)
    bodylen = Int64(ipc_message.bodyLength)
    0 <= bodylen <= limits.max_body_bytes || throw(
        ArrowParent.ValidationError(
            "body length $bodylen outside [0, $(limits.max_body_bytes)]",
        ),
    )
    bodylen % DEFAULT_IPC_ALIGNMENT == 0 ||
        throw(ArrowParent.ValidationError("body length $bodylen is not 8-byte aligned"))
    bodylen == length(message.data_body) || throw(
        ArgumentError(
            "FlightData body length $(length(message.data_body)) does not match Arrow IPC header $bodylen",
        ),
    )
    body = message.data_body
    return ArrowParent.FramedMessage(
        ipc_message,
        ArrowParent.AC.BufferSlice(ArrowParent.AC.heapregion(body), 0, bodylen),
        version,
        header_type,
        features,
    )
end

function _injected_schema_message(schema, alignment, limits, budget)
    bytes = schemaipc(schema; alignment=alignment)
    messages = ArrowParent._framemessages(
        ArrowParent.AC.heapregion(bytes),
        limits,
        Base.ENDIAN_BOM,
        budget,
    )
    length(messages) == 1 ||
        throw(ArgumentError("Flight schema must contain exactly one IPC schema message"))
    return only(messages)
end

mutable struct FlightIPCSource
    messages::Any
    decoder::ArrowParent.IPCMessageDecoder
    message_count::Int
    app_metadata::Dict{Int,Vector{UInt8}}
    names::Vector{Symbol}
    schema::Tables.Schema
    arrowtypes::ArrowParent._ArrowTypesContext
    arrowtypeslock::ReentrantLock
    exhausted::Bool
    pulling::Bool
end

mutable struct FlightMessageCursor
    iterator::Any
    state::Any
    started::Bool
    done::Bool
end

FlightMessageCursor(iterator) = FlightMessageCursor(iterator, nothing, false, false)

function _cursor_next!(cursor::FlightMessageCursor)
    cursor.done && return nothing
    item =
        cursor.started ? iterate(cursor.iterator, cursor.state) : iterate(cursor.iterator)
    cursor.started = true
    if item === nothing
        cursor.done = true
        return nothing
    end
    message, state = item
    cursor.state = state
    return message
end

function _materialize_flight_batch(source::FlightIPCSource, batch)
    decoder = source.decoder
    fields = decoder.corefields
    arrowtypes = source.arrowtypes
    lock(source.arrowtypeslock)
    try
        ArrowParent._chargevector!(
            decoder.budget,
            AbstractVector,
            length(fields),
            "table columns",
        )
        columns = AbstractVector[
            ArrowParent._facadesinglecolumn(field, batch.columns[index], arrowtypes) for
            (index, field) in enumerate(fields)
        ]
        pools = ArrowParent._retaineddictpools(fields, (batch,), arrowtypes)
        return ArrowParent._table(
            source.names,
            columns,
            decoder.schema,
            ArrowParent.AC.OwnerRegion[],
            Int(batch.nrows),
            pools,
            decoder.budget,
        )
    finally
        unlock(source.arrowtypeslock)
    end
end

function _retain_app_metadata!(source::FlightIPCSource, message::Protocol.FlightData)
    record_id = source.decoder.records_seen + 1
    ArrowParent._chargevector!(
        source.decoder.budget,
        UInt8,
        length(message.app_metadata),
        "retained Flight application metadata",
    )
    ArrowParent._chargedictentry!(
        source.decoder.budget,
        Int,
        Vector{UInt8},
        "pending Flight application metadata",
    )
    source.app_metadata[record_id] = Vector{UInt8}(message.app_metadata)
    return nothing
end

function _release_flight_source!(source::FlightIPCSource)
    close(source.decoder)
    empty!(source.decoder.batchslots)
    empty!(source.decoder.pending)
    empty!(source.decoder.dictionaries)
    empty!(source.decoder.validated_dictionaries)
    empty!(source.app_metadata)
    source.exhausted = true
    return nothing
end

function Base.close(source::FlightIPCSource)
    source.pulling && throw(
        Base.ConcurrencyViolationError(
            "FlightStream supports only one active iteration or close call",
        ),
    )
    source.pulling = true
    try
        return _release_flight_source!(source)
    finally
        source.pulling = false
    end
end

function _next_flight_batch!(source::FlightIPCSource)
    source.pulling && throw(
        Base.ConcurrencyViolationError(
            "FlightStream supports only one active iteration call",
        ),
    )
    source.pulling = true
    try
        while true
            ready = ArrowParent.takebatch!(source.decoder)
            if ready !== nothing
                batch, record_id = ready
                metadata = pop!(source.app_metadata, record_id)
                return _materialize_flight_batch(source, batch), metadata
            end
            source.exhausted && return nothing
            message = _cursor_next!(source.messages)
            if message === nothing
                ArrowParent.finish!(source.decoder)
                _release_flight_source!(source)
                continue
            end
            source.message_count < source.decoder.limits.max_messages || throw(
                ArrowParent.ValidationError(
                    "Flight message count exceeds limit $(source.decoder.limits.max_messages)",
                ),
            )
            source.message_count += 1
            framed = _flight_framed_message(
                message,
                source.decoder.limits,
                source.decoder.budget,
            )
            framed === nothing && continue
            if framed.msg.header isa ArrowParent.Meta.RecordBatch
                _retain_app_metadata!(source, message)
            end
            ArrowParent.pushmessage!(source.decoder, framed)
        end
    catch
        _release_flight_source!(source)
        rethrow()
    finally
        source.pulling = false
    end
end
