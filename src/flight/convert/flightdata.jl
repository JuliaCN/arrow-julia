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

struct FlightAppMetadataSource{T,M}
    source::T
    app_metadata::M
end

ArrowParent.getmetadata(x::FlightAppMetadataSource) = ArrowParent.getmetadata(x.source)
Tables.partitions(x::FlightAppMetadataSource) = Tables.partitions(x.source)

"""
    Arrow.Flight.withappmetadata(source; app_metadata)

Carry batch-wise Flight application metadata alongside a Tables.jl source.
The wrapper changes only Flight transport metadata; Arrow 3 owns all IPC
schema and value conversion.
"""
withappmetadata(source; app_metadata) =
    isnothing(app_metadata) ? source : FlightAppMetadataSource(source, app_metadata)

function _unwrap_app_metadata_source(source, app_metadata)
    source isa FlightAppMetadataSource || return source, app_metadata
    isnothing(app_metadata) || throw(
        ArgumentError(
            "app_metadata cannot be provided both via Arrow.Flight.withappmetadata(...) and the app_metadata keyword",
        ),
    )
    return source.source, source.app_metadata
end

_is_app_metadata_value(x) = x isa AbstractString || x isa AbstractVector{UInt8}

function _normalize_app_metadata_value(value)
    value === nothing && return UInt8[]
    value isa AbstractString && return Vector{UInt8}(codeunits(value))
    value isa AbstractVector{UInt8} && return Vector{UInt8}(value)
    throw(
        ArgumentError(
            "app_metadata entries must be AbstractString, AbstractVector{UInt8}, or nothing",
        ),
    )
end

mutable struct _FlightDataSink{S,M}
    sink::S
    descriptor::Union{Nothing,Protocol.FlightDescriptor}
    app_metadata::M
    app_metadata_state::Any
    app_metadata_started::Bool
end

function _FlightDataSink(sink, descriptor, app_metadata)
    values =
        isnothing(app_metadata) ? nothing :
        _is_app_metadata_value(app_metadata) ? (app_metadata,) : app_metadata
    return _FlightDataSink(sink, descriptor, values, nothing, false)
end

_emit_flightdata!(sink::AbstractVector, message::Protocol.FlightData) = push!(sink, message)
_emit_flightdata!(sink, message::Protocol.FlightData) = put!(sink, message)

function _next_app_metadata!(sink::_FlightDataSink)
    sink.app_metadata === nothing && return UInt8[]
    item =
        sink.app_metadata_started ?
        iterate(sink.app_metadata, sink.app_metadata_state) : iterate(sink.app_metadata)
    item === nothing && throw(
        ArgumentError("app_metadata was exhausted before all record batches were emitted"),
    )
    value, state = item
    sink.app_metadata_state = state
    sink.app_metadata_started = true
    return _normalize_app_metadata_value(value)
end

function _finish_app_metadata!(sink::_FlightDataSink)
    sink.app_metadata === nothing && return nothing
    item =
        sink.app_metadata_started ?
        iterate(sink.app_metadata, sink.app_metadata_state) : iterate(sink.app_metadata)
    item === nothing ||
        throw(ArgumentError("app_metadata contains more entries than record batches"))
    return nothing
end

function _drain_flightdata!(sink::_FlightDataSink, bytes::Vector{UInt8})
    for part in _split_ipc_stream(bytes)
        part_metadata =
            part.kind isa ArrowParent.Meta.RecordBatch ? _next_app_metadata!(sink) : UInt8[]
        _emit_flightdata!(
            sink.sink,
            Protocol.FlightData(
                sink.descriptor,
                part.header,
                part_metadata,
                part.body,
            ),
        )
        sink.descriptor = nothing
    end
    return length(bytes)
end

function _putflightdata!(
    sink,
    source;
    descriptor::Union{Nothing,Protocol.FlightDescriptor}=nothing,
    compress=nothing,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    metadata=nothing,
    colmetadata=nothing,
    app_metadata=nothing,
)
    alignment == DEFAULT_IPC_ALIGNMENT || throw(
        ArgumentError("Arrow 3 Flight IPC uses the standard 8-byte alignment"),
    )
    source, app_metadata = _unwrap_app_metadata_source(source, app_metadata)
    output = _FlightDataSink(sink, descriptor, app_metadata)
    buffer = IOBuffer()
    writer = ArrowParent.Writer(
        buffer;
        file=false,
        compress=compress,
        metadata=metadata,
        colmetadata=colmetadata,
    )
    try
        wrote_partition = false
        for partition in Tables.partitions(source)
            ArrowParent.write(writer, partition)
            _drain_flightdata!(output, take!(buffer))
            wrote_partition = true
        end
        wrote_partition || throw(ArgumentError("cannot encode an empty Flight source"))
        close(writer)
        _drain_flightdata!(output, take!(buffer))
        _finish_app_metadata!(output)
    catch
        try
            close(writer)
        catch
            # Preserve the encoding, metadata, or downstream sink error.
        end
        rethrow()
    end
    return sink
end

function _flightdata_messages(
    source;
    descriptor::Union{Nothing,Protocol.FlightDescriptor}=nothing,
    compress=nothing,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    metadata=nothing,
    colmetadata=nothing,
    app_metadata=nothing,
)
    messages = Protocol.FlightData[]
    _putflightdata!(
        messages,
        source;
        descriptor=descriptor,
        compress=compress,
        alignment=alignment,
        metadata=metadata,
        colmetadata=colmetadata,
        app_metadata=app_metadata,
    )
    return messages
end

"""
    Arrow.Flight.flightdata(source; kwargs...)

Encode a Tables.jl source with Arrow 3's canonical IPC writer, then expose its
schema, dictionary, and record-batch messages as Flight `FlightData` values.
"""
flightdata(source; kwargs...) = _flightdata_messages(source; kwargs...)

"""
    Arrow.Flight.putflightdata!(sink, source; close=false, kwargs...)

Incrementally write Arrow 3-backed `FlightData` messages to a channel-like
sink. Encoding and downstream backpressure are bounded to one source partition:
the schema and each record batch are published before the next partition is
requested. [`flightdata`](@ref) is the collecting convenience API.
"""
function putflightdata!(sink, source; close::Bool=false, kwargs...)
    try
        _putflightdata!(sink, source; kwargs...)
    finally
        close && Base.close(sink)
    end
    return sink
end
