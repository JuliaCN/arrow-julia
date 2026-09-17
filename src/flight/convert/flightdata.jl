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

function _record_app_metadata_values(app_metadata, count::Int)
    isnothing(app_metadata) && return [UInt8[] for _ = 1:count]
    values = _is_app_metadata_value(app_metadata) ? (app_metadata,) : app_metadata
    normalized = Vector{Vector{UInt8}}()
    for value in values
        push!(normalized, _normalize_app_metadata_value(value))
    end
    length(normalized) == count || throw(
        ArgumentError(
            length(normalized) < count ?
            "app_metadata was exhausted before all record batches were emitted" :
            "app_metadata contains more entries than source partitions",
        ),
    )
    return normalized
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
    alignment == DEFAULT_IPC_ALIGNMENT || throw(
        ArgumentError("Arrow 3 Flight IPC uses the standard 8-byte alignment"),
    )
    source, app_metadata = _unwrap_app_metadata_source(source, app_metadata)
    bytes = ArrowParent._writebytes(
        source;
        file=false,
        compress=compress,
        metadata=metadata,
        colmetadata=colmetadata,
    )
    parts = _split_ipc_stream(bytes)
    record_count = count(part -> part.kind isa ArrowParent.Meta.RecordBatch, parts)
    record_metadata = _record_app_metadata_values(app_metadata, record_count)
    metadata_index = 1
    descriptor_pending = descriptor
    messages = Protocol.FlightData[]
    for part in parts
        part_metadata = if part.kind isa ArrowParent.Meta.RecordBatch
            value = record_metadata[metadata_index]
            metadata_index += 1
            value
        else
            UInt8[]
        end
        push!(
            messages,
            Protocol.FlightData(
                descriptor_pending,
                part.header,
                part_metadata,
                part.body,
            ),
        )
        descriptor_pending = nothing
    end
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

Write Arrow 3-backed `FlightData` messages to a channel-like sink. Message
construction shares the same validated IPC path as [`flightdata`](@ref).
"""
function putflightdata!(sink, source; close::Bool=false, kwargs...)
    try
        for message in _flightdata_messages(source; kwargs...)
            put!(sink, message)
        end
    finally
        close && Base.close(sink)
    end
    return sink
end
