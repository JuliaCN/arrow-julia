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

module SQL

import ProtoBuf as PB

using ..Flight: Protocol, _decodeprotocolbytes, _protocolbytes

export FLIGHT_SQL_TYPE_URL_PREFIX,
    AnyMessage,
    DoPutUpdateResult,
    Generated,
    action,
    actiontype,
    anymessage,
    anypayload,
    anytypeurl,
    commanddescriptor,
    decodeany,
    doputpreparedstatementhandle,
    doputpreparedstatementresult,
    doputupdatecount,
    doputupdateresult,
    typeurl

const FLIGHT_SQL_TYPE_URL_PREFIX = "type.googleapis.com/arrow.flight.protocol.sql."
const FlightProtocol = Protocol
const Generated = Protocol.sql

"""
    Arrow.Flight.SQL.DoPutUpdateResult

Generated Flight SQL `DoPutUpdateResult` payload type.
"""
const DoPutUpdateResult = Generated.DoPutUpdateResult
const DoPutPreparedStatementResult = Generated.DoPutPreparedStatementResult

"""
    Arrow.Flight.SQL.AnyMessage

Minimal `google.protobuf.Any` representation used by the Flight SQL
command/action envelope helpers.
"""
struct AnyMessage
    type_url::String
    value::Vector{UInt8}
end

PB.default_values(::Type{AnyMessage}) = (; type_url="", value=UInt8[])
PB.field_numbers(::Type{AnyMessage}) = (; type_url=1, value=2)

function PB.decode(d::PB.AbstractProtoDecoder, ::Type{<:AnyMessage})
    type_url = ""
    value = UInt8[]
    while !PB.message_done(d)
        field_number, wire_type = PB.decode_tag(d)
        if field_number == 1
            type_url = PB.decode(d, String)
        elseif field_number == 2
            value = PB.decode(d, Vector{UInt8})
        else
            Base.skip(d, wire_type)
        end
    end
    return AnyMessage(type_url, value)
end

function PB.encode(e::PB.AbstractProtoEncoder, x::AnyMessage)
    initpos = position(e.io)
    !isempty(x.type_url) && PB.encode(e, 1, x.type_url)
    !isempty(x.value) && PB.encode(e, 2, x.value)
    return position(e.io) - initpos
end

function PB._encoded_size(x::AnyMessage)
    encoded_size = 0
    !isempty(x.type_url) && (encoded_size += PB._encoded_size(x.type_url, 1))
    !isempty(x.value) && (encoded_size += PB._encoded_size(x.value, 2))
    return encoded_size
end

_bytes(payload::AbstractVector{UInt8}; own::Bool=true) = Vector{UInt8}(payload)
_bytes(payload::Vector{UInt8}; own::Bool=true) = own ? Base.copy(payload) : payload
_messagename(message) = String(nameof(typeof(message)))

"""
    Arrow.Flight.SQL.typeurl(message_name)

Return the canonical Flight SQL protobuf type URL for `message_name`. Fully
qualified type URLs containing `/` are passed through unchanged.
"""
function typeurl(message_name::AbstractString)
    name = String(message_name)
    isempty(name) && throw(ArgumentError("Flight SQL message name must not be empty"))
    return occursin("/", name) ? name : string(FLIGHT_SQL_TYPE_URL_PREFIX, name)
end

"""
    Arrow.Flight.SQL.anymessage(message_name, payload=UInt8[])

Build a minimal `google.protobuf.Any` message for a serialized Flight SQL
command or action payload.
"""
function anymessage(
    message_name::AbstractString,
    payload::AbstractVector{UInt8}=UInt8[];
    copy_payload::Bool=true,
)
    return AnyMessage(typeurl(message_name), _bytes(payload; own=copy_payload))
end

anymessage(message) =
    anymessage(_messagename(message), _protocolbytes(message); copy_payload=false)

"""
    Arrow.Flight.SQL.decodeany(payload)

Decode serialized `google.protobuf.Any` bytes produced by
[`Arrow.Flight.SQL.anymessage`](@ref).
"""
function decodeany(payload::AbstractVector{UInt8})
    return _decodeprotocolbytes(AnyMessage, payload)
end

anytypeurl(payload::AbstractVector{UInt8}) = decodeany(payload).type_url
anypayload(payload::AbstractVector{UInt8}) = decodeany(payload).value

"""
    Arrow.Flight.SQL.commanddescriptor(message_name, payload=UInt8[])

Build a Flight `CMD` descriptor whose `cmd` field contains a Flight SQL
`google.protobuf.Any` command payload.
"""
function commanddescriptor(
    message_name::AbstractString,
    payload::AbstractVector{UInt8}=UInt8[],
    ;
    copy_payload::Bool=true,
)
    descriptor_type = FlightProtocol.var"FlightDescriptor.DescriptorType"
    return FlightProtocol.FlightDescriptor(
        descriptor_type.CMD,
        _protocolbytes(anymessage(message_name, payload; copy_payload=copy_payload)),
        String[],
    )
end

commanddescriptor(message) =
    commanddescriptor(_messagename(message), _protocolbytes(message); copy_payload=false)

function _default_action_type(message_name::AbstractString)
    name = String(message_name)
    name = startswith(name, "Action") ? name[(firstindex(name) + 6):end] : name
    return endswith(name, "Request") ? name[begin:(lastindex(name) - 7)] : name
end

"""
    Arrow.Flight.SQL.actiontype(message_name)

Derive the Flight action type for a Flight SQL action request name such as
`ActionCreatePreparedStatementRequest`.
"""
function actiontype(message_name::AbstractString)
    return _default_action_type(message_name)
end

"""
    Arrow.Flight.SQL.action(message_name, payload=UInt8[]; type=nothing)

Build a Flight action whose body contains a Flight SQL `google.protobuf.Any`
payload. By default the action type is derived from `message_name`.
"""
function action(
    message_name::AbstractString,
    payload::AbstractVector{UInt8}=UInt8[];
    type::Union{Nothing,AbstractString}=nothing,
    copy_payload::Bool=true,
)
    action_type = isnothing(type) ? actiontype(message_name) : String(type)
    isempty(action_type) && throw(ArgumentError("Flight SQL action type must not be empty"))
    return FlightProtocol.Action(
        action_type,
        _protocolbytes(anymessage(message_name, payload; copy_payload=copy_payload)),
    )
end

action(message; type::Union{Nothing,AbstractString}=nothing) =
    action(_messagename(message), _protocolbytes(message); type=type, copy_payload=false)

"""
    Arrow.Flight.SQL.doputupdateresult(record_count)

Build a Flight `PutResult` whose `app_metadata` contains a Flight SQL
`DoPutUpdateResult`.
"""
function doputupdateresult(record_count::Integer)
    record_count >= -1 || throw(
        ArgumentError("Flight SQL DoPut update record count must be -1 or non-negative"),
    )
    return FlightProtocol.PutResult(_protocolbytes(DoPutUpdateResult(Int64(record_count))))
end

"""
    Arrow.Flight.SQL.doputupdatecount(result)

Decode the Flight SQL update row count from a Flight `PutResult`.
"""
function doputupdatecount(result::FlightProtocol.PutResult)
    return _decodeprotocolbytes(DoPutUpdateResult, result.app_metadata).record_count
end

"""
    Arrow.Flight.SQL.doputpreparedstatementresult(handle)

Build a Flight `PutResult` whose `app_metadata` contains the optional Flight SQL
prepared-statement query DoPut response.
"""
function doputpreparedstatementresult(handle::AbstractVector{UInt8})
    return FlightProtocol.PutResult(
        _protocolbytes(DoPutPreparedStatementResult(_bytes(handle))),
    )
end

"""
    Arrow.Flight.SQL.doputpreparedstatementhandle(result)

Decode the optional updated prepared-statement handle from a Flight SQL DoPut
prepared-statement query `PutResult`.
"""
function doputpreparedstatementhandle(result::FlightProtocol.PutResult)
    return _decodeprotocolbytes(DoPutPreparedStatementResult, result.app_metadata).prepared_statement_handle
end

end # module SQL
