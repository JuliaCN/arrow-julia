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

const CANCEL_FLIGHT_INFO_ACTION_TYPE = "CancelFlightInfo"
const CANCEL_FLIGHT_INFO_ACTION_DESCRIPTION = "Cancel a FlightInfo query"

function _protocolbytes(message)
    io = IOBuffer()
    encoder = ProtoBuf.ProtoEncoder(io)
    ProtoBuf.encode(encoder, message)
    return take!(io)
end

function _decodeprotocolbytes(::Type{T}, payload::AbstractVector{UInt8}) where {T}
    decoder = ProtoBuf.ProtoDecoder(IOBuffer(payload))
    return ProtoBuf.decode(decoder, T)
end

function _actiontype(action::Protocol.Action)
    return getfield(action, Symbol("#type"))
end

"""
    cancelflightinfoactiontype([description])

Return the official Flight `ActionType` entry for the `CancelFlightInfo`
action.
"""
function cancelflightinfoactiontype(
    description::AbstractString=CANCEL_FLIGHT_INFO_ACTION_DESCRIPTION,
)
    return Protocol.ActionType(CANCEL_FLIGHT_INFO_ACTION_TYPE, String(description))
end

"""
    cancelflightinfoaction(info::Protocol.FlightInfo)
    cancelflightinfoaction(request::Protocol.CancelFlightInfoRequest)

Build a Flight `Action` whose type is `CancelFlightInfo` and whose body stores
the protobuf-encoded `CancelFlightInfoRequest`.
"""
function cancelflightinfoaction(info::Protocol.FlightInfo)
    return cancelflightinfoaction(Protocol.CancelFlightInfoRequest(info))
end

function cancelflightinfoaction(request::Protocol.CancelFlightInfoRequest)
    return Protocol.Action(CANCEL_FLIGHT_INFO_ACTION_TYPE, _protocolbytes(request))
end

"""
    cancelflightinforequest(action::Protocol.Action)

Validate and decode a `CancelFlightInfo` action body as a
`CancelFlightInfoRequest`.
"""
function cancelflightinforequest(action::Protocol.Action)
    actual = _actiontype(action)
    actual == CANCEL_FLIGHT_INFO_ACTION_TYPE || throw(
        ArgumentError(
            "expected Flight action type $(CANCEL_FLIGHT_INFO_ACTION_TYPE), got $(actual)",
        ),
    )
    return _decodeprotocolbytes(Protocol.CancelFlightInfoRequest, action.body)
end

"""
    cancelflightinforesult(status::Protocol.CancelStatus.T)
    cancelflightinforesult(result::Protocol.Result)

Build a Flight `Result` containing a protobuf-encoded
`CancelFlightInfoResult`, or decode such a result body back to the generated
protocol message.
"""
function cancelflightinforesult(status::Protocol.CancelStatus.T)
    return Protocol.Result(_protocolbytes(Protocol.CancelFlightInfoResult(status)))
end

function cancelflightinforesult(result::Protocol.Result)
    return _decodeprotocolbytes(Protocol.CancelFlightInfoResult, result.body)
end

"""
    cancelflightinfostatus(result::Protocol.Result)

Decode a `CancelFlightInfo` result body and return its `CancelStatus`.
"""
function cancelflightinfostatus(result::Protocol.Result)
    return cancelflightinforesult(result).status
end
