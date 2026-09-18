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

const ServerHeaderPair = HeaderPair

const FLIGHT_STATUS_CANCELLED = Int32(1)
const FLIGHT_STATUS_DEADLINE_EXCEEDED = Int32(4)

Base.@kwdef struct ServerCallContext
    headers::Vector{ServerHeaderPair} = ServerHeaderPair[]
    request_id::Union{Nothing,String} = nothing
    method::Union{Nothing,String} = nothing
    authority::Union{Nothing,String} = nothing
    peer::Union{Nothing,String} = nothing
    secure::Bool = false
    deadline::Any = nothing
    trace_context::Union{Nothing,Vector{UInt8}} = nothing
    payload::Any = nothing
    is_cancelled::Function = () -> false
    remaining_time::Function = () -> nothing
    set_response_header::Function = (name, value) -> nothing
    set_response_trailer::Function = (name, value) -> nothing
end

iscallcancelled(context::ServerCallContext) = Bool(context.is_cancelled())
callremainingtime(context::ServerCallContext) = context.remaining_time()

function setresponseheader!(
    context::ServerCallContext,
    name::AbstractString,
    value::HeaderValue,
)
    context.set_response_header(String(name), value)
    return context
end

function setresponsetrailer!(
    context::ServerCallContext,
    name::AbstractString,
    value::HeaderValue,
)
    context.set_response_trailer(String(name), value)
    return context
end

function checkcall(context::ServerCallContext)
    iscallcancelled(context) && throw(
        FlightStatusError(FLIGHT_STATUS_CANCELLED, "Arrow Flight request was cancelled"),
    )
    remaining = callremainingtime(context)
    !isnothing(remaining) &&
        remaining <= 0 &&
        throw(
            FlightStatusError(
                FLIGHT_STATUS_DEADLINE_EXCEEDED,
                "Arrow Flight request deadline was exceeded",
            ),
        )
    return nothing
end

Base.@kwdef struct Service
    handshake::Union{Nothing,Function} = nothing
    listflights::Union{Nothing,Function} = nothing
    getflightinfo::Union{Nothing,Function} = nothing
    pollflightinfo::Union{Nothing,Function} = nothing
    getschema::Union{Nothing,Function} = nothing
    doget::Union{Nothing,Function} = nothing
    doput::Union{Nothing,Function} = nothing
    doexchange::Union{Nothing,Function} = nothing
    doaction::Union{Nothing,Function} = nothing
    listactions::Union{Nothing,Function} = nothing
end

function callheader(context::ServerCallContext, name::AbstractString)
    needle = lowercase(String(name))
    for (header_name, header_value) in context.headers
        lowercase(header_name) == needle && return header_value
    end
    return nothing
end
