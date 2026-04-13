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

include("client/constants.jl")
include("client/locations.jl")
include("client/types.jl")
include("client/headers.jl")

function _flight_client_extension_required(surface::AbstractString)
    throw(
        ArgumentError(
            "Arrow.Flight.$surface requires the optional gRPC client extension; load gRPCClient before using the Flight client surface",
        ),
    )
end

_start_flight_producer(f::Function) = errormonitor(@async f())

FlightService_Handshake_Client(args...; kwargs...) =
    _flight_client_extension_required("FlightService_Handshake_Client")
FlightService_ListFlights_Client(args...; kwargs...) =
    _flight_client_extension_required("FlightService_ListFlights_Client")
FlightService_GetFlightInfo_Client(args...; kwargs...) =
    _flight_client_extension_required("FlightService_GetFlightInfo_Client")
FlightService_PollFlightInfo_Client(args...; kwargs...) =
    _flight_client_extension_required("FlightService_PollFlightInfo_Client")
FlightService_GetSchema_Client(args...; kwargs...) =
    _flight_client_extension_required("FlightService_GetSchema_Client")
FlightService_DoGet_Client(args...; kwargs...) =
    _flight_client_extension_required("FlightService_DoGet_Client")
FlightService_DoPut_Client(args...; kwargs...) =
    _flight_client_extension_required("FlightService_DoPut_Client")
FlightService_DoExchange_Client(args...; kwargs...) =
    _flight_client_extension_required("FlightService_DoExchange_Client")
FlightService_DoAction_Client(args...; kwargs...) =
    _flight_client_extension_required("FlightService_DoAction_Client")
FlightService_ListActions_Client(args...; kwargs...) =
    _flight_client_extension_required("FlightService_ListActions_Client")

_handshake_client(args...; kwargs...) =
    _flight_client_extension_required("_handshake_client")
_listflights_client(args...; kwargs...) =
    _flight_client_extension_required("_listflights_client")
_getflightinfo_client(args...; kwargs...) =
    _flight_client_extension_required("_getflightinfo_client")
_pollflightinfo_client(args...; kwargs...) =
    _flight_client_extension_required("_pollflightinfo_client")
_getschema_client(args...; kwargs...) =
    _flight_client_extension_required("_getschema_client")
_doget_client(args...; kwargs...) = _flight_client_extension_required("_doget_client")
_doput_client(args...; kwargs...) = _flight_client_extension_required("_doput_client")
_doexchange_client(args...; kwargs...) =
    _flight_client_extension_required("_doexchange_client")
_doaction_client(args...; kwargs...) = _flight_client_extension_required("_doaction_client")
_listactions_client(args...; kwargs...) =
    _flight_client_extension_required("_listactions_client")

authenticate(args...; kwargs...) = _flight_client_extension_required("authenticate")

handshake(client::Client, args...; kwargs...) =
    _flight_client_extension_required("handshake")
listflights(client::Client, args...; kwargs...) =
    _flight_client_extension_required("listflights")
getflightinfo(client::Client, args...; kwargs...) =
    _flight_client_extension_required("getflightinfo")
pollflightinfo(client::Client, args...; kwargs...) =
    _flight_client_extension_required("pollflightinfo")
getschema(client::Client, args...; kwargs...) =
    _flight_client_extension_required("getschema")
doget(client::Client, args...; kwargs...) = _flight_client_extension_required("doget")
doput(client::Client, args...; kwargs...) = _flight_client_extension_required("doput")
doexchange(client::Client, args...; kwargs...) =
    _flight_client_extension_required("doexchange")
doaction(client::Client, args...; kwargs...) = _flight_client_extension_required("doaction")
listactions(client::Client, args...; kwargs...) =
    _flight_client_extension_required("listactions")
