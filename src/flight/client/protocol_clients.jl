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

_resolve_grpc_handle(grpc) = isnothing(grpc) ? gRPCClient.grpc_global_handle() : grpc

function Flight.FlightService_Handshake_Client(
    host,
    port;
    secure=false,
    grpc=nothing,
    deadline=10,
    keepalive=60,
    max_send_message_length=4 * 1024 * 1024,
    max_recieve_message_length=4 * 1024 * 1024,
)
    return gRPCClient.gRPCServiceClient{
        Protocol.HandshakeRequest,
        true,
        Protocol.HandshakeResponse,
        true,
    }(
        host,
        port,
        "/arrow.flight.protocol.FlightService/Handshake";
        secure=secure,
        grpc=_resolve_grpc_handle(grpc),
        deadline=deadline,
        keepalive=keepalive,
        max_send_message_length=max_send_message_length,
        max_recieve_message_length=max_recieve_message_length,
    )
end

function Flight.FlightService_ListFlights_Client(
    host,
    port;
    secure=false,
    grpc=nothing,
    deadline=10,
    keepalive=60,
    max_send_message_length=4 * 1024 * 1024,
    max_recieve_message_length=4 * 1024 * 1024,
)
    return gRPCClient.gRPCServiceClient{Protocol.Criteria,false,Protocol.FlightInfo,true}(
        host,
        port,
        "/arrow.flight.protocol.FlightService/ListFlights";
        secure=secure,
        grpc=_resolve_grpc_handle(grpc),
        deadline=deadline,
        keepalive=keepalive,
        max_send_message_length=max_send_message_length,
        max_recieve_message_length=max_recieve_message_length,
    )
end

function Flight.FlightService_GetFlightInfo_Client(
    host,
    port;
    secure=false,
    grpc=nothing,
    deadline=10,
    keepalive=60,
    max_send_message_length=4 * 1024 * 1024,
    max_recieve_message_length=4 * 1024 * 1024,
)
    return gRPCClient.gRPCServiceClient{
        Protocol.FlightDescriptor,
        false,
        Protocol.FlightInfo,
        false,
    }(
        host,
        port,
        "/arrow.flight.protocol.FlightService/GetFlightInfo";
        secure=secure,
        grpc=_resolve_grpc_handle(grpc),
        deadline=deadline,
        keepalive=keepalive,
        max_send_message_length=max_send_message_length,
        max_recieve_message_length=max_recieve_message_length,
    )
end

function Flight.FlightService_PollFlightInfo_Client(
    host,
    port;
    secure=false,
    grpc=nothing,
    deadline=10,
    keepalive=60,
    max_send_message_length=4 * 1024 * 1024,
    max_recieve_message_length=4 * 1024 * 1024,
)
    return gRPCClient.gRPCServiceClient{
        Protocol.FlightDescriptor,
        false,
        Protocol.PollInfo,
        false,
    }(
        host,
        port,
        "/arrow.flight.protocol.FlightService/PollFlightInfo";
        secure=secure,
        grpc=_resolve_grpc_handle(grpc),
        deadline=deadline,
        keepalive=keepalive,
        max_send_message_length=max_send_message_length,
        max_recieve_message_length=max_recieve_message_length,
    )
end

function Flight.FlightService_GetSchema_Client(
    host,
    port;
    secure=false,
    grpc=nothing,
    deadline=10,
    keepalive=60,
    max_send_message_length=4 * 1024 * 1024,
    max_recieve_message_length=4 * 1024 * 1024,
)
    return gRPCClient.gRPCServiceClient{
        Protocol.FlightDescriptor,
        false,
        Protocol.SchemaResult,
        false,
    }(
        host,
        port,
        "/arrow.flight.protocol.FlightService/GetSchema";
        secure=secure,
        grpc=_resolve_grpc_handle(grpc),
        deadline=deadline,
        keepalive=keepalive,
        max_send_message_length=max_send_message_length,
        max_recieve_message_length=max_recieve_message_length,
    )
end

function Flight.FlightService_DoGet_Client(
    host,
    port;
    secure=false,
    grpc=nothing,
    deadline=10,
    keepalive=60,
    max_send_message_length=4 * 1024 * 1024,
    max_recieve_message_length=4 * 1024 * 1024,
)
    return gRPCClient.gRPCServiceClient{Protocol.Ticket,false,Protocol.FlightData,true}(
        host,
        port,
        "/arrow.flight.protocol.FlightService/DoGet";
        secure=secure,
        grpc=_resolve_grpc_handle(grpc),
        deadline=deadline,
        keepalive=keepalive,
        max_send_message_length=max_send_message_length,
        max_recieve_message_length=max_recieve_message_length,
    )
end

function Flight.FlightService_DoPut_Client(
    host,
    port;
    secure=false,
    grpc=nothing,
    deadline=10,
    keepalive=60,
    max_send_message_length=4 * 1024 * 1024,
    max_recieve_message_length=4 * 1024 * 1024,
)
    return gRPCClient.gRPCServiceClient{Protocol.FlightData,true,Protocol.PutResult,true}(
        host,
        port,
        "/arrow.flight.protocol.FlightService/DoPut";
        secure=secure,
        grpc=_resolve_grpc_handle(grpc),
        deadline=deadline,
        keepalive=keepalive,
        max_send_message_length=max_send_message_length,
        max_recieve_message_length=max_recieve_message_length,
    )
end

function Flight.FlightService_DoExchange_Client(
    host,
    port;
    secure=false,
    grpc=nothing,
    deadline=10,
    keepalive=60,
    max_send_message_length=4 * 1024 * 1024,
    max_recieve_message_length=4 * 1024 * 1024,
)
    return gRPCClient.gRPCServiceClient{Protocol.FlightData,true,Protocol.FlightData,true}(
        host,
        port,
        "/arrow.flight.protocol.FlightService/DoExchange";
        secure=secure,
        grpc=_resolve_grpc_handle(grpc),
        deadline=deadline,
        keepalive=keepalive,
        max_send_message_length=max_send_message_length,
        max_recieve_message_length=max_recieve_message_length,
    )
end

function Flight.FlightService_DoAction_Client(
    host,
    port;
    secure=false,
    grpc=nothing,
    deadline=10,
    keepalive=60,
    max_send_message_length=4 * 1024 * 1024,
    max_recieve_message_length=4 * 1024 * 1024,
)
    return gRPCClient.gRPCServiceClient{Protocol.Action,false,Protocol.Result,true}(
        host,
        port,
        "/arrow.flight.protocol.FlightService/DoAction";
        secure=secure,
        grpc=_resolve_grpc_handle(grpc),
        deadline=deadline,
        keepalive=keepalive,
        max_send_message_length=max_send_message_length,
        max_recieve_message_length=max_recieve_message_length,
    )
end

function Flight.FlightService_ListActions_Client(
    host,
    port;
    secure=false,
    grpc=nothing,
    deadline=10,
    keepalive=60,
    max_send_message_length=4 * 1024 * 1024,
    max_recieve_message_length=4 * 1024 * 1024,
)
    return gRPCClient.gRPCServiceClient{Protocol.Empty,false,Protocol.ActionType,true}(
        host,
        port,
        "/arrow.flight.protocol.FlightService/ListActions";
        secure=secure,
        grpc=_resolve_grpc_handle(grpc),
        deadline=deadline,
        keepalive=keepalive,
        max_send_message_length=max_send_message_length,
        max_recieve_message_length=max_recieve_message_length,
    )
end

Flight._handshake_client(client::Client; kwargs...) = Flight.FlightService_Handshake_Client(
    client.host,
    client.port;
    _rpc_options(client; kwargs...)...,
)

Flight._listflights_client(client::Client; kwargs...) =
    Flight.FlightService_ListFlights_Client(
        client.host,
        client.port;
        _rpc_options(client; kwargs...)...,
    )

Flight._getflightinfo_client(client::Client; kwargs...) =
    Flight.FlightService_GetFlightInfo_Client(
        client.host,
        client.port;
        _rpc_options(client; kwargs...)...,
    )

Flight._pollflightinfo_client(client::Client; kwargs...) =
    Flight.FlightService_PollFlightInfo_Client(
        client.host,
        client.port;
        _rpc_options(client; kwargs...)...,
    )

Flight._getschema_client(client::Client; kwargs...) = Flight.FlightService_GetSchema_Client(
    client.host,
    client.port;
    _rpc_options(client; kwargs...)...,
)

Flight._doget_client(client::Client; kwargs...) = Flight.FlightService_DoGet_Client(
    client.host,
    client.port;
    _rpc_options(client; kwargs...)...,
)

Flight._doput_client(client::Client; kwargs...) = Flight.FlightService_DoPut_Client(
    client.host,
    client.port;
    _rpc_options(client; kwargs...)...,
)

Flight._doexchange_client(client::Client; kwargs...) =
    Flight.FlightService_DoExchange_Client(
        client.host,
        client.port;
        _rpc_options(client; kwargs...)...,
    )

Flight._doaction_client(client::Client; kwargs...) = Flight.FlightService_DoAction_Client(
    client.host,
    client.port;
    _rpc_options(client; kwargs...)...,
)

Flight._listactions_client(client::Client; kwargs...) =
    Flight.FlightService_ListActions_Client(
        client.host,
        client.port;
        _rpc_options(client; kwargs...)...,
    )
