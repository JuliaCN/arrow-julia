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

function grpcserver_extension_test_direct_handlers(grpcserver, service, fixture)
    grpc_descriptor = grpcserver.service_descriptor(service)
    protocol = Arrow.Flight.Protocol
    metadata = grpcserver_extension_metadata()

    unary_context = grpcserver_extension_context(
        grpcserver,
        "/arrow.flight.protocol.FlightService/GetFlightInfo";
        metadata=metadata,
    )
    schema_context = grpcserver_extension_context(
        grpcserver,
        "/arrow.flight.protocol.FlightService/GetSchema";
        metadata=metadata,
    )

    direct_info =
        grpc_descriptor.methods["GetFlightInfo"].handler(unary_context, fixture.descriptor)
    @test direct_info.total_records == 3
    @test direct_info.endpoint[1].ticket.ticket == fixture.ticket.ticket

    direct_schema =
        grpc_descriptor.methods["GetSchema"].handler(schema_context, fixture.descriptor)
    @test Arrow.Flight.schemaipc(direct_schema) == fixture.schema_bytes

    doget_messages, doget_closed, doget_stream =
        grpcserver_capture_server_stream(grpcserver, protocol.FlightData)
    grpc_descriptor.methods["DoGet"].handler(
        grpcserver_extension_context(
            grpcserver,
            "/arrow.flight.protocol.FlightService/DoGet";
            metadata=metadata,
        ),
        fixture.ticket,
        doget_stream,
    )
    @test doget_closed[]
    @test length(doget_messages) == length(fixture.messages)
    doget_table = Arrow.Flight.table(doget_messages; schema=fixture.info)
    @test doget_table.name == ["one", "two", "three"]
    @test Arrow.getmetadata(doget_table)["dataset"] == "native"
    @test Arrow.getmetadata(doget_table.name)["lang"] == "en"

    actions_messages, actions_closed, actions_stream =
        grpcserver_capture_server_stream(grpcserver, protocol.ActionType)
    grpc_descriptor.methods["ListActions"].handler(
        grpcserver_extension_context(
            grpcserver,
            "/arrow.flight.protocol.FlightService/ListActions";
            metadata=metadata,
        ),
        protocol.Empty(),
        actions_stream,
    )
    @test actions_closed[]
    @test length(actions_messages) == 1
    @test actions_messages[1].var"#type" == "ping"

    action_messages, action_closed, action_stream =
        grpcserver_capture_server_stream(grpcserver, protocol.Result)
    grpc_descriptor.methods["DoAction"].handler(
        grpcserver_extension_context(
            grpcserver,
            "/arrow.flight.protocol.FlightService/DoAction";
            metadata=metadata,
        ),
        protocol.Action("ping", UInt8[]),
        action_stream,
    )
    @test action_closed[]
    @test length(action_messages) == 1
    @test String(action_messages[1].body) == "pong"

    handshake_messages, handshake_closed, handshake_stream = grpcserver_capture_bidi_stream(
        grpcserver,
        protocol.HandshakeRequest,
        protocol.HandshakeResponse,
        fixture.handshake_requests,
    )
    grpc_descriptor.methods["Handshake"].handler(
        grpcserver_extension_context(
            grpcserver,
            "/arrow.flight.protocol.FlightService/Handshake";
            metadata=metadata,
        ),
        handshake_stream,
    )
    @test handshake_closed[]
    @test length(handshake_messages) == 1
    @test handshake_messages[1].payload == fixture.handshake_token

    doput_messages, doput_closed, doput_stream = grpcserver_capture_bidi_stream(
        grpcserver,
        protocol.FlightData,
        protocol.PutResult,
        fixture.messages,
    )
    grpc_descriptor.methods["DoPut"].handler(
        grpcserver_extension_context(
            grpcserver,
            "/arrow.flight.protocol.FlightService/DoPut";
            metadata=metadata,
        ),
        doput_stream,
    )
    @test doput_closed[]
    @test length(doput_messages) == 1
    @test FlightTestSupport.app_metadata_string(doput_messages[1].app_metadata) == "stored"

    doexchange_messages, doexchange_closed, doexchange_stream =
        grpcserver_capture_bidi_stream(
            grpcserver,
            protocol.FlightData,
            protocol.FlightData,
            fixture.exchange_messages,
        )
    grpc_descriptor.methods["DoExchange"].handler(
        grpcserver_extension_context(
            grpcserver,
            "/arrow.flight.protocol.FlightService/DoExchange";
            metadata=metadata,
        ),
        doexchange_stream,
    )
    @test doexchange_closed[]
    @test length(doexchange_messages) == length(fixture.exchange_messages)
    doexchange_table = Arrow.Flight.table(doexchange_messages)
    @test doexchange_table.id == [10]
    @test doexchange_table.name == ["ten"]
    @test Arrow.getmetadata(doexchange_table)["dataset"] == "exchange"
    @test Arrow.getmetadata(doexchange_table.name)["lang"] == "exchange"
    @test filter(!isempty, getfield.(doexchange_messages, :app_metadata)) ==
          Vector{UInt8}.(fixture.exchange_app_metadata)

    failing_service = Arrow.Flight.Service(
        doexchange=(ctx, request, response) ->
            throw(ArgumentError("bidi streaming failed before first response")),
    )
    failing_descriptor = grpcserver.service_descriptor(failing_service)
    failing_messages, failing_closed, failing_stream = grpcserver_capture_bidi_stream(
        grpcserver,
        protocol.FlightData,
        protocol.FlightData,
        fixture.exchange_messages,
    )
    failure = try
        failing_descriptor.methods["DoExchange"].handler(
            grpcserver_extension_context(
                grpcserver,
                "/arrow.flight.protocol.FlightService/DoExchange";
                metadata=metadata,
            ),
            failing_stream,
        )
        nothing
    catch err
        err
    end
    @test failure isa ArgumentError
    @test occursin(
        "bidi streaming failed before first response",
        sprint(showerror, failure),
    )
    @test !failing_closed[]
    @test isempty(failing_messages)
end
