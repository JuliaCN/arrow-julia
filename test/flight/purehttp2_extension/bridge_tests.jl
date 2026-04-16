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

function purehttp2_extension_test_bridge(fixture)
    protocol = fixture.protocol

    unary_conn, unary_stream = purehttp2_extension_request(
        "GetFlightInfo",
        fixture.descriptor;
        headers=fixture.unary_headers,
    )
    unary_frames =
        Arrow.Flight.purehttp2_handle_grpc_stream!(unary_conn, fixture.service, UInt32(1))
    unary_headers = purehttp2_extension_decode_headers(unary_frames)
    @test unary_headers[1] ==
          [(":status", "200"), ("content-type", "application/grpc+proto")]
    @test unary_headers[2] == [("grpc-status", "0")]

    unary_messages = purehttp2_extension_decode_data(protocol.FlightInfo, unary_frames)
    @test length(unary_messages) == 1
    @test unary_messages[1].total_records == 7
    @test unary_messages[1].total_bytes == 42
    @test fixture.seen_unary_request[].path == fixture.descriptor.path
    @test Arrow.Flight.callheader(fixture.seen_unary_context[], "authorization") ==
          "Bearer purehttp2"
    @test Arrow.Flight.callheader(fixture.seen_unary_context[], "auth-token-bin") ==
          UInt8[0x01, 0x02]
    @test PureHTTP2.can_send(unary_stream) == false

    stream_conn, stream = purehttp2_extension_request("DoGet", protocol.Ticket(b"ticket-1"))
    stream_frames =
        Arrow.Flight.purehttp2_handle_grpc_stream!(stream_conn, fixture.service, UInt32(1))
    stream_headers = purehttp2_extension_decode_headers(stream_frames)
    @test stream_headers[1] ==
          [(":status", "200"), ("content-type", "application/grpc+proto")]
    @test stream_headers[2] == [("grpc-status", "0")]

    stream_messages = purehttp2_extension_decode_data(protocol.FlightData, stream_frames)
    @test length(stream_messages) == 2
    @test [message.data_header for message in stream_messages] == [UInt8[0x01], UInt8[0x04]]
    @test fixture.seen_stream_request[].ticket == b"ticket-1"
    @test fixture.seen_stream_context[] isa Arrow.Flight.ServerCallContext
    @test PureHTTP2.can_send(stream) == false

    handshake_conn, _ = purehttp2_extension_messages_request(
        "Handshake",
        fixture.handshake_requests;
        headers=fixture.unary_headers,
    )
    handshake_frames = Arrow.Flight.purehttp2_handle_grpc_stream!(
        handshake_conn,
        fixture.service,
        UInt32(1),
    )
    handshake_headers = purehttp2_extension_decode_headers(handshake_frames)
    @test handshake_headers[1] ==
          [(":status", "200"), ("content-type", "application/grpc+proto")]
    @test handshake_headers[2] == [("grpc-status", "0")]
    handshake_messages =
        purehttp2_extension_decode_data(protocol.HandshakeResponse, handshake_frames)
    @test length(handshake_messages) == 1
    @test handshake_messages[1].payload == fixture.handshake_token
    @test length(fixture.seen_handshake_messages[]) == length(fixture.handshake_requests)
    @test Arrow.Flight.callheader(fixture.seen_handshake_context[], "authorization") ==
          "Bearer purehttp2"

    doput_conn, _ = purehttp2_extension_messages_request(
        "DoPut",
        fixture.messages;
        headers=fixture.unary_headers,
        chunk_size=7,
    )
    doput_frames =
        Arrow.Flight.purehttp2_handle_grpc_stream!(doput_conn, fixture.service, UInt32(1))
    doput_headers = purehttp2_extension_decode_headers(doput_frames)
    @test doput_headers[1] ==
          [(":status", "200"), ("content-type", "application/grpc+proto")]
    @test doput_headers[2] == [("grpc-status", "0")]
    doput_messages = purehttp2_extension_decode_data(protocol.PutResult, doput_frames)
    @test length(doput_messages) == 1
    @test purehttp2_extension_app_metadata_string(doput_messages[1].app_metadata) ==
          "stored"
    @test fixture.seen_doput_context[] isa Arrow.Flight.ServerCallContext
    @test length(fixture.seen_doput_batches[]) == 2
    @test fixture.seen_doput_batches[][1].table.id == [1, 2]
    @test fixture.seen_doput_batches[][1].table.name == ["one", "two"]
    @test fixture.seen_doput_batches[][2].table.id == [3]
    @test fixture.seen_doput_batches[][2].table.name == ["three"]
    @test purehttp2_extension_app_metadata_strings(
        getproperty.(fixture.seen_doput_batches[], :app_metadata),
    ) == fixture.dataset_app_metadata

    doexchange_conn, _ = purehttp2_extension_messages_request(
        "DoExchange",
        fixture.exchange_messages;
        headers=fixture.unary_headers,
        chunk_size=7,
    )
    doexchange_frames = Arrow.Flight.purehttp2_handle_grpc_stream!(
        doexchange_conn,
        fixture.service,
        UInt32(1),
    )
    doexchange_headers = purehttp2_extension_decode_headers(doexchange_frames)
    @test doexchange_headers[1] ==
          [(":status", "200"), ("content-type", "application/grpc+proto")]
    @test doexchange_headers[2] == [("grpc-status", "0")]
    doexchange_messages =
        purehttp2_extension_decode_data(protocol.FlightData, doexchange_frames)
    @test length(doexchange_messages) == length(fixture.exchange_messages)
    doexchange_table = Arrow.Flight.table(doexchange_messages)
    @test doexchange_table.id == [10]
    @test doexchange_table.name == ["ten"]
    @test Arrow.getmetadata(doexchange_table)["dataset"] == "exchange"
    @test Arrow.getmetadata(doexchange_table.name)["lang"] == "exchange"
    @test purehttp2_extension_app_metadata_strings(
        filter(!isempty, getfield.(doexchange_messages, :app_metadata)),
    ) == fixture.exchange_app_metadata
    doexchange_table_with_app =
        Arrow.Flight.table(doexchange_messages; include_app_metadata=true)
    @test doexchange_table_with_app.table.id == [10]
    @test purehttp2_extension_app_metadata_strings(
        doexchange_table_with_app.app_metadata,
    ) == fixture.exchange_app_metadata
    @test fixture.seen_exchange_context[] isa Arrow.Flight.ServerCallContext
    @test length(fixture.seen_exchange_batches[]) == 1

    failure_conn, _ = purehttp2_extension_request(
        "GetFlightInfo",
        fixture.descriptor;
        headers=fixture.unary_headers,
    )
    failure_frames = Arrow.Flight.purehttp2_handle_grpc_stream!(
        failure_conn,
        fixture.failure_service,
        UInt32(1),
    )
    failure_headers = purehttp2_extension_decode_headers(failure_frames)
    @test failure_headers[1] ==
          [(":status", "200"), ("content-type", "application/grpc+proto")]
    @test failure_headers[2] == [("grpc-status", "16"), ("grpc-message", "bad token")]
    @test isempty(purehttp2_extension_decode_data(protocol.FlightInfo, failure_frames))

    failing_exchange_service = Arrow.Flight.Service(
        doexchange=(ctx, request, response) ->
            throw(ArgumentError("purehttp2 bidi failed before first response")),
    )
    failing_exchange_conn, _ = purehttp2_extension_messages_request(
        "DoExchange",
        fixture.exchange_messages;
        headers=fixture.unary_headers,
    )
    failing_exchange_frames = Arrow.Flight.purehttp2_handle_grpc_stream!(
        failing_exchange_conn,
        failing_exchange_service,
        UInt32(1),
    )
    failing_exchange_headers = purehttp2_extension_decode_headers(failing_exchange_frames)
    @test failing_exchange_headers[1] ==
          [(":status", "200"), ("content-type", "application/grpc+proto")]
    @test failing_exchange_headers[2][1] == ("grpc-status", "13")
    @test occursin(
        "purehttp2 bidi failed before first response",
        failing_exchange_headers[2][2][2],
    )
    @test isempty(
        purehttp2_extension_decode_data(protocol.FlightData, failing_exchange_frames),
    )

    context = Arrow.Flight.purehttp2_call_context(unary_stream)
    @test Arrow.Flight.callheader(context, "authorization") == "Bearer purehttp2"
    @test Arrow.Flight.callheader(context, "auth-token-bin") == UInt8[0x01, 0x02]
end
