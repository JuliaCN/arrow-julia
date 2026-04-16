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

function nghttp2_extension_test_live_listener()
    protocol = Arrow.Flight.Protocol
    live_fixture = flight_live_fixture(protocol)
    live_service = flight_live_service(protocol, live_fixture)
    server = Arrow.Flight.nghttp2_flight_server(
        live_service;
        host="127.0.0.1",
        port=0,
        request_capacity=4,
        response_capacity=4,
    )

    try
        purehttp2_extension_wait_for_live_server(server.host, server.port)
        @test isopen(server)
        @test server.port > 0

        FlightTestSupport.with_test_grpc_handle() do grpc
            base_client = Arrow.Flight.Client(
                "grpc://$(server.host):$(server.port)";
                grpc=grpc,
                deadline=30,
            )
            client = Arrow.Flight.withheaders(
                base_client,
                "authorization" => "Bearer $(String(copy(live_fixture.handshake_token)))",
            )

            flights_req, flights_channel = Arrow.Flight.listflights(client)
            flights = collect(flights_channel)
            gRPCClient.grpc_async_await(flights_req)
            @test length(flights) == 1
            @test flights[1].total_records == live_fixture.info.total_records
            @test flights[1].flight_descriptor.path == live_fixture.descriptor.path
            @test flights[1].endpoint[1].ticket.ticket == live_fixture.ticket.ticket

            actions_req, actions_channel = Arrow.Flight.listactions(client)
            actions = collect(actions_channel)
            gRPCClient.grpc_async_await(actions_req)
            @test length(actions) == 1
            @test actions[1].var"#type" == "ping"

            action_req, action_channel =
                Arrow.Flight.doaction(client, protocol.Action("ping", UInt8[]))
            action_results = collect(action_channel)
            gRPCClient.grpc_async_await(action_req)
            @test length(action_results) == 1
            @test String(action_results[1].body) == "pong"

            info = Arrow.Flight.getflightinfo(client, live_fixture.descriptor)
            @test info.total_records == live_fixture.info.total_records
            @test length(info.endpoint) == 1
            @test info.endpoint[1].ticket.ticket == live_fixture.ticket.ticket

            first_poll = Arrow.Flight.pollflightinfo(client, live_fixture.poll_descriptor)
            @test !isnothing(first_poll.info)
            @test !isnothing(first_poll.flight_descriptor)
            @test first_poll.flight_descriptor.path ==
                  live_fixture.poll_retry_descriptor.path
            @test first_poll.progress ≈ 0.5

            second_poll = Arrow.Flight.pollflightinfo(client, first_poll.flight_descriptor)
            @test !isnothing(second_poll.info)
            @test isnothing(second_poll.flight_descriptor)
            @test second_poll.progress ≈ 1.0

            schema = Arrow.Flight.getschema(client, live_fixture.descriptor)
            @test Arrow.Flight.schemaipc(info) == live_fixture.schema_bytes
            @test Arrow.Flight.schemaipc(schema) == live_fixture.schema_bytes

            doget_req, doget_channel = Arrow.Flight.doget(client, info.endpoint[1].ticket)
            doget_messages = collect(doget_channel)
            gRPCClient.grpc_async_await(doget_req)
            @test length(doget_messages) == length(live_fixture.messages)
            table = Arrow.Flight.table(doget_messages; schema=info)
            @test table.id == [1, 2, 3]
            @test table.name == ["one", "two", "three"]
            @test Arrow.getmetadata(table)["dataset"] == "native"
            @test Arrow.getmetadata(table.name)["lang"] == "en"
        end
    finally
        Arrow.Flight.stop!(server; force=true)
        @test !isopen(server)
    end
end
