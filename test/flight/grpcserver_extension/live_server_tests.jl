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

function grpcserver_extension_test_live_server(grpcserver, service, fixture)
    with_grpcserver_extension_live_server(grpcserver, service) do _, host, port
        FlightTestSupport.with_test_grpc_handle() do grpc
            base_client =
                Arrow.Flight.Client("grpc://$(host):$(port)"; grpc=grpc, deadline=30)
            authenticated_client, handshake_responses = Arrow.Flight.authenticate(
                base_client,
                fixture.handshake_username,
                fixture.handshake_password,
            )
            @test authenticated_client.headers ==
                  ["auth-token-bin" => fixture.handshake_token]
            @test length(handshake_responses) == 1
            @test handshake_responses[1].payload == fixture.handshake_token

            client = Arrow.Flight.withheaders(
                base_client,
                "authorization" => "Bearer $(String(copy(fixture.handshake_token)))",
            )

            flights_req, flights_channel = Arrow.Flight.listflights(client)
            flights = collect(flights_channel)
            gRPCClient.grpc_async_await(flights_req)

            @test length(flights) == 1
            @test flights[1].total_records == fixture.info.total_records
            @test flights[1].flight_descriptor.path == fixture.descriptor.path
            @test flights[1].endpoint[1].ticket.ticket == fixture.ticket.ticket

            actions_req, actions_channel = Arrow.Flight.listactions(client)
            actions = collect(actions_channel)
            gRPCClient.grpc_async_await(actions_req)

            @test length(actions) == 1
            @test actions[1].var"#type" == "ping"

            action_req, action_channel =
                Arrow.Flight.doaction(client, Arrow.Flight.Protocol.Action("ping", UInt8[]))
            action_results = collect(action_channel)
            gRPCClient.grpc_async_await(action_req)

            @test length(action_results) == 1
            @test String(action_results[1].body) == "pong"

            info = Arrow.Flight.getflightinfo(client, fixture.descriptor)
            @test info.total_records == fixture.info.total_records
            @test length(info.endpoint) == 1
            @test info.endpoint[1].ticket.ticket == fixture.ticket.ticket

            first_poll = Arrow.Flight.pollflightinfo(client, fixture.poll_descriptor)
            @test !isnothing(first_poll.info)
            @test !isnothing(first_poll.flight_descriptor)
            @test first_poll.flight_descriptor.path == fixture.poll_retry_descriptor.path
            @test first_poll.progress ≈ 0.5
            @test first_poll.info.flight_descriptor.path == fixture.poll_descriptor.path
            @test first_poll.info.endpoint[1].ticket.ticket == fixture.poll_ticket.ticket

            second_poll = Arrow.Flight.pollflightinfo(client, first_poll.flight_descriptor)
            @test !isnothing(second_poll.info)
            @test isnothing(second_poll.flight_descriptor)
            @test second_poll.progress ≈ 1.0
            @test second_poll.info.flight_descriptor.path == fixture.poll_descriptor.path
            @test second_poll.info.endpoint[1].ticket.ticket == fixture.poll_ticket.ticket

            schema = Arrow.Flight.getschema(client, fixture.descriptor)
            @test Arrow.Flight.schemaipc(info) == fixture.schema_bytes
            @test Arrow.Flight.schemaipc(schema) == fixture.schema_bytes

            req, channel = Arrow.Flight.doget(client, info.endpoint[1].ticket)
            messages = collect(channel)
            gRPCClient.grpc_async_await(req)

            @test length(messages) == length(fixture.messages)

            table = Arrow.Flight.table(messages; schema=info)
            @test table.id == [1, 2, 3]
            @test table.name == ["one", "two", "three"]
            @test Arrow.getmetadata(table)["dataset"] == "native"
            @test Arrow.getmetadata(table.name)["lang"] == "en"

            doput_source = Arrow.Flight.withappmetadata(
                Tables.partitioner((
                    (id=Int64[1, 2], name=["one", "two"]),
                    (id=Int64[3], name=["three"]),
                ));
                app_metadata=fixture.dataset_app_metadata,
            )
            doput_req, doput_response = Arrow.Flight.doput(
                client,
                doput_source;
                descriptor=fixture.descriptor,
                metadata=fixture.dataset_metadata,
                colmetadata=fixture.dataset_colmetadata,
            )
            put_results = collect(doput_response)
            gRPCClient.grpc_async_await(doput_req)

            @test length(put_results) == 1
            @test FlightTestSupport.app_metadata_string(put_results[1].app_metadata) ==
                  "stored"

            doexchange_source = Arrow.Flight.withappmetadata(
                Tables.partitioner(((id=Int64[10], name=["ten"]),));
                app_metadata=fixture.exchange_app_metadata,
            )
            doexchange_req, doexchange_response = Arrow.Flight.doexchange(
                client,
                doexchange_source;
                descriptor=fixture.descriptor,
                metadata=fixture.exchange_metadata,
                colmetadata=fixture.exchange_colmetadata,
            )
            exchanged_messages = collect(doexchange_response)
            gRPCClient.grpc_async_await(doexchange_req)

            @test length(exchanged_messages) == length(fixture.exchange_messages)
            doexchange_table = Arrow.Flight.table(exchanged_messages)
            @test doexchange_table.id == [10]
            @test doexchange_table.name == ["ten"]
            @test Arrow.getmetadata(doexchange_table)["dataset"] == "exchange"
            @test Arrow.getmetadata(doexchange_table.name)["lang"] == "exchange"
            @test filter(!isempty, getfield.(exchanged_messages, :app_metadata)) ==
                  Vector{UInt8}.(fixture.exchange_app_metadata)

            doexchange_table_with_app =
                Arrow.Flight.table(exchanged_messages; include_app_metadata=true)
            @test doexchange_table_with_app.table.id == [10]
            @test doexchange_table_with_app.table.name == ["ten"]
            @test FlightTestSupport.app_metadata_strings(
                doexchange_table_with_app.app_metadata,
            ) == fixture.exchange_app_metadata
        end

        pyarrow_smoke_ran = grpcserver_extension_live_pyarrow_smoke(host, port, fixture)
        @test pyarrow_smoke_ran || isnothing(FlightTestSupport.pyarrow_flight_python())
    end
end
