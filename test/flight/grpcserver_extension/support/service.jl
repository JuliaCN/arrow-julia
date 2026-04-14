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

function grpcserver_extension_assert_authenticated(ctx, fixture)
    auth_header = Arrow.Flight.callheader(ctx, "authorization")
    token_header = Arrow.Flight.callheader(ctx, "auth-token-bin")
    expected_token = String(copy(fixture.handshake_token))

    if auth_header == "Bearer $(expected_token)" ||
       (auth_header isa String && startswith(auth_header, "Basic "))
        @test isnothing(token_header)
        return
    end

    if token_header isa AbstractVector{UInt8}
        @test collect(token_header) == collect(fixture.handshake_token)
        return
    end

    if token_header isa String
        @test token_header == expected_token
        return
    end

    @test false
end

function grpcserver_extension_service(protocol, fixture)
    return Arrow.Flight.Service(
        handshake=(ctx, request, response) -> begin
            auth_header = Arrow.Flight.callheader(ctx, "authorization")
            @test isnothing(auth_header) ||
                  auth_header == "Bearer native" ||
                  startswith(auth_header, "Basic ")
            @test isnothing(Arrow.Flight.callheader(ctx, "auth-token-bin"))
            incoming = collect(request)
            token = if isempty(incoming)
                @test !isnothing(auth_header)
                @test startswith(auth_header, "Basic ")
                fixture.handshake_token
            elseif length(incoming) == 1
                @test incoming[1].payload == fixture.handshake_token
                incoming[1].payload
            else
                @test length(incoming) == 2
                @test String(copy(incoming[1].payload)) == fixture.handshake_username
                @test String(copy(incoming[2].payload)) == fixture.handshake_password
                fixture.handshake_token
            end
            put!(response, protocol.HandshakeResponse(UInt64(0), token))
            close(response)
            return :handshake_ok
        end,
        getflightinfo=(ctx, req) -> begin
            @test req.path == fixture.descriptor.path
            return fixture.info
        end,
        pollflightinfo=(ctx, req) -> begin
            grpcserver_extension_assert_authenticated(ctx, fixture)
            if req.path == fixture.poll_descriptor.path
                return fixture.initial_poll
            end
            @test req.path == fixture.poll_retry_descriptor.path
            return fixture.final_poll
        end,
        listflights=(ctx, criteria, response) -> begin
            grpcserver_extension_assert_authenticated(ctx, fixture)
            @test criteria.expression == UInt8[]
            put!(response, fixture.info)
            close(response)
            return :listflights_ok
        end,
        getschema=(ctx, req) -> begin
            grpcserver_extension_assert_authenticated(ctx, fixture)
            @test req.path == fixture.descriptor.path
            return protocol.SchemaResult(fixture.schema_bytes[5:end])
        end,
        doget=(ctx, req, response) -> begin
            grpcserver_extension_assert_authenticated(ctx, fixture)
            @test req.ticket == fixture.ticket.ticket
            Arrow.Flight.putflightdata!(
                response,
                Tables.partitioner((
                    (id=Int64[1, 2], name=["one", "two"]),
                    (id=Int64[3], name=["three"]),
                ));
                descriptor=fixture.descriptor,
                metadata=fixture.dataset_metadata,
                colmetadata=fixture.dataset_colmetadata,
                close=true,
            )
            return :doget_ok
        end,
        listactions=(ctx, response) -> begin
            grpcserver_extension_assert_authenticated(ctx, fixture)
            put!(response, protocol.ActionType("ping", "Ping action"))
            close(response)
            return :listactions_ok
        end,
        doaction=(ctx, action, response) -> begin
            grpcserver_extension_assert_authenticated(ctx, fixture)
            @test action.var"#type" == "ping"
            put!(response, protocol.Result(b"pong"))
            close(response)
            return :doaction_ok
        end,
        doput=(ctx, request, response) -> begin
            grpcserver_extension_assert_authenticated(ctx, fixture)
            incoming = collect(Arrow.Flight.stream(request; include_app_metadata=true))
            @test length(incoming) == 2
            @test incoming[1].table.id == [1, 2]
            @test incoming[1].table.name == ["one", "two"]
            @test Arrow.getmetadata(incoming[1].table)["dataset"] == "native"
            @test Arrow.getmetadata(incoming[1].table.name)["lang"] == "en"
            @test FlightTestSupport.app_metadata_strings(
                getproperty.(incoming, :app_metadata),
            ) == fixture.dataset_app_metadata
            @test incoming[2].table.id == [3]
            @test incoming[2].table.name == ["three"]
            put!(response, protocol.PutResult(b"stored"))
            close(response)
            return :doput_ok
        end,
        doexchange=(ctx, request, response) -> begin
            grpcserver_extension_assert_authenticated(ctx, fixture)
            incoming = collect(Arrow.Flight.stream(request; include_app_metadata=true))
            @test length(incoming) == 1
            @test FlightTestSupport.app_metadata_strings(
                getproperty.(incoming, :app_metadata),
            ) == fixture.exchange_app_metadata
            @test Arrow.getmetadata(incoming[1].table)["dataset"] == "exchange"
            @test Arrow.getmetadata(incoming[1].table.name)["lang"] == "exchange"
            Arrow.Flight.putflightdata!(
                response,
                Arrow.Flight.withappmetadata(
                    Tables.partitioner(getproperty.(incoming, :table));
                    app_metadata=getproperty.(incoming, :app_metadata),
                );
                close=true,
            )
            return :doexchange_ok
        end,
    )
end
