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

function flight_live_fixture(protocol)
    descriptor_type = protocol.var"FlightDescriptor.DescriptorType"
    handshake_token = b"native"
    handshake_username = "native"
    handshake_password = "token"
    descriptor =
        protocol.FlightDescriptor(descriptor_type.PATH, UInt8[], ["native", "dataset"])
    poll_descriptor =
        protocol.FlightDescriptor(descriptor_type.PATH, UInt8[], ["native", "poll"])
    poll_retry_descriptor = protocol.FlightDescriptor(
        descriptor_type.PATH,
        UInt8[],
        ["native", "poll", "retry"],
    )
    ticket = protocol.Ticket(b"native-ticket")
    poll_ticket = protocol.Ticket(b"native-poll-ticket")
    dataset_metadata = Dict("dataset" => "native")
    dataset_colmetadata = Dict(:name => Dict("lang" => "en"))
    dataset_app_metadata = ["put:0", "put:1"]
    messages = Arrow.Flight.flightdata(
        Tables.partitioner((
            (id=Int64[1, 2], name=["one", "two"]),
            (id=Int64[3], name=["three"]),
        ));
        descriptor=descriptor,
        metadata=dataset_metadata,
        colmetadata=dataset_colmetadata,
        app_metadata=dataset_app_metadata,
    )
    schema_bytes = Arrow.Flight.schemaipc(first(messages))
    info = protocol.FlightInfo(
        schema_bytes[5:end],
        descriptor,
        [protocol.FlightEndpoint(ticket, protocol.Location[], nothing, UInt8[])],
        Int64(3),
        Int64(-1),
        false,
        UInt8[],
    )
    poll_info = protocol.FlightInfo(
        schema_bytes[5:end],
        poll_descriptor,
        [protocol.FlightEndpoint(poll_ticket, protocol.Location[], nothing, UInt8[])],
        Int64(3),
        Int64(-1),
        false,
        UInt8[],
    )
    initial_poll = protocol.PollInfo(poll_info, poll_retry_descriptor, 0.5, nothing)
    final_poll = protocol.PollInfo(poll_info, nothing, 1.0, nothing)
    handshake_requests = [protocol.HandshakeRequest(UInt64(0), handshake_token)]
    exchange_metadata = Dict("dataset" => "exchange")
    exchange_colmetadata = Dict(:name => Dict("lang" => "exchange"))
    exchange_app_metadata = ["exchange:0"]
    exchange_messages = Arrow.Flight.flightdata(
        Tables.partitioner(((id=Int64[10], name=["ten"]),));
        descriptor=descriptor,
        metadata=exchange_metadata,
        colmetadata=exchange_colmetadata,
        app_metadata=exchange_app_metadata,
    )
    return (
        handshake_token=handshake_token,
        handshake_username=handshake_username,
        handshake_password=handshake_password,
        descriptor=descriptor,
        poll_descriptor=poll_descriptor,
        poll_retry_descriptor=poll_retry_descriptor,
        ticket=ticket,
        poll_ticket=poll_ticket,
        messages=messages,
        schema_bytes=schema_bytes,
        info=info,
        poll_info=poll_info,
        initial_poll=initial_poll,
        final_poll=final_poll,
        handshake_requests=handshake_requests,
        dataset_metadata=dataset_metadata,
        dataset_colmetadata=dataset_colmetadata,
        dataset_app_metadata=dataset_app_metadata,
        exchange_messages=exchange_messages,
        exchange_metadata=exchange_metadata,
        exchange_colmetadata=exchange_colmetadata,
        exchange_app_metadata=exchange_app_metadata,
    )
end

function flight_live_assert_authenticated(ctx, fixture)
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

function flight_live_service(protocol, fixture)
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
            flight_live_assert_authenticated(ctx, fixture)
            @test req.path == fixture.descriptor.path
            return fixture.info
        end,
        pollflightinfo=(ctx, req) -> begin
            flight_live_assert_authenticated(ctx, fixture)
            if req.path == fixture.poll_descriptor.path
                return fixture.initial_poll
            end
            @test req.path == fixture.poll_retry_descriptor.path
            return fixture.final_poll
        end,
        listflights=(ctx, criteria, response) -> begin
            flight_live_assert_authenticated(ctx, fixture)
            @test criteria.expression == UInt8[]
            put!(response, fixture.info)
            close(response)
            return :listflights_ok
        end,
        getschema=(ctx, req) -> begin
            flight_live_assert_authenticated(ctx, fixture)
            @test req.path == fixture.descriptor.path
            return protocol.SchemaResult(fixture.schema_bytes[5:end])
        end,
        doget=(ctx, req, response) -> begin
            flight_live_assert_authenticated(ctx, fixture)
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
            flight_live_assert_authenticated(ctx, fixture)
            put!(response, protocol.ActionType("ping", "Ping action"))
            put!(response, Arrow.Flight.cancelflightinfoactiontype())
            close(response)
            return :listactions_ok
        end,
        doaction=(ctx, action, response) -> begin
            flight_live_assert_authenticated(ctx, fixture)
            if action.var"#type" == "ping"
                put!(response, protocol.Result(b"pong"))
            else
                request = Arrow.Flight.cancelflightinforequest(action)
                @test request.info.flight_descriptor.path ==
                      fixture.info.flight_descriptor.path
                @test request.info.total_records == fixture.info.total_records
                put!(
                    response,
                    Arrow.Flight.cancelflightinforesult(
                        protocol.CancelStatus.CANCEL_STATUS_CANCELLED,
                    ),
                )
            end
            close(response)
            return :doaction_ok
        end,
        doput=(ctx, request, response) -> begin
            flight_live_assert_authenticated(ctx, fixture)
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
            flight_live_assert_authenticated(ctx, fixture)
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

function flight_live_pyarrow_smoke(host::AbstractString, port::Integer, fixture)
    python = FlightTestSupport.pyarrow_flight_python()
    isnothing(python) && return false
    run(
        Cmd([
            python,
            "-c",
            FLIGHT_LIVE_PYARROW_SMOKE,
            host,
            string(port),
            fixture.handshake_username,
            fixture.handshake_password,
            fixture.descriptor.path...,
        ]),
    )
    return true
end

function flight_live_pyarrow_readonly_smoke(host::AbstractString, port::Integer, fixture)
    python = FlightTestSupport.pyarrow_flight_python()
    isnothing(python) && return false
    run(
        Cmd([
            python,
            "-c",
            FLIGHT_LIVE_PYARROW_READONLY_SMOKE,
            host,
            string(port),
            fixture.handshake_username,
            fixture.handshake_password,
            fixture.descriptor.path...,
        ]),
    )
    return true
end

function flight_live_python_poll_smoke(host::AbstractString, port::Integer, fixture)
    python = FlightTestSupport.pyarrow_flight_python(
        required_modules=FlightTestSupport.POLL_FLIGHT_REQUIRED_MODULES,
    )
    isnothing(python) && return false
    proto_root =
        normpath(joinpath(FlightTestSupport.TEST_ROOT, "..", "src", "flight", "proto"))
    run(
        Cmd([
            python,
            "-c",
            FLIGHT_LIVE_PYTHON_POLL_SMOKE,
            proto_root,
            host,
            string(port),
            fixture.handshake_username,
            fixture.handshake_password,
            fixture.poll_descriptor.path...,
        ]),
    )
    return true
end

function flight_live_python_handshake_smoke(host::AbstractString, port::Integer, fixture)
    python = FlightTestSupport.pyarrow_flight_python(
        required_modules=FlightTestSupport.POLL_FLIGHT_REQUIRED_MODULES,
    )
    isnothing(python) && return false
    proto_root =
        normpath(joinpath(FlightTestSupport.TEST_ROOT, "..", "src", "flight", "proto"))
    run(
        Cmd([
            python,
            "-c",
            FLIGHT_LIVE_PYTHON_HANDSHAKE_SMOKE,
            proto_root,
            host,
            string(port),
            String(copy(fixture.handshake_token)),
        ]),
    )
    return true
end
