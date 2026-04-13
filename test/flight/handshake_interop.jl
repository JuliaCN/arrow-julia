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

@testset "Flight handshake interop" begin
    protocol = Arrow.Flight.Protocol
    client = Arrow.Flight.Client("grpc://127.0.0.1:47470"; deadline=30)

    token_client = Arrow.Flight.withtoken(client, b"secret:test")
    @test token_client.headers == ["auth-token-bin" => b"secret:test"]
    @test Arrow.Flight._header_lines(token_client.headers) ==
          ["auth-token-bin: c2VjcmV0OnRlc3Q="]

    replaced_token_client = Arrow.Flight.withtoken(token_client, b"secret:override")
    @test replaced_token_client.headers == ["auth-token-bin" => b"secret:override"]

    auth_context = Arrow.Flight.ServerCallContext(headers=token_client.headers)
    @test Arrow.Flight.callheader(auth_context, "auth-token-bin") == b"secret:test"
    @test Arrow.Flight.callheader(auth_context, "Auth-Token-Bin") == b"secret:test"
    @test isnothing(Arrow.Flight.callheader(auth_context, "authorization"))

    service = Arrow.Flight.Service(
        handshake=(ctx, request, response) -> begin
            @test isnothing(Arrow.Flight.callheader(ctx, "auth-token-bin"))
            incoming = collect(request)
            payloads = getproperty.(incoming, :payload)
            if payloads == [b"test", b"p4ssw0rd"]
                put!(response, protocol.HandshakeResponse(UInt64(0), b"secret:test"))
                close(response)
                return :handshake_ok
            end
            close(response)
            throw(
                gRPCClient.gRPCServiceCallException(
                    gRPCClient.GRPC_UNAUTHENTICATED,
                    "invalid username/password",
                ),
            )
        end,
        listactions=(ctx, response) -> begin
            token = Arrow.Flight.callheader(ctx, "auth-token-bin")
            if token != b"secret:test"
                close(response)
                throw(
                    gRPCClient.gRPCServiceCallException(
                        gRPCClient.GRPC_UNAUTHENTICATED,
                        "invalid token",
                    ),
                )
            end
            put!(
                response,
                protocol.ActionType("authenticated", "Requires a valid auth token"),
            )
            close(response)
            return :listactions_ok
        end,
    )

    handshake_request = Channel{protocol.HandshakeRequest}(2)
    put!(handshake_request, protocol.HandshakeRequest(UInt64(0), b"test"))
    put!(handshake_request, protocol.HandshakeRequest(UInt64(0), b"p4ssw0rd"))
    close(handshake_request)
    handshake_response = Channel{protocol.HandshakeResponse}(1)
    @test Arrow.Flight.handshake(
        service,
        Arrow.Flight.ServerCallContext(headers=Arrow.Flight.HeaderPair[]),
        handshake_request,
        handshake_response,
    ) == :handshake_ok

    handshake_messages = collect(handshake_response)
    @test length(handshake_messages) == 1
    @test handshake_messages[1].protocol_version == 0
    @test handshake_messages[1].payload == b"secret:test"

    action_response = Channel{protocol.ActionType}(1)
    @test Arrow.Flight.listactions(service, auth_context, action_response) ==
          :listactions_ok
    @test collect(action_response) ==
          [protocol.ActionType("authenticated", "Requires a valid auth token")]

    bad_handshake_request = Channel{protocol.HandshakeRequest}(2)
    put!(bad_handshake_request, protocol.HandshakeRequest(UInt64(0), b"test"))
    put!(bad_handshake_request, protocol.HandshakeRequest(UInt64(0), b"wrong"))
    close(bad_handshake_request)
    bad_handshake_response = Channel{protocol.HandshakeResponse}(1)
    @test_throws gRPCClient.gRPCServiceCallException Arrow.Flight.handshake(
        service,
        Arrow.Flight.ServerCallContext(headers=Arrow.Flight.HeaderPair[]),
        bad_handshake_request,
        bad_handshake_response,
    )
    @test isempty(collect(bad_handshake_response))

    bad_action_response = Channel{protocol.ActionType}(1)
    @test_throws gRPCClient.gRPCServiceCallException Arrow.Flight.listactions(
        service,
        Arrow.Flight.ServerCallContext(headers=Arrow.Flight.HeaderPair[]),
        bad_action_response,
    )
    @test isempty(collect(bad_action_response))
end
