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

@testset "Flight header interop" begin
    protocol = Arrow.Flight.Protocol
    base_client = Arrow.Flight.Client("grpc://127.0.0.1:47470"; deadline=30)
    client = Arrow.Flight.withheaders(
        base_client,
        "authorization" => "Bearer token1234",
        "x-trace-id" => "trace-1",
    )

    @test Arrow.Flight._header_lines(client.headers) ==
          ["authorization: Bearer token1234", "x-trace-id: trace-1"]

    call_headers =
        Arrow.Flight._merge_headers(base_client, ["authorization" => "Bearer call-level"])
    @test call_headers == ["authorization" => "Bearer call-level"]
    @test Arrow.Flight._header_lines(call_headers) == ["authorization: Bearer call-level"]

    merged_headers = Arrow.Flight._merge_headers(client, ["x-call-id" => "call-1"])
    @test merged_headers == [
        "authorization" => "Bearer token1234",
        "x-trace-id" => "trace-1",
        "x-call-id" => "call-1",
    ]

    context = Arrow.Flight.ServerCallContext(headers=merged_headers)
    @test Arrow.Flight.callheader(context, "authorization") == "Bearer token1234"
    @test Arrow.Flight.callheader(context, "Authorization") == "Bearer token1234"
    @test Arrow.Flight.callheader(context, "x-trace-id") == "trace-1"
    @test Arrow.Flight.callheader(context, "x-call-id") == "call-1"
    @test isnothing(Arrow.Flight.callheader(context, "missing"))

    service = Arrow.Flight.Service(
        listactions=(ctx, response) -> begin
            @test Arrow.Flight.callheader(ctx, "authorization") == "Bearer token1234"
            put!(
                response,
                protocol.ActionType(
                    "echo-authorization",
                    "Return the Authorization header",
                ),
            )
            close(response)
        end,
        doaction=(ctx, action, response) -> begin
            @test action.var"#type" == "echo-authorization"
            authorization = Arrow.Flight.callheader(ctx, "authorization")
            put!(response, protocol.Result(Vector{UInt8}(codeunits(authorization))))
            close(response)
        end,
    )

    action_types = Channel{protocol.ActionType}(1)
    service.listactions(context, action_types)
    @test collect(action_types) ==
          [protocol.ActionType("echo-authorization", "Return the Authorization header")]

    action_results = Channel{protocol.Result}(1)
    service.doaction(
        context,
        protocol.Action("echo-authorization", UInt8[]),
        action_results,
    )
    collected_results = collect(action_results)
    @test length(collected_results) == 1
    @test String(collected_results[1].body) == "Bearer token1234"

    call_context = Arrow.Flight.ServerCallContext(headers=call_headers)
    call_results = Channel{protocol.Result}(1)
    service.doaction(
        call_context,
        protocol.Action("echo-authorization", UInt8[]),
        call_results,
    )
    collected_call_results = collect(call_results)
    @test length(collected_call_results) == 1
    @test String(collected_call_results[1].body) == "Bearer call-level"
end
