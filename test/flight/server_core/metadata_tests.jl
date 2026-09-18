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

function flight_server_core_test_metadata(fixture)
    @test Arrow.Flight.callheader(fixture.context, "authorization") == "Bearer test"
    @test Arrow.Flight.callheader(fixture.context, "Authorization") == "Bearer test"
    @test Arrow.Flight.callheader(fixture.context, "auth-token-bin") == UInt8[0x01, 0x02]
    @test isnothing(Arrow.Flight.callheader(fixture.context, "missing"))
    @test !Arrow.Flight.iscallcancelled(fixture.context)
    @test isnothing(Arrow.Flight.callremainingtime(fixture.context))
    @test isnothing(Arrow.Flight.checkcall(fixture.context))

    cancelled = Ref(false)
    remaining = Ref{Union{Nothing,Float64}}(10.0)
    response_headers = Dict{String,Arrow.Flight.HeaderValue}()
    response_trailers = Dict{String,Arrow.Flight.HeaderValue}()
    lifecycle_context = Arrow.Flight.ServerCallContext(
        request_id="request-1",
        method="/arrow.flight.protocol.FlightService/DoGet",
        authority="flight.example.test",
        deadline=:test_deadline,
        trace_context=UInt8[0x01, 0x02],
        payload=:test_payload,
        is_cancelled=() -> cancelled[],
        remaining_time=() -> remaining[],
        set_response_header=(name, value) ->
            (response_headers[lowercase(name)] = value),
        set_response_trailer=(name, value) ->
            (response_trailers[lowercase(name)] = value),
    )
    @test lifecycle_context.request_id == "request-1"
    @test lifecycle_context.method == "/arrow.flight.protocol.FlightService/DoGet"
    @test lifecycle_context.authority == "flight.example.test"
    @test lifecycle_context.deadline === :test_deadline
    @test lifecycle_context.trace_context == UInt8[0x01, 0x02]
    @test lifecycle_context.payload === :test_payload
    @test isnothing(Arrow.Flight.checkcall(lifecycle_context))
    @test Arrow.Flight.setresponseheader!(
        lifecycle_context,
        "x-request-id",
        "request-1",
    ) === lifecycle_context
    @test Arrow.Flight.setresponsetrailer!(
        lifecycle_context,
        "result-bin",
        UInt8[0x03, 0x04],
    ) === lifecycle_context
    @test response_headers["x-request-id"] == "request-1"
    @test response_trailers["result-bin"] == UInt8[0x03, 0x04]

    cancelled[] = true
    cancelled_error = try
        Arrow.Flight.checkcall(lifecycle_context)
        nothing
    catch error
        error
    end
    @test cancelled_error isa Arrow.Flight.FlightStatusError
    @test cancelled_error.code == 1

    cancelled[] = false
    remaining[] = 0.0
    deadline_error = try
        Arrow.Flight.checkcall(lifecycle_context)
        nothing
    catch error
        error
    end
    @test deadline_error isa Arrow.Flight.FlightStatusError
    @test deadline_error.code == 4
    @test fixture.descriptor_info.name == "arrow.flight.protocol.FlightService"
    @test length(fixture.descriptor_info.methods) == 10
end
