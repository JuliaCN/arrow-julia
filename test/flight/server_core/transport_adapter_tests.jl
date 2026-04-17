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

function flight_server_core_test_transport_adapters(fixture)
    descriptor = Arrow.Flight.transportdescriptor(fixture.implemented)
    getflightinfo = Arrow.Flight.lookuptransportmethod(descriptor, "GetFlightInfo")
    @test !isnothing(getflightinfo)
    @test getflightinfo.request_type_name == "arrow.flight.protocol.FlightDescriptor"
    @test getflightinfo.response_type_name == "arrow.flight.protocol.FlightInfo"
    @test Arrow.Flight.lookuptransportmethod(
        descriptor,
        "/arrow.flight.protocol.FlightService/GetFlightInfo",
    ) === getflightinfo
    @test isnothing(Arrow.Flight.lookuptransportmethod(descriptor, "MissingMethod"))

    info = Arrow.Flight.transport_unary_call(
        fixture.implemented,
        fixture.context,
        getflightinfo,
        fixture.descriptor,
    )
    @test info.total_records == 7
    @test info.total_bytes == 42

    unary_error = try
        Arrow.Flight.transport_unary_call(
            fixture.service,
            fixture.context,
            Arrow.Flight.lookuptransportmethod(
                Arrow.Flight.transportdescriptor(fixture.service),
                "GetFlightInfo",
            ),
            fixture.descriptor;
            on_status_error=error -> throw(ArgumentError(error.message)),
        )
        nothing
    catch error
        error
    end
    @test unary_error isa ArgumentError
    @test occursin("not implemented", sprint(showerror, unary_error))

    doget = Arrow.Flight.lookuptransportmethod(descriptor, "DoGet")
    doget_messages = fixture.protocol.FlightData[]
    @test isnothing(
        Arrow.Flight.transport_server_streaming_call(
            fixture.implemented,
            fixture.context,
            doget,
            fixture.protocol.Ticket(b"ticket-1"),
            message -> push!(doget_messages, message),
        ),
    )
    @test length(doget_messages) == 1

    listactions = Arrow.Flight.lookuptransportmethod(descriptor, "ListActions")
    actions = fixture.protocol.ActionType[]
    @test isnothing(
        Arrow.Flight.transport_server_streaming_call(
            fixture.implemented,
            fixture.context,
            listactions,
            fixture.protocol.Empty(),
            action -> push!(actions, action),
        ),
    )
    @test length(actions) == 1
    @test getfield(actions[1], Symbol("#type")) == "ping"

    bidi_service = Arrow.Flight.Service(
        doexchange=(ctx, request, response) -> begin
            @test ctx === fixture.context
            incoming = collect(request)
            @test length(incoming) == 1
            put!(response, incoming[1])
            close(response)
            return :echo_ok
        end,
    )
    bidi_method = Arrow.Flight.lookuptransportmethod(
        Arrow.Flight.transportdescriptor(bidi_service),
        "DoExchange",
    )
    bidi_messages = fixture.protocol.FlightData[]
    echoed = fixture.protocol.FlightData(nothing, UInt8[0x01], UInt8[0x02], UInt8[])
    result = Arrow.Flight.transport_bidi_streaming_call(
        bidi_service,
        fixture.context,
        bidi_method,
        (echoed,),
        message -> push!(bidi_messages, message),
    )
    @test result === nothing
    @test length(bidi_messages) == 1
    @test bidi_messages[1].data_body == echoed.data_body
    @test bidi_messages[1].app_metadata == echoed.app_metadata

    live_bidi_service = Arrow.Flight.Service(
        doexchange=(ctx, request, response) -> begin
            @test ctx === fixture.context
            for message in request
                put!(response, message)
            end
            close(response)
            return :echo_live_ok
        end,
    )
    live_bidi_method = Arrow.Flight.lookuptransportmethod(
        Arrow.Flight.transportdescriptor(live_bidi_service),
        "DoExchange",
    )
    live_requests = Channel{fixture.protocol.FlightData}(2)
    live_messages = fixture.protocol.FlightData[]
    live_task = @async Arrow.Flight.transport_bidi_streaming_live_call(
        live_bidi_service,
        fixture.context,
        live_bidi_method,
        live_requests,
        message -> push!(live_messages, message),
    )
    put!(live_requests, echoed)
    @test timedwait(() -> !isempty(live_messages), 2.0) !== :timed_out
    @test length(live_messages) == 1
    @test live_messages[1].data_body == echoed.data_body
    close(live_requests)
    wait(live_task)
end
