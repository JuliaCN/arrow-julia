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

    cancelled = Ref(true)
    cancelled_context = Arrow.Flight.ServerCallContext(is_cancelled=() -> cancelled[])
    cancelled_error = try
        Arrow.Flight.transport_unary_call(
            fixture.implemented,
            cancelled_context,
            getflightinfo,
            fixture.descriptor;
            on_status_error=error -> throw(ArgumentError("status $(error.code)")),
        )
        nothing
    catch error
        error
    end
    @test cancelled_error isa ArgumentError
    @test occursin("status 1", sprint(showerror, cancelled_error))

    deadline_context = Arrow.Flight.ServerCallContext(remaining_time=() -> 0.0)
    deadline_error = try
        Arrow.Flight.transport_unary_call(
            fixture.implemented,
            deadline_context,
            getflightinfo,
            fixture.descriptor;
            on_status_error=error -> throw(ArgumentError("status $(error.code)")),
        )
        nothing
    catch error
        error
    end
    @test deadline_error isa ArgumentError
    @test occursin("status 4", sprint(showerror, deadline_error))

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

    cancel_after_first = Ref(false)
    streaming_context =
        Arrow.Flight.ServerCallContext(is_cancelled=() -> cancel_after_first[])
    streaming_service = Arrow.Flight.Service(
        doget=(ctx, ticket, response) -> begin
            put!(response, fixture.protocol.FlightData(nothing, UInt8[], UInt8[0x01], UInt8[]))
            put!(
                response,
                fixture.protocol.FlightData(nothing, UInt8[], UInt8[0x02], UInt8[]),
            )
            close(response)
        end,
    )
    streaming_method = Arrow.Flight.lookuptransportmethod(
        Arrow.Flight.transportdescriptor(streaming_service),
        "DoGet",
    )
    streamed = fixture.protocol.FlightData[]
    streaming_error = try
        Arrow.Flight.transport_server_streaming_call(
            streaming_service,
            streaming_context,
            streaming_method,
            fixture.protocol.Ticket(b"cancel"),
            message -> begin
                push!(streamed, message)
                cancel_after_first[] = true
            end;
            response_capacity=2,
            on_status_error=error -> throw(ArgumentError("status $(error.code)")),
        )
        nothing
    catch error
        error
    end
    @test length(streamed) == 1
    @test streaming_error isa ArgumentError
    @test occursin("status 1", sprint(showerror, streaming_error))

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
    @test length(actions) == 2
    @test getfield(actions[1], Symbol("#type")) == "ping"
    @test getfield(actions[2], Symbol("#type")) == "CancelFlightInfo"

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

    failing_bidi_service = Arrow.Flight.Service(
        doexchange=(ctx, request, response) ->
            throw(ArgumentError("bidi stream rejected before request drain")),
    )
    failing_bidi_method = Arrow.Flight.lookuptransportmethod(
        Arrow.Flight.transportdescriptor(failing_bidi_service),
        "DoExchange",
    )
    failing_bidi_task = @async try
        Arrow.Flight.transport_bidi_streaming_call(
            failing_bidi_service,
            fixture.context,
            failing_bidi_method,
            Iterators.repeated(echoed, 8),
            _ -> nothing;
            request_capacity=1,
            response_capacity=1,
        )
        nothing
    catch error
        error
    end
    @test timedwait(() -> istaskdone(failing_bidi_task), 2.0) !== :timed_out
    failing_bidi_error = fetch(failing_bidi_task)
    @test failing_bidi_error isa ArgumentError
    @test occursin(
        "bidi stream rejected before request drain",
        sprint(showerror, failing_bidi_error),
    )

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

    runtime = Arrow.Flight.FlightServerRuntime(
        max_active_calls=1,
        max_reserved_bytes=64,
        call_reservation_bytes=64,
        cleanup_grace_seconds=0.02,
    )
    unary_entered = Channel{Nothing}(1)
    unary_release = Channel{Nothing}(1)
    governed_service = Arrow.Flight.Service(
        getflightinfo=(ctx, descriptor) -> begin
            put!(unary_entered, nothing)
            take!(unary_release)
            fixture.protocol.FlightInfo(
                UInt8[],
                descriptor,
                fixture.protocol.FlightEndpoint[],
                1,
                8,
                false,
                UInt8[],
            )
        end,
    )
    governed_method = Arrow.Flight.lookuptransportmethod(
        Arrow.Flight.transportdescriptor(governed_service),
        "GetFlightInfo",
    )
    governed_task = @async Arrow.Flight.transport_unary_call(
        governed_service,
        fixture.context,
        governed_method,
        fixture.descriptor;
        runtime=runtime,
    )
    take!(unary_entered)
    rejected = try
        Arrow.Flight.transport_unary_call(
            governed_service,
            fixture.context,
            governed_method,
            fixture.descriptor;
            runtime=runtime,
        )
        nothing
    catch error
        error
    end
    @test rejected isa Arrow.Flight.FlightStatusError
    @test rejected.code == Arrow.Flight.FLIGHT_STATUS_RESOURCE_EXHAUSTED
    admitted_metrics = Arrow.Flight.flight_server_metrics(runtime)
    @test admitted_metrics.active_calls == 1
    @test admitted_metrics.reserved_bytes == 64
    @test admitted_metrics.calls_rejected == 1
    put!(unary_release, nothing)
    wait(governed_task)
    completed_metrics = Arrow.Flight.flight_server_metrics(runtime)
    @test completed_metrics.active_calls == 0
    @test completed_metrics.reserved_bytes == 0
    @test completed_metrics.calls_completed == 1
    @test completed_metrics.request_messages == 1
    @test completed_metrics.response_messages == 1

    cleanup_runtime = Arrow.Flight.FlightServerRuntime(
        max_active_calls=1,
        max_reserved_bytes=64,
        call_reservation_bytes=64,
        cleanup_grace_seconds=0.02,
    )
    handler_entered = Channel{Nothing}(1)
    handler_release = Channel{Nothing}(1)
    cancelled = Ref(false)
    noncooperative_service = Arrow.Flight.Service(
        doget=(ctx, ticket, response) -> begin
            put!(handler_entered, nothing)
            take!(handler_release)
        end,
    )
    noncooperative_method = Arrow.Flight.lookuptransportmethod(
        Arrow.Flight.transportdescriptor(noncooperative_service),
        "DoGet",
    )
    noncooperative_context = Arrow.Flight.ServerCallContext(is_cancelled=() -> cancelled[])
    transport_task = @async try
        Arrow.Flight.transport_server_streaming_call(
            noncooperative_service,
            noncooperative_context,
            noncooperative_method,
            fixture.protocol.Ticket(b"blocked"),
            _ -> nothing;
            runtime=cleanup_runtime,
        )
        nothing
    catch error
        error
    end
    take!(handler_entered)
    cancelled[] = true
    @test timedwait(() -> istaskdone(transport_task), 1.0) !== :timed_out
    cancelled_error = fetch(transport_task)
    @test cancelled_error isa Arrow.Flight.FlightStatusError
    @test cancelled_error.code == Arrow.Flight.FLIGHT_STATUS_CANCELLED
    orphan_metrics = Arrow.Flight.flight_server_metrics(cleanup_runtime)
    @test orphan_metrics.cleanup_timeouts == 1
    @test orphan_metrics.orphan_tasks == 1
    @test orphan_metrics.active_calls == 1
    @test orphan_metrics.reserved_bytes == 64
    rejected_while_orphaned = try
        Arrow.Flight.transport_server_streaming_call(
            noncooperative_service,
            Arrow.Flight.ServerCallContext(),
            noncooperative_method,
            fixture.protocol.Ticket(b"rejected-while-orphaned"),
            _ -> nothing;
            runtime=cleanup_runtime,
        )
        nothing
    catch error
        error
    end
    @test rejected_while_orphaned isa Arrow.Flight.FlightStatusError
    @test rejected_while_orphaned.code == Arrow.Flight.FLIGHT_STATUS_RESOURCE_EXHAUSTED
    @test Arrow.Flight.flight_server_metrics(cleanup_runtime).calls_rejected == 1
    put!(handler_release, nothing)
    @test timedwait(
        () -> begin
            metrics = Arrow.Flight.flight_server_metrics(cleanup_runtime)
            metrics.orphan_tasks == 0 &&
                metrics.active_calls == 0 &&
                metrics.reserved_bytes == 0
        end,
        1.0,
    ) !== :timed_out
    released_metrics = Arrow.Flight.flight_server_metrics(cleanup_runtime)
    @test released_metrics.calls_failed == 1
end
