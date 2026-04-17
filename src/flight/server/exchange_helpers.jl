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

struct PrimedFlightMessages{M,T,S}
    source::M
    first::T
    next_state::S
end

Base.IteratorSize(::Type{<:PrimedFlightMessages}) = Base.SizeUnknown()
Base.eltype(::Type{PrimedFlightMessages{M,T,S}}) where {M,T,S} = T
Base.iterate(messages::PrimedFlightMessages) = (messages.first, messages.next_state)
Base.iterate(messages::PrimedFlightMessages, state) = iterate(messages.source, state)

function _prime_flight_messages(request)
    state = iterate(request)
    state === nothing && throw(
        ArgumentError(
            "Arrow Flight exchange request must contain at least one FlightData message",
        ),
    )
    first_message, next_state = state
    return PrimedFlightMessages(request, first_message, next_state), first_message
end

function _request_descriptor(
    first_message::Protocol.FlightData,
    fallback_descriptor::Union{Nothing,Protocol.FlightDescriptor},
)
    descriptor = first_message.flight_descriptor
    return isnothing(descriptor) ? fallback_descriptor : descriptor
end

_response_metadata(metadata, output) = metadata
_response_metadata(metadata::Function, output) = metadata(output)
_response_app_metadata(app_metadata, output) = app_metadata
_response_app_metadata(app_metadata::Function, output) = app_metadata(output)

function _write_exchange_response!(
    response,
    output,
    request_descriptor,
    context;
    metadata=nothing,
    app_metadata=nothing,
)
    return putflightdata!(
        response,
        output;
        metadata=_response_metadata(metadata, output),
        app_metadata=_response_app_metadata(app_metadata, output),
    )
end

"""
    Arrow.Flight.exchangeservice(handler; descriptor=nothing, metadata=nothing, app_metadata=nothing, writer=nothing)

Build a native Julia Flight `Service` whose `DoExchange` handler consumes an
incoming Flight message stream and emits a response stream through the shared
`putflightdata!` runtime contract.
"""
function exchangeservice(
    handler::Function;
    descriptor::Union{Nothing,Protocol.FlightDescriptor}=nothing,
    metadata=nothing,
    app_metadata=nothing,
    writer::Union{Nothing,Function}=nothing,
)
    response_writer =
        isnothing(writer) ?
        (
            (response, output, request_descriptor, context) -> _write_exchange_response!(
                response,
                output,
                request_descriptor,
                context;
                metadata=metadata,
                app_metadata=app_metadata,
            )
        ) : writer
    return Service(
        doexchange=function (context, request, response)
            incoming_messages, first_message = _prime_flight_messages(request)
            request_descriptor = _request_descriptor(first_message, descriptor)
            try
                output = handler(incoming_messages, request_descriptor, context)
                response_writer(response, output, request_descriptor, context)
                return nothing
            finally
                isopen(response) && close(response)
            end
        end,
    )
end

"""
    Arrow.Flight.tableservice(processor; convert=true, descriptor=nothing, metadata=nothing, app_metadata=nothing, include_request_app_metadata=false)

Build a Flight `Service` that materializes each incoming `DoExchange` request
as an `Arrow.Table`, passes it to `processor`, and re-emits the returned source
through the native Flight runtime. When
`include_request_app_metadata=true`, `processor` receives the same
`(table=..., app_metadata=...)` wrapper returned by
[`Arrow.Flight.table`](@ref) when `include_app_metadata=true`.
"""
function tableservice(
    processor::Function;
    convert::Bool=true,
    descriptor::Union{Nothing,Protocol.FlightDescriptor}=nothing,
    metadata=nothing,
    app_metadata=nothing,
    include_request_app_metadata::Bool=false,
)
    return exchangeservice(
        (messages, _, _) -> processor(
            table(
                messages;
                convert=convert,
                include_app_metadata=include_request_app_metadata,
            ),
        );
        descriptor=descriptor,
        metadata=metadata,
        app_metadata=app_metadata,
    )
end

"""
    Arrow.Flight.streamservice(processor; convert=true, descriptor=nothing, metadata=nothing, app_metadata=nothing, include_request_app_metadata=false)

Build a Flight `Service` that exposes the incoming `DoExchange` request as a
native Julia Flight batch iterator and re-emits the `processor` result through
the same runtime contract. When `include_request_app_metadata=true`, the
iterator yields the same `(table=..., app_metadata=...)` values returned by
[`Arrow.Flight.stream`](@ref) when `include_app_metadata=true`.
"""
function streamservice(
    processor::Function;
    convert::Bool=true,
    descriptor::Union{Nothing,Protocol.FlightDescriptor}=nothing,
    metadata=nothing,
    app_metadata=nothing,
    include_request_app_metadata::Bool=false,
)
    return exchangeservice(
        (messages, _, _) -> processor(
            stream(
                messages;
                convert=convert,
                include_app_metadata=include_request_app_metadata,
            ),
        );
        descriptor=descriptor,
        metadata=metadata,
        app_metadata=app_metadata,
    )
end

"""
    Arrow.Flight.doexchange(service, context, source, response; kwargs...)
    Arrow.Flight.doexchange(service, context, source; kwargs...)

Invoke a native Julia Flight `Service` in-process through its `DoExchange`
handler. The source is encoded into Flight `FlightData` request messages with
[`Arrow.Flight.putflightdata!`](@ref), then dispatched through the same
runtime contract used by the packaged Flight server transports.

The four-argument form writes response messages into the provided `response`
channel and returns `nothing`. The three-argument form allocates a response
channel, returns it, and leaves consumption to the caller. Shared keyword
arguments control request buffering plus the Arrow IPC emission settings used
to encode `source`, including `descriptor`, `metadata`, `colmetadata`, and
batch-wise `app_metadata`.
"""
function doexchange(
    service::Service,
    context::ServerCallContext,
    source,
    response::Channel{Protocol.FlightData};
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    descriptor::Union{Nothing,Protocol.FlightDescriptor}=nothing,
    compress=nothing,
    largelists::Bool=false,
    denseunions::Bool=true,
    dictencode::Bool=false,
    dictencodenested::Bool=false,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    maxdepth::Integer=ArrowParent.DEFAULT_MAX_DEPTH,
    metadata::Union{Nothing,Any}=nothing,
    colmetadata::Union{Nothing,Any}=nothing,
    app_metadata=nothing,
)
    request = Channel{Protocol.FlightData}(request_capacity)
    producer = ArrowParent.Flight._start_flight_producer() do
        putflightdata!(
            request,
            source;
            close=true,
            descriptor=descriptor,
            compress=compress,
            largelists=largelists,
            denseunions=denseunions,
            dictencode=dictencode,
            dictencodenested=dictencodenested,
            alignment=alignment,
            maxdepth=maxdepth,
            metadata=metadata,
            colmetadata=colmetadata,
            app_metadata=app_metadata,
        )
    end
    try
        doexchange(service, context, request, response)
        wait(producer)
        return nothing
    catch
        isopen(request) && close(request)
        wait(producer)
        rethrow()
    end
end

function doexchange(
    service::Service,
    context::ServerCallContext,
    source;
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    descriptor::Union{Nothing,Protocol.FlightDescriptor}=nothing,
    compress=nothing,
    largelists::Bool=false,
    denseunions::Bool=true,
    dictencode::Bool=false,
    dictencodenested::Bool=false,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    maxdepth::Integer=ArrowParent.DEFAULT_MAX_DEPTH,
    metadata::Union{Nothing,Any}=nothing,
    colmetadata::Union{Nothing,Any}=nothing,
    app_metadata=nothing,
)
    response = Channel{Protocol.FlightData}(response_capacity)
    doexchange(
        service,
        context,
        source,
        response;
        request_capacity=request_capacity,
        descriptor=descriptor,
        compress=compress,
        largelists=largelists,
        denseunions=denseunions,
        dictencode=dictencode,
        dictencodenested=dictencodenested,
        alignment=alignment,
        maxdepth=maxdepth,
        metadata=metadata,
        colmetadata=colmetadata,
        app_metadata=app_metadata,
    )
    return response
end

function stream(
    service::Service,
    context::ServerCallContext,
    source;
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    descriptor::Union{Nothing,Protocol.FlightDescriptor}=nothing,
    compress=nothing,
    largelists::Bool=false,
    denseunions::Bool=true,
    dictencode::Bool=false,
    dictencodenested::Bool=false,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    maxdepth::Integer=ArrowParent.DEFAULT_MAX_DEPTH,
    metadata::Union{Nothing,Any}=nothing,
    colmetadata::Union{Nothing,Any}=nothing,
    app_metadata=nothing,
    convert::Bool=true,
    include_app_metadata::Bool=false,
)
    response = doexchange(
        service,
        context,
        source;
        request_capacity=request_capacity,
        response_capacity=response_capacity,
        descriptor=descriptor,
        compress=compress,
        largelists=largelists,
        denseunions=denseunions,
        dictencode=dictencode,
        dictencodenested=dictencodenested,
        alignment=alignment,
        maxdepth=maxdepth,
        metadata=metadata,
        colmetadata=colmetadata,
        app_metadata=app_metadata,
    )
    return stream(response; convert=convert, include_app_metadata=include_app_metadata)
end

function table(
    service::Service,
    context::ServerCallContext,
    source;
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    descriptor::Union{Nothing,Protocol.FlightDescriptor}=nothing,
    compress=nothing,
    largelists::Bool=false,
    denseunions::Bool=true,
    dictencode::Bool=false,
    dictencodenested::Bool=false,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    maxdepth::Integer=ArrowParent.DEFAULT_MAX_DEPTH,
    metadata::Union{Nothing,Any}=nothing,
    colmetadata::Union{Nothing,Any}=nothing,
    app_metadata=nothing,
    convert::Bool=true,
    include_app_metadata::Bool=false,
)
    response = doexchange(
        service,
        context,
        source;
        request_capacity=request_capacity,
        response_capacity=response_capacity,
        descriptor=descriptor,
        compress=compress,
        largelists=largelists,
        denseunions=denseunions,
        dictencode=dictencode,
        dictencodenested=dictencodenested,
        alignment=alignment,
        maxdepth=maxdepth,
        metadata=metadata,
        colmetadata=colmetadata,
        app_metadata=app_metadata,
    )
    return table(response; convert=convert, include_app_metadata=include_app_metadata)
end
