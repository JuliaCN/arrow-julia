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

Flight.doget(
    client::Client,
    ticket::Protocol.Ticket,
    response::Channel{Protocol.FlightData};
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
) = _grpc_async_request(
    client,
    Flight._doget_client(client; kwargs...),
    ticket,
    response;
    headers=Flight._merge_headers(client, headers),
)

function Flight.doget(
    client::Client,
    ticket::Protocol.Ticket;
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
)
    response = Channel{Protocol.FlightData}(response_capacity)
    req = Flight.doget(client, ticket, response; headers=headers, kwargs...)
    return req, response
end

Flight.doput(
    client::Client,
    request::Channel{Protocol.FlightData},
    response::Channel{Protocol.PutResult};
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
) = _grpc_async_request(
    client,
    Flight._doput_client(client; kwargs...),
    request,
    response;
    headers=Flight._merge_headers(client, headers),
)

function _buffer_flightdata_request(
    source;
    max_send_message_length::Integer,
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
    request = IOBuffer()
    _emitflightdata!(
        function (message)
            encoded = gRPCClient.grpc_encode_request_iobuffer(
                message;
                max_send_message_length=max_send_message_length,
            )
            write(request, take!(encoded))
        end,
        source;
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
    seekstart(request)
    return request
end

function Flight.doput(
    client::Client;
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
)
    request = Channel{Protocol.FlightData}(request_capacity)
    response = Channel{Protocol.PutResult}(response_capacity)
    req = Flight.doput(client, request, response; headers=headers, kwargs...)
    return req, request, response
end

function Flight.doput(
    client::Client,
    source,
    response::Channel{Protocol.PutResult};
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    headers::AbstractVector{<:Pair}=HeaderPair[],
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
    kwargs...,
)
    rpc_client = Flight._doput_client(client; kwargs...)
    request = _buffer_flightdata_request(
        source;
        max_send_message_length=rpc_client.max_send_message_length,
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
    grpc_request = _grpc_async_prebuffered_request(
        client,
        rpc_client,
        request,
        response;
        headers=Flight._merge_headers(client, headers),
    )
    return FlightAsyncRequest(grpc_request, nothing)
end

function Flight.doput(
    client::Client,
    source;
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    headers::AbstractVector{<:Pair}=HeaderPair[],
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
    kwargs...,
)
    response = Channel{Protocol.PutResult}(response_capacity)
    req = Flight.doput(
        client,
        source,
        response;
        request_capacity=request_capacity,
        headers=headers,
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
        kwargs...,
    )
    return req, response
end

Flight.doexchange(
    client::Client,
    request::Channel{Protocol.FlightData},
    response::Channel{Protocol.FlightData};
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
) = _grpc_async_request(
    client,
    Flight._doexchange_client(client; kwargs...),
    request,
    response,
    headers=Flight._merge_headers(client, headers),
)

function _putflightdata_or_stop!(
    sink::Channel{Protocol.FlightData},
    source;
    kwargs...,
)
    try
        return putflightdata!(sink, source; kwargs...)
    catch err
        if err isa InvalidStateException && !isopen(sink)
            return sink
        end
        rethrow()
    end
end

function Flight.doexchange(
    client::Client;
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
)
    request = Channel{Protocol.FlightData}(request_capacity)
    response = Channel{Protocol.FlightData}(response_capacity)
    req = Flight.doexchange(client, request, response; headers=headers, kwargs...)
    return req, request, response
end

function Flight.doexchange(
    client::Client,
    source,
    response::Channel{Protocol.FlightData};
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    headers::AbstractVector{<:Pair}=HeaderPair[],
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
    kwargs...,
)
    request = Channel{Protocol.FlightData}(request_capacity)
    producer = _start_flight_producer() do
        _putflightdata_or_stop!(
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
        grpc_request =
            Flight.doexchange(client, request, response; headers=headers, kwargs...)
        return FlightAsyncRequest(grpc_request, producer)
    catch
        isopen(request) && close(request)
        wait(producer)
        rethrow()
    end
end

"""
    Arrow.Flight.doexchange(client, source; kwargs...) -> (request, response)
    Arrow.Flight.doexchange(client, source, response; kwargs...) -> request

Open a Flight `DoExchange` call from a native Julia source. The request stream
is encoded with [`Arrow.Flight.putflightdata!`](@ref), so callers can pass a
Tables.jl-compatible source directly instead of manually constructing
`FlightData` messages.
"""
function Flight.doexchange(
    client::Client,
    source;
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    headers::AbstractVector{<:Pair}=HeaderPair[],
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
    kwargs...,
)
    response = Channel{Protocol.FlightData}(response_capacity)
    req = Flight.doexchange(
        client,
        source,
        response;
        request_capacity=request_capacity,
        headers=headers,
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
        kwargs...,
    )
    return req, response
end
