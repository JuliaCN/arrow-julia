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

const GENERATED_TYPE_PREFIX = "Arrow.Flight.Generated."

struct TransportMethodDescriptor
    method::MethodDescriptor
    request_type_name::String
    response_type_name::String
end

struct TransportServiceDescriptor
    name::String
    methods::Vector{TransportMethodDescriptor}
    method_lookup::Dict{String,TransportMethodDescriptor}
end

function transport_type_name(T::Type)
    type_name = string(T)
    if startswith(type_name, GENERATED_TYPE_PREFIX)
        return type_name[(ncodeunits(GENERATED_TYPE_PREFIX) + 1):end]
    end
    return type_name
end

function TransportMethodDescriptor(method::MethodDescriptor)
    return TransportMethodDescriptor(
        method,
        transport_type_name(method.request_type),
        transport_type_name(method.response_type),
    )
end

function TransportServiceDescriptor(
    name::AbstractString,
    methods::Vector{TransportMethodDescriptor},
)
    lookup = Dict{String,TransportMethodDescriptor}()
    for method in methods
        lookup[method.method.name] = method
        lookup[method.method.path] = method
    end
    return TransportServiceDescriptor(String(name), methods, lookup)
end

function transportdescriptor(descriptor::ServiceDescriptor)
    return TransportServiceDescriptor(
        descriptor.name,
        TransportMethodDescriptor.(descriptor.methods),
    )
end

transportdescriptor(service::Service) = transportdescriptor(servicedescriptor(service))

function lookuptransportmethod(descriptor::TransportServiceDescriptor, key::AbstractString)
    return get(descriptor.method_lookup, String(key), nothing)
end

lookuptransportmethod(service::Service, key::AbstractString) =
    lookuptransportmethod(transportdescriptor(service), key)

_default_transport_status_error(error::FlightStatusError) = throw(error)

function _rethrow_transport_status_error(error, on_status_error::Function)
    error isa FlightStatusError && return on_status_error(error)
    rethrow()
end

function _transport_handler_result(task::Task, producer::Union{Nothing,Task}=nothing)
    if !isnothing(producer)
        if istaskfailed(producer)
            throw(producer.exception)
        end
        wait(producer)
    end
    if istaskfailed(task)
        throw(task.exception)
    end
    wait(task)
    return nothing
end

function _transport_cleanup_task(task::Union{Nothing,Task})
    isnothing(task) && return nothing
    istaskdone(task) && return nothing
    try
        wait(task)
    catch
    end
    return nothing
end

function _pump_transport_messages!(request::Channel, messages)
    try
        for message in messages
            put!(request, message)
        end
    finally
        close(request)
    end
    return nothing
end

# Use non-sticky worker tasks so CPU-heavy Flight handlers can overlap on
# Julia's thread pool instead of pinning one event-loop thread.
_transport_spawn(f::Function) = Threads.@spawn f()

function transport_unary_call(
    service::Service,
    context::ServerCallContext,
    method::TransportMethodDescriptor,
    request;
    on_status_error::Function=_default_transport_status_error,
)
    try
        return dispatch(service, context, method.method, request)
    catch error
        _rethrow_transport_status_error(error, on_status_error)
    end
end

function transport_server_streaming_call(
    service::Service,
    context::ServerCallContext,
    method::TransportMethodDescriptor,
    request,
    emit::Function;
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    on_status_error::Function=_default_transport_status_error,
)
    response = Channel{method.method.response_type}(response_capacity)
    task = _transport_spawn() do
        try
            if method.method.handler_field === :listactions
                listactions(service, context, response)
            else
                dispatch(service, context, method.method, request, response)
            end
        catch error
            _rethrow_transport_status_error(error, on_status_error)
        finally
            close(response)
        end
    end
    try
        for message in response
            emit(message)
        end
        _transport_handler_result(task)
        return nothing
    finally
        isopen(response) && close(response)
        _transport_cleanup_task(task)
    end
end

function transport_client_streaming_call(
    service::Service,
    context::ServerCallContext,
    method::TransportMethodDescriptor,
    messages;
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    on_status_error::Function=_default_transport_status_error,
)
    request = Channel{method.method.request_type}(request_capacity)
    producer = _transport_spawn() do
        _pump_transport_messages!(request, messages)
    end
    task = _transport_spawn() do
        try
            dispatch(service, context, method.method, request)
        catch error
            _rethrow_transport_status_error(error, on_status_error)
        end
    end
    try
        return fetch(task)
    finally
        _transport_handler_result(task, producer)
    end
end

function transport_client_streaming_live_call(
    service::Service,
    context::ServerCallContext,
    method::TransportMethodDescriptor,
    request::Channel{T};
    on_status_error::Function=_default_transport_status_error,
) where {T}
    task = _transport_spawn() do
        try
            dispatch(service, context, method.method, request)
        catch error
            _rethrow_transport_status_error(error, on_status_error)
        end
    end
    try
        return fetch(task)
    finally
        _transport_handler_result(task)
    end
end

function transport_bidi_streaming_call(
    service::Service,
    context::ServerCallContext,
    method::TransportMethodDescriptor,
    messages,
    emit::Function;
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    on_status_error::Function=_default_transport_status_error,
)
    request = Channel{method.method.request_type}(request_capacity)
    response = Channel{method.method.response_type}(response_capacity)
    producer = _transport_spawn() do
        _pump_transport_messages!(request, messages)
    end
    task = _transport_spawn() do
        try
            dispatch(service, context, method.method, request, response)
        catch error
            _rethrow_transport_status_error(error, on_status_error)
        finally
            close(response)
        end
    end
    try
        for message in response
            emit(message)
        end
        _transport_handler_result(task, producer)
        return nothing
    finally
        isopen(response) && close(response)
        _transport_cleanup_task(task)
        _transport_cleanup_task(producer)
    end
end

function transport_bidi_streaming_live_call(
    service::Service,
    context::ServerCallContext,
    method::TransportMethodDescriptor,
    request::Channel{T},
    emit::Function;
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    on_status_error::Function=_default_transport_status_error,
) where {T}
    response = Channel{method.method.response_type}(response_capacity)
    task = _transport_spawn() do
        try
            dispatch(service, context, method.method, request, response)
        catch error
            _rethrow_transport_status_error(error, on_status_error)
        finally
            close(response)
        end
    end
    try
        for message in response
            emit(message)
        end
        _transport_handler_result(task)
        return nothing
    finally
        isopen(response) && close(response)
        _transport_cleanup_task(task)
    end
end
