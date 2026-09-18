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

# gRPCServer currently exposes cancellation as a query rather than a waitable
# notification.  Transport completion and Channel delivery are event-driven;
# this low-frequency timer is only the compatibility bridge that turns the
# upstream query into a local notification.
const TRANSPORT_CONTEXT_CHECK_SECONDS = 0.05

mutable struct _TransportContextMonitor
    event::Base.Event
    errors::Channel{Any}
    timer::Timer
    task::Task
    stopped::Threads.Atomic{Bool}
end

function _start_transport_context_monitor(
    context::ServerCallContext;
    on_abort::Function=() -> nothing,
)
    event = Base.Event()
    errors = Channel{Any}(1)
    timer = Timer(TRANSPORT_CONTEXT_CHECK_SECONDS; interval=TRANSPORT_CONTEXT_CHECK_SECONDS)
    stopped = Threads.Atomic{Bool}(false)
    task = errormonitor(@async begin
        try
            while !stopped[]
                wait(timer)
                stopped[] && break
                try
                    checkcall(context)
                catch error
                    put!(errors, error)
                    try
                        on_abort()
                    catch
                    end
                    notify(event)
                    break
                end
            end
        catch error
            error isa EOFError || rethrow()
        finally
            isopen(timer) && close(timer)
        end
    end)
    return _TransportContextMonitor(event, errors, timer, task, stopped)
end

_transport_monitor_error(monitor::_TransportContextMonitor) =
    isready(monitor.errors) ? fetch(monitor.errors) : nothing

function _stop_transport_context_monitor!(monitor::_TransportContextMonitor)
    Threads.atomic_xchg!(monitor.stopped, true)
    isopen(monitor.timer) && close(monitor.timer)
    notify(monitor.event)
    return nothing
end

function _rethrow_transport_status_error(error, on_status_error::Function)
    error isa FlightStatusError && return on_status_error(error)
    rethrow()
end

function _transport_handler_result(task::Task, producer::Union{Nothing,Task}=nothing)
    task_error = nothing
    try
        wait(task)
    catch
    end
    istaskfailed(task) && (task_error = task.exception)

    producer_error = nothing
    if !isnothing(producer) && istaskdone(producer)
        try
            wait(producer)
        catch
        end
        istaskfailed(producer) && (producer_error = producer.exception)
    end

    isnothing(task_error) || throw(task_error)
    isnothing(producer_error) || throw(producer_error)
    return nothing
end

function _transport_task_result(task::Task, context::ServerCallContext)
    istaskdone(task) || begin
        monitor = _start_transport_context_monitor(context)
        errormonitor(@async begin
            try
                wait(task)
            catch
            finally
                notify(monitor.event)
            end
        end)
        try
            wait(monitor.event)
            monitor_error = _transport_monitor_error(monitor)
            isnothing(monitor_error) || throw(monitor_error)
        finally
            _stop_transport_context_monitor!(monitor)
        end
    end
    checkcall(context)
    try
        wait(task)
    catch
    end
    istaskfailed(task) && throw(task.exception)
    return fetch(task)
end

function _take_transport_response!(
    response::Channel,
    context::ServerCallContext,
    monitor::_TransportContextMonitor,
)
    checkcall(context)
    value = try
        take!(response)
    catch error
        if error isa InvalidStateException && !isopen(response)
            monitor_error = _transport_monitor_error(monitor)
            isnothing(monitor_error) || throw(monitor_error)
            checkcall(context)
            return nothing
        end
        rethrow()
    end
    monitor_error = _transport_monitor_error(monitor)
    isnothing(monitor_error) || throw(monitor_error)
    checkcall(context)
    return Some(value)
end

function _wait_for_transport_cleanup(task::Task, timeout::Real)
    istaskdone(task) && return true
    timeout <= 0 && return false
    event = Base.Event()
    errormonitor(@async begin
        try
            wait(task)
        catch
        finally
            notify(event)
        end
    end)
    timer = Timer(_ -> notify(event), Float64(timeout))
    try
        wait(event)
        return istaskdone(task)
    finally
        isopen(timer) && close(timer)
    end
end

function _transport_cleanup_task(
    task::Union{Nothing,Task},
    runtime::Union{Nothing,FlightServerRuntime}=nothing,
)
    isnothing(task) && return nothing
    istaskdone(task) && return nothing
    if runtime !== nothing
        if !_wait_for_transport_cleanup(task, runtime.cleanup_grace_seconds)
            _observe_cleanup_timeout!(runtime)
            return task
        end
    end
    try
        wait(task)
    catch
    end
    return nothing
end

function _transport_cleanup_tasks(
    runtime::Union{Nothing,FlightServerRuntime},
    tasks::Task...,
)
    orphan_tasks = Task[]
    for task in tasks
        orphan_task = _transport_cleanup_task(task, runtime)
        !isnothing(orphan_task) && push!(orphan_tasks, orphan_task)
    end
    return orphan_tasks
end

function _finalize_flight_call!(
    lease::Union{Nothing,_FlightCallLease},
    failed::Bool,
    orphan_tasks::Vector{Task},
)
    isnothing(lease) && return nothing
    if isempty(orphan_tasks)
        _leave_flight_call!(lease, failed)
        return nothing
    end

    runtime = lease.runtime
    errormonitor(@async begin
        @sync for orphan_task in orphan_tasks
            @async begin
                try
                    wait(orphan_task)
                catch
                finally
                    _observe_orphan_finished!(runtime)
                end
            end
        end
        _leave_flight_call!(lease, failed)
    end)
    return nothing
end

function _transport_close_request!(request::Channel)
    isopen(request) || return nothing
    try
        close(request)
    catch
    end
    return nothing
end

function _pump_transport_messages!(
    request::Channel,
    messages,
    context::ServerCallContext,
    runtime::Union{Nothing,FlightServerRuntime}=nothing,
)
    try
        for message in messages
            checkcall(context)
            _observe_flight_request!(runtime, message)
            put!(request, message)
        end
        checkcall(context)
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
    runtime::Union{Nothing,FlightServerRuntime}=nothing,
)
    lease = nothing
    failed = true
    try
        runtime !== nothing && (lease = _enter_flight_call(runtime))
        _observe_flight_request!(runtime, request)
        checkcall(context)
        result = dispatch(service, context, method.method, request)
        checkcall(context)
        _observe_flight_response!(runtime, result)
        failed = false
        return result
    catch error
        _rethrow_transport_status_error(error, on_status_error)
    finally
        lease !== nothing && _leave_flight_call!(lease, failed)
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
    runtime::Union{Nothing,FlightServerRuntime}=nothing,
)
    lease = nothing
    failed = true
    response = Channel{method.method.response_type}(response_capacity)
    try
        runtime !== nothing && (lease = _enter_flight_call(runtime))
    catch error
        _rethrow_transport_status_error(error, on_status_error)
    end
    task = _transport_spawn() do
        try
            checkcall(context)
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
    monitor = _start_transport_context_monitor(
        context;
        on_abort=() -> begin
            isopen(response) && close(response)
        end,
    )
    try
        _observe_flight_request!(runtime, request)
        while true
            item = _take_transport_response!(response, context, monitor)
            item === nothing && break
            message = something(item)
            _observe_flight_response!(runtime, message)
            emit(message)
        end
        _transport_handler_result(task)
        checkcall(context)
        failed = false
        return nothing
    catch error
        _rethrow_transport_status_error(error, on_status_error)
    finally
        _stop_transport_context_monitor!(monitor)
        isopen(response) && close(response)
        orphan_tasks = _transport_cleanup_tasks(runtime, task)
        _finalize_flight_call!(lease, failed, orphan_tasks)
    end
end

function transport_client_streaming_call(
    service::Service,
    context::ServerCallContext,
    method::TransportMethodDescriptor,
    messages;
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    on_status_error::Function=_default_transport_status_error,
    runtime::Union{Nothing,FlightServerRuntime}=nothing,
)
    lease = nothing
    failed = true
    request = Channel{method.method.request_type}(request_capacity)
    try
        runtime !== nothing && (lease = _enter_flight_call(runtime))
    catch error
        _rethrow_transport_status_error(error, on_status_error)
    end
    producer = _transport_spawn() do
        _pump_transport_messages!(request, messages, context, runtime)
    end
    task = _transport_spawn() do
        try
            checkcall(context)
            result = dispatch(service, context, method.method, request)
            checkcall(context)
            result
        catch error
            _rethrow_transport_status_error(error, on_status_error)
        end
    end
    try
        result = _transport_task_result(task, context)
        _transport_handler_result(task, producer)
        checkcall(context)
        _observe_flight_response!(runtime, result)
        failed = false
        return result
    catch error
        _rethrow_transport_status_error(error, on_status_error)
    finally
        _transport_close_request!(request)
        orphan_tasks = _transport_cleanup_tasks(runtime, task, producer)
        _finalize_flight_call!(lease, failed, orphan_tasks)
    end
end

function transport_client_streaming_live_call(
    service::Service,
    context::ServerCallContext,
    method::TransportMethodDescriptor,
    request::Channel{T};
    on_status_error::Function=_default_transport_status_error,
    runtime::Union{Nothing,FlightServerRuntime}=nothing,
) where {T}
    lease = nothing
    failed = true
    try
        runtime !== nothing && (lease = _enter_flight_call(runtime))
    catch error
        _rethrow_transport_status_error(error, on_status_error)
    end
    task = _transport_spawn() do
        try
            checkcall(context)
            result = dispatch(service, context, method.method, request)
            checkcall(context)
            result
        catch error
            _rethrow_transport_status_error(error, on_status_error)
        end
    end
    try
        result = _transport_task_result(task, context)
        _transport_handler_result(task)
        checkcall(context)
        _observe_flight_response!(runtime, result)
        failed = false
        return result
    catch error
        _rethrow_transport_status_error(error, on_status_error)
    finally
        orphan_tasks = _transport_cleanup_tasks(runtime, task)
        _finalize_flight_call!(lease, failed, orphan_tasks)
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
    runtime::Union{Nothing,FlightServerRuntime}=nothing,
)
    lease = nothing
    failed = true
    request = Channel{method.method.request_type}(request_capacity)
    response = Channel{method.method.response_type}(response_capacity)
    try
        runtime !== nothing && (lease = _enter_flight_call(runtime))
    catch error
        _rethrow_transport_status_error(error, on_status_error)
    end
    producer = _transport_spawn() do
        _pump_transport_messages!(request, messages, context, runtime)
    end
    task = _transport_spawn() do
        try
            checkcall(context)
            dispatch(service, context, method.method, request, response)
        catch error
            _rethrow_transport_status_error(error, on_status_error)
        finally
            close(response)
        end
    end
    monitor = _start_transport_context_monitor(
        context;
        on_abort=() -> begin
            isopen(response) && close(response)
        end,
    )
    try
        while true
            item = _take_transport_response!(response, context, monitor)
            item === nothing && break
            message = something(item)
            _observe_flight_response!(runtime, message)
            emit(message)
        end
        _transport_close_request!(request)
        _transport_handler_result(task, producer)
        checkcall(context)
        failed = false
        return nothing
    catch error
        _rethrow_transport_status_error(error, on_status_error)
    finally
        _stop_transport_context_monitor!(monitor)
        _transport_close_request!(request)
        isopen(response) && close(response)
        orphan_tasks = _transport_cleanup_tasks(runtime, task, producer)
        _finalize_flight_call!(lease, failed, orphan_tasks)
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
    runtime::Union{Nothing,FlightServerRuntime}=nothing,
) where {T}
    lease = nothing
    failed = true
    response = Channel{method.method.response_type}(response_capacity)
    try
        runtime !== nothing && (lease = _enter_flight_call(runtime))
    catch error
        _rethrow_transport_status_error(error, on_status_error)
    end
    task = _transport_spawn() do
        try
            checkcall(context)
            dispatch(service, context, method.method, request, response)
        catch error
            _rethrow_transport_status_error(error, on_status_error)
        finally
            close(response)
        end
    end
    monitor = _start_transport_context_monitor(
        context;
        on_abort=() -> begin
            isopen(response) && close(response)
        end,
    )
    try
        while true
            item = _take_transport_response!(response, context, monitor)
            item === nothing && break
            message = something(item)
            _observe_flight_response!(runtime, message)
            emit(message)
        end
        _transport_handler_result(task)
        checkcall(context)
        failed = false
        return nothing
    catch error
        _rethrow_transport_status_error(error, on_status_error)
    finally
        _stop_transport_context_monitor!(monitor)
        isopen(response) && close(response)
        orphan_tasks = _transport_cleanup_tasks(runtime, task)
        _finalize_flight_call!(lease, failed, orphan_tasks)
    end
end
