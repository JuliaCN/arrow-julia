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

"""
    FlightServerRuntime(; max_active_calls, max_reserved_bytes,
                          call_reservation_bytes, cleanup_grace_seconds)

Shared admission and observability state for a Flight server.  A fixed
reservation is acquired before dispatch so concurrent calls cannot admit more
Arrow/gRPC buffering than the configured server-wide budget.  Reservations are
released when the transport call finishes, including error and cancellation
paths.

`cleanup_grace_seconds` bounds how long transport cleanup waits for a handler
that did not cooperate with cancellation.  Julia tasks are not force-killed;
timed-out tasks are detached and counted as orphans.
"""
mutable struct FlightServerRuntime
    max_active_calls::Int
    max_reserved_bytes::Int64
    call_reservation_bytes::Int64
    cleanup_grace_seconds::Float64
    lock::ReentrantLock
    active_calls::Int
    peak_active_calls::Int
    reserved_bytes::Int64
    peak_reserved_bytes::Int64
    calls_started::Int64
    calls_completed::Int64
    calls_failed::Int64
    calls_rejected::Int64
    request_messages::Int64
    request_bytes::Int64
    response_messages::Int64
    response_bytes::Int64
    cleanup_timeouts::Int64
    orphan_tasks::Int64
end

function FlightServerRuntime(;
    max_active_calls::Integer=1024,
    max_reserved_bytes::Integer=512 * 1024 * 1024,
    call_reservation_bytes::Integer=0,
    cleanup_grace_seconds::Real=1.0,
)
    max_active_calls > 0 || throw(ArgumentError("max_active_calls must be positive"))
    max_reserved_bytes >= 0 ||
        throw(ArgumentError("max_reserved_bytes must be non-negative"))
    call_reservation_bytes >= 0 ||
        throw(ArgumentError("call_reservation_bytes must be non-negative"))
    call_reservation_bytes <= max_reserved_bytes ||
        throw(ArgumentError("call_reservation_bytes cannot exceed max_reserved_bytes"))
    isfinite(cleanup_grace_seconds) && cleanup_grace_seconds >= 0 ||
        throw(ArgumentError("cleanup_grace_seconds must be finite and non-negative"))
    return FlightServerRuntime(
        Int(max_active_calls),
        Int64(max_reserved_bytes),
        Int64(call_reservation_bytes),
        Float64(cleanup_grace_seconds),
        ReentrantLock(),
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    )
end

struct _FlightCallLease
    runtime::FlightServerRuntime
end

function _enter_flight_call(runtime::FlightServerRuntime)
    lock(runtime.lock)
    try
        reservation = runtime.call_reservation_bytes
        rejected =
            runtime.active_calls >= runtime.max_active_calls ||
            reservation > runtime.max_reserved_bytes - runtime.reserved_bytes
        if rejected
            runtime.calls_rejected += 1
            throw(
                FlightStatusError(
                    FLIGHT_STATUS_RESOURCE_EXHAUSTED,
                    "Arrow Flight server resource budget is exhausted",
                ),
            )
        end
        runtime.active_calls += 1
        runtime.reserved_bytes += reservation
        runtime.peak_active_calls = max(runtime.peak_active_calls, runtime.active_calls)
        runtime.peak_reserved_bytes =
            max(runtime.peak_reserved_bytes, runtime.reserved_bytes)
        runtime.calls_started += 1
        return _FlightCallLease(runtime)
    finally
        unlock(runtime.lock)
    end
end

function _leave_flight_call!(lease::_FlightCallLease, failed::Bool)
    runtime = lease.runtime
    lock(runtime.lock)
    try
        runtime.active_calls -= 1
        runtime.reserved_bytes -= runtime.call_reservation_bytes
        if failed
            runtime.calls_failed += 1
        else
            runtime.calls_completed += 1
        end
    finally
        unlock(runtime.lock)
    end
    return nothing
end

function _flight_transport_payload_bytes(value)
    value isa Protocol.FlightData && return Int64(
        length(value.data_header) + length(value.app_metadata) + length(value.data_body),
    )
    try
        return Int64(sizeof(value))
    catch
        return Int64(0)
    end
end

function _observe_flight_request!(runtime::Union{Nothing,FlightServerRuntime}, value)
    runtime === nothing && return nothing
    bytes = _flight_transport_payload_bytes(value)
    lock(runtime.lock)
    try
        runtime.request_messages += 1
        runtime.request_bytes += bytes
    finally
        unlock(runtime.lock)
    end
    return nothing
end

function _observe_flight_response!(runtime::Union{Nothing,FlightServerRuntime}, value)
    runtime === nothing && return nothing
    bytes = _flight_transport_payload_bytes(value)
    lock(runtime.lock)
    try
        runtime.response_messages += 1
        runtime.response_bytes += bytes
    finally
        unlock(runtime.lock)
    end
    return nothing
end

function _observe_cleanup_timeout!(runtime::Union{Nothing,FlightServerRuntime})
    runtime === nothing && return nothing
    lock(runtime.lock)
    try
        runtime.cleanup_timeouts += 1
        runtime.orphan_tasks += 1
    finally
        unlock(runtime.lock)
    end
    return nothing
end

function _observe_orphan_finished!(runtime::FlightServerRuntime)
    lock(runtime.lock)
    try
        runtime.orphan_tasks -= 1
    finally
        unlock(runtime.lock)
    end
    return nothing
end

"""Return an atomic snapshot of Flight server admission and traffic metrics."""
function flight_server_metrics(runtime::FlightServerRuntime)
    lock(runtime.lock)
    try
        return (
            active_calls=runtime.active_calls,
            peak_active_calls=runtime.peak_active_calls,
            reserved_bytes=runtime.reserved_bytes,
            peak_reserved_bytes=runtime.peak_reserved_bytes,
            calls_started=runtime.calls_started,
            calls_completed=runtime.calls_completed,
            calls_failed=runtime.calls_failed,
            calls_rejected=runtime.calls_rejected,
            request_messages=runtime.request_messages,
            request_bytes=runtime.request_bytes,
            response_messages=runtime.response_messages,
            response_bytes=runtime.response_bytes,
            cleanup_timeouts=runtime.cleanup_timeouts,
            orphan_tasks=runtime.orphan_tasks,
        )
    finally
        unlock(runtime.lock)
    end
end
