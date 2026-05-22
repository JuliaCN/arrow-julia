# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

using Arrow
using Printf
using Tables

function measure(f)
    GC.gc()
    result = Ref{Any}()
    start = time_ns()
    allocated = @allocated begin
        result[] = f()
    end
    elapsed_ms = (time_ns() - start) / 1_000_000
    return (value=result[], elapsed_ms=elapsed_ms, allocated=allocated)
end

function better_sample(candidate, current)
    current === nothing && return true
    candidate.allocated < current.allocated && return true
    return candidate.allocated == current.allocated &&
           candidate.elapsed_ms < current.elapsed_ms
end

function steady_measure(operation; warmups::Int=3, samples::Int=7)
    for _ = 1:warmups
        operation()
    end
    best = nothing
    for _ = 1:samples
        sample = measure(operation)
        better_sample(sample, best) && (best = sample)
    end
    return best
end

function optional_int_limit(name::AbstractString)
    value = strip(get(ENV, name, ""))
    isempty(value) && return nothing
    parsed = tryparse(Int, value)
    parsed !== nothing && parsed >= 0 ||
        throw(ArgumentError("$name must be a non-negative integer"))
    return parsed
end

function enforce_max(label::AbstractString, value, limit)
    limit === nothing && return nothing
    println("$(label)_max=$(limit)")
    value <= limit || error("$(label)=$(value) exceeded configured max $(limit)")
    return nothing
end

function print_measure(label::AbstractString, sample)
    @printf("%s_ms=%.3f\n", label, sample.elapsed_ms)
    println("$(label)_alloc_bytes=$(sample.allocated)")
    println("$(label)_checksum=$(sample.value)")
    return nothing
end

function wire_source()
    return ((id=collect(Int64, 1:1024), payload=fill("payload"^16, 1024)),)
end

function wire_messages()
    return Arrow.Flight.flightdata(Tables.partitioner(wire_source()))
end

function wire_ticket_frame()
    ticket = Arrow.Flight.Protocol.Ticket(UInt8.(1:64))
    return length(Arrow.Flight.grpcmessage(ticket))
end

function wire_flightdata_size(message)
    return Arrow.Flight.grpcmessagesize(message)
end

function wire_flightdata_frame(message)
    return length(Arrow.Flight.grpcmessage(message))
end

function wire_flightdata_emit()
    checksum = 0
    for message in wire_messages()
        checksum += Arrow.Flight.grpcmessagesize(message)
    end
    return checksum
end

function main()
    warmups = parse(Int, get(ENV, "ARROW_FLIGHT_WIRE_REPORT_WARMUPS", "3"))
    samples = parse(Int, get(ENV, "ARROW_FLIGHT_WIRE_REPORT_SAMPLES", "7"))
    warmups >= 0 ||
        throw(ArgumentError("ARROW_FLIGHT_WIRE_REPORT_WARMUPS must be non-negative"))
    samples > 0 || throw(ArgumentError("ARROW_FLIGHT_WIRE_REPORT_SAMPLES must be positive"))

    messages = wire_messages()
    flightdata_message = last(messages)

    ticket_frame = steady_measure(wire_ticket_frame; warmups=warmups, samples=samples)
    flightdata_size = steady_measure(
        () -> wire_flightdata_size(flightdata_message);
        warmups=warmups,
        samples=samples,
    )
    flightdata_frame = steady_measure(
        () -> wire_flightdata_frame(flightdata_message);
        warmups=warmups,
        samples=samples,
    )
    flightdata_emit = steady_measure(wire_flightdata_emit; warmups=warmups, samples=samples)

    println("arrow_flight_wire_performance_report")
    println("message_count=$(length(messages))")
    println("julia_num_threads=$(Threads.nthreads())")
    println("warmups=$(warmups)")
    println("samples=$(samples)")
    print_measure("flight_wire_ticket_frame", ticket_frame)
    print_measure("flight_wire_flightdata_size", flightdata_size)
    print_measure("flight_wire_flightdata_frame", flightdata_frame)
    print_measure("flight_wire_flightdata_emit", flightdata_emit)

    enforce_max(
        "flight_wire_ticket_frame_alloc_bytes",
        ticket_frame.allocated,
        optional_int_limit("ARROW_FLIGHT_WIRE_MAX_TICKET_FRAME_ALLOC_BYTES"),
    )
    enforce_max(
        "flight_wire_flightdata_size_alloc_bytes",
        flightdata_size.allocated,
        optional_int_limit("ARROW_FLIGHT_WIRE_MAX_FLIGHTDATA_SIZE_ALLOC_BYTES"),
    )
    enforce_max(
        "flight_wire_flightdata_frame_alloc_bytes",
        flightdata_frame.allocated,
        optional_int_limit("ARROW_FLIGHT_WIRE_MAX_FLIGHTDATA_FRAME_ALLOC_BYTES"),
    )
    enforce_max(
        "flight_wire_flightdata_emit_alloc_bytes",
        flightdata_emit.allocated,
        optional_int_limit("ARROW_FLIGHT_WIRE_MAX_FLIGHTDATA_EMIT_ALLOC_BYTES"),
    )
    return nothing
end

main()
