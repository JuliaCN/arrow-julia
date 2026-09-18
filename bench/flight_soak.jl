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

# Fixed-workload production receipt.  CI pins the runner image, Julia and
# PyArrow versions, workload geometry, tail-latency ceilings, and RSS ceiling.

using Arrow
using gRPCServer
using JSON
using Tables
using Test

include(joinpath(@__DIR__, "..", "test", "flight", "support.jl"))
include(joinpath(@__DIR__, "..", "test", "flight", "live_service_support.jl"))

isnothing(FlightTestSupport.pyarrow_flight_python()) &&
    error("PyArrow Flight is required for the production soak receipt")

function _soak_positive_int(name::AbstractString, default::Integer)
    raw = get(ENV, name, string(default))
    value = tryparse(Int, raw)
    isnothing(value) && error("$name must parse as a positive integer; got $(repr(raw))")
    value > 0 || error("$name must be positive; got $(repr(raw))")
    return value
end

function _soak_nonnegative_float(name::AbstractString, default::Real)
    raw = get(ENV, name, string(default))
    value = tryparse(Float64, raw)
    isnothing(value) && error("$name must parse as a non-negative number; got $(repr(raw))")
    value >= 0 || error("$name must be non-negative; got $(repr(raw))")
    return value
end

function _soak_transport()
    return flight_live_transport_backend(
        backend=:grpcserver,
        start_server=service -> Arrow.Flight.grpcserver_flight_server(
            service;
            host="127.0.0.1",
            port=0,
            max_message_size=64 * 1024 * 1024,
            max_concurrent_streams=128,
            max_concurrent_requests=128,
            request_capacity=2,
            response_capacity=2,
            enable_health_check=true,
        ),
        wait_for_server=server -> nothing,
        stop_server=server -> Arrow.Flight.stop!(server; force=true),
        endpoint=server -> (server.host, server.port),
    )
end

soak_rounds = _soak_positive_int("ARROW_FLIGHT_SOAK_ROUNDS", 3)
cancel_rounds = _soak_positive_int("ARROW_FLIGHT_CANCEL_SOAK_ROUNDS", 25)
batch_count = _soak_positive_int("ARROW_FLIGHT_SOAK_BATCHES", 4)
rows_per_batch = _soak_positive_int("ARROW_FLIGHT_SOAK_ROWS_PER_BATCH", 256)
payload_bytes = _soak_positive_int("ARROW_FLIGHT_SOAK_PAYLOAD_BYTES", 1_024)
max_rss_mib = _soak_nonnegative_float("ARROW_FLIGHT_SOAK_MAX_RSS_MIB", Inf)

transport = _soak_transport()
tail_receipts = Dict{String,Any}()
for operation in (:doget, :doput, :doexchange)
    metrics = NamedTuple[]
    for _ = 1:soak_rounds
        metric = flight_live_transport_concurrent_benchmark(
            Arrow.Flight.Protocol,
            transport;
            batch_count=batch_count,
            rows_per_batch=rows_per_batch,
            payload_bytes=payload_bytes,
            operation=operation,
        )
        isnothing(metric) && error("missing concurrent $(operation) soak receipt")
        push!(metrics, metric)
    end
    flight_live_transport_print_concurrent_summary(stdout, metrics)
    tail_receipts[string(operation)] = Dict(
        "max_p95_ms" => maximum(metric.request_p95_ms for metric in metrics),
        "max_p99_ms" => maximum(metric.request_p99_ms for metric in metrics),
        "min_throughput_mib_per_sec" =>
            minimum(metric.throughput_mib_per_sec for metric in metrics),
    )
end

cancellation_fixture = flight_live_transport_fixture(
    Arrow.Flight.Protocol;
    batch_count=max(batch_count, 32),
    rows_per_batch=rows_per_batch,
    payload_bytes=payload_bytes,
)
cancellation_server = Arrow.Flight.grpcserver_flight_server(
    flight_live_transport_service(Arrow.Flight.Protocol, cancellation_fixture);
    host="127.0.0.1",
    port=0,
    max_message_size=64 * 1024 * 1024,
    max_concurrent_requests=32,
    request_capacity=1,
    response_capacity=1,
)
cancellation_receipt = try
    receipt = flight_live_pyarrow_cancellation_soak(
        cancellation_server.host,
        cancellation_server.port,
        cancellation_fixture;
        rounds=cancel_rounds,
    )
    isnothing(receipt) && error("missing cancellation soak receipt")
    settled = timedwait(
        () -> Arrow.Flight.flight_server_metrics(cancellation_server).active_calls == 0,
        10.0;
        pollint=0.05,
    )
    settled === :timed_out &&
        error("Flight calls remained active after cancellation soak")
    metrics = Arrow.Flight.flight_server_metrics(cancellation_server)
    metrics.orphan_tasks == 0 || error("cancellation soak left orphan handler tasks")
    Dict(
        "cancelled_streams" => Int(receipt["cancelled_streams"]),
        "calls_started" => metrics.calls_started,
        "calls_completed" => metrics.calls_completed,
        "calls_failed" => metrics.calls_failed,
        "cleanup_timeouts" => metrics.cleanup_timeouts,
        "orphan_tasks" => metrics.orphan_tasks,
    )
finally
    Arrow.Flight.stop!(cancellation_server; force=true)
end

GC.gc(true)
max_rss_bytes = Int(Sys.maxrss())
max_rss_mib_observed = max_rss_bytes / 1024.0^2
max_rss_mib_observed <= max_rss_mib || error(
    "Flight soak max RSS $(max_rss_mib_observed) MiB exceeds configured ceiling $(max_rss_mib) MiB",
)

println(
    JSON.json(
        Dict(
            "kind" => "arrow-flight-production-soak-v1",
            "runner" => Dict(
                "kernel" => string(Sys.KERNEL),
                "arch" => string(Sys.ARCH),
                "julia_version" => string(VERSION),
                "threads" => Threads.nthreads(),
            ),
            "workload" => Dict(
                "rounds" => soak_rounds,
                "concurrent_clients" => _flight_live_pyarrow_concurrent_clients(),
                "requests_per_client" => _flight_live_pyarrow_requests_per_client(),
                "batch_count" => batch_count,
                "rows_per_batch" => rows_per_batch,
                "payload_bytes" => payload_bytes,
            ),
            "tail_latency" => tail_receipts,
            "cancellation" => cancellation_receipt,
            "max_rss_bytes" => max_rss_bytes,
            "max_rss_mib" => max_rss_mib_observed,
        ),
    ),
)
