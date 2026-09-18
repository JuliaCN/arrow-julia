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

# Run with the test environment, which owns gRPCServer and PyArrow discovery:
#
#   julia --project=test bench/flight.jl

using Arrow
using gRPCServer
using JSON
using Tables
using Test

include(joinpath(@__DIR__, "..", "test", "flight", "support.jl"))
include(joinpath(@__DIR__, "..", "test", "flight", "live_service_support.jl"))

isnothing(FlightTestSupport.pyarrow_flight_python()) && error(
    "PyArrow Flight is required for a production benchmark receipt; no compatible Python was found",
)

function _positive_env_int(name::AbstractString, default::Integer)
    raw = get(ENV, name, string(default))
    value = tryparse(Int, raw)
    isnothing(value) && error("$name must be a positive integer; got $(repr(raw))")
    value > 0 || error("$name must be positive; got $(repr(raw))")
    return value
end

transport = flight_live_transport_backend(
    backend=:grpcserver,
    start_server=service -> Arrow.Flight.grpcserver_flight_server(
        service;
        host="127.0.0.1",
        port=0,
        max_message_size=64 * 1024 * 1024,
        max_concurrent_streams=128,
        max_concurrent_requests=128,
        request_capacity=4,
        response_capacity=4,
        enable_health_check=true,
    ),
    wait_for_server=server -> nothing,
    stop_server=server -> Arrow.Flight.stop!(server; force=true),
    endpoint=server -> (server.host, server.port),
)

iterations = _positive_env_int("ARROW_FLIGHT_BENCHMARK_ITERATIONS", 5)
batch_count = _positive_env_int("ARROW_FLIGHT_BENCHMARK_BATCHES", 4)
rows_per_batch = _positive_env_int("ARROW_FLIGHT_BENCHMARK_ROWS_PER_BATCH", 512)
payload_bytes = _positive_env_int("ARROW_FLIGHT_BENCHMARK_PAYLOAD_BYTES", 4_096)

metrics = flight_live_transport_benchmark(
    Arrow.Flight.Protocol,
    transport;
    iterations=iterations,
    batch_count=batch_count,
    rows_per_batch=rows_per_batch,
    payload_bytes=payload_bytes,
    operations=(:doget, :doput, :doput_reused_client, :doexchange),
)
length(metrics) == 4 ||
    error("Flight benchmark did not produce all four operation receipts")
flight_live_transport_print_metrics(stdout, metrics)

for operation in (:doget, :doput, :doexchange)
    metric = flight_live_transport_concurrent_benchmark(
        Arrow.Flight.Protocol,
        transport;
        batch_count=batch_count,
        rows_per_batch=rows_per_batch,
        payload_bytes=payload_bytes,
        operation=operation,
    )
    isnothing(metric) && error("Flight concurrent $operation benchmark produced no receipt")
    flight_live_transport_print_metrics(stdout, (metric,))
end
