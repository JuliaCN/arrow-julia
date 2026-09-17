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

function flight_live_pyarrow_doget_metric(
    host::AbstractString,
    port::Integer,
    fixture;
    backend::Symbol,
    iterations::Integer,
)
    python = FlightTestSupport.pyarrow_flight_python()
    isnothing(python) && return nothing
    median_ns = parse(
        Int,
        _flight_live_readchomp_with_timeout(
            Cmd([
                python,
                "-c",
                FLIGHT_LIVE_PYARROW_DOGET_BENCHMARK,
                host,
                string(port),
                string(iterations),
                string(fixture.total_records),
                fixture.payload_value,
                string(_flight_live_pyarrow_lookahead_bytes()),
                fixture.descriptor.path...,
            ]);
            timeout_sec=_flight_live_command_timeout_sec(),
            label="pyarrow Flight DoGet benchmark",
        ),
    )
    total_bytes = fixture.message_bytes
    return (
        backend=backend,
        operation=:doget,
        iterations=iterations,
        request_bytes=0,
        response_bytes=total_bytes,
        total_bytes=total_bytes,
        samples_ns=[median_ns],
        median_ns=median_ns,
        median_ms=median_ns / 1.0e6,
        throughput_mib_per_sec=(total_bytes / max(median_ns, 1) * 1.0e9) / 1024.0^2,
    )
end

function flight_live_pyarrow_doput_metric(
    host::AbstractString,
    port::Integer,
    fixture;
    backend::Symbol,
    iterations::Integer,
)
    python = FlightTestSupport.pyarrow_flight_python()
    isnothing(python) && return nothing
    median_ns = parse(
        Int,
        _flight_live_readchomp_with_timeout(
            Cmd([
                python,
                "-c",
                FLIGHT_LIVE_PYARROW_DOPUT_BENCHMARK,
                host,
                string(port),
                string(iterations),
                string(length(fixture.batches)),
                string(length(first(fixture.batches).id)),
                fixture.payload_value,
                string(_flight_live_pyarrow_lookahead_bytes()),
                fixture.descriptor.path...,
            ]);
            timeout_sec=_flight_live_command_timeout_sec(),
            label="pyarrow Flight DoPut benchmark",
        ),
    )
    request_bytes = fixture.message_bytes
    response_bytes = fixture.put_result_bytes
    total_bytes = request_bytes + response_bytes
    return (
        backend=backend,
        operation=:doput,
        iterations=iterations,
        request_bytes=request_bytes,
        response_bytes=response_bytes,
        total_bytes=total_bytes,
        samples_ns=[median_ns],
        median_ns=median_ns,
        median_ms=median_ns / 1.0e6,
        throughput_mib_per_sec=(total_bytes / max(median_ns, 1) * 1.0e9) / 1024.0^2,
    )
end

function flight_live_pyarrow_reused_doput_metric(
    host::AbstractString,
    port::Integer,
    fixture;
    backend::Symbol,
    iterations::Integer,
    reused_requests::Union{Nothing,Integer}=nothing,
)
    python = FlightTestSupport.pyarrow_flight_python()
    isnothing(python) && return nothing
    requests =
        isnothing(reused_requests) ? _flight_live_pyarrow_reused_doput_requests() :
        reused_requests
    requests >= 2 || throw(ArgumentError("reused_requests must be >= 2"))
    output = _flight_live_readchomp_with_timeout(
        Cmd([
            python,
            "-c",
            FLIGHT_LIVE_PYARROW_REUSED_DOPUT_BENCHMARK,
            host,
            string(port),
            string(requests),
            string(length(fixture.batches)),
            string(length(first(fixture.batches).id)),
            fixture.payload_value,
            string(_flight_live_pyarrow_lookahead_bytes()),
            fixture.descriptor.path...,
        ]);
        timeout_sec=_flight_live_command_timeout_sec(),
        label="pyarrow Flight reused-client DoPut benchmark",
    )
    result = JSON3.read(output)
    total_requests = Int(result["total_requests"])
    wall_ns = Int(result["wall_ns"])
    request_bytes = fixture.message_bytes * total_requests
    response_bytes = fixture.put_result_bytes * total_requests
    total_bytes = request_bytes + response_bytes
    request_median_ns = Int(result["request_median_ns"])
    request_p95_ns = Int(result["request_p95_ns"])
    request_p99_ns = Int(result["request_p99_ns"])
    request_max_ns = Int(result["request_max_ns"])
    return (
        backend=backend,
        operation=:doput_reused_client,
        iterations=total_requests,
        request_bytes=request_bytes,
        response_bytes=response_bytes,
        total_bytes=total_bytes,
        samples_ns=[wall_ns],
        median_ns=wall_ns,
        median_ms=wall_ns / 1.0e6,
        throughput_mib_per_sec=(total_bytes / max(wall_ns, 1) * 1.0e9) / 1024.0^2,
        total_requests=total_requests,
        request_median_ns=request_median_ns,
        request_median_ms=request_median_ns / 1.0e6,
        request_p95_ns=request_p95_ns,
        request_p95_ms=request_p95_ns / 1.0e6,
        request_p99_ns=request_p99_ns,
        request_p99_ms=request_p99_ns / 1.0e6,
        request_max_ns=request_max_ns,
        request_max_ms=request_max_ns / 1.0e6,
    )
end

function flight_live_pyarrow_doexchange_metric(
    host::AbstractString,
    port::Integer,
    fixture;
    backend::Symbol,
    iterations::Integer,
)
    python = FlightTestSupport.pyarrow_flight_python()
    isnothing(python) && return nothing
    median_ns = parse(
        Int,
        _flight_live_readchomp_with_timeout(
            Cmd([
                python,
                "-c",
                FLIGHT_LIVE_PYARROW_DOEXCHANGE_BENCHMARK,
                host,
                string(port),
                string(iterations),
                string(length(fixture.batches)),
                string(length(first(fixture.batches).id)),
                string(fixture.total_records),
                fixture.payload_value,
                string(_flight_live_pyarrow_lookahead_bytes()),
                fixture.descriptor.path...,
            ]);
            timeout_sec=_flight_live_command_timeout_sec(),
            label="pyarrow Flight DoExchange benchmark",
        ),
    )
    request_bytes = fixture.message_bytes
    response_bytes = fixture.message_bytes
    total_bytes = request_bytes + response_bytes
    return (
        backend=backend,
        operation=:doexchange,
        iterations=iterations,
        request_bytes=request_bytes,
        response_bytes=response_bytes,
        total_bytes=total_bytes,
        samples_ns=[median_ns],
        median_ns=median_ns,
        median_ms=median_ns / 1.0e6,
        throughput_mib_per_sec=(total_bytes / max(median_ns, 1) * 1.0e9) / 1024.0^2,
    )
end

function flight_live_pyarrow_concurrent_doget(
    host::AbstractString,
    port::Integer,
    fixture;
    concurrent_clients::Integer,
    requests_per_client::Integer,
)
    python = FlightTestSupport.pyarrow_flight_python()
    isnothing(python) && return nothing
    output = _flight_live_readchomp_with_timeout(
        Cmd([
            python,
            "-c",
            FLIGHT_LIVE_PYARROW_CONCURRENT_DOGET_BENCHMARK,
            host,
            string(port),
            string(concurrent_clients),
            string(requests_per_client),
            string(fixture.total_records),
            fixture.payload_value,
            string(_flight_live_pyarrow_lookahead_bytes()),
            fixture.descriptor.path...,
        ]);
        timeout_sec=_flight_live_command_timeout_sec(),
        label="pyarrow Flight concurrent DoGet benchmark",
    )
    return JSON3.read(output)
end

function flight_live_pyarrow_concurrent_doget_metric(
    host::AbstractString,
    port::Integer,
    fixture;
    backend::Symbol,
    concurrent_clients::Integer,
    requests_per_client::Integer,
)
    result = flight_live_pyarrow_concurrent_doget(
        host,
        port,
        fixture;
        concurrent_clients=concurrent_clients,
        requests_per_client=requests_per_client,
    )
    isnothing(result) && return nothing
    total_requests = Int(result["total_requests"])
    wall_ns = Int(result["wall_ns"])
    total_bytes = fixture.message_bytes * total_requests
    request_median_ns = Int(result["request_median_ns"])
    request_p95_ns = Int(result["request_p95_ns"])
    request_p99_ns = Int(result["request_p99_ns"])
    request_max_ns = Int(result["request_max_ns"])
    return (
        backend=backend,
        operation=:doget_concurrent,
        iterations=total_requests,
        request_bytes=0,
        response_bytes=total_bytes,
        total_bytes=total_bytes,
        samples_ns=[wall_ns],
        median_ns=wall_ns,
        median_ms=wall_ns / 1.0e6,
        throughput_mib_per_sec=(total_bytes / max(wall_ns, 1) * 1.0e9) / 1024.0^2,
        concurrent_clients=Int(result["concurrent_clients"]),
        requests_per_client=Int(result["requests_per_client"]),
        total_requests=total_requests,
        request_median_ns=request_median_ns,
        request_median_ms=request_median_ns / 1.0e6,
        request_p95_ns=request_p95_ns,
        request_p95_ms=request_p95_ns / 1.0e6,
        request_p99_ns=request_p99_ns,
        request_p99_ms=request_p99_ns / 1.0e6,
        request_max_ns=request_max_ns,
        request_max_ms=request_max_ns / 1.0e6,
    )
end

function flight_live_pyarrow_concurrent_doput(
    host::AbstractString,
    port::Integer,
    fixture;
    concurrent_clients::Integer,
    requests_per_client::Integer,
)
    python = FlightTestSupport.pyarrow_flight_python()
    isnothing(python) && return nothing
    output = _flight_live_readchomp_with_timeout(
        Cmd([
            python,
            "-c",
            FLIGHT_LIVE_PYARROW_CONCURRENT_DOPUT_BENCHMARK,
            host,
            string(port),
            string(concurrent_clients),
            string(requests_per_client),
            string(length(fixture.batches)),
            string(length(first(fixture.batches).id)),
            fixture.payload_value,
            string(_flight_live_pyarrow_lookahead_bytes()),
            fixture.descriptor.path...,
        ]);
        timeout_sec=_flight_live_command_timeout_sec(),
        label="pyarrow Flight concurrent DoPut benchmark",
    )
    return JSON3.read(output)
end

function flight_live_pyarrow_concurrent_doput_metric(
    host::AbstractString,
    port::Integer,
    fixture;
    backend::Symbol,
    concurrent_clients::Integer,
    requests_per_client::Integer,
)
    result = flight_live_pyarrow_concurrent_doput(
        host,
        port,
        fixture;
        concurrent_clients=concurrent_clients,
        requests_per_client=requests_per_client,
    )
    isnothing(result) && return nothing
    total_requests = Int(result["total_requests"])
    wall_ns = Int(result["wall_ns"])
    request_bytes = fixture.message_bytes * total_requests
    response_bytes = fixture.put_result_bytes * total_requests
    total_bytes = request_bytes + response_bytes
    request_median_ns = Int(result["request_median_ns"])
    request_p95_ns = Int(result["request_p95_ns"])
    request_p99_ns = Int(result["request_p99_ns"])
    request_max_ns = Int(result["request_max_ns"])
    return (
        backend=backend,
        operation=:doput_concurrent,
        iterations=total_requests,
        request_bytes=request_bytes,
        response_bytes=response_bytes,
        total_bytes=total_bytes,
        samples_ns=[wall_ns],
        median_ns=wall_ns,
        median_ms=wall_ns / 1.0e6,
        throughput_mib_per_sec=(total_bytes / max(wall_ns, 1) * 1.0e9) / 1024.0^2,
        concurrent_clients=Int(result["concurrent_clients"]),
        requests_per_client=Int(result["requests_per_client"]),
        total_requests=total_requests,
        request_median_ns=request_median_ns,
        request_median_ms=request_median_ns / 1.0e6,
        request_p95_ns=request_p95_ns,
        request_p95_ms=request_p95_ns / 1.0e6,
        request_p99_ns=request_p99_ns,
        request_p99_ms=request_p99_ns / 1.0e6,
        request_max_ns=request_max_ns,
        request_max_ms=request_max_ns / 1.0e6,
    )
end

function flight_live_pyarrow_concurrent_doexchange(
    host::AbstractString,
    port::Integer,
    fixture;
    concurrent_clients::Integer,
    requests_per_client::Integer,
)
    python = FlightTestSupport.pyarrow_flight_python()
    isnothing(python) && return nothing
    output = _flight_live_readchomp_with_timeout(
        Cmd([
            python,
            "-c",
            FLIGHT_LIVE_PYARROW_CONCURRENT_DOEXCHANGE_BENCHMARK,
            host,
            string(port),
            string(concurrent_clients),
            string(requests_per_client),
            string(length(fixture.batches)),
            string(length(first(fixture.batches).id)),
            string(fixture.total_records),
            fixture.payload_value,
            string(_flight_live_pyarrow_lookahead_bytes()),
            fixture.descriptor.path...,
        ]);
        timeout_sec=_flight_live_command_timeout_sec(),
        label="pyarrow Flight concurrent DoExchange benchmark",
    )
    return JSON3.read(output)
end

function flight_live_pyarrow_concurrent_doexchange_metric(
    host::AbstractString,
    port::Integer,
    fixture;
    backend::Symbol,
    concurrent_clients::Integer,
    requests_per_client::Integer,
)
    result = flight_live_pyarrow_concurrent_doexchange(
        host,
        port,
        fixture;
        concurrent_clients=concurrent_clients,
        requests_per_client=requests_per_client,
    )
    isnothing(result) && return nothing
    total_requests = Int(result["total_requests"])
    wall_ns = Int(result["wall_ns"])
    request_bytes = fixture.message_bytes * total_requests
    response_bytes = fixture.message_bytes * total_requests
    total_bytes = request_bytes + response_bytes
    request_median_ns = Int(result["request_median_ns"])
    request_p95_ns = Int(result["request_p95_ns"])
    request_p99_ns = Int(result["request_p99_ns"])
    request_max_ns = Int(result["request_max_ns"])
    return (
        backend=backend,
        operation=:doexchange_concurrent,
        iterations=total_requests,
        request_bytes=request_bytes,
        response_bytes=response_bytes,
        total_bytes=total_bytes,
        samples_ns=[wall_ns],
        median_ns=wall_ns,
        median_ms=wall_ns / 1.0e6,
        throughput_mib_per_sec=(total_bytes / max(wall_ns, 1) * 1.0e9) / 1024.0^2,
        concurrent_clients=Int(result["concurrent_clients"]),
        requests_per_client=Int(result["requests_per_client"]),
        total_requests=total_requests,
        request_median_ns=request_median_ns,
        request_median_ms=request_median_ns / 1.0e6,
        request_p95_ns=request_p95_ns,
        request_p95_ms=request_p95_ns / 1.0e6,
        request_p99_ns=request_p99_ns,
        request_p99_ms=request_p99_ns / 1.0e6,
        request_max_ns=request_max_ns,
        request_max_ms=request_max_ns / 1.0e6,
    )
end
