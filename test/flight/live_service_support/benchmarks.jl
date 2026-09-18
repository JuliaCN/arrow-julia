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

function _flight_live_transport_measure(
    backend::Symbol,
    operation::Symbol;
    request_bytes::Integer,
    response_bytes::Integer,
    iterations::Integer,
    run_once,
)
    iterations > 0 || throw(ArgumentError("iterations must be positive"))
    samples_ns = Int[]
    for _ = 1:iterations
        GC.gc()
        started = time_ns()
        run_once()
        push!(samples_ns, Int(time_ns() - started))
    end
    sorted_samples = sort(copy(samples_ns))
    median_ns = sorted_samples[cld(length(sorted_samples), 2)]
    total_bytes = Int(request_bytes) + Int(response_bytes)
    return (
        backend=backend,
        operation=operation,
        iterations=iterations,
        request_bytes=Int(request_bytes),
        response_bytes=Int(response_bytes),
        total_bytes=total_bytes,
        samples_ns=samples_ns,
        median_ns=median_ns,
        median_ms=median_ns / 1.0e6,
        throughput_mib_per_sec=(total_bytes / max(median_ns, 1) * 1.0e9) / 1024.0^2,
    )
end

function _flight_live_enforce_throughput(metric, operation::Symbol; env_name=nothing)
    minimum = _flight_live_pyarrow_min_throughput_mib_per_sec(operation; env_name=env_name)
    metric.throughput_mib_per_sec >= minimum || error(
        "$(metric.backend) $(metric.operation) throughput " *
        "$(metric.throughput_mib_per_sec) MiB/s is below the configured minimum " *
        "$(minimum) MiB/s",
    )
    return metric
end

function flight_live_transport_benchmark(
    protocol,
    transport;
    iterations::Integer=3,
    batch_count::Integer=2,
    rows_per_batch::Integer=256,
    payload_bytes::Integer=4_096,
    reused_doput_requests::Union{Nothing,Integer}=nothing,
    operations::Tuple{Vararg{Symbol}}=(:doget, :doput, :doexchange),
)
    fixture = flight_live_transport_fixture(
        protocol;
        batch_count=batch_count,
        rows_per_batch=rows_per_batch,
        payload_bytes=payload_bytes,
    )
    service = flight_live_transport_service(protocol, fixture)
    server = transport.start_server(service)
    try
        transport.wait_for_server(server)
        host, port = transport.endpoint(server)
        metrics = NamedTuple[]
        if :doget in operations
            doget_metric = flight_live_pyarrow_doget_metric(
                host,
                port,
                fixture;
                backend=transport.backend,
                iterations=iterations,
            )
            if isnothing(doget_metric)
                @test true
            else
                push!(metrics, _flight_live_enforce_throughput(doget_metric, :doget))
            end
        end
        if :doput in operations
            doput_metric = flight_live_pyarrow_doput_metric(
                host,
                port,
                fixture;
                backend=transport.backend,
                iterations=iterations,
            )
            if isnothing(doput_metric)
                @test true
            else
                push!(metrics, _flight_live_enforce_throughput(doput_metric, :doput))
            end
        end
        if :doput_reused_client in operations
            doput_reused_metric = flight_live_pyarrow_reused_doput_metric(
                host,
                port,
                fixture;
                backend=transport.backend,
                iterations=iterations,
                reused_requests=reused_doput_requests,
            )
            if isnothing(doput_reused_metric)
                @test true
            else
                push!(
                    metrics,
                    _flight_live_enforce_throughput(
                        doput_reused_metric,
                        :doput_reused_client;
                        env_name="ARROW_FLIGHT_PYARROW_REUSED_DOPUT_MIN_THROUGHPUT_MIB_PER_SEC",
                    ),
                )
            end
        end
        if :doexchange in operations
            doexchange_metric = flight_live_pyarrow_doexchange_metric(
                host,
                port,
                fixture;
                backend=transport.backend,
                iterations=iterations,
            )
            if isnothing(doexchange_metric)
                @test true
            else
                push!(
                    metrics,
                    _flight_live_enforce_throughput(doexchange_metric, :doexchange),
                )
            end
        end
        return metrics
    finally
        transport.stop_server(server)
    end
end

function flight_live_transport_concurrent_benchmark(
    protocol,
    transport;
    batch_count::Integer=2,
    rows_per_batch::Integer=256,
    payload_bytes::Integer=4_096,
    operation::Symbol=:doget,
    concurrent_clients::Integer=_flight_live_pyarrow_concurrent_clients(),
    requests_per_client::Integer=_flight_live_pyarrow_requests_per_client(),
)
    fixture = flight_live_transport_fixture(
        protocol;
        batch_count=batch_count,
        rows_per_batch=rows_per_batch,
        payload_bytes=payload_bytes,
    )
    service = flight_live_transport_service(protocol, fixture)
    server = transport.start_server(service)
    try
        transport.wait_for_server(server)
        host, port = transport.endpoint(server)
        if operation == :doget
            metric = flight_live_pyarrow_concurrent_doget_metric(
                host,
                port,
                fixture;
                backend=transport.backend,
                concurrent_clients=concurrent_clients,
                requests_per_client=requests_per_client,
            )
            return isnothing(metric) ? nothing :
                   _flight_live_enforce_throughput(
                metric,
                :doget;
                env_name="ARROW_FLIGHT_PYARROW_CONCURRENT_DOGET_MIN_THROUGHPUT_MIB_PER_SEC",
            )
        elseif operation == :doput
            metric = flight_live_pyarrow_concurrent_doput_metric(
                host,
                port,
                fixture;
                backend=transport.backend,
                concurrent_clients=concurrent_clients,
                requests_per_client=requests_per_client,
            )
            return isnothing(metric) ? nothing :
                   _flight_live_enforce_throughput(
                metric,
                :doput;
                env_name="ARROW_FLIGHT_PYARROW_CONCURRENT_DOPUT_MIN_THROUGHPUT_MIB_PER_SEC",
            )
        elseif operation == :doexchange
            metric = flight_live_pyarrow_concurrent_doexchange_metric(
                host,
                port,
                fixture;
                backend=transport.backend,
                concurrent_clients=concurrent_clients,
                requests_per_client=requests_per_client,
            )
            return isnothing(metric) ? nothing :
                   _flight_live_enforce_throughput(
                metric,
                :doexchange;
                env_name="ARROW_FLIGHT_PYARROW_CONCURRENT_DOEXCHANGE_MIN_THROUGHPUT_MIB_PER_SEC",
            )
        end
        throw(ArgumentError("unsupported concurrent Flight operation: $(operation)"))
    finally
        transport.stop_server(server)
    end
end
