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
                push!(metrics, doget_metric)
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
                push!(metrics, doput_metric)
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
                push!(metrics, doput_reused_metric)
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
                push!(metrics, doexchange_metric)
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
            return flight_live_pyarrow_concurrent_doget_metric(
                host,
                port,
                fixture;
                backend=transport.backend,
                concurrent_clients=concurrent_clients,
                requests_per_client=requests_per_client,
            )
        elseif operation == :doput
            return flight_live_pyarrow_concurrent_doput_metric(
                host,
                port,
                fixture;
                backend=transport.backend,
                concurrent_clients=concurrent_clients,
                requests_per_client=requests_per_client,
            )
        elseif operation == :doexchange
            return flight_live_pyarrow_concurrent_doexchange_metric(
                host,
                port,
                fixture;
                backend=transport.backend,
                concurrent_clients=concurrent_clients,
                requests_per_client=requests_per_client,
            )
        end
        throw(ArgumentError("unsupported concurrent Flight operation: $(operation)"))
    finally
        transport.stop_server(server)
    end
end
