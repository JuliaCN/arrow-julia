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

const PUREHTTP2_EXTENSION_LARGE_TRANSPORT_BYTES = 2 * 1024 * 1024

function purehttp2_extension_perf_transport(;
    max_active_requests::Integer=4,
    request_capacity::Integer=4,
    response_capacity::Integer=4,
)
    return flight_live_transport_backend(
        backend=:grpcserver,
        start_server=service -> Arrow.Flight.grpcserver_flight_server(
            service;
            host="127.0.0.1",
            port=0,
            max_active_requests=max_active_requests,
            request_capacity=request_capacity,
            response_capacity=response_capacity,
        ),
        wait_for_server=server ->
            purehttp2_extension_wait_for_live_server(server.host, server.port),
        stop_server=server -> Arrow.Flight.stop!(server; force=true),
        endpoint=server -> (server.host, server.port),
    )
end

function purehttp2_extension_test_large_transport_performance(;
    iterations::Integer=3,
    batch_count::Integer=2,
    rows_per_batch::Integer=256,
    payload_bytes::Integer=4_096,
)
    metrics = flight_live_transport_benchmark(
        Arrow.Flight.Protocol,
        purehttp2_extension_perf_transport();
        iterations=iterations,
        batch_count=batch_count,
        rows_per_batch=rows_per_batch,
        payload_bytes=payload_bytes,
        operations=(:doget,),
    )
    isempty(metrics) && return metrics
    @test length(metrics) == 1
    @test all(metric.backend == :grpcserver for metric in metrics)
    @test all(
        metric.total_bytes >= PUREHTTP2_EXTENSION_LARGE_TRANSPORT_BYTES for
        metric in metrics
    )
    @test all(metric.median_ns > 0 for metric in metrics)
    @test all(metric.throughput_mib_per_sec > 0 for metric in metrics)
    flight_live_transport_print_metrics(stdout, metrics)
    println(
        stdout,
        "deferred_large_upload_ops=[:doput,:doexchange] reason=\"gRPCServer-owned large client-streaming uploads are not yet proven on this benchmark seam\"",
    )
    return metrics
end

function purehttp2_extension_test_large_transport_concurrent_soak(;
    rounds::Integer=_flight_live_pyarrow_soak_rounds(),
    concurrent_clients::Integer=_flight_live_pyarrow_concurrent_clients(),
    requests_per_client::Integer=_flight_live_pyarrow_requests_per_client(),
    batch_count::Integer=2,
    rows_per_batch::Integer=256,
    payload_bytes::Integer=4_096,
)
    rounds > 0 || throw(ArgumentError("rounds must be positive"))
    concurrent_clients > 0 || throw(ArgumentError("concurrent_clients must be positive"))
    requests_per_client > 0 || throw(ArgumentError("requests_per_client must be positive"))

    metrics = NamedTuple[]
    for _ = 1:rounds
        metric = flight_live_transport_concurrent_benchmark(
            Arrow.Flight.Protocol,
            purehttp2_extension_perf_transport(
                max_active_requests=max(concurrent_clients, 4),
                request_capacity=max(concurrent_clients * 2, 4),
                response_capacity=max(concurrent_clients * 2, 4),
            );
            batch_count=batch_count,
            rows_per_batch=rows_per_batch,
            payload_bytes=payload_bytes,
            concurrent_clients=concurrent_clients,
            requests_per_client=requests_per_client,
        )
        isnothing(metric) && return metrics
        push!(metrics, metric)
    end

    @test length(metrics) == rounds
    expected_total_requests = concurrent_clients * requests_per_client
    expected_total_bytes =
        PUREHTTP2_EXTENSION_LARGE_TRANSPORT_BYTES * expected_total_requests
    @test all(metric.backend == :grpcserver for metric in metrics)
    @test all(metric.operation == :doget_concurrent for metric in metrics)
    @test all(metric.total_requests == expected_total_requests for metric in metrics)
    @test all(metric.concurrent_clients == concurrent_clients for metric in metrics)
    @test all(metric.requests_per_client == requests_per_client for metric in metrics)
    @test all(metric.total_bytes >= expected_total_bytes for metric in metrics)
    @test all(metric.median_ns > 0 for metric in metrics)
    @test all(metric.request_median_ns > 0 for metric in metrics)
    @test all(metric.request_p95_ns >= metric.request_median_ns for metric in metrics)
    @test all(metric.request_p99_ns >= metric.request_p95_ns for metric in metrics)
    @test all(metric.request_max_ns >= metric.request_p99_ns for metric in metrics)
    @test all(metric.request_max_ns >= metric.request_median_ns for metric in metrics)
    @test all(metric.throughput_mib_per_sec > 0 for metric in metrics)
    flight_live_transport_print_metrics(stdout, metrics)
    flight_live_transport_print_concurrent_summary(stdout, metrics)
    return metrics
end
