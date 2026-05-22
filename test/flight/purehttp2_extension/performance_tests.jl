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
const PUREHTTP2_EXTENSION_EXCHANGE_TRANSPORT_BYTES = 512 * 1024

function purehttp2_extension_assert_minimum_throughput(
    metric,
    minimum_throughput_mib_per_sec::Real,
)
    @test metric.throughput_mib_per_sec > 0
    if minimum_throughput_mib_per_sec > 0
        @test metric.throughput_mib_per_sec >= minimum_throughput_mib_per_sec
    end
    return nothing
end

function purehttp2_extension_assert_reused_doput_metric(
    metric;
    expected_requests::Integer,
    minimum_throughput_mib_per_sec::Real=0,
)
    @test metric.backend == :grpcserver
    @test metric.operation == :doput_reused_client
    @test metric.total_requests == expected_requests
    @test metric.request_bytes >=
          PUREHTTP2_EXTENSION_LARGE_TRANSPORT_BYTES * expected_requests
    @test metric.response_bytes > 0
    @test metric.total_bytes >= metric.request_bytes
    @test metric.request_median_ns > 0
    @test metric.request_p95_ns >= metric.request_median_ns
    @test metric.request_p99_ns >= metric.request_p95_ns
    @test metric.request_max_ns >= metric.request_p99_ns
    purehttp2_extension_assert_minimum_throughput(metric, minimum_throughput_mib_per_sec)
    return nothing
end

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
    exchange_payload_bytes::Integer=1_024,
)
    doget_metrics = flight_live_transport_benchmark(
        Arrow.Flight.Protocol,
        purehttp2_extension_perf_transport();
        iterations=iterations,
        batch_count=batch_count,
        rows_per_batch=rows_per_batch,
        payload_bytes=payload_bytes,
        operations=(:doget,),
    )
    doput_metrics = flight_live_transport_benchmark(
        Arrow.Flight.Protocol,
        purehttp2_extension_perf_transport();
        iterations=iterations,
        batch_count=batch_count,
        rows_per_batch=rows_per_batch,
        payload_bytes=payload_bytes,
        operations=(:doput, :doput_reused_client),
    )
    doexchange_metrics = flight_live_transport_benchmark(
        Arrow.Flight.Protocol,
        purehttp2_extension_perf_transport();
        iterations=iterations,
        batch_count=batch_count,
        rows_per_batch=rows_per_batch,
        payload_bytes=exchange_payload_bytes,
        operations=(:doexchange,),
    )
    metrics = vcat(doget_metrics, doput_metrics, doexchange_metrics)
    isempty(metrics) && return metrics
    @test length(metrics) == 4
    @test all(metric.backend == :grpcserver for metric in metrics)
    @test Set(metric.operation for metric in metrics) ==
          Set([:doget, :doput, :doput_reused_client, :doexchange])
    doget_metric = flight_live_transport_metric(metrics, :grpcserver, :doget)
    doput_metric = flight_live_transport_metric(metrics, :grpcserver, :doput)
    doput_reused_metric =
        flight_live_transport_metric(metrics, :grpcserver, :doput_reused_client)
    doexchange_metric = flight_live_transport_metric(metrics, :grpcserver, :doexchange)
    @test doget_metric.total_bytes >= PUREHTTP2_EXTENSION_LARGE_TRANSPORT_BYTES
    @test doput_metric.request_bytes >= PUREHTTP2_EXTENSION_LARGE_TRANSPORT_BYTES
    purehttp2_extension_assert_minimum_throughput(
        doget_metric,
        _flight_live_pyarrow_min_throughput_mib_per_sec(:doget),
    )
    purehttp2_extension_assert_minimum_throughput(
        doput_metric,
        _flight_live_pyarrow_min_throughput_mib_per_sec(:doput),
    )
    purehttp2_extension_assert_reused_doput_metric(
        doput_reused_metric;
        expected_requests=_flight_live_pyarrow_reused_doput_requests(),
        minimum_throughput_mib_per_sec=_flight_live_pyarrow_reused_doput_min_throughput_mib_per_sec(),
    )
    purehttp2_extension_assert_minimum_throughput(
        doexchange_metric,
        _flight_live_pyarrow_min_throughput_mib_per_sec(:doexchange),
    )
    @test doexchange_metric.total_bytes >= PUREHTTP2_EXTENSION_EXCHANGE_TRANSPORT_BYTES
    @test all(metric.request_bytes >= 0 for metric in metrics)
    @test all(metric.response_bytes > 0 for metric in metrics)
    @test all(metric.median_ns > 0 for metric in metrics)
    @test all(metric.throughput_mib_per_sec > 0 for metric in metrics)
    flight_live_transport_print_metrics(stdout, metrics)
    return metrics
end

function purehttp2_extension_test_reused_doput_soak(;
    rounds::Integer=_flight_live_pyarrow_reused_doput_soak_rounds(),
    reused_requests::Integer=_flight_live_pyarrow_reused_doput_requests(),
    batch_count::Integer=2,
    rows_per_batch::Integer=256,
    payload_bytes::Integer=4_096,
)
    rounds > 0 || throw(ArgumentError("rounds must be positive"))
    reused_requests >= 2 || throw(ArgumentError("reused_requests must be >= 2"))
    fixture = flight_live_transport_fixture(
        Arrow.Flight.Protocol,
        batch_count=batch_count,
        rows_per_batch=rows_per_batch,
        payload_bytes=payload_bytes,
    )
    transport = purehttp2_extension_perf_transport()
    service = flight_live_transport_service(Arrow.Flight.Protocol, fixture)
    server = transport.start_server(service)
    metrics = NamedTuple[]
    try
        transport.wait_for_server(server)
        host, port = transport.endpoint(server)
        for _ = 1:rounds
            metric = flight_live_pyarrow_reused_doput_metric(
                host,
                port,
                fixture;
                backend=transport.backend,
                iterations=1,
                reused_requests=reused_requests,
            )
            isnothing(metric) && return metrics
            push!(metrics, metric)
        end
    finally
        transport.stop_server(server)
    end

    @test length(metrics) == rounds
    minimum_throughput = _flight_live_pyarrow_reused_doput_min_throughput_mib_per_sec()
    for metric in metrics
        purehttp2_extension_assert_reused_doput_metric(
            metric;
            expected_requests=reused_requests,
            minimum_throughput_mib_per_sec=minimum_throughput,
        )
    end
    @test sum(metric.total_requests for metric in metrics) == rounds * reused_requests
    @test sum(metric.request_bytes for metric in metrics) >=
          PUREHTTP2_EXTENSION_LARGE_TRANSPORT_BYTES * rounds * reused_requests
    flight_live_transport_print_metrics(stdout, metrics)
    foreach(
        metric -> flight_live_transport_print_reused_doput_summary(stdout, metric),
        metrics,
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
    exchange_payload_bytes::Integer=1_024,
)
    rounds > 0 || throw(ArgumentError("rounds must be positive"))
    concurrent_clients > 0 || throw(ArgumentError("concurrent_clients must be positive"))
    requests_per_client > 0 || throw(ArgumentError("requests_per_client must be positive"))

    metrics = NamedTuple[]
    for _ = 1:rounds
        doget_metric = flight_live_transport_concurrent_benchmark(
            Arrow.Flight.Protocol,
            purehttp2_extension_perf_transport(
                max_active_requests=max(concurrent_clients, 4),
                request_capacity=max(concurrent_clients * 2, 4),
                response_capacity=max(concurrent_clients * 2, 4),
            );
            batch_count=batch_count,
            rows_per_batch=rows_per_batch,
            payload_bytes=payload_bytes,
            operation=:doget,
            concurrent_clients=concurrent_clients,
            requests_per_client=requests_per_client,
        )
        isnothing(doget_metric) && return metrics
        push!(metrics, doget_metric)

        doput_metric = flight_live_transport_concurrent_benchmark(
            Arrow.Flight.Protocol,
            purehttp2_extension_perf_transport(
                max_active_requests=max(concurrent_clients, 4),
                request_capacity=max(concurrent_clients * 2, 4),
                response_capacity=max(concurrent_clients * 2, 4),
            );
            batch_count=batch_count,
            rows_per_batch=rows_per_batch,
            payload_bytes=payload_bytes,
            operation=:doput,
            concurrent_clients=concurrent_clients,
            requests_per_client=requests_per_client,
        )
        isnothing(doput_metric) && return metrics
        push!(metrics, doput_metric)

        doexchange_metric = flight_live_transport_concurrent_benchmark(
            Arrow.Flight.Protocol,
            purehttp2_extension_perf_transport(
                max_active_requests=max(concurrent_clients, 4),
                request_capacity=max(concurrent_clients * 2, 4),
                response_capacity=max(concurrent_clients * 2, 4),
            );
            batch_count=batch_count,
            rows_per_batch=rows_per_batch,
            payload_bytes=exchange_payload_bytes,
            operation=:doexchange,
            concurrent_clients=concurrent_clients,
            requests_per_client=requests_per_client,
        )
        isnothing(doexchange_metric) && return metrics
        push!(metrics, doexchange_metric)
    end

    @test length(metrics) == 3 * rounds
    doget_metrics = filter(metric -> metric.operation == :doget_concurrent, metrics)
    doput_metrics = filter(metric -> metric.operation == :doput_concurrent, metrics)
    doexchange_metrics =
        filter(metric -> metric.operation == :doexchange_concurrent, metrics)
    @test length(doget_metrics) == rounds
    @test length(doput_metrics) == rounds
    @test length(doexchange_metrics) == rounds
    expected_total_requests = concurrent_clients * requests_per_client
    expected_doget_total_bytes =
        PUREHTTP2_EXTENSION_LARGE_TRANSPORT_BYTES * expected_total_requests
    expected_doput_request_bytes =
        PUREHTTP2_EXTENSION_LARGE_TRANSPORT_BYTES * expected_total_requests
    expected_doexchange_total_bytes =
        PUREHTTP2_EXTENSION_EXCHANGE_TRANSPORT_BYTES * expected_total_requests
    minimum_throughputs = (
        doget_concurrent=_flight_live_pyarrow_min_throughput_mib_per_sec(:doget_concurrent),
        doput_concurrent=_flight_live_pyarrow_min_throughput_mib_per_sec(:doput_concurrent),
        doexchange_concurrent=_flight_live_pyarrow_min_throughput_mib_per_sec(
            :doexchange_concurrent,
        ),
    )
    for operation_metrics in (doget_metrics, doput_metrics, doexchange_metrics)
        @test all(metric.backend == :grpcserver for metric in operation_metrics)
        @test all(
            metric.total_requests == expected_total_requests for metric in operation_metrics
        )
        @test all(
            metric.concurrent_clients == concurrent_clients for metric in operation_metrics
        )
        @test all(
            metric.requests_per_client == requests_per_client for
            metric in operation_metrics
        )
        @test all(metric.median_ns > 0 for metric in operation_metrics)
        @test all(metric.request_median_ns > 0 for metric in operation_metrics)
        @test all(
            metric.request_p95_ns >= metric.request_median_ns for
            metric in operation_metrics
        )
        @test all(
            metric.request_p99_ns >= metric.request_p95_ns for metric in operation_metrics
        )
        @test all(
            metric.request_max_ns >= metric.request_p99_ns for metric in operation_metrics
        )
        @test all(
            metric.request_max_ns >= metric.request_median_ns for
            metric in operation_metrics
        )
        for metric in operation_metrics
            purehttp2_extension_assert_minimum_throughput(
                metric,
                getproperty(minimum_throughputs, metric.operation),
            )
        end
    end
    @test all(metric.total_bytes >= expected_doget_total_bytes for metric in doget_metrics)
    @test all(
        metric.request_bytes >= expected_doput_request_bytes for metric in doput_metrics
    )
    @test all(metric.response_bytes > 0 for metric in doput_metrics)
    @test all(
        metric.total_bytes >= expected_doexchange_total_bytes for
        metric in doexchange_metrics
    )
    flight_live_transport_print_metrics(stdout, metrics)
    flight_live_transport_print_concurrent_summary(stdout, doget_metrics)
    flight_live_transport_print_concurrent_summary(stdout, doput_metrics)
    flight_live_transport_print_concurrent_summary(stdout, doexchange_metrics)
    return metrics
end
