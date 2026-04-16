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

const NGHTTP2_EXTENSION_LARGE_TRANSPORT_BYTES = 2 * 1024 * 1024

function nghttp2_extension_perf_transport()
    return flight_live_transport_backend(
        backend=:nghttp2,
        start_server=service -> Arrow.Flight.nghttp2_flight_server(
            service;
            host="127.0.0.1",
            port=0,
            request_capacity=4,
            response_capacity=4,
        ),
        wait_for_server=server ->
            purehttp2_extension_wait_for_live_server(server.host, server.port),
        stop_server=server -> Arrow.Flight.stop!(server; force=true),
        endpoint=server -> (server.host, server.port),
    )
end

function nghttp2_extension_purehttp2_compare_transport()
    return flight_live_transport_backend(
        backend=:purehttp2,
        start_server=service -> Arrow.Flight.purehttp2_flight_server(
            service;
            host="127.0.0.1",
            port=0,
            request_capacity=4,
            response_capacity=4,
        ),
        wait_for_server=server ->
            purehttp2_extension_wait_for_live_server(server.host, server.port),
        stop_server=server -> Arrow.Flight.stop!(server; force=true),
        endpoint=server -> (server.host, server.port),
    )
end

function nghttp2_extension_test_large_transport_compare(;
    iterations::Integer=3,
    batch_count::Integer=2,
    rows_per_batch::Integer=256,
    payload_bytes::Integer=4_096,
)
    protocol = Arrow.Flight.Protocol
    pure_metrics = flight_live_transport_benchmark(
        protocol,
        nghttp2_extension_purehttp2_compare_transport();
        iterations=iterations,
        batch_count=batch_count,
        rows_per_batch=rows_per_batch,
        payload_bytes=payload_bytes,
        operations=(:doget,),
    )
    nghttp2_metrics = flight_live_transport_benchmark(
        protocol,
        nghttp2_extension_perf_transport();
        iterations=iterations,
        batch_count=batch_count,
        rows_per_batch=rows_per_batch,
        payload_bytes=payload_bytes,
        operations=(:doget,),
    )

    metrics = vcat(pure_metrics, nghttp2_metrics)
    @test length(metrics) == 2

    purehttp2_metric = flight_live_transport_metric(metrics, :purehttp2, :doget)
    nghttp2_metric = flight_live_transport_metric(metrics, :nghttp2, :doget)

    @test purehttp2_metric.total_bytes >= NGHTTP2_EXTENSION_LARGE_TRANSPORT_BYTES
    @test nghttp2_metric.total_bytes >= NGHTTP2_EXTENSION_LARGE_TRANSPORT_BYTES
    @test purehttp2_metric.median_ns > 0
    @test nghttp2_metric.median_ns > 0
    @test purehttp2_metric.throughput_mib_per_sec > 0
    @test nghttp2_metric.throughput_mib_per_sec > 0

    flight_live_transport_print_metrics(stdout, metrics)
    comparison = flight_live_transport_print_comparison(
        stdout,
        nghttp2_metric,
        purehttp2_metric,
    )
    @test comparison.candidate_backend == :nghttp2
    @test comparison.baseline_backend == :purehttp2
    return metrics
end
