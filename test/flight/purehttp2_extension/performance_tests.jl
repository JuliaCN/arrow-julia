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

function purehttp2_extension_perf_transport()
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
    @test length(metrics) == 1
    @test all(metric.backend == :purehttp2 for metric in metrics)
    @test all(
        metric.total_bytes >= PUREHTTP2_EXTENSION_LARGE_TRANSPORT_BYTES for
        metric in metrics
    )
    @test all(metric.median_ns > 0 for metric in metrics)
    @test all(metric.throughput_mib_per_sec > 0 for metric in metrics)
    flight_live_transport_print_metrics(stdout, metrics)
    println(
        stdout,
        "deferred_large_upload_ops=[:doput,:doexchange] reason=\"PureHTTP2 large client-streaming uploads are not yet proven on this benchmark seam\"",
    )
    return metrics
end
