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

function flight_live_transport_print_metrics(io::IO, metrics)
    for metric in metrics
        samples_ms = [round(sample / 1.0e6; digits=2) for sample in metric.samples_ns]
        request_latency_summary =
            hasproperty(metric, :request_median_ms) ?
            " request_median_ms=$(round(metric.request_median_ms; digits=2))" *
            " request_p95_ms=$(round(metric.request_p95_ms; digits=2))" *
            " request_p99_ms=$(round(metric.request_p99_ms; digits=2))" *
            " request_max_ms=$(round(metric.request_max_ms; digits=2))" : ""
        println(
            io,
            "$(metric.backend) $(metric.operation): total_bytes=$(metric.total_bytes) " *
            "median_ms=$(round(metric.median_ms; digits=2)) " *
            "throughput_mib_per_sec=$(round(metric.throughput_mib_per_sec; digits=2)) " *
            "$(request_latency_summary) samples_ms=$(samples_ms)",
        )
    end
    return nothing
end

function flight_live_transport_print_concurrent_summary(io::IO, metrics)
    isempty(metrics) && return nothing
    operation = metrics[1].operation
    all(metric.operation == operation for metric in metrics) ||
        throw(ArgumentError("concurrent summary metrics must use one operation"))
    throughputs = [metric.throughput_mib_per_sec for metric in metrics]
    request_latencies = [metric.request_median_ms for metric in metrics]
    request_p95_latencies = [metric.request_p95_ms for metric in metrics]
    request_p99_latencies = [metric.request_p99_ms for metric in metrics]
    wall_latencies = [metric.median_ms for metric in metrics]
    println(
        io,
        "$(operation) soak: rounds=$(length(metrics)) " *
        "concurrent_clients=$(metrics[1].concurrent_clients) " *
        "requests_per_client=$(metrics[1].requests_per_client) " *
        "wall_ms_range=$(round(minimum(wall_latencies); digits=2))-$(round(maximum(wall_latencies); digits=2)) " *
        "request_median_ms_range=$(round(minimum(request_latencies); digits=2))-$(round(maximum(request_latencies); digits=2)) " *
        "request_p95_ms_range=$(round(minimum(request_p95_latencies); digits=2))-$(round(maximum(request_p95_latencies); digits=2)) " *
        "request_p99_ms_range=$(round(minimum(request_p99_latencies); digits=2))-$(round(maximum(request_p99_latencies); digits=2)) " *
        "throughput_mib_per_sec_range=$(round(minimum(throughputs); digits=2))-$(round(maximum(throughputs); digits=2))",
    )
    return nothing
end

function flight_live_transport_print_reused_doput_summary(io::IO, metric)
    metric.operation == :doput_reused_client ||
        throw(ArgumentError("reused DoPut summary requires operation :doput_reused_client"))
    println(
        io,
        "doput_reused_client soak: total_requests=$(metric.total_requests) " *
        "wall_ms=$(round(metric.median_ms; digits=2)) " *
        "request_median_ms=$(round(metric.request_median_ms; digits=2)) " *
        "request_p95_ms=$(round(metric.request_p95_ms; digits=2)) " *
        "request_p99_ms=$(round(metric.request_p99_ms; digits=2)) " *
        "request_max_ms=$(round(metric.request_max_ms; digits=2)) " *
        "throughput_mib_per_sec=$(round(metric.throughput_mib_per_sec; digits=2))",
    )
    return nothing
end

function flight_live_transport_metric(metrics, backend::Symbol, operation::Symbol)
    for metric in metrics
        metric.backend == backend && metric.operation == operation && return metric
    end
    error(
        "Missing Flight live transport metric for backend $(backend) operation $(operation)",
    )
end

function flight_live_transport_print_comparison(
    io::IO,
    candidate,
    baseline;
    digits::Integer=2,
)
    candidate.operation == baseline.operation || throw(
        ArgumentError(
            "Cannot compare different operations: $(candidate.operation) vs $(baseline.operation)",
        ),
    )
    throughput_ratio =
        baseline.throughput_mib_per_sec == 0 ? Inf :
        candidate.throughput_mib_per_sec / baseline.throughput_mib_per_sec
    latency_ratio = baseline.median_ns == 0 ? Inf : candidate.median_ns / baseline.median_ns
    println(
        io,
        "compare $(candidate.backend) vs $(baseline.backend) $(candidate.operation): " *
        "median_ms=$(round(candidate.median_ms; digits=digits))/" *
        "$(round(baseline.median_ms; digits=digits)) " *
        "throughput_mib_per_sec=$(round(candidate.throughput_mib_per_sec; digits=digits))/" *
        "$(round(baseline.throughput_mib_per_sec; digits=digits)) " *
        "latency_ratio=$(round(latency_ratio; digits=digits)) " *
        "throughput_ratio=$(round(throughput_ratio; digits=digits))",
    )
    return (
        operation=candidate.operation,
        candidate_backend=candidate.backend,
        baseline_backend=baseline.backend,
        candidate_median_ms=candidate.median_ms,
        baseline_median_ms=baseline.median_ms,
        candidate_throughput_mib_per_sec=candidate.throughput_mib_per_sec,
        baseline_throughput_mib_per_sec=baseline.throughput_mib_per_sec,
        latency_ratio=latency_ratio,
        throughput_ratio=throughput_ratio,
    )
end
