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

using Pkg
using TOML

const TEST_ROOT = @__DIR__
const ARROW_ROOT = normpath(joinpath(TEST_ROOT, ".."))
const ARROWTYPES_ROOT = joinpath(ARROW_ROOT, "src", "ArrowTypes")
const NGHTTP2WRAPPER_URL = "https://github.com/s-celles/Nghttp2Wrapper.jl"
const NGHTTP2WRAPPER_REV = "aeb161cdd438ccbeae5d4508322a6c65859bb9ff"

auto_local_flight_dev_deps() = get(ENV, "ARROW_FLIGHT_DISABLE_AUTO_LOCAL_DEPS", "0") != "1"

function maybe_git_root(path::AbstractString)
    try
        return readchomp(pipeline(`git -C $path rev-parse --show-toplevel`; stderr=devnull))
    catch
        return nothing
    end
end

function flight_nghttp2_probe_roots(path::AbstractString)
    roots = String[]
    current = abspath(path)
    while true
        root = maybe_git_root(current)
        if !isnothing(root) && root ∉ roots
            push!(roots, root)
        end
        parent = dirname(current)
        parent == current && break
        current = parent
    end
    return roots
end

function maybe_locate_purehttp2()
    if haskey(ENV, "ARROW_FLIGHT_PUREHTTP2_PATH")
        candidate = abspath(ENV["ARROW_FLIGHT_PUREHTTP2_PATH"])
        isdir(candidate) || error("ARROW_FLIGHT_PUREHTTP2_PATH does not exist: $candidate")
        return candidate
    end

    auto_local_flight_dev_deps() || return nothing

    for root in flight_nghttp2_probe_roots(TEST_ROOT)
        for candidate in (
            joinpath(root, ".data", "PureHTTP2.jl"),
            joinpath(root, "PureHTTP2.jl"),
            joinpath(root, ".cache", "vendor", "PureHTTP2.jl"),
            "/tmp/PureHTTP2.jl",
        )
            isdir(candidate) && return candidate
        end
    end

    return nothing
end

function maybe_locate_nghttp2wrapper()
    if haskey(ENV, "ARROW_FLIGHT_NGHTTP2WRAPPER_PATH")
        candidate = abspath(ENV["ARROW_FLIGHT_NGHTTP2WRAPPER_PATH"])
        isdir(candidate) ||
            error("ARROW_FLIGHT_NGHTTP2WRAPPER_PATH does not exist: $candidate")
        return candidate
    end

    auto_local_flight_dev_deps() || return nothing

    for root in flight_nghttp2_probe_roots(TEST_ROOT)
        for candidate in (
            joinpath(root, ".data", "Nghttp2Wrapper.jl"),
            joinpath(root, "Nghttp2Wrapper.jl"),
            joinpath(root, ".cache", "vendor", "Nghttp2Wrapper.jl"),
            "/tmp/Nghttp2Wrapper.jl",
        )
            isdir(candidate) && return candidate
        end
    end

    return nothing
end

function strip_temp_source_override!(
    project_path::AbstractString,
    package_name::AbstractString,
)
    project = TOML.parsefile(project_path)
    sources = get(project, "sources", nothing)
    sources isa AbstractDict || return nothing
    haskey(sources, package_name) || return nothing
    delete!(sources, package_name)
    isempty(sources) && delete!(project, "sources")
    open(project_path, "w") do io
        TOML.print(io, project)
    end
    return nothing
end

const TEMP_ENV = mktempdir()
const TEMP_PROJECT = joinpath(TEMP_ENV, "Project.toml")
cp(joinpath(TEST_ROOT, "Project.toml"), TEMP_PROJECT)

local_purehttp2 = maybe_locate_purehttp2()
!isnothing(local_purehttp2) && strip_temp_source_override!(TEMP_PROJECT, "PureHTTP2")

Pkg.activate(TEMP_ENV)
dev_packages = PackageSpec[PackageSpec(path=ARROW_ROOT), PackageSpec(path=ARROWTYPES_ROOT)]
if !isnothing(local_purehttp2)
    push!(dev_packages, PackageSpec(path=local_purehttp2))
end
Pkg.develop(dev_packages)

local_nghttp2wrapper = maybe_locate_nghttp2wrapper()
if isnothing(local_nghttp2wrapper)
    Pkg.add(
        PackageSpec(name="Nghttp2Wrapper", url=NGHTTP2WRAPPER_URL, rev=NGHTTP2WRAPPER_REV),
    )
else
    Pkg.develop(PackageSpec(path=local_nghttp2wrapper))
end
Pkg.instantiate()

using Test
using Nghttp2Wrapper
using PureHTTP2
using Sockets
using Statistics

const NGHTTP2_PROBE_PATH = "/nghttp2-probe"
const NGHTTP2_PROBE_INTEROP_PAYLOAD_BYTES = 32 * 1024
const NGHTTP2_PROBE_LARGE_PAYLOAD_BYTES = 2 * 1024 * 1024
const NGHTTP2_PROBE_INTEROP_ITERATIONS = 3

function nghttp2_probe_connect_tcp(host::AbstractString, port::Integer)
    deadline = time() + 5.0
    last_error = nothing
    while time() < deadline
        try
            return Sockets.connect(parse(Sockets.IPv4, host), port)
        catch error
            last_error = error
        end
        sleep(0.02)
    end
    detail =
        isnothing(last_error) ? "unknown connect failure" : sprint(showerror, last_error)
    error("Nghttp2Wrapper probe failed to connect to $(host):$(port): $(detail)")
end

function nghttp2_probe_header(headers, name::AbstractString)
    for header in headers
        key, value =
            header isa Tuple{String,String} ? header :
            (String(copy(header.name)), String(copy(header.value)))
        key == String(name) && return value
    end
    return nothing
end

function nghttp2_probe_required_low_level_hooks()
    return (
        :Callbacks,
        :Session,
        :HTTP2Server,
        :nghttp2_session_server_new,
        :nghttp2_session_callbacks_set_on_header_callback,
        :nghttp2_session_callbacks_set_on_data_chunk_recv_callback,
        :nghttp2_session_callbacks_set_on_frame_recv_callback,
        :nghttp2_submit_response2,
        :nghttp2_submit_headers,
        :nghttp2_submit_window_update,
        :nghttp2_submit_rst_stream,
        :nghttp2_submit_goaway,
    )
end

function nghttp2_probe_test_low_level_hooks()
    hooks = nghttp2_probe_required_low_level_hooks()
    for hook in hooks
        @test isdefined(Nghttp2Wrapper, hook)
    end
    println(stdout, "nghttp2wrapper low_level_hooks=$(collect(hooks))")
    return hooks
end

function nghttp2_probe_make_payload(size_bytes::Integer, byte::UInt8=UInt8('x'))
    return fill(byte, size_bytes)
end

function nghttp2_probe_start_nghttp2_server(
    payload::Vector{UInt8};
    host::AbstractString="127.0.0.1",
    path::AbstractString=NGHTTP2_PROBE_PATH,
)
    server = Nghttp2Wrapper.HTTP2Server(0; host=host) do req
        if req.method != "GET" || req.path != path
            return Nghttp2Wrapper.ServerResponse(404, "not found")
        end
        return Nghttp2Wrapper.ServerResponse(
            200;
            headers=Nghttp2Wrapper.NVPair[
                Nghttp2Wrapper.NVPair("content-type", "application/octet-stream"),
                Nghttp2Wrapper.NVPair("content-length", string(length(payload))),
            ],
            body=payload,
        )
    end
    sleep(0.05)
    return (
        backend=:nghttp2,
        host=String(host),
        port=Nghttp2Wrapper.listener_port(server),
        stop=() -> close(server),
        errors=nothing,
    )
end

function nghttp2_probe_start_purehttp2_server(
    payload::Vector{UInt8};
    host::AbstractString="127.0.0.1",
    path::AbstractString=NGHTTP2_PROBE_PATH,
)
    listener = Sockets.listen(parse(Sockets.IPv4, host), 0)
    _, port = getsockname(listener)
    isopen = Ref(true)
    errors = Channel{Any}(8)
    connection_tasks = Task[]

    function purehttp2_handler(req::PureHTTP2.Request, res::PureHTTP2.Response)
        if PureHTTP2.request_method(req) != "GET" || PureHTTP2.request_path(req) != path
            PureHTTP2.set_status!(res, 404)
            PureHTTP2.write_body!(res, "not found")
            return nothing
        end
        PureHTTP2.set_status!(res, 200)
        PureHTTP2.set_header!(res, "content-type", "application/octet-stream")
        PureHTTP2.set_header!(res, "content-length", string(length(payload)))
        PureHTTP2.write_body!(res, payload)
        return nothing
    end

    accept_task = @async begin
        while isopen[]
            socket = try
                accept(listener)
            catch error
                isopen[] || break
                try
                    put!(errors, error)
                catch
                end
                break
            end
            task = @async begin
                try
                    conn = PureHTTP2.HTTP2Connection()
                    PureHTTP2.serve_with_handler!(purehttp2_handler, conn, socket)
                catch error
                    if isopen[]
                        try
                            put!(errors, error)
                        catch
                        end
                    end
                finally
                    try
                        close(socket)
                    catch
                    end
                end
            end
            push!(connection_tasks, task)
        end
    end

    sleep(0.05)
    return (
        backend=:purehttp2,
        host=String(host),
        port=Int(port),
        stop=() -> begin
            isopen[] = false
            try
                close(listener)
            catch
            end
            try
                wait(accept_task)
            catch
            end
            for task in copy(connection_tasks)
                try
                    wait(task)
                catch
                end
            end
            return nothing
        end,
        errors=errors,
    )
end

function nghttp2_probe_rethrow_server_error(server)
    errors = get(server, :errors, nothing)
    errors isa Channel{Any} || return nothing
    isready(errors) || return nothing
    throw(take!(errors))
end

function nghttp2_probe_request_raw_client(
    host::AbstractString,
    port::Integer;
    path::AbstractString=NGHTTP2_PROBE_PATH,
    expected_body_bytes::Integer,
    timeout_sec::Real=5.0,
)
    socket = nghttp2_probe_connect_tcp(host, port)
    callbacks = Nghttp2Wrapper.Callbacks()
    chunks = Channel{Vector{UInt8}}(32)
    reader = @async begin
        try
            while isopen(socket)
                Base.wait_readnb(socket, 1)
                buffer = readavailable(socket)
                isempty(buffer) && break
                put!(chunks, collect(buffer))
            end
        catch
        finally
            try
                close(chunks)
            catch
            end
        end
    end
    try
        Nghttp2Wrapper.nghttp2_session_callbacks_set_on_header_callback(
            callbacks.ptr,
            Nghttp2Wrapper._on_header_cb_ptr(),
        )
        Nghttp2Wrapper.nghttp2_session_callbacks_set_on_data_chunk_recv_callback(
            callbacks.ptr,
            Nghttp2Wrapper._on_data_chunk_cb_ptr(),
        )
        Nghttp2Wrapper.nghttp2_session_callbacks_set_on_stream_close_callback(
            callbacks.ptr,
            Nghttp2Wrapper._on_stream_close_cb_ptr(),
        )

        ctx = Nghttp2Wrapper.ClientContext(
            Dict{Int32,Nghttp2Wrapper.StreamState}(),
            ReentrantLock(),
        )
        done = lock(ctx.lock) do
            state = Nghttp2Wrapper.StreamState(Int32(1))
            ctx.streams[Int32(1)] = state
            return state.done
        end

        GC.@preserve ctx begin
            rv, session_ptr = Nghttp2Wrapper.nghttp2_session_client_new(
                callbacks.ptr,
                pointer_from_objref(ctx),
            )
            @test rv == 0
            try
                Nghttp2Wrapper.check_error(
                    Nghttp2Wrapper.nghttp2_submit_settings(session_ptr),
                )
                headers = Nghttp2Wrapper.NVPair[
                    Nghttp2Wrapper.NVPair(":method", "GET"),
                    Nghttp2Wrapper.NVPair(":path", path),
                    Nghttp2Wrapper.NVPair(":scheme", "http"),
                    Nghttp2Wrapper.NVPair(":authority", "$(host):$(port)"),
                ]
                nva = [Nghttp2Wrapper.to_nghttp2_nv(header) for header in headers]
                GC.@preserve headers nva begin
                    stream_id = Nghttp2Wrapper.nghttp2_submit_request2(
                        session_ptr,
                        C_NULL,
                        pointer(nva),
                        length(nva),
                        C_NULL,
                        C_NULL,
                    )
                    Nghttp2Wrapper.check_error(stream_id)
                    @test stream_id == 1
                end

                initial = Nghttp2Wrapper._session_send_all(session_ptr)
                isempty(initial) || write(socket, initial)

                deadline = time() + timeout_sec
                while time() < deadline
                    if isready(done)
                        return take!(done)
                    end
                    while isready(chunks)
                        buffer = take!(chunks)
                        Nghttp2Wrapper.check_error(
                            Nghttp2Wrapper.nghttp2_session_mem_recv2(session_ptr, buffer),
                        )
                        pending = Nghttp2Wrapper._session_send_all(session_ptr)
                        isempty(pending) || write(socket, pending)
                    end
                    isready(done) && return take!(done)
                    status, headers, body = lock(ctx.lock) do
                        if haskey(ctx.streams, Int32(1))
                            state = ctx.streams[Int32(1)]
                            return (state.status, copy(state.headers), copy(state.body))
                        end
                        return (0, Nghttp2Wrapper.NVPair[], UInt8[])
                    end
                    if status == 200 && length(body) >= expected_body_bytes
                        return Nghttp2Wrapper.Response(status, headers, body)
                    end
                    sleep(0.01)
                end
                error(
                    "Nghttp2Wrapper raw client did not finish response from $(host):$(port) " *
                    "within $(timeout_sec) seconds; expected $(expected_body_bytes) bytes",
                )
            finally
                Nghttp2Wrapper.nghttp2_session_del(session_ptr)
            end
        end
    finally
        try
            close(callbacks)
        catch
        end
        try
            close(socket)
        catch
        end
        try
            wait(reader)
        catch
        end
    end
end

function nghttp2_probe_metric(
    backend::Symbol,
    total_bytes::Integer,
    durations_ns::Vector{Int},
)
    median_ns = Int(round(median(durations_ns)))
    throughput_mib_per_sec = (Int(total_bytes) / max(median_ns, 1) * 1.0e9) / 1024.0^2
    return (
        backend=backend,
        total_bytes=Int(total_bytes),
        median_ns=median_ns,
        throughput_mib_per_sec=throughput_mib_per_sec,
    )
end

function nghttp2_probe_print_metric(metric)
    println(
        stdout,
        "$(metric.backend) raw_h2c_probe: total_bytes=$(metric.total_bytes) " *
        "median_ms=$(round(metric.median_ns / 1.0e6; digits=2)) " *
        "throughput_mib_per_sec=$(round(metric.throughput_mib_per_sec; digits=2))",
    )
    return metric
end

function nghttp2_probe_compare_metrics(candidate, baseline)
    ratio =
        baseline.throughput_mib_per_sec == 0 ? Inf :
        candidate.throughput_mib_per_sec / baseline.throughput_mib_per_sec
    println(
        stdout,
        "raw_h2c_compare: candidate=$(candidate.backend) baseline=$(baseline.backend) " *
        "throughput_ratio=$(round(ratio; digits=3))",
    )
    return (
        candidate_backend=candidate.backend,
        baseline_backend=baseline.backend,
        throughput_ratio=ratio,
        candidate_throughput_mib_per_sec=candidate.throughput_mib_per_sec,
        baseline_throughput_mib_per_sec=baseline.throughput_mib_per_sec,
    )
end

function nghttp2_probe_test_purehttp2_client_smoke()
    payload = nghttp2_probe_make_payload(NGHTTP2_PROBE_INTEROP_PAYLOAD_BYTES, UInt8('i'))
    server = nghttp2_probe_start_nghttp2_server(payload)
    try
        socket = nghttp2_probe_connect_tcp(server.host, server.port)
        try
            started = time_ns()
            result = PureHTTP2.open_connection!(
                PureHTTP2.HTTP2Connection(),
                socket;
                request_headers=Tuple{String,String}[
                    (":method", "GET"),
                    (":path", NGHTTP2_PROBE_PATH),
                    (":scheme", "http"),
                    (":authority", "$(server.host):$(server.port)"),
                ],
            )
            elapsed_ns = time_ns() - started
            @test result.status == 200
            @test nghttp2_probe_header(result.headers, "content-type") ==
                  "application/octet-stream"
            @test result.body == payload
            throughput_mib_per_sec =
                (length(result.body) / max(elapsed_ns, 1) * 1.0e9) / 1024.0^2
            println(
                stdout,
                "purehttp2_client_smoke: total_bytes=$(length(result.body)) " *
                "elapsed_ms=$(round(elapsed_ns / 1.0e6; digits=2)) " *
                "throughput_mib_per_sec=$(round(throughput_mib_per_sec; digits=2))",
            )
            return (
                total_bytes=length(result.body),
                elapsed_ns=elapsed_ns,
                throughput_mib_per_sec=throughput_mib_per_sec,
            )
        finally
            close(socket)
        end
    finally
        server.stop()
    end
end

function nghttp2_probe_benchmark_raw_h2c_backend(
    backend::Symbol;
    iterations::Integer=NGHTTP2_PROBE_INTEROP_ITERATIONS,
    timeout_sec::Real=5.0,
)
    payload = nghttp2_probe_make_payload(NGHTTP2_PROBE_LARGE_PAYLOAD_BYTES)
    server =
        backend == :purehttp2 ? nghttp2_probe_start_purehttp2_server(payload) :
        backend == :nghttp2 ? nghttp2_probe_start_nghttp2_server(payload) :
        error("unsupported backend: $(backend)")
    durations_ns = Int[]
    try
        for _ = 1:iterations
            started = time_ns()
            response = nghttp2_probe_request_raw_client(
                server.host,
                server.port;
                expected_body_bytes=length(payload),
                timeout_sec=timeout_sec,
            )
            elapsed_ns = time_ns() - started
            @test response.status == 200
            @test nghttp2_probe_header(response.headers, "content-type") ==
                  "application/octet-stream"
            @test response.body == payload
            push!(durations_ns, elapsed_ns)
            nghttp2_probe_rethrow_server_error(server)
        end
    finally
        server.stop()
    end
    metric = nghttp2_probe_metric(backend, length(payload), durations_ns)
    nghttp2_probe_print_metric(metric)
    return metric
end

function nghttp2_probe_attempt_raw_h2c_backend(
    backend::Symbol;
    iterations::Integer=NGHTTP2_PROBE_INTEROP_ITERATIONS,
    timeout_sec::Real=5.0,
)
    try
        metric = nghttp2_probe_benchmark_raw_h2c_backend(
            backend;
            iterations=iterations,
            timeout_sec=timeout_sec,
        )
        return (backend=backend, supported=true, detail=nothing, metric=metric)
    catch error
        detail = sprint(showerror, error)
        println(stdout, "$(backend) raw_h2c_probe unsupported reason=\"$(detail)\"")
        return (backend=backend, supported=false, detail=detail, metric=nothing)
    end
end

@testset "Flight Nghttp2Wrapper substrate probe" begin
    hooks = nghttp2_probe_test_low_level_hooks()
    @test length(hooks) == 12

    smoke = nghttp2_probe_test_purehttp2_client_smoke()
    @test smoke.total_bytes == NGHTTP2_PROBE_INTEROP_PAYLOAD_BYTES
    @test smoke.elapsed_ns > 0
    @test smoke.throughput_mib_per_sec > 0

    purehttp2_attempt = nghttp2_probe_attempt_raw_h2c_backend(:purehttp2; timeout_sec=3.0)
    @test purehttp2_attempt.backend == :purehttp2
    if purehttp2_attempt.supported
        @test purehttp2_attempt.metric.total_bytes == NGHTTP2_PROBE_LARGE_PAYLOAD_BYTES
        @test purehttp2_attempt.metric.throughput_mib_per_sec > 0
    else
        @test occursin("did not finish response", purehttp2_attempt.detail)
    end

    nghttp2_attempt = nghttp2_probe_attempt_raw_h2c_backend(:nghttp2)
    @test nghttp2_attempt.backend == :nghttp2
    @test nghttp2_attempt.supported
    @test nghttp2_attempt.metric.total_bytes == NGHTTP2_PROBE_LARGE_PAYLOAD_BYTES
    @test nghttp2_attempt.metric.median_ns > 0
    @test nghttp2_attempt.metric.throughput_mib_per_sec > 0

    if purehttp2_attempt.supported
        comparison =
            nghttp2_probe_compare_metrics(nghttp2_attempt.metric, purehttp2_attempt.metric)
        @test comparison.candidate_backend == :nghttp2
        @test comparison.baseline_backend == :purehttp2
        @test comparison.throughput_ratio > 0
    else
        println(
            stdout,
            "raw_h2c_compare deferred reason=\"PureHTTP2 stock h2c handler benchmark remains blocked; use Arrow.jl package-owned PureHTTP2 performance runner for large-transport proof\"",
        )
    end
end
