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

const PYARROW_FLIGHT_SERVER_READY_TIMEOUT_SECS = 60.0
const PYARROW_FLIGHT_PROBE_DEADLINE_SECS = 5.0
const TLS_FLIGHT_TEST_DEADLINE_SECS = 30.0
const POLL_FLIGHT_REQUIRED_MODULES = ["pyarrow.flight", "grpc", "grpc_tools"]

function read_python_flight_server_port(process::Base.Process, stdout::Pipe, stderr::Pipe)
    line = ""
    while true
        line = try
            readline(stdout)
        catch err
            errout = read(stderr, String)
            wait(process)
            error(
                "failed to start pyarrow Flight server: $(sprint(showerror, err)); stderr=$(repr(errout))",
            )
        end
        stripped = strip(line)
        if isempty(stripped)
            if process_exited(process)
                errout = read(stderr, String)
                wait(process)
                error(
                    "pyarrow Flight server exited before reporting a port; stderr=$(repr(errout))",
                )
            end
            continue
        end
        port = tryparse(Int, stripped)
        !isnothing(port) && return port
        if process_exited(process)
            errout = read(stderr, String)
            wait(process)
            error(
                "pyarrow Flight server reported a non-numeric startup line $(repr(stripped)); stderr=$(repr(errout))",
            )
        end
    end
end

function flight_readiness_client(port::Integer, grpc)
    return Arrow.Flight.Client(
        "grpc://127.0.0.1:$(port)";
        grpc=grpc,
        deadline=PYARROW_FLIGHT_PROBE_DEADLINE_SECS,
    )
end

function wait_for_readiness_probe(readiness_probe::Function, port::Integer)
    try
        readiness_probe(port)
        return true
    catch err
        return false, err
    end
end

function wait_for_python_flight_server(
    process::Base.Process,
    stderr::Pipe,
    port::Integer;
    timeout_secs::Real=PYARROW_FLIGHT_SERVER_READY_TIMEOUT_SECS,
    readiness_probe::Union{Nothing,Function}=nothing,
)
    deadline = time() + Float64(timeout_secs)
    last_error = nothing

    while time() < deadline
        if process_exited(process)
            wait(process)
            errout = String(read(stderr))
            error(
                "pyarrow Flight server exited before becoming ready: stderr=$(repr(errout))",
            )
        end

        try
            socket = Sockets.connect(ip"127.0.0.1", port)
            close(socket)
            if isnothing(readiness_probe)
                return
            end
            probe_result = wait_for_readiness_probe(readiness_probe, port)
            probe_result === true && return
            _, last_error = probe_result
        catch err
            last_error = err
        end
        sleep(0.05)
    end

    detail_message = "pyarrow Flight server did not become ready on 127.0.0.1:$(port) within $(timeout_secs)s"
    detail =
        isnothing(last_error) ? "unknown readiness failure" : sprint(showerror, last_error)
    error("$(detail_message): $(detail)")
end

function start_python_flight_server(
    script_name::AbstractString;
    env_overrides::AbstractDict{<:AbstractString,<:AbstractString}=Dict{String,String}(),
    readiness_probe::Union{Nothing,Function}=nothing,
    required_modules::AbstractVector{<:AbstractString}=String["pyarrow.flight"],
)
    python = pyarrow_flight_python(required_modules=required_modules)
    isnothing(python) && return nothing

    stdout = Pipe()
    stderr = Pipe()
    env = merge(
        Dict{String,String}(ENV),
        Dict("PYTHONUNBUFFERED" => "1"),
        Dict{String,String}(string(k) => string(v) for (k, v) in pairs(env_overrides)),
    )
    cmd = setenv(Cmd([python, joinpath(TEST_ROOT, script_name)]), env)
    process = run(pipeline(cmd; stdout=stdout, stderr=stderr), wait=false)
    close(stdout.in)
    close(stderr.in)

    port = read_python_flight_server_port(process, stdout, stderr)
    wait_for_python_flight_server(process, stderr, port; readiness_probe=readiness_probe)
    return PyArrowFlightServer(process, stdout, stderr, port)
end

function probe_pyarrow_flight_server(port::Integer)
    with_test_grpc_handle() do grpc
        client = flight_readiness_client(port, grpc)
        req, channel = Arrow.Flight.listactions(client)
        collect(channel)
        gRPCClient.grpc_async_await(req)
    end
    return
end

start_pyarrow_flight_server() = start_python_flight_server(
    "flight_pyarrow_server.py";
    readiness_probe=probe_pyarrow_flight_server,
)
start_poll_flight_server() = start_python_flight_server(
    "flight_poll_server.py";
    required_modules=POLL_FLIGHT_REQUIRED_MODULES,
)
function start_tls_flight_server(cert_path::AbstractString, key_path::AbstractString)
    start_python_flight_server(
        "flight_tls_server.py";
        env_overrides=Dict(
            "ARROW_FLIGHT_TLS_CERT" => String(cert_path),
            "ARROW_FLIGHT_TLS_KEY" => String(key_path),
        ),
    )
end

function stop_pyarrow_flight_server(server::PyArrowFlightServer)
    try
        kill(server.process)
    catch
    end
    try
        wait(server.process)
    catch
    end
    close(server.stdout)
    close(server.stderr)
    return
end

stop_pyarrow_flight_server(::Nothing) = nothing
