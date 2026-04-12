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

const PYARROW_FLIGHT_SERVER_READY_TIMEOUT_SECS = 10.0

function wait_for_python_flight_server(
    process::Base.Process,
    stderr::Pipe,
    port::Integer;
    timeout_secs::Real=PYARROW_FLIGHT_SERVER_READY_TIMEOUT_SECS,
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
            return
        catch err
            last_error = err
            sleep(0.05)
        end
    end

    detail =
        isnothing(last_error) ? "unknown readiness failure" : sprint(showerror, last_error)
    error(
        "pyarrow Flight server did not become ready on 127.0.0.1:$(port) within $(timeout_secs)s: $(detail)",
    )
end

function start_python_flight_server(
    script_name::AbstractString;
    env_overrides::AbstractDict{<:AbstractString,<:AbstractString}=Dict{String,String}(),
)
    python = pyarrow_flight_python()
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

    line = try
        readline(stdout)
    catch err
        errout = read(stderr, String)
        wait(process)
        error(
            "failed to start pyarrow Flight server: $(sprint(showerror, err)); stderr=$(repr(errout))",
        )
    end
    port = parse(Int, chomp(line))
    wait_for_python_flight_server(process, stderr, port)
    return PyArrowFlightServer(process, stdout, stderr, port)
end

start_pyarrow_flight_server() = start_python_flight_server("flight_pyarrow_server.py")
start_headers_flight_server() = start_python_flight_server("flight_headers_server.py")
start_handshake_flight_server() = start_python_flight_server("flight_handshake_server.py")
start_poll_flight_server() = start_python_flight_server("flight_poll_server.py")
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
