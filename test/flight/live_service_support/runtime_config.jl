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

function _flight_live_command_timeout_sec()
    raw_timeout = get(ENV, "ARROW_FLIGHT_PYARROW_BENCHMARK_TIMEOUT_SEC", "30")
    timeout_sec = tryparse(Float64, raw_timeout)
    isnothing(timeout_sec) && throw(
        ArgumentError(
            "ARROW_FLIGHT_PYARROW_BENCHMARK_TIMEOUT_SEC must parse as a positive number; got $(repr(raw_timeout))",
        ),
    )
    timeout_sec > 0 || throw(
        ArgumentError(
            "ARROW_FLIGHT_PYARROW_BENCHMARK_TIMEOUT_SEC must be positive; got $(repr(raw_timeout))",
        ),
    )
    return timeout_sec
end

function _flight_live_pyarrow_lookahead_bytes()
    raw_value = get(ENV, "ARROW_FLIGHT_PYARROW_LOOKAHEAD_BYTES", string(4 * 1024 * 1024))
    lookahead_bytes = tryparse(Int, raw_value)
    isnothing(lookahead_bytes) && throw(
        ArgumentError(
            "ARROW_FLIGHT_PYARROW_LOOKAHEAD_BYTES must parse as a non-negative integer; got $(repr(raw_value))",
        ),
    )
    lookahead_bytes >= 0 || throw(
        ArgumentError(
            "ARROW_FLIGHT_PYARROW_LOOKAHEAD_BYTES must be non-negative; got $(repr(raw_value))",
        ),
    )
    return lookahead_bytes
end

function _flight_live_pyarrow_concurrent_clients()
    raw_value = get(ENV, "ARROW_FLIGHT_PYARROW_CONCURRENT_CLIENTS", "4")
    concurrent_clients = tryparse(Int, raw_value)
    isnothing(concurrent_clients) && throw(
        ArgumentError(
            "ARROW_FLIGHT_PYARROW_CONCURRENT_CLIENTS must parse as a positive integer; got $(repr(raw_value))",
        ),
    )
    concurrent_clients > 0 || throw(
        ArgumentError(
            "ARROW_FLIGHT_PYARROW_CONCURRENT_CLIENTS must be positive; got $(repr(raw_value))",
        ),
    )
    return concurrent_clients
end

function _flight_live_pyarrow_requests_per_client()
    raw_value = get(ENV, "ARROW_FLIGHT_PYARROW_REQUESTS_PER_CLIENT", "2")
    requests_per_client = tryparse(Int, raw_value)
    isnothing(requests_per_client) && throw(
        ArgumentError(
            "ARROW_FLIGHT_PYARROW_REQUESTS_PER_CLIENT must parse as a positive integer; got $(repr(raw_value))",
        ),
    )
    requests_per_client > 0 || throw(
        ArgumentError(
            "ARROW_FLIGHT_PYARROW_REQUESTS_PER_CLIENT must be positive; got $(repr(raw_value))",
        ),
    )
    return requests_per_client
end

function _flight_live_pyarrow_soak_rounds()
    raw_value = get(ENV, "ARROW_FLIGHT_PYARROW_SOAK_ROUNDS", "3")
    soak_rounds = tryparse(Int, raw_value)
    isnothing(soak_rounds) && throw(
        ArgumentError(
            "ARROW_FLIGHT_PYARROW_SOAK_ROUNDS must parse as a positive integer; got $(repr(raw_value))",
        ),
    )
    soak_rounds > 0 || throw(
        ArgumentError(
            "ARROW_FLIGHT_PYARROW_SOAK_ROUNDS must be positive; got $(repr(raw_value))",
        ),
    )
    return soak_rounds
end

function _flight_live_pyarrow_reused_doput_requests()
    raw_value = get(ENV, "ARROW_FLIGHT_PYARROW_REUSED_DOPUT_REQUESTS", "2")
    requests = tryparse(Int, raw_value)
    isnothing(requests) && throw(
        ArgumentError(
            "ARROW_FLIGHT_PYARROW_REUSED_DOPUT_REQUESTS must parse as an integer >= 2; got $(repr(raw_value))",
        ),
    )
    requests >= 2 || throw(
        ArgumentError(
            "ARROW_FLIGHT_PYARROW_REUSED_DOPUT_REQUESTS must be >= 2; got $(repr(raw_value))",
        ),
    )
    return requests
end

function _flight_live_command_output_excerpt(output::AbstractString; limit::Integer=2_000)
    stripped = strip(output)
    isempty(stripped) && return "(empty)"
    return length(stripped) <= limit ? stripped :
           string(first(stripped, limit), "\n...[truncated]")
end

function _flight_live_readchomp_with_timeout(
    cmd::Cmd;
    timeout_sec::Real,
    label::AbstractString,
)
    stdout_path, stdout_io = mktemp()
    stderr_path, stderr_io = mktemp()
    process = nothing
    try
        process =
            run(pipeline(ignorestatus(cmd); stdout=stdout_io, stderr=stderr_io); wait=false)
        close(stdout_io)
        close(stderr_io)

        wait_status = timedwait(() -> !process_running(process), timeout_sec; pollint=0.05)
        if wait_status === :timed_out
            try
                kill(process)
            catch
            end
            try
                wait(process)
            catch
            end
            stdout_output = read(stdout_path, String)
            stderr_output = read(stderr_path, String)
            error(
                "$(label) timed out after $(timeout_sec) seconds\nstdout:\n$(_flight_live_command_output_excerpt(stdout_output))\nstderr:\n$(_flight_live_command_output_excerpt(stderr_output))",
            )
        end

        wait(process)
        stdout_output = read(stdout_path, String)
        stderr_output = read(stderr_path, String)
        process.exitcode == 0 || error(
            "$(label) failed with exit code $(process.exitcode)\nstdout:\n$(_flight_live_command_output_excerpt(stdout_output))\nstderr:\n$(_flight_live_command_output_excerpt(stderr_output))",
        )
        return chomp(stdout_output)
    finally
        try
            close(stdout_io)
        catch
        end
        try
            close(stderr_io)
        catch
        end
        if !isnothing(process) && process_running(process)
            try
                kill(process)
            catch
            end
            try
                wait(process)
            catch
            end
        end
        rm(stdout_path; force=true)
        rm(stderr_path; force=true)
    end
end
