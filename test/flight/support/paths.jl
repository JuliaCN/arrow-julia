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

const TEST_ROOT = normpath(joinpath(@__DIR__, "..", ".."))

const FLIGHT_PYTHON_IMPORT_PROBE = """
import importlib
import sys

for name in sys.argv[1:]:
    importlib.import_module(name)
"""

function git_toplevel(path::AbstractString)
    try
        cmd = pipeline(
            Cmd(["git", "-C", path, "rev-parse", "--show-toplevel"]);
            stderr=devnull,
        )
        return normpath(chomp(read(cmd, String)))
    catch
        return nothing
    end
end

function flight_test_roots()
    roots = String[]
    path = abspath(TEST_ROOT)
    while true
        top = git_toplevel(path)
        !isnothing(top) && push!(roots, top)
        parent = dirname(path)
        parent == path && break
        path = parent
    end
    push!(roots, TEST_ROOT)
    unique!(roots)
    return roots
end

function flight_test_python_candidates()
    candidates = String[]
    haskey(ENV, "ARROW_FLIGHT_PYTHON") && push!(candidates, ENV["ARROW_FLIGHT_PYTHON"])
    cache_home = get(ENV, "PRJ_CACHE_HOME", ".cache")
    for root in flight_test_roots()
        push!(
            candidates,
            joinpath(root, cache_home, "arrow-julia-flight-pyenv", "bin", "python"),
        )
        push!(candidates, joinpath(root, ".venv", "bin", "python"))
    end
    python3 = Sys.which("python3")
    !isnothing(python3) && push!(candidates, python3)
    python = Sys.which("python")
    !isnothing(python) && push!(candidates, python)
    unique!(filter!(isfile, candidates))
    return candidates
end

function python_supports_modules(
    python::AbstractString,
    modules::AbstractVector{<:AbstractString},
)
    cmd = ignorestatus(Cmd([python, "-c", FLIGHT_PYTHON_IMPORT_PROBE, modules...]))
    return success(pipeline(cmd; stdout=devnull, stderr=devnull))
end

function pyarrow_flight_python(;
    required_modules::AbstractVector{<:AbstractString}=String["pyarrow.flight"],
)
    for python in flight_test_python_candidates()
        python_supports_modules(python, required_modules) && return python
    end
    return nothing
end
