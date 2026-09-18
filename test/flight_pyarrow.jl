# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

using Test
using Arrow
using DataAPI
using JSON
using Tables

include("flight/support.jl")

python = FlightTestSupport.pyarrow_flight_python()
isnothing(python) && error(
    "PyArrow Flight is required for this compatibility suite; set ARROW_FLIGHT_PYTHON to a Python executable with pyarrow.flight",
)

pyarrow_version = strip(
    read(
        Cmd([
            python,
            "-c",
            "import pyarrow; import pyarrow.flight; print(pyarrow.__version__)",
        ]),
        String,
    ),
)
@info "Running required PyArrow Flight compatibility suite" python pyarrow_version

include("flight/live_service_support.jl")
include("flight/grpcserver_extension.jl")
