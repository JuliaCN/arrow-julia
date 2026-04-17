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

using Tables

include("flight/support.jl")
include("flight/live_service_support.jl")
include("flight/generated_protocol_tests.jl")
include("flight/server_core.jl")
include("flight/ipc_conversion.jl")
include("flight/ipc_schema_separation.jl")

@testset "Flight generated protocol surface" begin
    flight_test_generated_protocol_formatter_surface()
end
