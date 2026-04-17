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

using gRPCClient
using Tables

include("server_core/support.jl")
include("server_core/metadata_tests.jl")
include("server_core/descriptor_tests.jl")
include("server_core/grpc_wire_tests.jl")
include("server_core/backend_tests.jl")
include("server_core/direct_handler_tests.jl")
include("server_core/transport_adapter_tests.jl")
include("server_core/exchange_helper_tests.jl")
include("server_core/dispatch_tests.jl")

@testset "Flight server core surface" begin
    fixture = flight_server_core_fixture()
    flight_server_core_test_metadata(fixture)
    flight_server_core_test_descriptors(fixture)
    flight_server_core_test_grpc_wire(fixture)
    flight_server_core_test_backend_profiles()
    flight_server_core_test_direct_handlers(fixture)
    flight_server_core_test_transport_adapters(fixture)
    flight_server_core_test_exchange_helpers(fixture)
    flight_server_core_test_dispatch(fixture)
end
