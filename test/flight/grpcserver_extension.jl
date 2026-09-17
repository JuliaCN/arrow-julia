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

using gRPCServer

include("grpcserver_extension/support.jl")
include("grpcserver_extension/descriptor_tests.jl")
include("grpcserver_extension/direct_handler_tests.jl")
include("grpcserver_extension/live_listener_tests.jl")

@testset "Flight gRPCServer extension" begin
    protocol = Arrow.Flight.Protocol
    fixture = flight_live_fixture(protocol)
    service = flight_live_service(protocol, fixture)
    grpcserver_extension_test_backend_profiles()
    grpcserver_extension_test_descriptor(gRPCServer, service)
    grpcserver_extension_test_direct_handlers(gRPCServer, service, fixture)
    grpcserver_extension_test_live_listener(gRPCServer, service, fixture)
end
