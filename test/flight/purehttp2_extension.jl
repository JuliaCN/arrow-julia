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

include("purehttp2_extension/support.jl")
include("purehttp2_extension/live_listener_tests.jl")

@testset "Flight gRPCServer transport over PureHTTP2 wire" begin
    fixture = purehttp2_extension_fixture()
    purehttp2_extension_test_live_listener(fixture)
    purehttp2_extension_test_large_pyarrow_doexchange_listener(fixture)
    purehttp2_extension_test_concurrent_large_doget_listener()
    purehttp2_extension_test_concurrent_listener(fixture)
    purehttp2_extension_test_overload_listener(fixture)
end
