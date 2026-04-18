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
include("purehttp2_extension/bridge_tests.jl")
include("purehttp2_extension/live_connection_tests.jl")
include("purehttp2_extension/live_listener_tests.jl")

@testset "Flight PureHTTP2 transport" begin
    fixture = purehttp2_extension_fixture()
    purehttp2_extension_test_bridge(fixture)
    purehttp2_extension_test_live_connection(fixture)
    purehttp2_extension_test_large_request_window_updates(fixture)
    purehttp2_extension_test_single_large_request_message(fixture)
    purehttp2_extension_test_large_flightdata_request_stream(fixture)
    purehttp2_extension_test_live_listener(fixture)
    purehttp2_extension_test_large_pyarrow_doexchange_listener(fixture)
    purehttp2_extension_test_concurrent_listener(fixture)
    purehttp2_extension_test_overload_listener(fixture)
end
