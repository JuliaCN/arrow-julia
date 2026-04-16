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

include("nghttp2_extension/backend_tests.jl")
include("nghttp2_extension/live_listener_tests.jl")
include("nghttp2_extension/performance_tests.jl")

@testset "Flight Nghttp2Wrapper extension" begin
    nghttp2_extension_test_backend_profiles()
    nghttp2_extension_test_live_listener()
    nghttp2_extension_test_large_transport_compare()
end
