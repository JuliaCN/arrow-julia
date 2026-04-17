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

function nghttp2_extension_test_live_listener()
    protocol = Arrow.Flight.Protocol
    live_fixture = flight_live_fixture(protocol)
    live_service = flight_live_service(protocol, live_fixture)
    server = Arrow.Flight.nghttp2_flight_server(
        live_service;
        host="127.0.0.1",
        port=0,
        request_capacity=4,
        response_capacity=4,
    )

    try
        purehttp2_extension_wait_for_live_server(server.host, server.port)
        @test isopen(server)
        @test server.port > 0
        pyarrow_smoke_ran =
            flight_live_pyarrow_readonly_smoke(server.host, server.port, live_fixture)
        @test pyarrow_smoke_ran || isnothing(FlightTestSupport.pyarrow_flight_python())
    finally
        Arrow.Flight.stop!(server; force=true)
        @test !isopen(server)
    end
end
