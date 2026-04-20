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

function grpcserver_extension_test_live_listener(grpcserver, service, fixture)
    with_grpcserver_extension_live_server(grpcserver, service) do server, host, port
        @test server.status == grpcserver.ServerStatus.RUNNING
        pyarrow_smoke_ran = flight_live_pyarrow_smoke(host, port, fixture)
        @test pyarrow_smoke_ran || isnothing(FlightTestSupport.pyarrow_flight_python())
    end
end
