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

function purehttp2_extension_test_flight_sql_endpoint()
    !isnothing(
        FlightTestSupport.pyarrow_flight_python(
            required_modules=FLIGHT_SQL_ENDPOINT_REQUIRED_MODULES,
        ),
    ) || return nothing

    protocol = Arrow.Flight.Protocol
    fixture = flight_sql_endpoint_fixture(protocol)
    service = flight_sql_endpoint_service(protocol, fixture)
    server = Arrow.Flight.grpcserver_flight_server(
        service;
        host="127.0.0.1",
        port=0,
        request_capacity=4,
        response_capacity=4,
    )

    try
        purehttp2_extension_wait_for_live_server(server.host, server.port)
        result = flight_live_python_sql_endpoint_smoke(server.host, server.port, fixture)
        @test !isnothing(result)
        @test Bool(result["ok"])
        @test Int(result["query_rows"]) == 3
        @test Int(result["prepared_rows"]) == 1
        @test Int(result["ingested_rows"]) == 2
        @test Float64(result["query_ms"]) >= 0
        @test Float64(result["prepared_ms"]) >= 0
        @test Float64(result["ingest_ms"]) >= 0
    finally
        Arrow.Flight.stop!(server; force=true)
        @test !isopen(server)
    end
end
