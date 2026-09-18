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

    configured = Ref(false)
    flight_server = Arrow.Flight.grpcserver_flight_server(
        service;
        host="127.0.0.1",
        port=0,
        max_concurrent_requests=2,
        request_capacity=4,
        response_capacity=4,
        max_message_size=8 * 1024 * 1024,
        enable_health_check=true,
        configure_server=server -> begin
            configured[] = true
            grpcserver.add_interceptor!(server, grpcserver.MetricsInterceptor())
        end,
    )

    try
        @test isopen(flight_server)
        @test flight_server.port > 0
        @test flight_server.port == grpcserver.HTTP.port(flight_server.server)
        @test flight_server.server.config.max_concurrent_requests == 2
        @test flight_server.server.config.max_message_size == 8 * 1024 * 1024
        @test flight_server.server.config.enable_health_check
        @test configured[]
        pyarrow_smoke_ran =
            flight_live_pyarrow_smoke(flight_server.host, flight_server.port, fixture)
        @test pyarrow_smoke_ran || isnothing(FlightTestSupport.pyarrow_flight_python())
        metrics = Arrow.Flight.flight_server_metrics(flight_server)
        @test metrics.active_calls == 0
        @test metrics.calls_started >= (pyarrow_smoke_ran ? 1 : 0)
        @test metrics.calls_completed >= (pyarrow_smoke_ran ? 1 : 0)
    finally
        Arrow.Flight.stop!(flight_server; force=true)
    end

    python = FlightTestSupport.pyarrow_flight_python()
    if !isnothing(python)
        mktempdir() do dir
            certificates = FlightTestSupport.generate_test_tls_certificate(dir)
            @test !isnothing(certificates)
            if !isnothing(certificates)
                cert_path, key_path = certificates
                tls_server = Arrow.Flight.grpcserver_flight_server(
                    service;
                    host="127.0.0.1",
                    port=0,
                    tls=grpcserver.TLSConfig(
                        cert_chain=cert_path,
                        private_key=key_path,
                        min_version=:TLSv1_2,
                        alpn_protocols=["h2"],
                    ),
                )
                try
                    @test tls_server.port > 0
                    @test tls_server.port == grpcserver.HTTP.port(tls_server.server)
                    @test tls_server.server.config.tls !== nothing
                    @test flight_live_pyarrow_tls_smoke(
                        tls_server.host,
                        tls_server.port,
                        cert_path,
                        fixture,
                    )
                finally
                    Arrow.Flight.stop!(tls_server; force=true)
                end
            end
        end
    end
end
