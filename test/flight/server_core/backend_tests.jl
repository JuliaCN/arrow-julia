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

function flight_server_core_test_backend_profiles()
    purehttp2 = Arrow.Flight.flight_server_backend_capabilities()
    @test purehttp2.backend == :purehttp2
    @test purehttp2.request_streaming
    @test purehttp2.response_streaming
    @test purehttp2.response_trailers
    @test purehttp2.bidirectional_doexchange
    @test isempty(purehttp2.blockers)
    @test Arrow.Flight.flight_server_backend_supported()
    @test isnothing(Arrow.Flight.require_flight_server_backend())

    grpcserver = Arrow.Flight.flight_server_backend_capabilities(:grpcserver)
    @test grpcserver.backend == :grpcserver
    @test !grpcserver.request_streaming
    @test !grpcserver.response_streaming
    @test !grpcserver.response_trailers
    @test !grpcserver.bidirectional_doexchange
    @test length(grpcserver.blockers) == 2
    @test occursin("gRPCServer.jl", grpcserver.blockers[1])
    @test occursin("PureHTTP2", grpcserver.blockers[2])
    @test !Arrow.Flight.flight_server_backend_supported(:grpcserver)

    legacy_failure = try
        Arrow.Flight.require_flight_server_backend(
            :grpcserver;
            subject="test Flight server backend",
        )
        nothing
    catch error
        error
    end
    @test legacy_failure isa ArgumentError
    legacy_message = sprint(showerror, legacy_failure)
    @test occursin("backend :grpcserver", legacy_message)
    @test occursin("gRPCServer.jl", legacy_message)
    @test occursin("PureHTTP2", legacy_message)

    nghttp2 = Arrow.Flight.flight_server_backend_capabilities(:nghttp2)
    @test nghttp2.backend == :nghttp2
    @test !nghttp2.request_streaming
    @test !nghttp2.response_streaming
    @test !nghttp2.response_trailers
    @test !nghttp2.bidirectional_doexchange
    @test length(nghttp2.blockers) >= 2
    @test occursin("Nghttp2Wrapper", nghttp2.blockers[1])
    @test occursin("PureHTTP2", nghttp2.blockers[2])
    @test !Arrow.Flight.flight_server_backend_supported(:nghttp2)
    @test_throws ArgumentError Arrow.Flight.flight_server_backend_capabilities(:unknown)

    failure = try
        Arrow.Flight.require_flight_server_backend(
            :nghttp2;
            subject="test Flight server backend",
        )
        nothing
    catch error
        error
    end
    @test failure isa ArgumentError
    message = sprint(showerror, failure)
    @test occursin("backend :nghttp2", message)
    @test occursin("Nghttp2Wrapper", message)
    @test occursin("PureHTTP2", message)
end
