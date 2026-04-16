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

function nghttp2_extension_test_backend_profiles()
    @test !isnothing(Base.get_extension(Arrow, :ArrowFlightNghttp2Ext))

    capabilities = Arrow.Flight.flight_server_backend_capabilities(:nghttp2)
    @test capabilities.backend == :nghttp2
    @test !capabilities.request_streaming
    @test capabilities.response_streaming
    @test capabilities.response_trailers
    @test !capabilities.bidirectional_doexchange
    @test length(capabilities.blockers) == 3
    @test occursin("Handshake", capabilities.blockers[1])
    @test occursin("PureHTTP2", capabilities.blockers[2])
    @test occursin("bidirectional", lowercase(capabilities.blockers[3]))
    @test !Arrow.Flight.flight_server_backend_supported(:nghttp2)

    failure = try
        Arrow.Flight.require_flight_server_backend(
            :nghttp2;
            subject="nghttp2 extension test backend",
        )
        nothing
    catch error
        error
    end
    @test failure isa ArgumentError
    message = sprint(showerror, failure)
    @test occursin("Handshake", message)
    @test occursin("DoExchange", message)
    @test occursin("PureHTTP2", message)
end
