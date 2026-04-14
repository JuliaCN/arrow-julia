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

function flight_client_surface_test_producer_shutdown(_fixture)
    descriptor = Arrow.Flight.pathdescriptor(("client", "producer", "shutdown"))
    sink = Channel{Arrow.Flight.Protocol.FlightData}(1)
    close(sink)
    flight_ext = Base.get_extension(Arrow, :ArrowFlightgRPCClientExt)
    @test !isnothing(flight_ext)

    @test flight_ext._putflightdata_or_stop!(
        sink,
        (id=[1], value=["one"]);
        close=true,
        descriptor=descriptor,
    ) === sink
end
