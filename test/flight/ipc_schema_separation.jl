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

@testset "Flight IPC schema separation" begin
    source = Tables.partitioner(((word=["red", "blue"],), (word=["green"],)))
    messages = Arrow.Flight.flightdata(source)
    schema_bytes = Arrow.Flight.schemaipc(first(messages))
    info = Arrow.Flight.Protocol.FlightInfo(
        schema_bytes,
        nothing,
        Arrow.Flight.Protocol.FlightEndpoint[],
        Int64(-1),
        Int64(-1),
        false,
        UInt8[],
    )

    @test Arrow.Flight.schemaipc(info) == schema_bytes
    @test [batch.word for batch in Arrow.Flight.stream(messages[2:end]; schema=info)] ==
          [["red", "blue"], ["green"]]
    @test Arrow.Flight.table(messages[2:end]; schema=info).word ==
          ["red", "blue", "green"]
end
