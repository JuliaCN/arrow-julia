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

using DataAPI
using Tables

@testset "Flight IPC conversion helpers" begin
    descriptor = Arrow.Flight.pathdescriptor(("datasets", "roundtrip"))
    source = Tables.partitioner((
        (id=Int64[1, 2], label=["one", "two"]),
        (id=Int64[3], label=["three"]),
    ))
    messages = Arrow.Flight.flightdata(
        source;
        descriptor=descriptor,
        metadata=Dict("dataset" => "flight"),
        colmetadata=Dict(:label => Dict("lang" => "en")),
        app_metadata=("batch:0", "batch:1"),
    )

    @test length(messages) == 3
    @test messages[1].flight_descriptor == descriptor
    @test all(isnothing(message.flight_descriptor) for message in messages[2:end])
    @test isempty(messages[1].data_body)
    @test all(!isempty(message.data_header) for message in messages)

    bytes = Arrow.Flight.streambytes(messages)
    @test reinterpret(UInt32, bytes[1:4])[1] == UInt32(0xffffffff)
    @test reinterpret(Int32, bytes[(end - 3):end])[1] == 0

    batches = collect(Arrow.Flight.stream(messages; include_app_metadata=true))
    @test length(batches) == 2
    @test batches[1].table.id == [1, 2]
    @test batches[2].table.label == ["three"]
    @test String(batches[1].app_metadata) == "batch:0"
    @test String(batches[2].app_metadata) == "batch:1"

    result = Arrow.Flight.table(messages; include_app_metadata=true)
    @test result.table.id == [1, 2, 3]
    @test result.table.label == ["one", "two", "three"]
    @test String.(result.app_metadata) == ["batch:0", "batch:1"]
    @test DataAPI.metadata(result.table, "dataset") == "flight"
    @test DataAPI.colmetadata(result.table, :label, "lang") == "en"

    schema_bytes = Arrow.Flight.schemaipc(first(messages))
    schema = Arrow.Flight.Protocol.SchemaResult(schema_bytes)
    separated = Arrow.Flight.table(messages[2:end]; schema=schema)
    @test separated.id == [1, 2, 3]
    @test separated.label == ["one", "two", "three"]

    wrapped = Arrow.Flight.withappmetadata(
        source;
        app_metadata=("wrapped:0", "wrapped:1"),
    )
    wrapped_result = Arrow.Flight.table(
        Arrow.Flight.flightdata(wrapped);
        include_app_metadata=true,
    )
    @test String.(wrapped_result.app_metadata) == ["wrapped:0", "wrapped:1"]

    channel = Channel{Arrow.Flight.Protocol.FlightData}(8)
    task = @async Arrow.Flight.putflightdata!(channel, source; close=true)
    channel_messages = collect(channel)
    wait(task)
    @test Arrow.Flight.table(channel_messages).id == [1, 2, 3]

    dictionary_messages = Arrow.Flight.flightdata((
        value=Arrow.DictEncode(["alpha", "beta", "alpha"]),
    ))
    @test Arrow.Flight.table(dictionary_messages).value == ["alpha", "beta", "alpha"]

    @test_throws ArgumentError Arrow.Flight.table(Arrow.Flight.Protocol.FlightData[])
    @test_throws ArgumentError Arrow.Flight.flightdata(source; alignment=64)
    @test_throws ArgumentError Arrow.Flight.flightdata(source; app_metadata=("only-one",))
    @test_throws ArgumentError Arrow.Flight.flightdata(
        wrapped;
        app_metadata=("extra:0", "extra:1"),
    )

    damaged = copy(messages)
    damaged[2] = Arrow.Flight.Protocol.FlightData(
        nothing,
        copy(messages[2].data_header),
        copy(messages[2].app_metadata),
        messages[2].data_body[1:(end - 1)],
    )
    @test_throws ArgumentError Arrow.Flight.table(damaged)
end
