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

function flight_test_generated_protocol_formatter_surface()
    generated_path = joinpath(
        dirname(pathof(Arrow)),
        "flight",
        "generated",
        "arrow",
        "flight",
        "protocol",
        "Flight_pb.jl",
    )
    source = read(generated_path, String)

    @test !occursin("::Type{<:var\"", source)
    @test occursin("where {T<:var\"SessionOptionValue.StringListValue\"}", source)
    @test occursin("where {T<:var\"SetSessionOptionsResult.Error\"}", source)
    @test !occursin("import gRPCClient", source)
    @test !occursin("# gRPCClient.jl BEGIN", source)

    protocol = Arrow.Flight.Protocol
    descriptor_type = protocol.var"FlightDescriptor.DescriptorType"
    descriptor =
        protocol.FlightDescriptor(descriptor_type.PATH, UInt8[], ["cancel", "query"])
    info = protocol.FlightInfo(
        UInt8[0x01],
        descriptor,
        protocol.FlightEndpoint[],
        3,
        9,
        false,
        UInt8[],
    )

    action_type = Arrow.Flight.cancelflightinfoactiontype()
    @test getfield(action_type, Symbol("#type")) == "CancelFlightInfo"
    @test occursin("Cancel", action_type.description)

    action = Arrow.Flight.cancelflightinfoaction(info)
    @test getfield(action, Symbol("#type")) == "CancelFlightInfo"
    @test !isempty(action.body)

    request = Arrow.Flight.cancelflightinforequest(action)
    @test request.info.total_records == 3
    @test request.info.total_bytes == 9
    @test request.info.flight_descriptor.path == ["cancel", "query"]

    wrapped_request = protocol.CancelFlightInfoRequest(info)
    wrapped_action = Arrow.Flight.cancelflightinfoaction(wrapped_request)
    @test Arrow.Flight.cancelflightinforequest(wrapped_action).info.total_records == 3

    @test_throws ArgumentError Arrow.Flight.cancelflightinforequest(
        protocol.Action("OtherAction", action.body),
    )

    result =
        Arrow.Flight.cancelflightinforesult(protocol.CancelStatus.CANCEL_STATUS_CANCELLED)
    decoded_result = Arrow.Flight.cancelflightinforesult(result)
    @test decoded_result.status == protocol.CancelStatus.CANCEL_STATUS_CANCELLED
    @test Arrow.Flight.cancelflightinfostatus(result) ==
          protocol.CancelStatus.CANCEL_STATUS_CANCELLED
end
