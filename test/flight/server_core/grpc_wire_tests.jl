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

function flight_server_core_test_grpc_wire(fixture)
    protocol = fixture.protocol

    @test Arrow.Flight.grpccontenttype("application/grpc")
    @test Arrow.Flight.grpccontenttype("application/grpc+proto")
    @test Arrow.Flight.grpccontenttype("application/grpc; charset=utf-8")
    @test !Arrow.Flight.grpccontenttype("application/json")
    @test !Arrow.Flight.grpccontenttype(nothing)

    ticket = protocol.Ticket(b"ticket-1")
    framed = Arrow.Flight.grpcmessage(ticket)
    @test Arrow.Flight.decodegrpcmessage(protocol.Ticket, framed).ticket == ticket.ticket

    multi_payload = vcat(framed, Arrow.Flight.grpcmessage(protocol.Ticket(b"ticket-2")))
    decoded = Arrow.Flight.decodegrpcmessages(protocol.Ticket, multi_payload)
    @test [String(message.ticket) for message in decoded] == ["ticket-1", "ticket-2"]

    raw_payload = UInt8[0x10, 0x20]
    @test Arrow.Flight.decodegrpcmessage(
        Vector{UInt8},
        Arrow.Flight.grpcmessage(raw_payload),
    ) == raw_payload

    @test_throws ArgumentError Arrow.Flight.decodegrpcmessages(
        protocol.Ticket,
        framed[1:(end - 1)],
    )

    @test Arrow.Flight.grpcresponseheaders() ==
          [(":status", "200"), ("content-type", "application/grpc+proto")]
    @test Arrow.Flight.grpcresponsetrailers(0; message="ok") ==
          [("grpc-status", "0"), ("grpc-message", "ok")]

    status_error = Arrow.Flight.FlightStatusError(Int32(16), "bad token")
    @test Arrow.Flight.grpcstatus(status_error) == (Int32(16), "bad token")

    generic_code, generic_message = Arrow.Flight.grpcstatus(ArgumentError("boom"))
    @test generic_code == Int32(13)
    @test occursin("boom", generic_message)
end
