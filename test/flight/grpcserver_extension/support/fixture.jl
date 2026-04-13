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

function grpcserver_extension_fixture(protocol)
    descriptor_type = protocol.var"FlightDescriptor.DescriptorType"
    handshake_token = b"native"
    handshake_username = "native"
    handshake_password = "token"
    descriptor =
        protocol.FlightDescriptor(descriptor_type.PATH, UInt8[], ["native", "dataset"])
    poll_descriptor =
        protocol.FlightDescriptor(descriptor_type.PATH, UInt8[], ["native", "poll"])
    poll_retry_descriptor = protocol.FlightDescriptor(
        descriptor_type.PATH,
        UInt8[],
        ["native", "poll", "retry"],
    )
    ticket = protocol.Ticket(b"native-ticket")
    poll_ticket = protocol.Ticket(b"native-poll-ticket")
    dataset_metadata = Dict("dataset" => "native")
    dataset_colmetadata = Dict(:name => Dict("lang" => "en"))
    dataset_app_metadata = ["put:0", "put:1"]
    messages = Arrow.Flight.flightdata(
        Tables.partitioner((
            (id=Int64[1, 2], name=["one", "two"]),
            (id=Int64[3], name=["three"]),
        ));
        descriptor=descriptor,
        metadata=dataset_metadata,
        colmetadata=dataset_colmetadata,
        app_metadata=dataset_app_metadata,
    )
    schema_bytes = Arrow.Flight.schemaipc(first(messages))
    info = protocol.FlightInfo(
        schema_bytes[5:end],
        descriptor,
        [protocol.FlightEndpoint(ticket, protocol.Location[], nothing, UInt8[])],
        Int64(3),
        Int64(-1),
        false,
        UInt8[],
    )
    poll_info = protocol.FlightInfo(
        schema_bytes[5:end],
        poll_descriptor,
        [protocol.FlightEndpoint(poll_ticket, protocol.Location[], nothing, UInt8[])],
        Int64(3),
        Int64(-1),
        false,
        UInt8[],
    )
    initial_poll = protocol.PollInfo(poll_info, poll_retry_descriptor, 0.5, nothing)
    final_poll = protocol.PollInfo(poll_info, nothing, 1.0, nothing)
    handshake_requests = [protocol.HandshakeRequest(UInt64(0), handshake_token)]
    exchange_metadata = Dict("dataset" => "exchange")
    exchange_colmetadata = Dict(:name => Dict("lang" => "exchange"))
    exchange_app_metadata = ["exchange:0"]
    exchange_messages = Arrow.Flight.flightdata(
        Tables.partitioner(((id=Int64[10], name=["ten"]),));
        descriptor=descriptor,
        metadata=exchange_metadata,
        colmetadata=exchange_colmetadata,
        app_metadata=exchange_app_metadata,
    )
    return (
        handshake_token=handshake_token,
        handshake_username=handshake_username,
        handshake_password=handshake_password,
        descriptor=descriptor,
        poll_descriptor=poll_descriptor,
        poll_retry_descriptor=poll_retry_descriptor,
        ticket=ticket,
        poll_ticket=poll_ticket,
        messages=messages,
        schema_bytes=schema_bytes,
        info=info,
        poll_info=poll_info,
        initial_poll=initial_poll,
        final_poll=final_poll,
        handshake_requests=handshake_requests,
        dataset_metadata=dataset_metadata,
        dataset_colmetadata=dataset_colmetadata,
        dataset_app_metadata=dataset_app_metadata,
        exchange_messages=exchange_messages,
        exchange_metadata=exchange_metadata,
        exchange_colmetadata=exchange_colmetadata,
        exchange_app_metadata=exchange_app_metadata,
    )
end
