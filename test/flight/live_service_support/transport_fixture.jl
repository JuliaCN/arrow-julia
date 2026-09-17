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

function _flight_live_transport_message_bytes(messages)
    total = 0
    for message in messages
        total += length(Arrow.Flight.grpcmessage(message))
    end
    return total
end

function flight_live_transport_fixture(
    protocol;
    batch_count::Integer=2,
    rows_per_batch::Integer=256,
    payload_bytes::Integer=4_096,
)
    descriptor_type = protocol.var"FlightDescriptor.DescriptorType"
    descriptor =
        protocol.FlightDescriptor(descriptor_type.PATH, UInt8[], ["perf", "dataset"])
    ticket = protocol.Ticket(b"perf-ticket")
    endpoint = protocol.FlightEndpoint(ticket, protocol.Location[], nothing, UInt8[])
    payload_value = repeat("x", payload_bytes)
    batches = ntuple(batch_count) do index
        first_id = Int64(((index - 1) * rows_per_batch) + 1)
        last_id = Int64(index * rows_per_batch)
        return (id=collect(first_id:last_id), payload=fill(payload_value, rows_per_batch))
    end
    dataset_metadata = Dict("dataset" => "perf")
    dataset_colmetadata = Dict(:payload => Dict("kind" => "large-transport"))
    dataset_app_metadata = ["perf:$(index)" for index = 1:batch_count]
    messages = Arrow.Flight.flightdata(
        Tables.partitioner(batches);
        descriptor=descriptor,
        metadata=dataset_metadata,
        colmetadata=dataset_colmetadata,
        app_metadata=dataset_app_metadata,
    )
    schema_bytes = Arrow.Flight.schemaipc(first(messages))
    info = protocol.FlightInfo(
        schema_bytes[5:end],
        descriptor,
        [endpoint],
        Int64(batch_count * rows_per_batch),
        Int64(-1),
        false,
        UInt8[],
    )
    put_result = protocol.PutResult(b"stored")
    return (
        descriptor=descriptor,
        ticket=ticket,
        batches=batches,
        payload_value=payload_value,
        total_records=batch_count * rows_per_batch,
        dataset_metadata=dataset_metadata,
        dataset_colmetadata=dataset_colmetadata,
        dataset_app_metadata=dataset_app_metadata,
        messages=messages,
        message_bytes=_flight_live_transport_message_bytes(messages),
        put_result=put_result,
        put_result_bytes=length(Arrow.Flight.grpcmessage(put_result)),
        info=info,
        schema_bytes=schema_bytes,
    )
end

function _flight_live_transport_source(fixture)
    return Arrow.Flight.withappmetadata(
        Tables.partitioner(fixture.batches);
        app_metadata=fixture.dataset_app_metadata,
    )
end

function flight_live_transport_service(protocol, fixture)
    return Arrow.Flight.Service(
        getflightinfo=(ctx, req) -> begin
            @test req.path == fixture.descriptor.path
            return fixture.info
        end,
        doget=(ctx, req, response) -> begin
            @test req.ticket == fixture.ticket.ticket
            Arrow.Flight.putflightdata!(
                response,
                Tables.partitioner(fixture.batches);
                descriptor=fixture.descriptor,
                metadata=fixture.dataset_metadata,
                colmetadata=fixture.dataset_colmetadata,
                close=true,
            )
            return :doget_ok
        end,
        doput=(ctx, request, response) -> begin
            incoming = collect(Arrow.Flight.stream(request; include_app_metadata=true))
            @test length(incoming) == length(fixture.batches)
            @test sum(length(batch.table.id) for batch in incoming) ==
                  fixture.total_records
            @test FlightTestSupport.app_metadata_strings(
                getproperty.(incoming, :app_metadata),
            ) == fixture.dataset_app_metadata
            put!(response, fixture.put_result)
            close(response)
            return :doput_ok
        end,
        doexchange=(ctx, request, response) -> begin
            incoming = collect(Arrow.Flight.stream(request; include_app_metadata=true))
            @test length(incoming) == length(fixture.batches)
            @test sum(length(batch.table.id) for batch in incoming) ==
                  fixture.total_records
            @test FlightTestSupport.app_metadata_strings(
                getproperty.(incoming, :app_metadata),
            ) == fixture.dataset_app_metadata
            Arrow.Flight.putflightdata!(
                response,
                Arrow.Flight.withappmetadata(
                    Tables.partitioner(getproperty.(incoming, :table));
                    app_metadata=getproperty.(incoming, :app_metadata),
                );
                close=true,
            )
            return :doexchange_ok
        end,
    )
end

flight_live_transport_backend(;
    backend::Symbol,
    start_server,
    wait_for_server,
    stop_server,
    endpoint,
) = (
    backend=backend,
    start_server=start_server,
    wait_for_server=wait_for_server,
    stop_server=stop_server,
    endpoint=endpoint,
)
