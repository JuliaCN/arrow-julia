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

const FLIGHT_LIVE_PYARROW_SMOKE = raw"""
import base64
import pyarrow as pa
import pyarrow.flight as fl
import sys

host = sys.argv[1]
port = int(sys.argv[2])
username = sys.argv[3]
password = sys.argv[4]
path = sys.argv[5:]

client = fl.FlightClient(f"grpc://{host}:{port}")
basic_auth = base64.b64encode(f"{username}:{password}".encode("utf-8"))
options = fl.FlightCallOptions(
    timeout=30,
    headers=[(b"authorization", b"Basic " + basic_auth)],
)
descriptor = fl.FlightDescriptor.for_path(*path)

flights = list(client.list_flights(options=options))
assert len(flights) == 1
assert flights[0].total_records == 3
assert [part.decode("utf-8") for part in flights[0].descriptor.path] == path

actions = list(client.list_actions(options=options))
assert len(actions) == 1
assert actions[0].type == "ping"

results = list(client.do_action(fl.Action("ping", b""), options=options))
assert len(results) == 1
assert results[0].body.to_pybytes() == b"pong"

info = client.get_flight_info(descriptor, options=options)
assert info.total_records == 3
assert [part.decode("utf-8") for part in info.descriptor.path] == path
assert len(info.endpoints) == 1

schema = client.get_schema(descriptor, options=options).schema
assert schema.names == ["id", "name"]
assert str(schema.field("id").type) == "int64"
assert str(schema.field("name").type) == "string"

reader = client.do_get(info.endpoints[0].ticket, options=options)
table = reader.read_all()
assert table.column("id").to_pylist() == [1, 2, 3]
assert table.column("name").to_pylist() == ["one", "two", "three"]
assert table.schema.metadata[b"dataset"] == b"native"
assert table.schema.field("name").metadata[b"lang"] == b"en"

put_schema = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("name", pa.string(), metadata={b"lang": b"en"}),
    ],
    metadata={b"dataset": b"native"},
)
put_writer, put_reader = client.do_put(descriptor, put_schema, options=options)
put_writer.write_with_metadata(
    pa.record_batch(
        [pa.array([1, 2], type=pa.int64()), pa.array(["one", "two"])],
        schema=put_schema,
    ),
    b"put:0",
)
put_writer.write_with_metadata(
    pa.record_batch(
        [pa.array([3], type=pa.int64()), pa.array(["three"])],
        schema=put_schema,
    ),
    b"put:1",
)
put_writer.done_writing()
put_result = put_reader.read()
put_writer.close()
assert put_result.to_pybytes() == b"stored"

exchange_schema = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("name", pa.string(), metadata={b"lang": b"exchange"}),
    ],
    metadata={b"dataset": b"exchange"},
)
exchange_writer, exchange_reader = client.do_exchange(descriptor, options=options)
exchange_writer.begin(exchange_schema)
exchange_writer.write_with_metadata(
    pa.record_batch(
        [pa.array([10], type=pa.int64()), pa.array(["ten"])],
        schema=exchange_schema,
    ),
    b"exchange:0",
)
exchange_writer.done_writing()
exchange_chunk = exchange_reader.read_chunk()
exchange_writer.close()
assert exchange_chunk.data.column(0).to_pylist() == [10]
assert exchange_chunk.data.column(1).to_pylist() == ["ten"]
assert exchange_chunk.data.schema.metadata[b"dataset"] == b"exchange"
assert exchange_chunk.data.schema.field("name").metadata[b"lang"] == b"exchange"
assert exchange_chunk.app_metadata.to_pybytes() == b"exchange:0"
"""

function flight_live_fixture(protocol)
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

function flight_live_assert_authenticated(ctx, fixture)
    auth_header = Arrow.Flight.callheader(ctx, "authorization")
    token_header = Arrow.Flight.callheader(ctx, "auth-token-bin")
    expected_token = String(copy(fixture.handshake_token))

    if auth_header == "Bearer $(expected_token)" ||
       (auth_header isa String && startswith(auth_header, "Basic "))
        @test isnothing(token_header)
        return
    end

    if token_header isa AbstractVector{UInt8}
        @test collect(token_header) == collect(fixture.handshake_token)
        return
    end

    if token_header isa String
        @test token_header == expected_token
        return
    end

    @test false
end

function flight_live_service(protocol, fixture)
    return Arrow.Flight.Service(
        handshake=(ctx, request, response) -> begin
            auth_header = Arrow.Flight.callheader(ctx, "authorization")
            @test isnothing(auth_header) ||
                  auth_header == "Bearer native" ||
                  startswith(auth_header, "Basic ")
            @test isnothing(Arrow.Flight.callheader(ctx, "auth-token-bin"))
            incoming = collect(request)
            token = if isempty(incoming)
                @test !isnothing(auth_header)
                @test startswith(auth_header, "Basic ")
                fixture.handshake_token
            elseif length(incoming) == 1
                @test incoming[1].payload == fixture.handshake_token
                incoming[1].payload
            else
                @test length(incoming) == 2
                @test String(copy(incoming[1].payload)) == fixture.handshake_username
                @test String(copy(incoming[2].payload)) == fixture.handshake_password
                fixture.handshake_token
            end
            put!(response, protocol.HandshakeResponse(UInt64(0), token))
            close(response)
            return :handshake_ok
        end,
        getflightinfo=(ctx, req) -> begin
            flight_live_assert_authenticated(ctx, fixture)
            @test req.path == fixture.descriptor.path
            return fixture.info
        end,
        pollflightinfo=(ctx, req) -> begin
            flight_live_assert_authenticated(ctx, fixture)
            if req.path == fixture.poll_descriptor.path
                return fixture.initial_poll
            end
            @test req.path == fixture.poll_retry_descriptor.path
            return fixture.final_poll
        end,
        listflights=(ctx, criteria, response) -> begin
            flight_live_assert_authenticated(ctx, fixture)
            @test criteria.expression == UInt8[]
            put!(response, fixture.info)
            close(response)
            return :listflights_ok
        end,
        getschema=(ctx, req) -> begin
            flight_live_assert_authenticated(ctx, fixture)
            @test req.path == fixture.descriptor.path
            return protocol.SchemaResult(fixture.schema_bytes[5:end])
        end,
        doget=(ctx, req, response) -> begin
            flight_live_assert_authenticated(ctx, fixture)
            @test req.ticket == fixture.ticket.ticket
            Arrow.Flight.putflightdata!(
                response,
                Tables.partitioner((
                    (id=Int64[1, 2], name=["one", "two"]),
                    (id=Int64[3], name=["three"]),
                ));
                descriptor=fixture.descriptor,
                metadata=fixture.dataset_metadata,
                colmetadata=fixture.dataset_colmetadata,
                close=true,
            )
            return :doget_ok
        end,
        listactions=(ctx, response) -> begin
            flight_live_assert_authenticated(ctx, fixture)
            put!(response, protocol.ActionType("ping", "Ping action"))
            close(response)
            return :listactions_ok
        end,
        doaction=(ctx, action, response) -> begin
            flight_live_assert_authenticated(ctx, fixture)
            @test action.var"#type" == "ping"
            put!(response, protocol.Result(b"pong"))
            close(response)
            return :doaction_ok
        end,
        doput=(ctx, request, response) -> begin
            flight_live_assert_authenticated(ctx, fixture)
            incoming = collect(Arrow.Flight.stream(request; include_app_metadata=true))
            @test length(incoming) == 2
            @test incoming[1].table.id == [1, 2]
            @test incoming[1].table.name == ["one", "two"]
            @test Arrow.getmetadata(incoming[1].table)["dataset"] == "native"
            @test Arrow.getmetadata(incoming[1].table.name)["lang"] == "en"
            @test FlightTestSupport.app_metadata_strings(
                getproperty.(incoming, :app_metadata),
            ) == fixture.dataset_app_metadata
            @test incoming[2].table.id == [3]
            @test incoming[2].table.name == ["three"]
            put!(response, protocol.PutResult(b"stored"))
            close(response)
            return :doput_ok
        end,
        doexchange=(ctx, request, response) -> begin
            flight_live_assert_authenticated(ctx, fixture)
            incoming = collect(Arrow.Flight.stream(request; include_app_metadata=true))
            @test length(incoming) == 1
            @test FlightTestSupport.app_metadata_strings(
                getproperty.(incoming, :app_metadata),
            ) == fixture.exchange_app_metadata
            @test Arrow.getmetadata(incoming[1].table)["dataset"] == "exchange"
            @test Arrow.getmetadata(incoming[1].table.name)["lang"] == "exchange"
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

function flight_live_pyarrow_smoke(host::AbstractString, port::Integer, fixture)
    python = FlightTestSupport.pyarrow_flight_python()
    isnothing(python) && return false
    run(
        Cmd([
            python,
            "-c",
            FLIGHT_LIVE_PYARROW_SMOKE,
            host,
            string(port),
            fixture.handshake_username,
            fixture.handshake_password,
            fixture.descriptor.path...,
        ]),
    )
    return true
end

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

function _flight_live_transport_doget(client, ticket)
    request, response = Arrow.Flight.doget(client, ticket)
    messages = collect(response)
    gRPCClient.grpc_async_await(request)
    return messages
end

function _flight_live_transport_doput(client, fixture)
    request, response = Arrow.Flight.doput(
        client,
        _flight_live_transport_source(fixture);
        descriptor=fixture.descriptor,
        metadata=fixture.dataset_metadata,
        colmetadata=fixture.dataset_colmetadata,
    )
    results = collect(response)
    gRPCClient.grpc_async_await(request)
    return results
end

function _flight_live_transport_doexchange(client, fixture)
    request, response = Arrow.Flight.doexchange(
        client,
        _flight_live_transport_source(fixture);
        descriptor=fixture.descriptor,
        metadata=fixture.dataset_metadata,
        colmetadata=fixture.dataset_colmetadata,
    )
    messages = collect(response)
    gRPCClient.grpc_async_await(request)
    return messages
end

function _flight_live_transport_measure(
    backend::Symbol,
    operation::Symbol;
    request_bytes::Integer,
    response_bytes::Integer,
    iterations::Integer,
    run_once,
)
    iterations > 0 || throw(ArgumentError("iterations must be positive"))
    samples_ns = Int[]
    for _ = 1:iterations
        GC.gc()
        started = time_ns()
        run_once()
        push!(samples_ns, Int(time_ns() - started))
    end
    sorted_samples = sort(copy(samples_ns))
    median_ns = sorted_samples[cld(length(sorted_samples), 2)]
    total_bytes = Int(request_bytes) + Int(response_bytes)
    return (
        backend=backend,
        operation=operation,
        iterations=iterations,
        request_bytes=Int(request_bytes),
        response_bytes=Int(response_bytes),
        total_bytes=total_bytes,
        samples_ns=samples_ns,
        median_ns=median_ns,
        median_ms=median_ns / 1.0e6,
        throughput_mib_per_sec=(total_bytes / max(median_ns, 1) * 1.0e9) / 1024.0^2,
    )
end

function flight_live_transport_benchmark(
    protocol,
    transport;
    iterations::Integer=3,
    batch_count::Integer=2,
    rows_per_batch::Integer=256,
    payload_bytes::Integer=4_096,
    operations::Tuple{Vararg{Symbol}}=(:doget, :doput, :doexchange),
)
    fixture = flight_live_transport_fixture(
        protocol;
        batch_count=batch_count,
        rows_per_batch=rows_per_batch,
        payload_bytes=payload_bytes,
    )
    service = flight_live_transport_service(protocol, fixture)
    server = transport.start_server(service)
    try
        transport.wait_for_server(server)
        host, port = transport.endpoint(server)
        return FlightTestSupport.with_test_grpc_handle() do grpc
            client = Arrow.Flight.Client("grpc://$(host):$(port)"; grpc=grpc, deadline=60)
            info = Arrow.Flight.getflightinfo(client, fixture.descriptor)
            @test info.total_records == fixture.total_records

            metrics = NamedTuple[]

            if :doget in operations
                doget_messages =
                    _flight_live_transport_doget(client, info.endpoint[1].ticket)
                @test length(doget_messages) == length(fixture.messages)
                doget_table = Arrow.Flight.table(doget_messages; schema=info)
                @test length(doget_table.id) == fixture.total_records
                @test first(doget_table.payload) == fixture.payload_value
                push!(
                    metrics,
                    _flight_live_transport_measure(
                        transport.backend,
                        :doget;
                        request_bytes=0,
                        response_bytes=fixture.message_bytes,
                        iterations=iterations,
                        run_once=() ->
                            _flight_live_transport_doget(client, info.endpoint[1].ticket),
                    ),
                )
            end

            if :doput in operations
                doput_results = _flight_live_transport_doput(client, fixture)
                @test length(doput_results) == 1
                @test String(copy(doput_results[1].app_metadata)) == "stored"
                push!(
                    metrics,
                    _flight_live_transport_measure(
                        transport.backend,
                        :doput;
                        request_bytes=fixture.message_bytes,
                        response_bytes=fixture.put_result_bytes,
                        iterations=iterations,
                        run_once=() -> _flight_live_transport_doput(client, fixture),
                    ),
                )
            end

            if :doexchange in operations
                doexchange_messages = _flight_live_transport_doexchange(client, fixture)
                @test length(doexchange_messages) == length(fixture.messages)
                doexchange_table =
                    Arrow.Flight.table(doexchange_messages; include_app_metadata=true)
                @test length(doexchange_table.table.id) == fixture.total_records
                @test FlightTestSupport.app_metadata_strings(
                    doexchange_table.app_metadata,
                ) == fixture.dataset_app_metadata
                push!(
                    metrics,
                    _flight_live_transport_measure(
                        transport.backend,
                        :doexchange;
                        request_bytes=fixture.message_bytes,
                        response_bytes=fixture.message_bytes,
                        iterations=iterations,
                        run_once=() -> _flight_live_transport_doexchange(client, fixture),
                    ),
                )
            end

            return metrics
        end
    finally
        transport.stop_server(server)
    end
end

function flight_live_transport_print_metrics(io::IO, metrics)
    for metric in metrics
        samples_ms = [round(sample / 1.0e6; digits=2) for sample in metric.samples_ns]
        println(
            io,
            "$(metric.backend) $(metric.operation): total_bytes=$(metric.total_bytes) " *
            "median_ms=$(round(metric.median_ms; digits=2)) " *
            "throughput_mib_per_sec=$(round(metric.throughput_mib_per_sec; digits=2)) " *
            "samples_ms=$(samples_ms)",
        )
    end
    return nothing
end

function flight_live_transport_metric(
    metrics,
    backend::Symbol,
    operation::Symbol,
)
    for metric in metrics
        metric.backend == backend && metric.operation == operation && return metric
    end
    error("Missing Flight live transport metric for backend $(backend) operation $(operation)")
end

function flight_live_transport_print_comparison(
    io::IO,
    candidate,
    baseline;
    digits::Integer=2,
)
    candidate.operation == baseline.operation || throw(
        ArgumentError(
            "Cannot compare different operations: $(candidate.operation) vs $(baseline.operation)",
        ),
    )
    throughput_ratio =
        baseline.throughput_mib_per_sec == 0 ? Inf :
        candidate.throughput_mib_per_sec / baseline.throughput_mib_per_sec
    latency_ratio = baseline.median_ns == 0 ? Inf : candidate.median_ns / baseline.median_ns
    println(
        io,
        "compare $(candidate.backend) vs $(baseline.backend) $(candidate.operation): " *
        "median_ms=$(round(candidate.median_ms; digits=digits))/" *
        "$(round(baseline.median_ms; digits=digits)) " *
        "throughput_mib_per_sec=$(round(candidate.throughput_mib_per_sec; digits=digits))/" *
        "$(round(baseline.throughput_mib_per_sec; digits=digits)) " *
        "latency_ratio=$(round(latency_ratio; digits=digits)) " *
        "throughput_ratio=$(round(throughput_ratio; digits=digits))",
    )
    return (
        operation=candidate.operation,
        candidate_backend=candidate.backend,
        baseline_backend=baseline.backend,
        candidate_median_ms=candidate.median_ms,
        baseline_median_ms=baseline.median_ms,
        candidate_throughput_mib_per_sec=candidate.throughput_mib_per_sec,
        baseline_throughput_mib_per_sec=baseline.throughput_mib_per_sec,
        latency_ratio=latency_ratio,
        throughput_ratio=throughput_ratio,
    )
end
