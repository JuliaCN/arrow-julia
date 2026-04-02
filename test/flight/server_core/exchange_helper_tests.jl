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

function _flight_server_core_request(messages)
    request = Channel{eltype(messages)}(max(length(messages), 1))
    for message in messages
        put!(request, message)
    end
    close(request)
    return request
end

function flight_server_core_test_exchange_helpers(fixture)
    descriptor = Arrow.Flight.pathdescriptor(("server", "exchange"))
    request_messages = Arrow.Flight.flightdata(fixture.sample_table; descriptor=descriptor)
    service = Arrow.Flight.exchangeservice(
        function (messages, request_descriptor, context)
            @test context === fixture.context
            @test request_descriptor.path == ["server", "exchange"]
            table = Arrow.Flight.table(messages; convert=true)
            columns = Tables.columntable(table)
            @test Arrow.getmetadata(table) == Dict("request" => "sample")
            @test Arrow.getmetadata(columns.doc_id) ==
                  Dict("semantic.role" => "document-id")
            @test Arrow.getmetadata(columns.vector_score) ==
                  Dict("semantic.role" => "score")
            return Arrow.withmetadata(
                (
                    doc_id=collect(columns.doc_id),
                    vector_score=collect(columns.vector_score),
                );
                colmetadata=Dict(
                    :doc_id => Dict("response.role" => "document-id"),
                    :vector_score => Dict("response.role" => "score"),
                ),
            )
        end;
        metadata=["handler" => "exchange"],
        app_metadata=("response:exchange",),
    )
    response = Channel{fixture.protocol.FlightData}(length(request_messages) + 1)
    @test Arrow.Flight.doexchange(
        service,
        fixture.context,
        _flight_server_core_request(request_messages),
        response,
    ) === nothing
    response_messages = collect(response)
    result = Arrow.Flight.table(response_messages; convert=true)
    columns = Tables.columntable(result)
    @test collect(columns.doc_id) == ["doc-a", "doc-b"]
    @test collect(columns.vector_score) == [0.9, 0.5]
    @test Arrow.getmetadata(result) == Dict("handler" => "exchange")
    @test Arrow.getmetadata(columns.doc_id) == Dict("response.role" => "document-id")
    @test Arrow.getmetadata(columns.vector_score) == Dict("response.role" => "score")
    @test filter(!isempty, getfield.(response_messages, :app_metadata)) ==
          [b"response:exchange"]

    fallback_service = Arrow.Flight.exchangeservice(
        (messages, request_descriptor, _) -> begin
            @test request_descriptor.path == ["fallback", "route"]
            return (doc_id=["doc-a"], vector_score=[0.9])
        end;
        descriptor=Arrow.Flight.pathdescriptor(("fallback", "route")),
    )
    fallback_response = Channel{fixture.protocol.FlightData}(2)
    fallback_request =
        _flight_server_core_request(Arrow.Flight.flightdata(fixture.sample_table))
    @test Arrow.Flight.doexchange(
        fallback_service,
        fixture.context,
        fallback_request,
        fallback_response,
    ) === nothing
    @test collect(Arrow.Flight.table(collect(fallback_response)).doc_id) == ["doc-a"]

    table_service = Arrow.Flight.tableservice(
        table -> begin
            columns = Tables.columntable(table)
            @test Arrow.getmetadata(table) == Dict("request" => "sample")
            @test Arrow.getmetadata(columns.doc_id) ==
                  Dict("semantic.role" => "document-id")
            return Arrow.withmetadata(
                (
                    doc_id=reverse(collect(columns.doc_id)),
                    vector_score=reverse(collect(columns.vector_score)),
                );
                colmetadata=Dict(
                    :doc_id => Dict("response.role" => "table-doc-id"),
                    :vector_score => Dict("response.role" => "table-score"),
                ),
            )
        end;
        metadata=["service" => "table"],
        app_metadata=("response:table",),
    )
    table_response = Channel{fixture.protocol.FlightData}(length(request_messages) + 1)
    Arrow.Flight.doexchange(
        table_service,
        fixture.context,
        _flight_server_core_request(request_messages),
        table_response,
    )
    table_messages = collect(table_response)
    table_result = Arrow.Flight.table(table_messages; convert=true)
    @test collect(table_result.doc_id) == ["doc-b", "doc-a"]
    @test Arrow.getmetadata(table_result) == Dict("service" => "table")
    @test Arrow.getmetadata(table_result.doc_id) == Dict("response.role" => "table-doc-id")
    @test Arrow.getmetadata(table_result.vector_score) ==
          Dict("response.role" => "table-score")
    @test filter(!isempty, getfield.(table_messages, :app_metadata)) == [b"response:table"]
    table_result_with_app = Arrow.Flight.table(
        Arrow.Flight.doexchange(
            table_service,
            fixture.context,
            fixture.sample_table;
            descriptor=descriptor,
        );
        include_app_metadata=true,
    )
    @test String.(table_result_with_app.app_metadata) == ["response:table"]

    request_app_table_service = Arrow.Flight.tableservice(
        request -> begin
            @test collect(request.table.doc_id) == ["doc-a", "doc-b"]
            @test String.(request.app_metadata) == ["request:table"]
            return request.table
        end;
        include_request_app_metadata=true,
        app_metadata=("response:table-request-app",),
    )
    request_app_table = Arrow.Flight.table(
        request_app_table_service,
        fixture.context,
        Arrow.Flight.withappmetadata(fixture.sample_table; app_metadata=("request:table",));
        descriptor=descriptor,
        include_app_metadata=true,
    )
    @test collect(request_app_table.table.doc_id) == ["doc-a", "doc-b"]
    @test String.(request_app_table.app_metadata) == ["response:table-request-app"]

    batch_a = Arrow.withmetadata(
        (doc_id=["doc-a"], vector_score=[0.9]);
        metadata=Dict("request" => "stream-a"),
        colmetadata=Dict(
            :doc_id => Dict("semantic.role" => "document-id"),
            :vector_score => Dict("semantic.role" => "score"),
        ),
    )
    batch_b = Arrow.withmetadata(
        (doc_id=["doc-b"], vector_score=[0.5]);
        metadata=Dict("request" => "stream-b"),
        colmetadata=Dict(
            :doc_id => Dict("semantic.role" => "document-id"),
            :vector_score => Dict("semantic.role" => "score"),
        ),
    )
    stream_service = Arrow.Flight.streamservice(
        stream -> begin
            tables = collect(stream)
            @test length(tables) == 2
            @test Arrow.getmetadata(tables[1]) == Dict("request" => "stream-a")
            @test Arrow.getmetadata(tables[2]) == Dict("request" => "stream-a")
            @test Arrow.getmetadata(tables[1].doc_id) ==
                  Dict("semantic.role" => "document-id")
            @test Arrow.getmetadata(tables[2].doc_id) ==
                  Dict("semantic.role" => "document-id")
            columns = Tables.columntable(vcat(Tables.rowtable.(tables)...))
            return Arrow.withmetadata(
                (
                    doc_id=collect(columns.doc_id),
                    vector_score=collect(columns.vector_score),
                );
                metadata=Dict("service" => "stream"),
                colmetadata=Dict(
                    :doc_id => Dict("response.role" => "stream-doc-id"),
                    :vector_score => Dict("response.role" => "stream-score"),
                ),
            )
        end,
        app_metadata=("response:stream",),
    )
    stream_request = _flight_server_core_request(
        Arrow.Flight.flightdata(
            Tables.partitioner((batch_a, batch_b));
            descriptor=descriptor,
        ),
    )
    stream_response = Channel{fixture.protocol.FlightData}(3)
    Arrow.Flight.doexchange(
        stream_service,
        fixture.context,
        stream_request,
        stream_response,
    )
    stream_messages = collect(stream_response)
    stream_result = Arrow.Flight.table(stream_messages; convert=true)
    @test collect(stream_result.doc_id) == ["doc-a", "doc-b"]
    @test collect(stream_result.vector_score) == [0.9, 0.5]
    @test Arrow.getmetadata(stream_result) == Dict("service" => "stream")
    @test Arrow.getmetadata(stream_result.doc_id) ==
          Dict("response.role" => "stream-doc-id")
    @test Arrow.getmetadata(stream_result.vector_score) ==
          Dict("response.role" => "stream-score")
    @test filter(!isempty, getfield.(stream_messages, :app_metadata)) ==
          [b"response:stream"]

    local_result = Arrow.Flight.table(
        table_service,
        fixture.context,
        fixture.sample_table;
        descriptor=descriptor,
    )
    @test collect(local_result.doc_id) == ["doc-b", "doc-a"]
    @test Arrow.getmetadata(local_result) == Dict("service" => "table")
    @test Arrow.getmetadata(local_result.doc_id) == Dict("response.role" => "table-doc-id")

    local_stream = Arrow.Flight.stream(
        stream_service,
        fixture.context,
        Tables.partitioner((batch_a, batch_b));
        descriptor=descriptor,
        include_app_metadata=true,
    )
    local_batches = collect(local_stream)
    @test length(local_batches) == 1
    @test collect(local_batches[1].table.doc_id) == ["doc-a", "doc-b"]
    @test String(local_batches[1].app_metadata) == "response:stream"
    @test Arrow.getmetadata(local_batches[1].table) == Dict("service" => "stream")
    @test Arrow.getmetadata(local_batches[1].table.doc_id) ==
          Dict("response.role" => "stream-doc-id")

    request_app_stream_service = Arrow.Flight.streamservice(
        request_stream -> begin
            incoming = collect(request_stream)
            @test length(incoming) == 2
            @test collect(incoming[1].table.doc_id) == ["doc-a"]
            @test collect(incoming[2].table.doc_id) == ["doc-b"]
            @test String.(getproperty.(incoming, :app_metadata)) ==
                  ["request:stream:0", "request:stream:1"]
            return Tables.partitioner(getproperty.(incoming, :table))
        end;
        include_request_app_metadata=true,
        app_metadata=("response:stream-request-app", "response:stream-request-app"),
    )
    request_app_stream = Arrow.Flight.stream(
        request_app_stream_service,
        fixture.context,
        Arrow.Flight.withappmetadata(
            Tables.partitioner((batch_a, batch_b));
            app_metadata=("request:stream:0", "request:stream:1"),
        );
        descriptor=descriptor,
        include_app_metadata=true,
    )
    request_app_stream_batches = collect(request_app_stream)
    @test length(request_app_stream_batches) == 2
    @test collect(request_app_stream_batches[1].table.doc_id) == ["doc-a"]
    @test collect(request_app_stream_batches[2].table.doc_id) == ["doc-b"]
    @test String.(getproperty.(request_app_stream_batches, :app_metadata)) ==
          ["response:stream-request-app", "response:stream-request-app"]

    local_exchange_with_app = Arrow.Flight.table(
        service,
        fixture.context,
        fixture.sample_table;
        descriptor=descriptor,
        include_app_metadata=true,
    )
    @test String.(local_exchange_with_app.app_metadata) == ["response:exchange"]
end
