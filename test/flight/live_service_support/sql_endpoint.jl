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

function flight_sql_endpoint_fixture(protocol)
    descriptor_type = protocol.var"FlightDescriptor.DescriptorType"
    query_descriptor = protocol.FlightDescriptor(descriptor_type.CMD, UInt8[], String[])
    query_ticket = protocol.Ticket(
        Arrow.Flight._protocolbytes(
            Arrow.Flight.SQL.Generated.TicketStatementQuery(b"query-handle"),
        ),
    )
    prepared_ticket = protocol.Ticket(
        Arrow.Flight._protocolbytes(
            Arrow.Flight.SQL.Generated.TicketStatementQuery(b"prepared-bound-handle"),
        ),
    )
    query_rows = ((id=Int64[1, 2, 3], label=["sql-one", "sql-two", "sql-three"]),)
    prepared_rows = ((id=Int64[7], label=["prepared-seven"]),)
    query_messages = Arrow.Flight.flightdata(
        Tables.partitioner(query_rows);
        descriptor=query_descriptor,
        metadata=Dict("flight_sql" => "query"),
    )
    prepared_messages = Arrow.Flight.flightdata(
        Tables.partitioner(prepared_rows);
        descriptor=query_descriptor,
        metadata=Dict("flight_sql" => "prepared"),
    )
    query_schema_bytes = Arrow.Flight.schemaipc(first(query_messages))
    prepared_schema_bytes = Arrow.Flight.schemaipc(first(prepared_messages))
    query_info = protocol.FlightInfo(
        query_schema_bytes[5:end],
        query_descriptor,
        [protocol.FlightEndpoint(query_ticket, protocol.Location[], nothing, UInt8[])],
        Int64(3),
        Int64(-1),
        false,
        UInt8[],
    )
    prepared_info = protocol.FlightInfo(
        prepared_schema_bytes[5:end],
        query_descriptor,
        [
            protocol.FlightEndpoint(
                prepared_ticket,
                protocol.Location[],
                nothing,
                UInt8[],
            ),
        ],
        Int64(1),
        Int64(-1),
        false,
        UInt8[],
    )
    return (
        username="sql",
        password="token",
        query_ticket=query_ticket,
        prepared_ticket=prepared_ticket,
        query_messages=query_messages,
        prepared_messages=prepared_messages,
        query_schema_bytes=query_schema_bytes,
        prepared_schema_bytes=prepared_schema_bytes,
        query_info=query_info,
        prepared_info=prepared_info,
    )
end

function flight_sql_endpoint_assert_authenticated(ctx, fixture)
    auth_header = Arrow.Flight.callheader(ctx, "authorization")
    @test auth_header isa String
    @test startswith(auth_header, "Basic ")
    return nothing
end

function flight_sql_endpoint_command(descriptor, ::Type{T}) where {T}
    protocol = Arrow.Flight.Protocol
    descriptor_type = protocol.var"FlightDescriptor.DescriptorType"
    @test getfield(descriptor, Symbol("#type")) == descriptor_type.CMD
    any = Arrow.Flight.SQL.decodeany(descriptor.cmd)
    @test any.type_url == Arrow.Flight.SQL.typeurl(String(nameof(T)))
    return Arrow.Flight._decodeprotocolbytes(T, any.value)
end

function flight_sql_endpoint_action(action, ::Type{T}) where {T}
    any = Arrow.Flight.SQL.decodeany(action.body)
    @test any.type_url == Arrow.Flight.SQL.typeurl(String(nameof(T)))
    return Arrow.Flight._decodeprotocolbytes(T, any.value)
end

function flight_sql_endpoint_ticket(ticket)
    return Arrow.Flight._decodeprotocolbytes(
        Arrow.Flight.SQL.Generated.TicketStatementQuery,
        ticket.ticket,
    )
end

function flight_sql_endpoint_info_for_command(descriptor, fixture)
    generated = Arrow.Flight.SQL.Generated
    any = Arrow.Flight.SQL.decodeany(descriptor.cmd)
    if any.type_url == Arrow.Flight.SQL.typeurl("CommandStatementQuery")
        command = Arrow.Flight._decodeprotocolbytes(generated.CommandStatementQuery, any.value)
        @test command.query == "select * from production_flight_sql"
        @test command.transaction_id == b"tx-query"
        return fixture.query_info
    end

    @test any.type_url == Arrow.Flight.SQL.typeurl("CommandPreparedStatementQuery")
    command = Arrow.Flight._decodeprotocolbytes(
        generated.CommandPreparedStatementQuery,
        any.value,
    )
    if command.prepared_statement_handle == b"prepared-handle"
        throw(
            Arrow.Flight.FlightStatusError(
                Arrow.Flight.GRPC_STATUS_INTERNAL,
                "stale prepared statement handle",
            ),
        )
    end
    @test command.prepared_statement_handle == b"prepared-bound-handle"
    return fixture.prepared_info
end

function flight_sql_endpoint_stream_put(request)
    first_state = iterate(request)
    first_state === nothing &&
        throw(ArgumentError("Flight SQL DoPut request must include FlightData"))
    first_message, request_state = first_state
    descriptor = first_message.flight_descriptor
    @test !isnothing(descriptor)
    messages = Iterators.flatten(((first_message,), Iterators.rest(request, request_state)))
    return descriptor, Arrow.Flight.stream(messages)
end

function flight_sql_endpoint_ingest_count(descriptor, batches)
    generated = Arrow.Flight.SQL.Generated
    command = flight_sql_endpoint_command(descriptor, generated.CommandStatementIngest)
    @test command.table == "target_table"
    @test command.schema == "target_schema"
    @test command.catalog == "target_catalog"
    @test command.temporary
    @test command.transaction_id == b"tx-ingest"
    @test command.options == Dict("mode" => "append")
    table_definition_options = command.table_definition_options
    @test !isnothing(table_definition_options)
    @test table_definition_options.if_not_exist ==
          generated.var"CommandStatementIngest.TableDefinitionOptions.TableNotExistOption".TABLE_NOT_EXIST_OPTION_CREATE
    @test table_definition_options.if_exists ==
          generated.var"CommandStatementIngest.TableDefinitionOptions.TableExistsOption".TABLE_EXISTS_OPTION_APPEND

    rows = 0
    for batch in batches
        @test batch.id isa AbstractVector
        @test batch.label isa AbstractVector
        rows += length(batch.id)
    end
    return rows
end

function flight_sql_endpoint_prepared_bind_handle(descriptor, batches)
    generated = Arrow.Flight.SQL.Generated
    command =
        flight_sql_endpoint_command(descriptor, generated.CommandPreparedStatementQuery)
    @test command.prepared_statement_handle == b"prepared-handle"
    batch_count = 0
    for batch in batches
        batch_count += 1
        @test batch.parameter == [7]
    end
    @test batch_count == 1
    return b"prepared-bound-handle"
end

function flight_sql_endpoint_session_option(protocol, value::AbstractString)
    return protocol.SessionOptionValue(protocol.PB.OneOf(:string_value, String(value)))
end

function flight_sql_endpoint_session_result(protocol, session_options)
    values = Dict{String,protocol.SessionOptionValue}()
    for (name, value) in session_options
        values[String(name)] = flight_sql_endpoint_session_option(protocol, value)
    end
    return protocol.Result(
        Arrow.Flight._protocolbytes(protocol.GetSessionOptionsResult(values)),
    )
end

function flight_sql_endpoint_service(protocol, fixture)
    return Arrow.Flight.Service(
        getflightinfo=(ctx, descriptor) -> begin
            flight_sql_endpoint_assert_authenticated(ctx, fixture)
            return flight_sql_endpoint_info_for_command(descriptor, fixture)
        end,
        doget=(ctx, ticket, response) -> begin
            flight_sql_endpoint_assert_authenticated(ctx, fixture)
            statement = flight_sql_endpoint_ticket(ticket)
            if statement.statement_handle == b"query-handle"
                for message in fixture.query_messages
                    put!(response, message)
                end
            else
                @test statement.statement_handle == b"prepared-bound-handle"
                for message in fixture.prepared_messages
                    put!(response, message)
                end
            end
            close(response)
            return :flight_sql_doget_ok
        end,
        doput=(ctx, request, response) -> begin
            flight_sql_endpoint_assert_authenticated(ctx, fixture)
            descriptor, batches = flight_sql_endpoint_stream_put(request)
            any = Arrow.Flight.SQL.decodeany(descriptor.cmd)
            if any.type_url == Arrow.Flight.SQL.typeurl("CommandStatementIngest")
                row_count = flight_sql_endpoint_ingest_count(descriptor, batches)
                put!(response, Arrow.Flight.SQL.doputupdateresult(row_count))
            else
                @test any.type_url ==
                      Arrow.Flight.SQL.typeurl("CommandPreparedStatementQuery")
                handle = flight_sql_endpoint_prepared_bind_handle(descriptor, batches)
                put!(response, Arrow.Flight.SQL.doputpreparedstatementresult(handle))
            end
            close(response)
            return :flight_sql_doput_ok
        end,
        doaction=(ctx, action, response) -> begin
            flight_sql_endpoint_assert_authenticated(ctx, fixture)
            generated = Arrow.Flight.SQL.Generated
            action_type = getfield(action, Symbol("#type"))
            if action_type == "CreatePreparedStatement"
                request = flight_sql_endpoint_action(
                    action,
                    generated.ActionCreatePreparedStatementRequest,
                )
                @test request.query == "select prepared"
                @test request.transaction_id == b"tx-prepared"
                result = generated.ActionCreatePreparedStatementResult(
                    b"prepared-handle",
                    fixture.prepared_schema_bytes,
                    UInt8[],
                )
                put!(
                    response,
                    protocol.Result(
                        Arrow.Flight._protocolbytes(Arrow.Flight.SQL.anymessage(result)),
                    ),
                )
            elseif action_type == "ClosePreparedStatement"
                request = flight_sql_endpoint_action(
                    action,
                    generated.ActionClosePreparedStatementRequest,
                )
                @test request.prepared_statement_handle == b"prepared-bound-handle"
            elseif action_type == "SetSessionOptions"
                request = Arrow.Flight._decodeprotocolbytes(
                    protocol.SetSessionOptionsRequest,
                    action.body,
                )
                @test request.session_options["catalog"].option_value.name ===
                      :string_value
                @test request.session_options["catalog"].option_value[] == "analytics"
                put!(
                    response,
                    protocol.Result(
                        Arrow.Flight._protocolbytes(
                            protocol.SetSessionOptionsResult(
                                Dict{
                                    String,
                                    protocol.var"SetSessionOptionsResult.Error",
                                }(),
                            ),
                        ),
                    ),
                )
            elseif action_type == "GetSessionOptions"
                Arrow.Flight._decodeprotocolbytes(
                    protocol.GetSessionOptionsRequest,
                    action.body,
                )
                put!(
                    response,
                    flight_sql_endpoint_session_result(
                        protocol,
                        Dict("catalog" => "analytics"),
                    ),
                )
            else
                @test action_type == "CloseSession"
                Arrow.Flight._decodeprotocolbytes(protocol.CloseSessionRequest, action.body)
                put!(
                    response,
                    protocol.Result(
                        Arrow.Flight._protocolbytes(
                            protocol.CloseSessionResult(
                                protocol.var"CloseSessionResult.Status".CLOSED,
                            ),
                        ),
                    ),
                )
            end
            close(response)
            return :flight_sql_doaction_ok
        end,
    )
end

function flight_live_python_sql_endpoint_smoke(host::AbstractString, port::Integer, fixture)
    python = FlightTestSupport.pyarrow_flight_python(
        required_modules=FLIGHT_SQL_ENDPOINT_REQUIRED_MODULES,
    )
    isnothing(python) && return nothing
    proto_root =
        normpath(joinpath(FlightTestSupport.TEST_ROOT, "..", "src", "flight", "proto"))
    output = readchomp(
        Cmd([
            python,
            "-c",
            FLIGHT_SQL_ENDPOINT_SMOKE,
            proto_root,
            host,
            string(port),
            fixture.username,
            fixture.password,
        ]),
    )
    return JSON3.read(output)
end
