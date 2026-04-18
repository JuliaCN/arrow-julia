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

function purehttp2_extension_test_live_connection(fixture)
    protocol = fixture.protocol
    first_request = protocol.FlightData(nothing, UInt8[0x01], UInt8[0x0A], UInt8[0x02])
    second_request = protocol.FlightData(nothing, UInt8[0x03], UInt8[0x0B], UInt8[0x04])

    seen_context = Ref{Union{Nothing,Arrow.Flight.ServerCallContext}}(nothing)
    seen_requests = Ref(Vector{protocol.FlightData}())

    service = Arrow.Flight.Service(
        doexchange=(ctx, request, response) -> begin
            seen_context[] = ctx
            for message in request
                push!(seen_requests[], message)
                put!(
                    response,
                    protocol.FlightData(
                        message.flight_descriptor,
                        copy(message.data_header),
                        copy(message.app_metadata),
                        copy(message.data_body),
                    ),
                )
            end
            close(response)
            return :doexchange_live_ok
        end,
    )

    client_to_server = Base.BufferStream()
    server_to_client = Base.BufferStream()
    server_io = PureHTTP2ExtensionPairedIO(client_to_server, server_to_client)
    server_conn = PureHTTP2.HTTP2Connection()

    server_task = @async Arrow.Flight.purehttp2_serve_grpc_connection!(
        service,
        server_conn,
        server_io;
        request_capacity=2,
        response_capacity=2,
    )

    purehttp2_extension_write_client_preface!(client_to_server)
    header_block =
        purehttp2_extension_header_block("DoExchange"; headers=fixture.unary_headers)
    write(
        client_to_server,
        PureHTTP2.encode_frame(
            PureHTTP2.headers_frame(
                UInt32(1),
                header_block;
                end_stream=false,
                end_headers=true,
            ),
        ),
    )
    write(
        client_to_server,
        PureHTTP2.encode_frame(
            PureHTTP2.data_frame(
                UInt32(1),
                Arrow.Flight.grpcmessage(first_request);
                end_stream=false,
            ),
        ),
    )

    first_response_frames = try
        purehttp2_extension_collect_stream_frames(server_to_client, UInt32(1), 2)
    catch err
        detail = "server_task_done=$(istaskdone(server_task)) server_task_failed=$(istaskfailed(server_task))"
        if istaskdone(server_task)
            try
                wait(server_task)
            catch server_err
                detail *= " server_error=$(sprint(showerror, server_err))"
            end
        end
        error("$(sprint(showerror, err)); $(detail)")
    end
    @test first_response_frames[1].header.frame_type == PureHTTP2.FrameType.HEADERS
    @test !PureHTTP2.has_flag(
        first_response_frames[1].header,
        PureHTTP2.FrameFlags.END_STREAM,
    )
    @test first_response_frames[2].header.frame_type == PureHTTP2.FrameType.DATA
    @test !PureHTTP2.has_flag(
        first_response_frames[2].header,
        PureHTTP2.FrameFlags.END_STREAM,
    )
    first_response_messages =
        purehttp2_extension_decode_data(protocol.FlightData, [first_response_frames[2]])
    @test length(first_response_messages) == 1
    @test first_response_messages[1].data_header == first_request.data_header
    @test first_response_messages[1].data_body == first_request.data_body
    @test first_response_messages[1].app_metadata == first_request.app_metadata
    @test length(seen_requests[]) == 1
    @test seen_requests[][1].data_header == first_request.data_header
    @test Arrow.Flight.callheader(seen_context[], "authorization") == "Bearer purehttp2"

    write(
        client_to_server,
        PureHTTP2.encode_frame(
            PureHTTP2.data_frame(
                UInt32(1),
                Arrow.Flight.grpcmessage(second_request);
                end_stream=true,
            ),
        ),
    )

    remaining_frames =
        purehttp2_extension_collect_stream_frames(server_to_client, UInt32(1), 2)
    @test remaining_frames[1].header.frame_type == PureHTTP2.FrameType.DATA
    @test !PureHTTP2.has_flag(remaining_frames[1].header, PureHTTP2.FrameFlags.END_STREAM)
    @test remaining_frames[2].header.frame_type == PureHTTP2.FrameType.HEADERS
    @test PureHTTP2.has_flag(remaining_frames[2].header, PureHTTP2.FrameFlags.END_STREAM)

    remaining_messages =
        purehttp2_extension_decode_data(protocol.FlightData, [remaining_frames[1]])
    @test length(remaining_messages) == 1
    @test remaining_messages[1].data_header == second_request.data_header
    @test remaining_messages[1].data_body == second_request.data_body
    @test remaining_messages[1].app_metadata == second_request.app_metadata

    trailers = purehttp2_extension_decode_headers([remaining_frames[2]])
    @test trailers == [[("grpc-status", "0")]]
    @test length(seen_requests[]) == 2
    @test seen_requests[][2].data_header == second_request.data_header

    write(
        client_to_server,
        PureHTTP2.encode_frame(PureHTTP2.goaway_frame(1, PureHTTP2.ErrorCode.NO_ERROR)),
    )
    close(client_to_server)
    wait(server_task)
    close(server_to_client)
end

function purehttp2_extension_test_large_request_window_updates(fixture)
    protocol = fixture.protocol
    request_messages = [
        protocol.FlightData(
            nothing,
            UInt8[index],
            UInt8[index + 0x20],
            fill(UInt8(index + 0x40), 12_000),
        ) for index in UInt8.(1:6)
    ]
    seen_requests = Ref(Vector{protocol.FlightData}())

    service = Arrow.Flight.Service(
        doexchange=(ctx, request, response) -> begin
            for message in request
                push!(seen_requests[], message)
            end
            close(response)
            return :doexchange_large_window_update_ok
        end,
    )

    client_to_server = Base.BufferStream()
    server_to_client = Base.BufferStream()
    server_io = PureHTTP2ExtensionPairedIO(client_to_server, server_to_client)
    server_conn = PureHTTP2.HTTP2Connection()

    server_task = @async Arrow.Flight.purehttp2_serve_grpc_connection!(
        service,
        server_conn,
        server_io;
        request_capacity=2,
        response_capacity=2,
    )

    purehttp2_extension_write_client_preface!(client_to_server)
    header_block =
        purehttp2_extension_header_block("DoExchange"; headers=fixture.unary_headers)
    write(
        client_to_server,
        PureHTTP2.encode_frame(
            PureHTTP2.headers_frame(
                UInt32(1),
                header_block;
                end_stream=false,
                end_headers=true,
            ),
        ),
    )

    for (index, request_message) in enumerate(request_messages)
        write(
            client_to_server,
            PureHTTP2.encode_frame(
                PureHTTP2.data_frame(
                    UInt32(1),
                    Arrow.Flight.grpcmessage(request_message);
                    end_stream=index == length(request_messages),
                ),
            ),
        )
    end

    try
        frames = purehttp2_extension_collect_frames_until(
            server_to_client,
            (_, seen_frames) -> begin
                any(
                        frame ->
                            frame.header.frame_type == PureHTTP2.FrameType.WINDOW_UPDATE &&
                            frame.header.stream_id == 0,
                        seen_frames,
                    ) &&
                    any(
                        frame ->
                            frame.header.frame_type == PureHTTP2.FrameType.WINDOW_UPDATE &&
                            frame.header.stream_id == UInt32(1),
                        seen_frames,
                    ) &&
                    any(
                        frame ->
                            frame.header.frame_type == PureHTTP2.FrameType.HEADERS &&
                            frame.header.stream_id == UInt32(1) &&
                            PureHTTP2.has_flag(
                                frame.header,
                                PureHTTP2.FrameFlags.END_STREAM,
                            ),
                        seen_frames,
                    )
            end;
            timeout_secs=10.0,
        )

        window_updates = filter(
            frame -> frame.header.frame_type == PureHTTP2.FrameType.WINDOW_UPDATE,
            frames,
        )
        @test any(frame -> frame.header.stream_id == 0, window_updates)
        @test any(frame -> frame.header.stream_id == UInt32(1), window_updates)
        response_headers = filter(
            frame ->
                frame.header.frame_type == PureHTTP2.FrameType.HEADERS &&
                frame.header.stream_id == UInt32(1),
            frames,
        )
        @test !isempty(response_headers)
        @test any(
            frame -> PureHTTP2.has_flag(frame.header, PureHTTP2.FrameFlags.END_STREAM),
            response_headers,
        )
    finally
        if isopen(client_to_server)
            try
                write(
                    client_to_server,
                    PureHTTP2.encode_frame(
                        PureHTTP2.goaway_frame(1, PureHTTP2.ErrorCode.NO_ERROR),
                    ),
                )
            catch
            end
            close(client_to_server)
        end
        isopen(server_to_client) && close(server_to_client)
        timedwait(() -> istaskdone(server_task), 5) === :ok ||
            error("Timed out waiting for PureHTTP2 large-request server task shutdown")
        wait(server_task)
    end

    @test length(seen_requests[]) == length(request_messages)
    @test getfield.(seen_requests[], :data_header) ==
          getfield.(request_messages, :data_header)
    @test getfield.(seen_requests[], :data_body) == getfield.(request_messages, :data_body)
end

function purehttp2_extension_test_single_large_request_message(fixture)
    protocol = fixture.protocol
    request_message =
        protocol.FlightData(nothing, UInt8[0x11], UInt8[0x22], fill(UInt8(0x33), 152_147))
    response_message = protocol.FlightData(nothing, UInt8[0x44], UInt8[0x55], UInt8[0x66])
    seen_requests = Ref(Vector{protocol.FlightData}())

    service = Arrow.Flight.Service(
        doexchange=(ctx, request, response) -> begin
            for message in request
                push!(seen_requests[], message)
                put!(
                    response,
                    protocol.FlightData(
                        response_message.flight_descriptor,
                        copy(response_message.data_header),
                        copy(response_message.app_metadata),
                        copy(response_message.data_body),
                    ),
                )
            end
            close(response)
            return :doexchange_single_large_request_ok
        end,
    )

    client_to_server = Base.BufferStream()
    server_to_client = Base.BufferStream()
    server_io = PureHTTP2ExtensionPairedIO(client_to_server, server_to_client)
    server_conn = PureHTTP2.HTTP2Connection()

    server_task = @async Arrow.Flight.purehttp2_serve_grpc_connection!(
        service,
        server_conn,
        server_io;
        request_capacity=2,
        response_capacity=2,
    )

    purehttp2_extension_write_client_preface!(client_to_server)
    header_block =
        purehttp2_extension_header_block("DoExchange"; headers=fixture.unary_headers)
    write(
        client_to_server,
        PureHTTP2.encode_frame(
            PureHTTP2.headers_frame(
                UInt32(1),
                header_block;
                end_stream=false,
                end_headers=true,
            ),
        ),
    )

    payload_chunks = _purehttp2_extension_chunk_payload(
        Arrow.Flight.grpcmessage(request_message),
        16_384,
    )
    for (index, payload_chunk) in enumerate(payload_chunks)
        write(
            client_to_server,
            PureHTTP2.encode_frame(
                PureHTTP2.data_frame(
                    UInt32(1),
                    payload_chunk;
                    end_stream=index == length(payload_chunks),
                ),
            ),
        )
    end

    frames = purehttp2_extension_collect_frames_until(
        server_to_client,
        (_, seen_frames) -> begin
            any(
                frame ->
                    frame.header.frame_type == PureHTTP2.FrameType.DATA &&
                    frame.header.stream_id == UInt32(1),
                seen_frames,
            ) && any(
                frame ->
                    frame.header.frame_type == PureHTTP2.FrameType.HEADERS &&
                    frame.header.stream_id == UInt32(1) &&
                    PureHTTP2.has_flag(frame.header, PureHTTP2.FrameFlags.END_STREAM),
                seen_frames,
            )
        end;
        timeout_secs=5.0,
    )

    window_updates = filter(
        frame -> frame.header.frame_type == PureHTTP2.FrameType.WINDOW_UPDATE,
        frames,
    )
    @test any(frame -> frame.header.stream_id == 0, window_updates)
    @test any(frame -> frame.header.stream_id == UInt32(1), window_updates)

    response_frames = filter(frame -> frame.header.stream_id == UInt32(1), frames)
    response_messages =
        purehttp2_extension_decode_data(protocol.FlightData, response_frames)
    @test length(response_messages) == 1
    @test response_messages[1].data_header == response_message.data_header
    @test response_messages[1].data_body == response_message.data_body

    trailers = purehttp2_extension_decode_headers(
        filter(
            frame ->
                frame.header.frame_type == PureHTTP2.FrameType.HEADERS &&
                frame.header.stream_id == UInt32(1),
            response_frames,
        ),
    )
    @test trailers[end] == [("grpc-status", "0")]

    write(
        client_to_server,
        PureHTTP2.encode_frame(PureHTTP2.goaway_frame(1, PureHTTP2.ErrorCode.NO_ERROR)),
    )
    close(client_to_server)
    wait(server_task)
    close(server_to_client)

    @test length(seen_requests[]) == 1
    @test seen_requests[][1].data_header == request_message.data_header
    @test seen_requests[][1].data_body == request_message.data_body
end

function purehttp2_extension_test_large_flightdata_request_stream(fixture)
    protocol = fixture.protocol
    request_messages = Arrow.Flight.flightdata(
        Tables.partitioner(((doc_id=[repeat("x", 152_147)], vector_score=[1.0]),));
        descriptor=fixture.descriptor,
        metadata=Dict("wendao.schema_version" => "v1"),
    )
    response_message = protocol.FlightData(nothing, UInt8[0x66], UInt8[0x77], UInt8[0x88])
    seen_requests = Ref(Vector{protocol.FlightData}())

    service = Arrow.Flight.Service(
        doexchange=(ctx, request, response) -> begin
            for message in request
                push!(seen_requests[], message)
            end
            put!(
                response,
                protocol.FlightData(
                    response_message.flight_descriptor,
                    copy(response_message.data_header),
                    copy(response_message.app_metadata),
                    copy(response_message.data_body),
                ),
            )
            close(response)
            return :doexchange_large_flightdata_request_stream_ok
        end,
    )

    client_to_server = Base.BufferStream()
    server_to_client = Base.BufferStream()
    server_io = PureHTTP2ExtensionPairedIO(client_to_server, server_to_client)
    server_conn = PureHTTP2.HTTP2Connection()

    server_task = @async Arrow.Flight.purehttp2_serve_grpc_connection!(
        service,
        server_conn,
        server_io;
        request_capacity=2,
        response_capacity=2,
    )

    purehttp2_extension_write_client_preface!(client_to_server)
    header_block =
        purehttp2_extension_header_block("DoExchange"; headers=fixture.unary_headers)
    write(
        client_to_server,
        PureHTTP2.encode_frame(
            PureHTTP2.headers_frame(
                UInt32(1),
                header_block;
                end_stream=false,
                end_headers=true,
            ),
        ),
    )

    payload_chunks = Vector{Vector{UInt8}}()
    for request_message in request_messages
        append!(
            payload_chunks,
            _purehttp2_extension_chunk_payload(
                Arrow.Flight.grpcmessage(request_message),
                16_384,
            ),
        )
    end
    for (index, payload_chunk) in enumerate(payload_chunks)
        write(
            client_to_server,
            PureHTTP2.encode_frame(
                PureHTTP2.data_frame(
                    UInt32(1),
                    payload_chunk;
                    end_stream=index == length(payload_chunks),
                ),
            ),
        )
    end

    frames = purehttp2_extension_collect_frames_until(
        server_to_client,
        (_, seen_frames) -> begin
            any(
                frame ->
                    frame.header.frame_type == PureHTTP2.FrameType.DATA &&
                    frame.header.stream_id == UInt32(1),
                seen_frames,
            ) && any(
                frame ->
                    frame.header.frame_type == PureHTTP2.FrameType.HEADERS &&
                    frame.header.stream_id == UInt32(1) &&
                    PureHTTP2.has_flag(frame.header, PureHTTP2.FrameFlags.END_STREAM),
                seen_frames,
            )
        end;
        timeout_secs=5.0,
    )

    response_frames = filter(frame -> frame.header.stream_id == UInt32(1), frames)
    response_messages =
        purehttp2_extension_decode_data(protocol.FlightData, response_frames)
    @test length(response_messages) == 1
    @test response_messages[1].data_header == response_message.data_header
    @test response_messages[1].data_body == response_message.data_body

    trailers = purehttp2_extension_decode_headers(
        filter(
            frame ->
                frame.header.frame_type == PureHTTP2.FrameType.HEADERS &&
                frame.header.stream_id == UInt32(1),
            response_frames,
        ),
    )
    @test trailers[end] == [("grpc-status", "0")]

    write(
        client_to_server,
        PureHTTP2.encode_frame(PureHTTP2.goaway_frame(1, PureHTTP2.ErrorCode.NO_ERROR)),
    )
    close(client_to_server)
    wait(server_task)
    close(server_to_client)

    @test length(seen_requests[]) == length(request_messages)
    @test getfield.(seen_requests[], :data_header) ==
          getfield.(request_messages, :data_header)
    @test getfield.(seen_requests[], :data_body) == getfield.(request_messages, :data_body)
end
