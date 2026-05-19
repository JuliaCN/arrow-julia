# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

@testset "exports a single-batch ArrowArrayStream" begin
    table = Arrow.Table(Arrow.tobuffer((id=Int32[1, 2], name=["a", "b"])))
    exported = CData.exportstream(table)
    stream_ptr = CData.stream_ptr(exported)
    stream = CData.stream(exported)

    @test stream.get_schema != C_NULL
    @test stream.get_next != C_NULL
    @test stream.get_last_error != C_NULL
    @test stream.release != C_NULL
    @test !CData.isreleased(exported)

    schema_ref = Ref{CData.ArrowSchema}()
    array_ref = Ref{CData.ArrowArray}()
    eos_ref = Ref{CData.ArrowArray}()
    GC.@preserve schema_ref array_ref eos_ref begin
        schema_out = Base.unsafe_convert(Ptr{CData.ArrowSchema}, schema_ref)
        array_out = Base.unsafe_convert(Ptr{CData.ArrowArray}, array_ref)
        eos_out = Base.unsafe_convert(Ptr{CData.ArrowArray}, eos_ref)

        @test ccall(
            stream.get_schema,
            Cint,
            (Ptr{CData.ArrowArrayStream}, Ptr{CData.ArrowSchema}),
            stream_ptr,
            schema_out,
        ) == 0
        @test ccall(
            stream.get_next,
            Cint,
            (Ptr{CData.ArrowArrayStream}, Ptr{CData.ArrowArray}),
            stream_ptr,
            array_out,
        ) == 0

        imported = CData.importtable(schema_out, array_out)
        @test collect(Tables.getcolumn(imported, :id)) == Int32[1, 2]
        @test collect(Tables.getcolumn(imported, :name)) == ["a", "b"]
        CData.release!(imported)

        @test ccall(
            stream.get_next,
            Cint,
            (Ptr{CData.ArrowArrayStream}, Ptr{CData.ArrowArray}),
            stream_ptr,
            eos_out,
        ) == 0
        @test eos_ref[].release == C_NULL
        @test unsafe_string(
            ccall(
                stream.get_last_error,
                Ptr{UInt8},
                (Ptr{CData.ArrowArrayStream},),
                stream_ptr,
            ),
        ) == ""
    end

    CData.release!(exported)
    @test CData.isreleased(exported)
end

@testset "reports stream callback errors" begin
    table = Arrow.Table(Arrow.tobuffer((id=Int32[1],)))
    exported = CData.exportstream(table)
    stream_ptr = CData.stream_ptr(exported)
    stream = CData.stream(exported)

    @test ccall(
        stream.get_schema,
        Cint,
        (Ptr{CData.ArrowArrayStream}, Ptr{CData.ArrowSchema}),
        stream_ptr,
        Ptr{CData.ArrowSchema}(C_NULL),
    ) != 0
    error_ptr =
        ccall(stream.get_last_error, Ptr{UInt8}, (Ptr{CData.ArrowArrayStream},), stream_ptr)
    @test occursin(
        "ArrowSchema output pointer must not be C_NULL",
        unsafe_string(error_ptr),
    )

    CData.release!(exported)
end

@testset "C smoke consumer validates stream callbacks" begin
    smoke_lib = _compile_cdata_smoke()
    before = CData._retained_handle_count()
    table = Arrow.Table(Arrow.tobuffer((id=Int32[1, 2], name=["a", "b"])))
    stream_ref = Ref{CData.ArrowArrayStream}()
    GC.@preserve stream_ref begin
        stream_out = Base.unsafe_convert(Ptr{CData.ArrowArrayStream}, stream_ref)
        exported = CData.exportstream!(stream_out, table)
        smoke_handle = Libdl.dlopen(smoke_lib)
        validate_stream =
            Libdl.dlsym(smoke_handle, :arrow_julia_cdata_smoke_validate_stream)

        @test ccall(validate_stream, Cint, (Ptr{CData.ArrowArrayStream},), stream_out) == 0
        @test CData.isreleased(exported)
        @test CData._retained_handle_count() == before
    end
end

@testset "rejects null stream output pointer" begin
    table = Arrow.Table(Arrow.tobuffer((id=Int32[1],)))
    @test_throws ArgumentError("ArrowArrayStream output pointer must not be C_NULL") CData.exportstream!(
        Ptr{CData.ArrowArrayStream}(C_NULL),
        table,
    )
end
