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

using Test
using Arrow
using Libdl
using Tables

const ADBC = Arrow.ADBC

function adbc_mock_driver_init(version::Cint, driver::Ptr{Cvoid}, error::Ptr{ADBC.Error})
    driver == C_NULL && return UInt8(ADBC.STATUS_INVALID_ARGUMENT)
    version == Cint(ADBC.ADBC_VERSION_1_1_0) || return UInt8(ADBC.STATUS_NOT_IMPLEMENTED)
    driver_fields = ntuple(
        index -> index == 1 ? Ptr{Cvoid}(UInt(1)) : Ptr{Cvoid}(C_NULL),
        fieldcount(ADBC.Driver),
    )
    unsafe_store!(Ptr{ADBC.Driver}(driver), ADBC.Driver(driver_fields...))
    return UInt8(ADBC.STATUS_OK)
end

const ADBC_MOCK_DRIVER_INIT =
    @cfunction(adbc_mock_driver_init, UInt8, (Cint, Ptr{Cvoid}, Ptr{ADBC.Error}),)

function build_adbc_mock_driver_library()
    directory = mktempdir()
    source = joinpath(directory, "adbc_mock_driver.c")
    output = joinpath(directory, "libadbc_driver_sqlite.$(Libdl.dlext)")
    write(
        source,
        raw"""
#include <stdint.h>

#define ADBC_VERSION_1_1_0 1001000
#define ADBC_STATUS_OK 0
#define ADBC_STATUS_NOT_IMPLEMENTED 2
#define ADBC_STATUS_INVALID_ARGUMENT 5

struct AdbcError {
    const char* message;
    int32_t vendor_code;
    char sqlstate[5];
    void (*release)(struct AdbcError*);
    void* private_data;
    void* private_driver;
};

#if defined(_WIN32)
__declspec(dllexport)
#else
__attribute__((visibility("default")))
#endif
uint8_t AdbcDriverSqliteInit(int version, void* driver, struct AdbcError* error) {
    (void)error;
    if (driver == 0) {
        return ADBC_STATUS_INVALID_ARGUMENT;
    }
    if (version != ADBC_VERSION_1_1_0) {
        return ADBC_STATUS_NOT_IMPLEMENTED;
    }
    ((void**)driver)[0] = (void*)(uintptr_t)1;
    return ADBC_STATUS_OK;
}
""",
    )
    cc = get(ENV, "CC", "cc")
    shared_flag = Sys.isapple() ? "-dynamiclib" : "-shared"
    run(`$cc $shared_flag -fPIC $source -o $output`)
    return output
end

@testset "ADBC ABI constants and handles" begin
    @test UInt8(ADBC.STATUS_OK) == 0
    @test UInt8(ADBC.STATUS_UNAUTHORIZED) == 14
    @test ADBC.ADBC_VERSION_1_0_0 == 1_000_000
    @test ADBC.ADBC_VERSION_1_1_0 == 1_001_000
    @test ADBC.OPTION_URI == "uri"
    @test ADBC.CONNECTION_OPTION_AUTOCOMMIT == "adbc.connection.autocommit"
    @test ADBC.STATEMENT_OPTION_INCREMENTAL == "adbc.statement.exec.incremental"
    @test ADBC.INGEST_OPTION_MODE_CREATE_APPEND == "adbc.ingest.mode.create_append"
    @test ADBC.INFO_DRIVER_ADBC_VERSION == UInt32(103)

    @test isbitstype(ADBC.Error)
    @test isbitstype(ADBC.ErrorDetail)
    @test isbitstype(ADBC.Database)
    @test isbitstype(ADBC.Connection)
    @test isbitstype(ADBC.Statement)
    @test isbitstype(ADBC.Partitions)
    @test isbitstype(ADBC.Driver)
    @test sizeof(ADBC.Database) == 2 * sizeof(Ptr{Cvoid})
    @test sizeof(ADBC.Connection) == 2 * sizeof(Ptr{Cvoid})
    @test sizeof(ADBC.Statement) == 2 * sizeof(Ptr{Cvoid})
    @test ADBC.DRIVER_1_0_0_SIZE == 29 * sizeof(Ptr{Cvoid})
    @test ADBC.DRIVER_1_1_0_SIZE == fieldcount(ADBC.Driver) * sizeof(Ptr{Cvoid})
    @test ADBC.driverabisize(ADBC.ADBC_VERSION_1_0_0) == ADBC.DRIVER_1_0_0_SIZE
    @test ADBC.driverabisize(ADBC.ADBC_VERSION_1_1_0) == ADBC.DRIVER_1_1_0_SIZE

    error = ADBC.Error()
    @test error.vendor_code == ADBC.ERROR_VENDOR_CODE_PRIVATE_DATA
    @test !ADBC.releaseable(error)
    @test ADBC.error_message(error) == ""
    @test ADBC.ErrorDetail().value_length == 0

    @test !ADBC.isinitialized(ADBC.Database())
    @test !ADBC.isinitialized(ADBC.Connection())
    @test !ADBC.isinitialized(ADBC.Statement())
    @test !ADBC.isinitialized(ADBC.Partitions())
    @test !ADBC.isinitialized(ADBC.Driver())
    @test !ADBC.releaseable(ADBC.Partitions())
    @test !ADBC.releaseable(ADBC.Driver())

    @test ADBC.statusok(ADBC.STATUS_OK)
    @test ADBC.statusok(0)
    @test ADBC.statusname(ADBC.STATUS_NOT_IMPLEMENTED) == "STATUS_NOT_IMPLEMENTED"
    @test_throws ADBC.StatusException ADBC.assertok(ADBC.STATUS_NOT_IMPLEMENTED)
    @test_throws ArgumentError ADBC.driverabisize(0)
end

@testset "ADBC driver initialization ABI" begin
    driver = Ref(ADBC.Driver())
    initialized = ADBC.driverinit!(ADBC_MOCK_DRIVER_INIT, driver)

    @test initialized == driver[]
    @test ADBC.isinitialized(initialized)
    @test ADBC.driverinit!(ADBC_MOCK_DRIVER_INIT; error=nothing) isa ADBC.Driver
    @test_throws ArgumentError ADBC.driverinit!(Ptr{Cvoid}(C_NULL))
    @test_throws ADBC.StatusException ADBC.driverinit!(
        ADBC_MOCK_DRIVER_INIT;
        version=ADBC.ADBC_VERSION_1_0_0,
    )
end

@testset "ADBC driver library resolution" begin
    @test ADBC.defaultdriverentrypoint("/tmp/libadbc_driver_sqlite.so.2.0.0") ==
          :AdbcDriverSqliteInit
    @test ADBC.defaultdriverentrypoint("adbc_driver_sqlite.dll") == :AdbcDriverSqliteInit
    @test ADBC.defaultdriverentrypoint("proprietary_driver.dll") ==
          :AdbcProprietaryDriverInit

    library_path = build_adbc_mock_driver_library()
    library = ADBC.opendriver(library_path)
    @test Base.isopen(library)
    @test library.entrypoint == :AdbcDriverSqliteInit
    @test ADBC.driverentrypoint(library) != C_NULL
    ADBC.closedriver!(library)
    @test !Base.isopen(library)
    @test_throws ArgumentError ADBC.driverentrypoint(library)

    loaded = ADBC.loaddriver(library_path)
    @test loaded.library.path == library_path
    @test loaded.library.entrypoint == :AdbcDriverSqliteInit
    @test Base.isopen(loaded.library)
    @test ADBC.isinitialized(loaded.driver)
    close(loaded)
    @test !Base.isopen(loaded.library)
    @test_throws ADBC.StatusException ADBC.loaddriver(
        library_path;
        version=ADBC.ADBC_VERSION_1_0_0,
    )
end

@testset "ADBC statement result stream boundary" begin
    table = (id=Int64[1, 2, 3], label=["a", "b", "c"])
    exported = Arrow.CData.exportstream(Arrow.Table(Arrow.tobuffer(table)))
    result = ADBC.importstream(Arrow.CData.stream_ptr(exported); rows_affected=3)

    @test ADBC.rowsaffected(result) == 3
    batches = collect(ADBC.resultstream(result))
    @test length(batches) == 1
    @test collect(Tables.getcolumn(batches[1], :id)) == [1, 2, 3]
    @test collect(Tables.getcolumn(batches[1], :label)) == ["a", "b", "c"]

    Arrow.CData.release!.(batches)
    Arrow.CData.release!(ADBC.resultstream(result))
    Arrow.CData.release!(exported)

    @test_throws ArgumentError ADBC.statementresult(nothing; rows_affected=-2)
end
