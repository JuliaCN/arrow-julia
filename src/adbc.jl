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

"""
Low-level Apache Arrow ADBC constants and ABI structs.

This module exposes the stable ADBC status, option, information, ingestion,
and handle layout boundary needed by future driver-manager and Flight SQL
driver work. It does not load database drivers or implement a high-level
database client.
"""
module ADBC

using ..CData
using Libdl

export ADBC_VERSION_1_0_0,
    ADBC_VERSION_1_1_0,
    CONNECTION_OPTION_AUTOCOMMIT,
    CONNECTION_OPTION_CURRENT_CATALOG,
    CONNECTION_OPTION_CURRENT_DB_SCHEMA,
    CONNECTION_OPTION_READ_ONLY,
    DRIVER_1_0_0_SIZE,
    DRIVER_1_1_0_SIZE,
    ERROR_VENDOR_CODE_PRIVATE_DATA,
    Error,
    ErrorDetail,
    Database,
    Connection,
    Driver,
    DriverLibrary,
    LoadedDriver,
    Partitions,
    Statement,
    STATUS_ALREADY_EXISTS,
    STATUS_CANCELLED,
    STATUS_INTERNAL,
    STATUS_INVALID_ARGUMENT,
    STATUS_INVALID_DATA,
    STATUS_INVALID_STATE,
    STATUS_INTEGRITY,
    STATUS_IO,
    STATUS_NOT_FOUND,
    STATUS_NOT_IMPLEMENTED,
    STATUS_OK,
    STATUS_TIMEOUT,
    STATUS_UNAUTHENTICATED,
    STATUS_UNAUTHORIZED,
    StatementResult,
    STATUS_UNKNOWN,
    STATEMENT_OPTION_INCREMENTAL,
    STATEMENT_OPTION_MAX_PROGRESS,
    STATEMENT_OPTION_PROGRESS,
    StatusCode,
    StatusException,
    INFO_DRIVER_ADBC_VERSION,
    INFO_DRIVER_ARROW_VERSION,
    INFO_DRIVER_NAME,
    INFO_DRIVER_VERSION,
    INFO_VENDOR_ARROW_VERSION,
    INFO_VENDOR_NAME,
    INFO_VENDOR_SQL,
    INFO_VENDOR_SUBSTRAIT,
    INFO_VENDOR_SUBSTRAIT_MAX_VERSION,
    INFO_VENDOR_SUBSTRAIT_MIN_VERSION,
    INFO_VENDOR_VERSION,
    INGEST_OPTION_MODE,
    INGEST_OPTION_MODE_APPEND,
    INGEST_OPTION_MODE_CREATE,
    INGEST_OPTION_MODE_CREATE_APPEND,
    INGEST_OPTION_MODE_REPLACE,
    INGEST_OPTION_TARGET_CATALOG,
    INGEST_OPTION_TARGET_DB_SCHEMA,
    INGEST_OPTION_TARGET_TABLE,
    INGEST_OPTION_TEMPORARY,
    OPTION_PASSWORD,
    OPTION_URI,
    OPTION_USERNAME,
    OPTION_VALUE_DISABLED,
    OPTION_VALUE_ENABLED,
    assertok,
    closedriver!,
    defaultdriverentrypoint,
    driverabisize,
    driverentrypoint,
    driverinit!,
    error_message,
    importstream,
    isinitialized,
    loaddriver,
    opendriver,
    releaseable,
    resultstream,
    rowsaffected,
    statementresult,
    statusname,
    statusok

const ADBC_VERSION_1_0_0 = 1_000_000
const ADBC_VERSION_1_1_0 = 1_001_000

const ERROR_VENDOR_CODE_PRIVATE_DATA = typemin(Int32)

@enum StatusCode::UInt8 begin
    STATUS_OK = 0
    STATUS_UNKNOWN = 1
    STATUS_NOT_IMPLEMENTED = 2
    STATUS_NOT_FOUND = 3
    STATUS_ALREADY_EXISTS = 4
    STATUS_INVALID_ARGUMENT = 5
    STATUS_INVALID_STATE = 6
    STATUS_INVALID_DATA = 7
    STATUS_INTEGRITY = 8
    STATUS_INTERNAL = 9
    STATUS_IO = 10
    STATUS_CANCELLED = 11
    STATUS_TIMEOUT = 12
    STATUS_UNAUTHENTICATED = 13
    STATUS_UNAUTHORIZED = 14
end

const OPTION_VALUE_ENABLED = "true"
const OPTION_VALUE_DISABLED = "false"
const OPTION_URI = "uri"
const OPTION_USERNAME = "username"
const OPTION_PASSWORD = "password"

const INFO_VENDOR_NAME = UInt32(0)
const INFO_VENDOR_VERSION = UInt32(1)
const INFO_VENDOR_ARROW_VERSION = UInt32(2)
const INFO_VENDOR_SQL = UInt32(3)
const INFO_VENDOR_SUBSTRAIT = UInt32(4)
const INFO_VENDOR_SUBSTRAIT_MIN_VERSION = UInt32(5)
const INFO_VENDOR_SUBSTRAIT_MAX_VERSION = UInt32(6)
const INFO_DRIVER_NAME = UInt32(100)
const INFO_DRIVER_VERSION = UInt32(101)
const INFO_DRIVER_ARROW_VERSION = UInt32(102)
const INFO_DRIVER_ADBC_VERSION = UInt32(103)

const CONNECTION_OPTION_AUTOCOMMIT = "adbc.connection.autocommit"
const CONNECTION_OPTION_READ_ONLY = "adbc.connection.readonly"
const CONNECTION_OPTION_CURRENT_CATALOG = "adbc.connection.catalog"
const CONNECTION_OPTION_CURRENT_DB_SCHEMA = "adbc.connection.db_schema"

const STATEMENT_OPTION_INCREMENTAL = "adbc.statement.exec.incremental"
const STATEMENT_OPTION_PROGRESS = "adbc.statement.exec.progress"
const STATEMENT_OPTION_MAX_PROGRESS = "adbc.statement.exec.max_progress"

const INGEST_OPTION_TARGET_TABLE = "adbc.ingest.target_table"
const INGEST_OPTION_MODE = "adbc.ingest.mode"
const INGEST_OPTION_MODE_CREATE = "adbc.ingest.mode.create"
const INGEST_OPTION_MODE_APPEND = "adbc.ingest.mode.append"
const INGEST_OPTION_MODE_REPLACE = "adbc.ingest.mode.replace"
const INGEST_OPTION_MODE_CREATE_APPEND = "adbc.ingest.mode.create_append"
const INGEST_OPTION_TARGET_CATALOG = "adbc.ingest.target_catalog"
const INGEST_OPTION_TARGET_DB_SCHEMA = "adbc.ingest.target_db_schema"
const INGEST_OPTION_TEMPORARY = "adbc.ingest.temporary"

"""
    Arrow.ADBC.Error

ABI-compatible representation of ADBC 1.1.0 `AdbcError`.
"""
struct Error
    message::Ptr{Cchar}
    vendor_code::Int32
    sqlstate::NTuple{5,Cchar}
    release::Ptr{Cvoid}
    private_data::Ptr{Cvoid}
    private_driver::Ptr{Cvoid}
end

Error() = Error(
    Ptr{Cchar}(C_NULL),
    ERROR_VENDOR_CODE_PRIVATE_DATA,
    ntuple(_ -> Cchar(0), 5),
    Ptr{Cvoid}(C_NULL),
    Ptr{Cvoid}(C_NULL),
    Ptr{Cvoid}(C_NULL),
)

"""
    Arrow.ADBC.ErrorDetail

ABI-compatible representation of ADBC 1.1.0 `AdbcErrorDetail`.
"""
struct ErrorDetail
    key::Ptr{Cchar}
    value::Ptr{UInt8}
    value_length::Csize_t
end

ErrorDetail() = ErrorDetail(Ptr{Cchar}(C_NULL), Ptr{UInt8}(C_NULL), Csize_t(0))

"""
    Arrow.ADBC.Database

ABI-compatible representation of `AdbcDatabase`.
"""
struct Database
    private_data::Ptr{Cvoid}
    private_driver::Ptr{Cvoid}
end

Database() = Database(Ptr{Cvoid}(C_NULL), Ptr{Cvoid}(C_NULL))

"""
    Arrow.ADBC.Connection

ABI-compatible representation of `AdbcConnection`.
"""
struct Connection
    private_data::Ptr{Cvoid}
    private_driver::Ptr{Cvoid}
end

Connection() = Connection(Ptr{Cvoid}(C_NULL), Ptr{Cvoid}(C_NULL))

"""
    Arrow.ADBC.Statement

ABI-compatible representation of `AdbcStatement`.
"""
struct Statement
    private_data::Ptr{Cvoid}
    private_driver::Ptr{Cvoid}
end

Statement() = Statement(Ptr{Cvoid}(C_NULL), Ptr{Cvoid}(C_NULL))

"""
    Arrow.ADBC.Partitions

ABI-compatible representation of `AdbcPartitions`.
"""
struct Partitions
    num_partitions::Csize_t
    partitions::Ptr{Ptr{UInt8}}
    partition_lengths::Ptr{Csize_t}
    private_data::Ptr{Cvoid}
    release::Ptr{Cvoid}
end

Partitions() = Partitions(
    Csize_t(0),
    Ptr{Ptr{UInt8}}(C_NULL),
    Ptr{Csize_t}(C_NULL),
    Ptr{Cvoid}(C_NULL),
    Ptr{Cvoid}(C_NULL),
)

"""
    Arrow.ADBC.Driver

ABI-compatible representation of ADBC 1.1.0 `AdbcDriver`. Callback fields are
stored as raw function pointers so a future driver manager can validate and
call them through narrow wrappers.
"""
struct Driver
    private_data::Ptr{Cvoid}
    private_manager::Ptr{Cvoid}
    release::Ptr{Cvoid}
    database_init::Ptr{Cvoid}
    database_new::Ptr{Cvoid}
    database_set_option::Ptr{Cvoid}
    database_release::Ptr{Cvoid}
    connection_commit::Ptr{Cvoid}
    connection_get_info::Ptr{Cvoid}
    connection_get_objects::Ptr{Cvoid}
    connection_get_table_schema::Ptr{Cvoid}
    connection_get_table_types::Ptr{Cvoid}
    connection_init::Ptr{Cvoid}
    connection_new::Ptr{Cvoid}
    connection_set_option::Ptr{Cvoid}
    connection_read_partition::Ptr{Cvoid}
    connection_release::Ptr{Cvoid}
    connection_rollback::Ptr{Cvoid}
    statement_bind::Ptr{Cvoid}
    statement_bind_stream::Ptr{Cvoid}
    statement_execute_query::Ptr{Cvoid}
    statement_execute_partitions::Ptr{Cvoid}
    statement_get_parameter_schema::Ptr{Cvoid}
    statement_new::Ptr{Cvoid}
    statement_prepare::Ptr{Cvoid}
    statement_release::Ptr{Cvoid}
    statement_set_option::Ptr{Cvoid}
    statement_set_sql_query::Ptr{Cvoid}
    statement_set_substrait_plan::Ptr{Cvoid}
    error_get_detail_count::Ptr{Cvoid}
    error_get_detail::Ptr{Cvoid}
    error_from_array_stream::Ptr{Cvoid}
    database_get_option::Ptr{Cvoid}
    database_get_option_bytes::Ptr{Cvoid}
    database_get_option_double::Ptr{Cvoid}
    database_get_option_int::Ptr{Cvoid}
    database_set_option_bytes::Ptr{Cvoid}
    database_set_option_double::Ptr{Cvoid}
    database_set_option_int::Ptr{Cvoid}
    connection_cancel::Ptr{Cvoid}
    connection_get_option::Ptr{Cvoid}
    connection_get_option_bytes::Ptr{Cvoid}
    connection_get_option_double::Ptr{Cvoid}
    connection_get_option_int::Ptr{Cvoid}
    connection_get_statistics::Ptr{Cvoid}
    connection_get_statistic_names::Ptr{Cvoid}
    connection_set_option_bytes::Ptr{Cvoid}
    connection_set_option_double::Ptr{Cvoid}
    connection_set_option_int::Ptr{Cvoid}
    statement_cancel::Ptr{Cvoid}
    statement_execute_schema::Ptr{Cvoid}
    statement_get_option::Ptr{Cvoid}
    statement_get_option_bytes::Ptr{Cvoid}
    statement_get_option_double::Ptr{Cvoid}
    statement_get_option_int::Ptr{Cvoid}
    statement_set_option_bytes::Ptr{Cvoid}
    statement_set_option_double::Ptr{Cvoid}
    statement_set_option_int::Ptr{Cvoid}
end

Driver() = Driver(ntuple(_ -> Ptr{Cvoid}(C_NULL), fieldcount(Driver))...)

const DRIVER_1_0_0_SIZE =
    fieldoffset(Driver, findfirst(==(:error_get_detail_count), fieldnames(Driver)))
const DRIVER_1_1_0_SIZE = sizeof(Driver)
const DRIVER_DLOPEN_FLAGS = Libdl.RTLD_LAZY | Libdl.RTLD_LOCAL

isinitialized(x::Union{Database,Connection,Statement}) = x.private_data != C_NULL
isinitialized(x::Partitions) = x.private_data != C_NULL
isinitialized(x::Driver) = x.private_data != C_NULL || x.release != C_NULL
releaseable(x::Union{Error,Partitions}) = x.release != C_NULL
releaseable(x::Driver) = x.release != C_NULL

statusok(status::StatusCode) = status == STATUS_OK
statusok(status::Integer) = status == UInt8(STATUS_OK)
statusname(status::StatusCode) = String(Symbol(status))
statusname(status::Integer) = statusname(StatusCode(UInt8(status)))

function error_message(error::Error)
    error.message == C_NULL && return ""
    return unsafe_string(error.message)
end

struct StatusException <: Exception
    status::StatusCode
    message::String
end

function Base.showerror(io::IO, err::StatusException)
    print(io, "ADBC ", statusname(err.status))
    isempty(err.message) || print(io, ": ", err.message)
end

function assertok(status::Union{StatusCode,Integer}, error::Error=Error())
    code = status isa StatusCode ? status : StatusCode(UInt8(status))
    statusok(code) && return nothing
    throw(StatusException(code, error_message(error)))
end

function driverabisize(version::Integer)
    if version >= ADBC_VERSION_1_1_0
        return DRIVER_1_1_0_SIZE
    elseif version >= ADBC_VERSION_1_0_0
        return DRIVER_1_0_0_SIZE
    else
        throw(ArgumentError("unsupported ADBC driver ABI version: $version"))
    end
end

"""
    Arrow.ADBC.driverinit!(init, driver=Ref(Arrow.ADBC.Driver()); version=Arrow.ADBC.ADBC_VERSION_1_1_0, error=Ref(Arrow.ADBC.Error()))

Call an ADBC `AdbcDriverInitFunc` function pointer and return the initialized
driver table.

The official ABI accepts `(int version, void *driver, AdbcError *error)`. This
wrapper validates the requested ABI version, passes a `Ref{Driver}` through the
same pointer slot, and raises `StatusException` for non-OK ADBC statuses.
"""
function driverinit!(
    init::Ptr{Cvoid},
    driver::Ref{Driver}=Ref(Driver());
    version::Integer=ADBC_VERSION_1_1_0,
    error::Union{Ref{Error},Nothing}=Ref(Error()),
)
    init == C_NULL &&
        throw(ArgumentError("ADBC driver init function pointer must not be C_NULL"))
    driverabisize(version)
    status = if error === nothing
        ccall(
            init,
            UInt8,
            (Cint, Ref{Driver}, Ptr{Error}),
            Cint(version),
            driver,
            Ptr{Error}(C_NULL),
        )
    else
        ccall(init, UInt8, (Cint, Ref{Driver}, Ref{Error}), Cint(version), driver, error)
    end
    assertok(StatusCode(status), error === nothing ? Error() : error[])
    return driver[]
end

mutable struct DriverLibrary
    handle::Ptr{Cvoid}
    path::String
    entrypoint::Symbol
end

struct LoadedDriver
    library::DriverLibrary
    driver::Driver
end

Base.isopen(library::DriverLibrary) = library.handle != C_NULL

function _pascalpart(part::AbstractString)
    text = lowercase(part)
    isempty(text) && return ""
    return uppercasefirst(text)
end

"""
    Arrow.ADBC.defaultdriverentrypoint(path)

Derive the recommended ADBC driver initialization symbol from a shared-library
path.
"""
function defaultdriverentrypoint(path::AbstractString)
    base = basename(path)
    while true
        stem, ext = splitext(base)
        isempty(ext) && break
        base = stem
    end
    startswith(base, "lib") && (base = base[4:end])
    parts = filter(!isempty, split(base, r"[^A-Za-z0-9]+"))
    isempty(parts) &&
        throw(ArgumentError("cannot derive ADBC driver entrypoint from $path"))
    name = join(_pascalpart.(parts))
    startswith(lowercase(name), "adbc") || (name = "Adbc" * name)
    return Symbol(name * "Init")
end

_entrypointsymbol(entrypoint::Symbol) = entrypoint
_entrypointsymbol(entrypoint::AbstractString) = Symbol(entrypoint)

"""
    Arrow.ADBC.opendriver(path; entrypoint=Arrow.ADBC.defaultdriverentrypoint(path), flags=...)

Open an ADBC driver shared library and remember the initialization entrypoint
that should be resolved from it.
"""
function opendriver(
    path::AbstractString;
    entrypoint::Union{Symbol,AbstractString}=defaultdriverentrypoint(path),
    flags::Integer=DRIVER_DLOPEN_FLAGS,
)
    return DriverLibrary(
        Libdl.dlopen(path, flags),
        String(path),
        _entrypointsymbol(entrypoint),
    )
end

"""
    Arrow.ADBC.driverentrypoint(library)

Resolve the configured `AdbcDriverInitFunc` entrypoint from an open driver
library.
"""
function driverentrypoint(library::DriverLibrary)
    Base.isopen(library) || throw(ArgumentError("ADBC driver library is closed"))
    return Libdl.dlsym(library.handle, library.entrypoint)
end

function closedriver!(library::DriverLibrary)
    Base.isopen(library) || return nothing
    Libdl.dlclose(library.handle)
    library.handle = Ptr{Cvoid}(C_NULL)
    return nothing
end

Base.close(library::DriverLibrary) = closedriver!(library)
Base.close(driver::LoadedDriver) = closedriver!(driver.library)

"""
    Arrow.ADBC.loaddriver(path; entrypoint=Arrow.ADBC.defaultdriverentrypoint(path), version=Arrow.ADBC.ADBC_VERSION_1_1_0)

Open an ADBC driver library, resolve its initialization entrypoint, and call
`driverinit!`. The returned `LoadedDriver` keeps the shared library handle alive
next to the initialized driver function table.
"""
function loaddriver(
    path::AbstractString;
    entrypoint::Union{Symbol,AbstractString}=defaultdriverentrypoint(path),
    version::Integer=ADBC_VERSION_1_1_0,
    flags::Integer=DRIVER_DLOPEN_FLAGS,
    error::Union{Ref{Error},Nothing}=Ref(Error()),
)
    library = opendriver(path; entrypoint=entrypoint, flags=flags)
    try
        driver = driverinit!(driverentrypoint(library); version=version, error=error)
        return LoadedDriver(library, driver)
    catch
        closedriver!(library)
        rethrow()
    end
end

"""
    Arrow.ADBC.StatementResult

Lightweight representation of an ADBC statement execution result: an Arrow C
Stream iterator plus the optional rows-affected count returned by ADBC
statement execution APIs.
"""
struct StatementResult{S}
    stream::S
    rows_affected::Int64
end

function _rows_affected(value::Integer)
    value >= -1 || throw(ArgumentError("ADBC rows_affected must be -1 or non-negative"))
    return Int64(value)
end

"""
    Arrow.ADBC.statementresult(stream; rows_affected=-1)

Wrap an imported Arrow C Stream result with the ADBC rows-affected count.
"""
function statementresult(stream; rows_affected::Integer=-1)
    return StatementResult(stream, _rows_affected(rows_affected))
end

"""
    Arrow.ADBC.importstream(stream_ptr; rows_affected=-1)

Borrow an ADBC `ArrowArrayStream` result as an Arrow.jl imported C Stream and
return it with the ADBC rows-affected count.
"""
function importstream(stream_ptr::Ptr{CData.ArrowArrayStream}; rows_affected::Integer=-1)
    return statementresult(CData.importstream(stream_ptr); rows_affected=rows_affected)
end

resultstream(result::StatementResult) = result.stream
rowsaffected(result::StatementResult) = result.rows_affected

end # module ADBC
