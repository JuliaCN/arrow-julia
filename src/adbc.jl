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

export ADBC_VERSION_1_0_0,
    ADBC_VERSION_1_1_0,
    CONNECTION_OPTION_AUTOCOMMIT,
    CONNECTION_OPTION_CURRENT_CATALOG,
    CONNECTION_OPTION_CURRENT_DB_SCHEMA,
    CONNECTION_OPTION_READ_ONLY,
    ERROR_VENDOR_CODE_PRIVATE_DATA,
    Error,
    Database,
    Connection,
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
    error_message,
    isinitialized,
    releaseable,
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

isinitialized(x::Union{Database,Connection,Statement}) = x.private_data != C_NULL
isinitialized(x::Partitions) = x.private_data != C_NULL
releaseable(x::Union{Error,Partitions}) = x.release != C_NULL

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

end # module ADBC
