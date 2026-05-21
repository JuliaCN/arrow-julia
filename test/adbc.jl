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

const ADBC = Arrow.ADBC

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
    @test isbitstype(ADBC.Database)
    @test isbitstype(ADBC.Connection)
    @test isbitstype(ADBC.Statement)
    @test isbitstype(ADBC.Partitions)
    @test sizeof(ADBC.Database) == 2 * sizeof(Ptr{Cvoid})
    @test sizeof(ADBC.Connection) == 2 * sizeof(Ptr{Cvoid})
    @test sizeof(ADBC.Statement) == 2 * sizeof(Ptr{Cvoid})

    error = ADBC.Error()
    @test error.vendor_code == ADBC.ERROR_VENDOR_CODE_PRIVATE_DATA
    @test !ADBC.releaseable(error)
    @test ADBC.error_message(error) == ""

    @test !ADBC.isinitialized(ADBC.Database())
    @test !ADBC.isinitialized(ADBC.Connection())
    @test !ADBC.isinitialized(ADBC.Statement())
    @test !ADBC.isinitialized(ADBC.Partitions())
    @test !ADBC.releaseable(ADBC.Partitions())

    @test ADBC.statusok(ADBC.STATUS_OK)
    @test ADBC.statusok(0)
    @test ADBC.statusname(ADBC.STATUS_NOT_IMPLEMENTED) == "STATUS_NOT_IMPLEMENTED"
    @test_throws ADBC.StatusException ADBC.assertok(ADBC.STATUS_NOT_IMPLEMENTED)
end
