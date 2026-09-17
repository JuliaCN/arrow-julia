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

function flight_test_sql_protocol_helpers()
    protocol = Arrow.Flight.Protocol
    descriptor_type = protocol.var"FlightDescriptor.DescriptorType"

    descriptor = Arrow.Flight.SQL.commanddescriptor("CommandStatementQuery", b"select 1")
    @test descriptor.var"#type" == descriptor_type.CMD
    @test isempty(descriptor.path)
    @test Arrow.Flight.SQL.anytypeurl(descriptor.cmd) ==
          "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
    @test Arrow.Flight.SQL.anypayload(descriptor.cmd) == b"select 1"

    typed_query = Arrow.Flight.SQL.Generated.CommandStatementQuery("select 1", UInt8[])
    typed_descriptor = Arrow.Flight.SQL.commanddescriptor(typed_query)
    @test typed_descriptor.var"#type" == descriptor_type.CMD
    @test Arrow.Flight.SQL.anytypeurl(typed_descriptor.cmd) ==
          "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
    @test Arrow.Flight.SQL.anypayload(typed_descriptor.cmd) ==
          Arrow.Flight._protocolbytes(typed_query)

    full_url = "type.example.test/custom.Command"
    custom_descriptor = Arrow.Flight.SQL.commanddescriptor(full_url, UInt8[0x01])
    @test Arrow.Flight.SQL.anytypeurl(custom_descriptor.cmd) == full_url
    @test Arrow.Flight.SQL.anypayload(custom_descriptor.cmd) == UInt8[0x01]

    create_action =
        Arrow.Flight.SQL.action("ActionCreatePreparedStatementRequest", b"select ?")
    @test getfield(create_action, Symbol("#type")) == "CreatePreparedStatement"
    @test Arrow.Flight.SQL.anytypeurl(create_action.body) ==
          "type.googleapis.com/arrow.flight.protocol.sql.ActionCreatePreparedStatementRequest"
    @test Arrow.Flight.SQL.anypayload(create_action.body) == b"select ?"

    typed_action = Arrow.Flight.SQL.action(
        Arrow.Flight.SQL.Generated.ActionCreatePreparedStatementRequest(
            "select ?",
            UInt8[],
        ),
    )
    @test getfield(typed_action, Symbol("#type")) == "CreatePreparedStatement"
    @test Arrow.Flight.SQL.anytypeurl(typed_action.body) ==
          "type.googleapis.com/arrow.flight.protocol.sql.ActionCreatePreparedStatementRequest"

    explicit_action = Arrow.Flight.SQL.action(
        "ActionClosePreparedStatementRequest",
        b"handle";
        type="ClosePreparedStatement",
    )
    @test getfield(explicit_action, Symbol("#type")) == "ClosePreparedStatement"
    @test Arrow.Flight.SQL.anypayload(explicit_action.body) == b"handle"

    update_result = Arrow.Flight.SQL.doputupdateresult(42)
    @test update_result isa protocol.PutResult
    @test Arrow.Flight.SQL.doputupdatecount(update_result) == 42
    @test Arrow.Flight.SQL.doputupdatecount(Arrow.Flight.SQL.doputupdateresult(-1)) == -1
    decoded_update = Arrow.Flight._decodeprotocolbytes(
        Arrow.Flight.SQL.Generated.DoPutUpdateResult,
        update_result.app_metadata,
    )
    @test decoded_update.record_count == 42
    @test_throws ArgumentError Arrow.Flight.SQL.doputupdateresult(-2)
    @test_throws ArgumentError Arrow.Flight.SQL.commanddescriptor("", UInt8[])
end

function assert_flight_sql_command_roundtrip(message)
    descriptor = Arrow.Flight.SQL.commanddescriptor(message)
    any = Arrow.Flight.SQL.decodeany(descriptor.cmd)
    @test any.type_url == Arrow.Flight.SQL.typeurl(String(nameof(typeof(message))))
    decoded = Arrow.Flight._decodeprotocolbytes(typeof(message), any.value)
    @test Arrow.Flight._protocolbytes(decoded) == Arrow.Flight._protocolbytes(message)
end

function assert_flight_sql_action_roundtrip(message, expected_type::AbstractString)
    action = Arrow.Flight.SQL.action(message)
    @test getfield(action, Symbol("#type")) == expected_type
    any = Arrow.Flight.SQL.decodeany(action.body)
    @test any.type_url == Arrow.Flight.SQL.typeurl(String(nameof(typeof(message))))
    decoded = Arrow.Flight._decodeprotocolbytes(typeof(message), any.value)
    @test Arrow.Flight._protocolbytes(decoded) == Arrow.Flight._protocolbytes(message)
end

function flight_sql_command_fixtures()
    generated = Arrow.Flight.SQL.Generated
    return (
        generated.CommandGetCatalogs(),
        generated.CommandGetSqlInfo(UInt32[0, 4, 8]),
        generated.CommandGetTables(
            "catalog",
            "schema%",
            "table%",
            ["BASE TABLE", "VIEW"],
            true,
        ),
        generated.CommandStatementQuery("select * from t", UInt8[0x01]),
        generated.CommandStatementUpdate("update t set x = 1", UInt8[]),
        generated.CommandPreparedStatementQuery(UInt8[0x10, 0x20]),
        generated.CommandPreparedStatementUpdate(UInt8[0x30, 0x40]),
        generated.CommandStatementIngest(
            nothing,
            "target_table",
            "target_schema",
            "target_catalog",
            true,
            UInt8[0x55],
            Dict("mode" => "append"),
        ),
    )
end

function flight_sql_action_fixtures()
    generated = Arrow.Flight.SQL.Generated
    return (
        (
            generated.ActionCreatePreparedStatementRequest("select ?", UInt8[0x01]),
            "CreatePreparedStatement",
        ),
        (
            generated.ActionClosePreparedStatementRequest(UInt8[0x02]),
            "ClosePreparedStatement",
        ),
        (generated.ActionBeginTransactionRequest(), "BeginTransaction"),
        (
            generated.ActionBeginSavepointRequest(UInt8[0x03], "savepoint_1"),
            "BeginSavepoint",
        ),
        (generated.ActionCancelQueryRequest(UInt8[0x04]), "CancelQuery"),
    )
end

function flight_test_sql_stability_fixtures()
    for message in flight_sql_command_fixtures()
        assert_flight_sql_command_roundtrip(message)
    end
    for (message, expected_type) in flight_sql_action_fixtures()
        assert_flight_sql_action_roundtrip(message, expected_type)
    end

    prepared = Arrow.Flight.SQL.doputpreparedstatementresult(UInt8[0xaa, 0xbb])
    @test Arrow.Flight.SQL.doputpreparedstatementhandle(prepared) == UInt8[0xaa, 0xbb]

    @test_throws Exception Arrow.Flight.SQL.decodeany(UInt8[0xff])
    @test_throws Exception Arrow.Flight.SQL.anytypeurl(UInt8[0xff])
    @test_throws Exception Arrow.Flight.SQL.doputupdatecount(
        Arrow.Flight.Protocol.PutResult(UInt8[0xff]),
    )
    @test_throws Exception Arrow.Flight.SQL.doputpreparedstatementhandle(
        Arrow.Flight.Protocol.PutResult(UInt8[0xff]),
    )
    @test_throws ArgumentError Arrow.Flight.SQL.action("ActionRequest", UInt8[])
end
