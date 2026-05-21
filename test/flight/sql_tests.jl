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
    @test_throws ArgumentError Arrow.Flight.SQL.doputupdateresult(-1)
    @test_throws ArgumentError Arrow.Flight.SQL.commanddescriptor("", UInt8[])
end
