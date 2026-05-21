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

const FLIGHT_SQL_ENDPOINT_REQUIRED_MODULES = String[
    "grpc_tools",
    "google.protobuf.any_pb2",
    "pyarrow",
    "pyarrow.flight",
]

const FLIGHT_SQL_ENDPOINT_SMOKE = raw"""
import base64
import importlib
import json
import pathlib
import sys
import tempfile
import time

import grpc_tools
import pyarrow as pa
import pyarrow.flight as fl
from google.protobuf.any_pb2 import Any
from grpc_tools import protoc

proto_root = pathlib.Path(sys.argv[1])
host = sys.argv[2]
port = int(sys.argv[3])
username = sys.argv[4]
password = sys.argv[5]

generated_root = pathlib.Path(tempfile.mkdtemp(prefix="arrow-julia-flight-sql-"))
grpc_tools_proto = pathlib.Path(grpc_tools.__file__).resolve().parent / "_proto"
rc = protoc.main(
    [
        "grpc_tools.protoc",
        f"-I{proto_root}",
        f"-I{grpc_tools_proto}",
        f"--python_out={generated_root}",
        str(proto_root / "FlightSql.proto"),
    ]
)
if rc != 0:
    raise RuntimeError(f"FlightSql.proto generation failed with status {rc}")
sys.path.insert(0, str(generated_root))
FlightSql_pb2 = importlib.import_module("FlightSql_pb2")

client = fl.FlightClient(
    f"grpc://{host}:{port}",
    generic_options=[("grpc.http2.lookahead_bytes", 0)],
)
basic_auth = base64.b64encode(f"{username}:{password}".encode("utf-8"))
options = fl.FlightCallOptions(
    timeout=30,
    headers=[(b"authorization", b"Basic " + basic_auth)],
)

def pack(message):
    envelope = Any()
    envelope.Pack(message)
    return envelope.SerializeToString()

def unpack(payload, message):
    envelope = Any()
    envelope.ParseFromString(payload)
    if not envelope.Unpack(message):
        raise AssertionError(f"could not unpack {message.DESCRIPTOR.full_name}")
    return message

def body_bytes(result):
    body = result.body if hasattr(result, "body") else result
    return body.to_pybytes() if hasattr(body, "to_pybytes") else bytes(body)

def command_descriptor(message):
    return fl.FlightDescriptor.for_command(pack(message))

def elapsed_ms(start):
    return (time.perf_counter() - start) * 1000.0

metrics = {}

start = time.perf_counter()
query_descriptor = command_descriptor(
    FlightSql_pb2.CommandStatementQuery(
        query="select * from production_flight_sql",
        transaction_id=b"tx-query",
    )
)
query_info = client.get_flight_info(query_descriptor, options=options)
query_table = client.do_get(query_info.endpoints[0].ticket, options=options).read_all()
metrics["query_ms"] = elapsed_ms(start)
assert query_info.total_records == 3
assert query_table.column("id").to_pylist() == [1, 2, 3]
assert query_table.column("label").to_pylist() == ["sql-one", "sql-two", "sql-three"]

start = time.perf_counter()
prepared_results = list(
    client.do_action(
        fl.Action(
            "CreatePreparedStatement",
            pack(
                FlightSql_pb2.ActionCreatePreparedStatementRequest(
                    query="select prepared",
                    transaction_id=b"tx-prepared",
                )
            ),
        ),
        options=options,
    )
)
assert len(prepared_results) == 1
prepared_result = unpack(
    body_bytes(prepared_results[0]),
    FlightSql_pb2.ActionCreatePreparedStatementResult(),
)
assert prepared_result.prepared_statement_handle == b"prepared-handle"
assert prepared_result.dataset_schema

prepared_put_writer, prepared_put_reader = client.do_put(
    command_descriptor(
        FlightSql_pb2.CommandPreparedStatementQuery(
            prepared_statement_handle=prepared_result.prepared_statement_handle,
        )
    ),
    pa.schema([("parameter", pa.int64())]),
    options=options,
)
prepared_put_writer.write_table(pa.table({"parameter": [7]}))
prepared_put_writer.done_writing()
prepared_put_result = body_bytes(prepared_put_reader.read())
prepared_put_writer.close()
prepared_put = FlightSql_pb2.DoPutPreparedStatementResult()
prepared_put.ParseFromString(prepared_put_result)
assert prepared_put.prepared_statement_handle == b"prepared-bound-handle"

prepared_descriptor = command_descriptor(
    FlightSql_pb2.CommandPreparedStatementQuery(
        prepared_statement_handle=prepared_put.prepared_statement_handle,
    )
)
prepared_info = client.get_flight_info(prepared_descriptor, options=options)
prepared_table = client.do_get(prepared_info.endpoints[0].ticket, options=options).read_all()
metrics["prepared_ms"] = elapsed_ms(start)
assert prepared_info.total_records == 1
assert prepared_table.column("id").to_pylist() == [7]
assert prepared_table.column("label").to_pylist() == ["prepared-seven"]

close_results = list(
    client.do_action(
        fl.Action(
            "ClosePreparedStatement",
            pack(
                FlightSql_pb2.ActionClosePreparedStatementRequest(
                    prepared_statement_handle=prepared_put.prepared_statement_handle,
                )
            ),
        ),
        options=options,
    )
)
assert close_results == []

start = time.perf_counter()
ingest_descriptor = command_descriptor(
    FlightSql_pb2.CommandStatementIngest(
        table_definition_options=FlightSql_pb2.CommandStatementIngest.TableDefinitionOptions(
            if_not_exist=FlightSql_pb2.CommandStatementIngest.TableDefinitionOptions.TABLE_NOT_EXIST_OPTION_CREATE,
            if_exists=FlightSql_pb2.CommandStatementIngest.TableDefinitionOptions.TABLE_EXISTS_OPTION_APPEND,
        ),
        table="target_table",
        schema="target_schema",
        catalog="target_catalog",
        temporary=True,
        transaction_id=b"tx-ingest",
        options={"mode": "append"},
    )
)
ingest_writer, ingest_reader = client.do_put(
    ingest_descriptor,
    pa.schema([("id", pa.int64()), ("label", pa.string())]),
    options=options,
)
ingest_writer.write_table(pa.table({"id": [10, 11], "label": ["ten", "eleven"]}))
ingest_writer.done_writing()
ingest_result = FlightSql_pb2.DoPutUpdateResult()
ingest_result.ParseFromString(body_bytes(ingest_reader.read()))
ingest_writer.close()
metrics["ingest_ms"] = elapsed_ms(start)
assert ingest_result.record_count == 2

print(
    json.dumps(
        {
            "ok": True,
            "query_rows": query_table.num_rows,
            "prepared_rows": prepared_table.num_rows,
            "ingested_rows": ingest_result.record_count,
            "query_bytes": query_table.nbytes,
            "prepared_bytes": prepared_table.nbytes,
            **metrics,
        },
        sort_keys=True,
    )
)
"""
