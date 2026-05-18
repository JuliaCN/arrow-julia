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

const FLIGHT_LIVE_PYARROW_SMOKE = raw"""
import base64
import pyarrow as pa
import pyarrow.flight as fl
import sys

host = sys.argv[1]
port = int(sys.argv[2])
username = sys.argv[3]
password = sys.argv[4]
path = sys.argv[5:]

client = fl.FlightClient(
    f"grpc://{host}:{port}",
    generic_options=[("grpc.http2.lookahead_bytes", 0)],
)
basic_auth = base64.b64encode(f"{username}:{password}".encode("utf-8"))
options = fl.FlightCallOptions(
    timeout=30,
    headers=[(b"authorization", b"Basic " + basic_auth)],
)
descriptor = fl.FlightDescriptor.for_path(*path)

def _encode_varint(value):
    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7f) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)

def _decode_varint(payload, offset=0):
    shift = 0
    value = 0
    while True:
        byte = payload[offset]
        offset += 1
        value |= (byte & 0x7f) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7

def _cancel_flight_info_request_body(info):
    serialized = info.serialize()
    serialized_info = (
        serialized.to_pybytes()
        if hasattr(serialized, "to_pybytes")
        else bytes(serialized)
    )
    return b"\x0a" + _encode_varint(len(serialized_info)) + serialized_info

def _assert_cancelled_result(result):
    body = result.body.to_pybytes()
    tag, offset = _decode_varint(body)
    status, offset = _decode_varint(body, offset)
    assert tag == 0x08
    assert status == 1
    assert offset == len(body)

flights = list(client.list_flights(options=options))
assert len(flights) == 1
assert flights[0].total_records == 3
assert [part.decode("utf-8") for part in flights[0].descriptor.path] == path

actions = list(client.list_actions(options=options))
assert [action.type for action in actions] == ["ping", "CancelFlightInfo"]

results = list(client.do_action(fl.Action("ping", b""), options=options))
assert len(results) == 1
assert results[0].body.to_pybytes() == b"pong"

info = client.get_flight_info(descriptor, options=options)
assert info.total_records == 3
assert [part.decode("utf-8") for part in info.descriptor.path] == path
assert len(info.endpoints) == 1

cancel_results = list(client.do_action(
    fl.Action("CancelFlightInfo", _cancel_flight_info_request_body(info)),
    options=options,
))
assert len(cancel_results) == 1
_assert_cancelled_result(cancel_results[0])

schema = client.get_schema(descriptor, options=options).schema
assert schema.names == ["id", "name"]
assert str(schema.field("id").type) == "int64"
assert str(schema.field("name").type) == "string"

reader = client.do_get(info.endpoints[0].ticket, options=options)
table = reader.read_all()
assert table.column("id").to_pylist() == [1, 2, 3]
assert table.column("name").to_pylist() == ["one", "two", "three"]
assert table.schema.metadata[b"dataset"] == b"native"
assert table.schema.field("name").metadata[b"lang"] == b"en"

put_schema = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("name", pa.string(), metadata={b"lang": b"en"}),
    ],
    metadata={b"dataset": b"native"},
)
put_writer, put_reader = client.do_put(descriptor, put_schema, options=options)
put_writer.write_with_metadata(
    pa.record_batch(
        [pa.array([1, 2], type=pa.int64()), pa.array(["one", "two"])],
        schema=put_schema,
    ),
    b"put:0",
)
put_writer.write_with_metadata(
    pa.record_batch(
        [pa.array([3], type=pa.int64()), pa.array(["three"])],
        schema=put_schema,
    ),
    b"put:1",
)
put_writer.done_writing()
put_result = put_reader.read()
put_writer.close()
assert put_result.to_pybytes() == b"stored"

exchange_schema = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("name", pa.string(), metadata={b"lang": b"exchange"}),
    ],
    metadata={b"dataset": b"exchange"},
)
exchange_writer, exchange_reader = client.do_exchange(descriptor, options=options)
exchange_writer.begin(exchange_schema)
exchange_writer.write_with_metadata(
    pa.record_batch(
        [pa.array([10], type=pa.int64()), pa.array(["ten"])],
        schema=exchange_schema,
    ),
    b"exchange:0",
)
exchange_writer.done_writing()
exchange_chunk = exchange_reader.read_chunk()
exchange_writer.close()
assert exchange_chunk.data.column(0).to_pylist() == [10]
assert exchange_chunk.data.column(1).to_pylist() == ["ten"]
assert exchange_chunk.data.schema.metadata[b"dataset"] == b"exchange"
assert exchange_chunk.data.schema.field("name").metadata[b"lang"] == b"exchange"
assert exchange_chunk.app_metadata.to_pybytes() == b"exchange:0"
"""

const FLIGHT_LIVE_PYARROW_READONLY_SMOKE = raw"""
import base64
import pyarrow.flight as fl
import sys

host = sys.argv[1]
port = int(sys.argv[2])
username = sys.argv[3]
password = sys.argv[4]
path = sys.argv[5:]

client = fl.FlightClient(f"grpc://{host}:{port}")
basic_auth = base64.b64encode(f"{username}:{password}".encode("utf-8"))
options = fl.FlightCallOptions(
    timeout=30,
    headers=[(b"authorization", b"Basic " + basic_auth)],
)
descriptor = fl.FlightDescriptor.for_path(*path)

def _encode_varint(value):
    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7f) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)

def _decode_varint(payload, offset=0):
    shift = 0
    value = 0
    while True:
        byte = payload[offset]
        offset += 1
        value |= (byte & 0x7f) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7

def _cancel_flight_info_request_body(info):
    serialized = info.serialize()
    serialized_info = (
        serialized.to_pybytes()
        if hasattr(serialized, "to_pybytes")
        else bytes(serialized)
    )
    return b"\x0a" + _encode_varint(len(serialized_info)) + serialized_info

def _assert_cancelled_result(result):
    body = result.body.to_pybytes()
    tag, offset = _decode_varint(body)
    status, offset = _decode_varint(body, offset)
    assert tag == 0x08
    assert status == 1
    assert offset == len(body)

flights = list(client.list_flights(options=options))
assert len(flights) == 1
assert flights[0].total_records == 3
assert [part.decode("utf-8") for part in flights[0].descriptor.path] == path

actions = list(client.list_actions(options=options))
assert [action.type for action in actions] == ["ping", "CancelFlightInfo"]

results = list(client.do_action(fl.Action("ping", b""), options=options))
assert len(results) == 1
assert results[0].body.to_pybytes() == b"pong"

info = client.get_flight_info(descriptor, options=options)
assert info.total_records == 3
assert [part.decode("utf-8") for part in info.descriptor.path] == path
assert len(info.endpoints) == 1

cancel_results = list(client.do_action(
    fl.Action("CancelFlightInfo", _cancel_flight_info_request_body(info)),
    options=options,
))
assert len(cancel_results) == 1
_assert_cancelled_result(cancel_results[0])

schema = client.get_schema(descriptor, options=options).schema
assert schema.names == ["id", "name"]
assert str(schema.field("id").type) == "int64"
assert str(schema.field("name").type) == "string"

reader = client.do_get(info.endpoints[0].ticket, options=options)
table = reader.read_all()
assert table.column("id").to_pylist() == [1, 2, 3]
assert table.column("name").to_pylist() == ["one", "two", "three"]
assert table.schema.metadata[b"dataset"] == b"native"
assert table.schema.field("name").metadata[b"lang"] == b"en"
"""

const FLIGHT_LIVE_PYTHON_POLL_SMOKE = raw"""
import base64
import pathlib
import sys
import tempfile

import grpc
import grpc_tools
from grpc_tools import protoc

proto_root = pathlib.Path(sys.argv[1])
grpc_tools_proto = pathlib.Path(grpc_tools.__file__).resolve().parent / "_proto"
host = sys.argv[2]
port = int(sys.argv[3])
username = sys.argv[4]
password = sys.argv[5]
path = sys.argv[6:]

out = pathlib.Path(tempfile.mkdtemp(prefix="flight_poll_client_proto_"))
result = protoc.main([
    "grpc_tools.protoc",
    f"-I{proto_root}",
    f"-I{grpc_tools_proto}",
    f"--python_out={out}",
    f"--grpc_python_out={out}",
    str(proto_root / "Flight.proto"),
])
if result != 0:
    raise RuntimeError(f"protoc failed with exit code {result}")
sys.path.insert(0, str(out))

import Flight_pb2 as pb2
import Flight_pb2_grpc as pb2_grpc

channel = grpc.insecure_channel(f"{host}:{port}")
stub = pb2_grpc.FlightServiceStub(channel)
basic_auth = base64.b64encode(f"{username}:{password}".encode("utf-8"))
metadata = (("authorization", "Basic " + basic_auth.decode("ascii")),)

descriptor = pb2.FlightDescriptor(type=pb2.FlightDescriptor.PATH, path=path)
first = stub.PollFlightInfo(descriptor, metadata=metadata, timeout=30)
assert round(first.progress, 6) == 0.5
assert tuple(first.info.flight_descriptor.path) == tuple(path)
assert tuple(first.flight_descriptor.path) == tuple(path + ["retry"])
assert first.info.total_records == 3

second = stub.PollFlightInfo(first.flight_descriptor, metadata=metadata, timeout=30)
assert round(second.progress, 6) == 1.0
assert not second.HasField("flight_descriptor")
assert tuple(second.info.flight_descriptor.path) == tuple(path)
assert second.info.total_records == 3
"""

const FLIGHT_LIVE_PYTHON_HANDSHAKE_SMOKE = raw"""
import pathlib
import sys
import tempfile

import grpc
import grpc_tools
from grpc_tools import protoc

proto_root = pathlib.Path(sys.argv[1])
grpc_tools_proto = pathlib.Path(grpc_tools.__file__).resolve().parent / "_proto"
host = sys.argv[2]
port = int(sys.argv[3])
token_payload = sys.argv[4].encode("utf-8")

out = pathlib.Path(tempfile.mkdtemp(prefix="flight_handshake_client_proto_"))
result = protoc.main([
    "grpc_tools.protoc",
    f"-I{proto_root}",
    f"-I{grpc_tools_proto}",
    f"--python_out={out}",
    f"--grpc_python_out={out}",
    str(proto_root / "Flight.proto"),
])
if result != 0:
    raise RuntimeError(f"protoc failed with exit code {result}")
sys.path.insert(0, str(out))

import Flight_pb2 as pb2
import Flight_pb2_grpc as pb2_grpc

channel = grpc.insecure_channel(f"{host}:{port}")
stub = pb2_grpc.FlightServiceStub(channel)

responses = list(stub.Handshake(
    iter([pb2.HandshakeRequest(protocol_version=0, payload=token_payload)]),
    timeout=30,
))
assert len(responses) == 1
assert responses[0].protocol_version == 0
assert responses[0].payload == token_payload

actions = list(stub.ListActions(
    pb2.Empty(),
    metadata=(("auth-token-bin", responses[0].payload),),
    timeout=30,
))
assert [action.type for action in actions] == ["ping", "CancelFlightInfo"]
"""
