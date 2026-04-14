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

using Sockets

const GRPCSERVER_EXTENSION_PYARROW_SMOKE = raw"""
import base64
import pyarrow as pa
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

flights = list(client.list_flights(options=options))
assert len(flights) == 1
assert flights[0].total_records == 3
assert [part.decode("utf-8") for part in flights[0].descriptor.path] == path

actions = list(client.list_actions(options=options))
assert len(actions) == 1
assert actions[0].type == "ping"

results = list(client.do_action(fl.Action("ping", b""), options=options))
assert len(results) == 1
assert results[0].body.to_pybytes() == b"pong"

info = client.get_flight_info(descriptor, options=options)
assert info.total_records == 3
assert [part.decode("utf-8") for part in info.descriptor.path] == path
assert len(info.endpoints) == 1

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

function grpcserver_extension_live_port()
    socket = Sockets.listen(parse(Sockets.IPv4, "127.0.0.1"), 0)
    _, port = getsockname(socket)
    close(socket)
    return Int(port)
end

function wait_for_grpcserver_extension_live_server(host::AbstractString, port::Integer)
    deadline = time() + 5.0
    last_error = nothing

    while time() < deadline
        try
            socket = Sockets.connect(parse(Sockets.IPv4, host), port)
            close(socket)
            return
        catch err
            last_error = err
        end
        sleep(0.05)
    end

    detail =
        isnothing(last_error) ? "unknown readiness failure" : sprint(showerror, last_error)
    error(
        "gRPCServer Flight test server did not become ready on $(host):$(port): $(detail)",
    )
end

function with_grpcserver_extension_live_server(f::F, grpcserver, service) where {F}
    host = "127.0.0.1"
    port = grpcserver_extension_live_port()
    server = grpcserver.GRPCServer(host, port)
    grpcserver.register!(server, service)
    grpcserver.start!(server)

    try
        wait_for_grpcserver_extension_live_server(host, port)
        return f(server, host, port)
    finally
        grpcserver.stop!(server; force=true)
    end
end

function grpcserver_extension_live_pyarrow_smoke(
    host::AbstractString,
    port::Integer,
    fixture,
)
    python = FlightTestSupport.pyarrow_flight_python()
    isnothing(python) && return false
    run(
        Cmd([
            python,
            "-c",
            GRPCSERVER_EXTENSION_PYARROW_SMOKE,
            host,
            string(port),
            fixture.handshake_username,
            fixture.handshake_password,
            fixture.descriptor.path...,
        ]),
    )
    return true
end
