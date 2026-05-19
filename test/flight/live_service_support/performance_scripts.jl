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

const FLIGHT_LIVE_PYARROW_DOGET_BENCHMARK = raw"""
import pyarrow.flight as fl
import sys
import time

host = sys.argv[1]
port = int(sys.argv[2])
iterations = int(sys.argv[3])
expected_rows = int(sys.argv[4])
expected_payload = sys.argv[5]
lookahead_bytes = int(sys.argv[6])
path = sys.argv[7:]

client = fl.FlightClient(
    f"grpc://{host}:{port}",
    generic_options=[
        ("grpc.http2.lookahead_bytes", lookahead_bytes),
        ("grpc.http2.bdp_probe", 1),
    ],
)
descriptor = fl.FlightDescriptor.for_path(*path)
info = client.get_flight_info(descriptor)

samples = []
for _ in range(iterations):
    started = time.perf_counter_ns()
    reader = client.do_get(info.endpoints[0].ticket)
    table = reader.read_all()
    finished = time.perf_counter_ns()
    assert table.num_rows == expected_rows
    payload_index = table.schema.get_field_index("payload")
    first_payload_value = table.column(payload_index)[0].as_py()
    assert first_payload_value == expected_payload
    samples.append(finished - started)

samples.sort()
print(samples[(len(samples) - 1) // 2])
"""

const FLIGHT_LIVE_PYARROW_DOPUT_BENCHMARK = raw"""
import pyarrow as pa
import pyarrow.flight as fl
import sys
import time

host = sys.argv[1]
port = int(sys.argv[2])
iterations = int(sys.argv[3])
batch_count = int(sys.argv[4])
rows_per_batch = int(sys.argv[5])
expected_payload = sys.argv[6]
lookahead_bytes = int(sys.argv[7])
path = sys.argv[8:]

descriptor = fl.FlightDescriptor.for_path(*path)
schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("payload", pa.string()),
])

def make_client():
    return fl.FlightClient(
        f"grpc://{host}:{port}",
        generic_options=[
            ("grpc.http2.lookahead_bytes", lookahead_bytes),
            ("grpc.http2.bdp_probe", 1),
        ],
    )

def close_client(client):
    close = getattr(client, "close", None)
    if close is not None:
        close()

def make_batches():
    batches = []
    next_id = 1
    for batch_index in range(batch_count):
        ids = list(range(next_id, next_id + rows_per_batch))
        next_id += rows_per_batch
        batch = pa.record_batch(
            [
                pa.array(ids, type=pa.int64()),
                pa.array([expected_payload] * rows_per_batch, type=pa.string()),
            ],
            schema=schema,
        )
        batches.append((batch, f"perf:{batch_index + 1}".encode("utf-8")))
    return batches

def write_batches(writer, batches):
    for batch, metadata in batches:
        writer.write_with_metadata(batch, metadata)

def run_doput():
    # Keep the large-upload receipt connection-isolated; reused/concurrent
    # upload streams are tracked separately from this single-call baseline.
    client = make_client()
    try:
        writer, reader = client.do_put(descriptor, schema)
        write_batches(writer, make_batches())
        writer.done_writing()
        put_result = reader.read()
        writer.close()
        assert put_result is not None
        assert put_result.to_pybytes() == b"stored"
    finally:
        close_client(client)

samples = []
for _ in range(iterations):
    started = time.perf_counter_ns()
    run_doput()
    finished = time.perf_counter_ns()
    samples.append(finished - started)

samples.sort()
print(samples[(len(samples) - 1) // 2])
"""

const FLIGHT_LIVE_PYARROW_REUSED_DOPUT_BENCHMARK = raw"""
import json
import pyarrow as pa
import pyarrow.flight as fl
import sys
import time

host = sys.argv[1]
port = int(sys.argv[2])
iterations = int(sys.argv[3])
batch_count = int(sys.argv[4])
rows_per_batch = int(sys.argv[5])
expected_payload = sys.argv[6]
lookahead_bytes = int(sys.argv[7])
path = sys.argv[8:]

descriptor = fl.FlightDescriptor.for_path(*path)
schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("payload", pa.string()),
])

def make_client():
    return fl.FlightClient(
        f"grpc://{host}:{port}",
        generic_options=[
            ("grpc.http2.lookahead_bytes", lookahead_bytes),
            ("grpc.http2.bdp_probe", 1),
        ],
    )

def close_client(client):
    close = getattr(client, "close", None)
    if close is not None:
        close()

def make_batches():
    batches = []
    next_id = 1
    for batch_index in range(batch_count):
        ids = list(range(next_id, next_id + rows_per_batch))
        next_id += rows_per_batch
        batch = pa.record_batch(
            [
                pa.array(ids, type=pa.int64()),
                pa.array([expected_payload] * rows_per_batch, type=pa.string()),
            ],
            schema=schema,
        )
        batches.append((batch, f"perf:{batch_index + 1}".encode("utf-8")))
    return batches

def write_batches(writer, batches):
    for batch, metadata in batches:
        writer.write_with_metadata(batch, metadata)

def run_doput(client):
    writer, reader = client.do_put(descriptor, schema)
    try:
        write_batches(writer, make_batches())
        writer.done_writing()
        put_result = reader.read()
    finally:
        writer.close()
    while reader.read() is not None:
        pass
    assert put_result is not None
    assert put_result.to_pybytes() == b"stored"

client = make_client()
try:
    samples = []
    started_all = time.perf_counter_ns()
    for _ in range(iterations):
        started = time.perf_counter_ns()
        run_doput(client)
        finished = time.perf_counter_ns()
        samples.append(finished - started)
    finished_all = time.perf_counter_ns()
finally:
    close_client(client)

samples.sort()
def percentile_index(count: int, numerator: int, denominator: int) -> int:
    if count <= 1:
        return 0
    return min(count - 1, max(0, (count * numerator + denominator - 1) // denominator - 1))

print(json.dumps({
    "total_requests": len(samples),
    "wall_ns": finished_all - started_all,
    "request_median_ns": samples[(len(samples) - 1) // 2],
    "request_p95_ns": samples[percentile_index(len(samples), 95, 100)],
    "request_p99_ns": samples[percentile_index(len(samples), 99, 100)],
    "request_max_ns": max(samples),
}))
"""

const FLIGHT_LIVE_PYARROW_DOEXCHANGE_BENCHMARK = raw"""
import pyarrow as pa
import pyarrow.flight as fl
import sys
import time

host = sys.argv[1]
port = int(sys.argv[2])
iterations = int(sys.argv[3])
batch_count = int(sys.argv[4])
rows_per_batch = int(sys.argv[5])
expected_rows = int(sys.argv[6])
expected_payload = sys.argv[7]
lookahead_bytes = int(sys.argv[8])
path = sys.argv[9:]

client = fl.FlightClient(
    f"grpc://{host}:{port}",
    generic_options=[
        ("grpc.http2.lookahead_bytes", lookahead_bytes),
        ("grpc.http2.bdp_probe", 1),
    ],
)
descriptor = fl.FlightDescriptor.for_path(*path)
schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("payload", pa.string()),
])

def make_batches():
    batches = []
    next_id = 1
    for batch_index in range(batch_count):
        ids = list(range(next_id, next_id + rows_per_batch))
        next_id += rows_per_batch
        batch = pa.record_batch(
            [
                pa.array(ids, type=pa.int64()),
                pa.array([expected_payload] * rows_per_batch, type=pa.string()),
            ],
            schema=schema,
        )
        batches.append((batch, f"perf:{batch_index + 1}".encode("utf-8")))
    return batches

def write_batches(writer, batches):
    for batch, metadata in batches:
        writer.write_with_metadata(batch, metadata)

def run_doexchange():
    writer, reader = client.do_exchange(descriptor)
    writer.begin(schema)
    write_batches(writer, make_batches())
    writer.done_writing()
    table = reader.read_all()
    writer.close()
    assert table.num_rows == expected_rows
    payload_index = table.schema.get_field_index("payload")
    assert table.column(payload_index)[0].as_py() == expected_payload

samples = []
for _ in range(iterations):
    started = time.perf_counter_ns()
    run_doexchange()
    finished = time.perf_counter_ns()
    samples.append(finished - started)

samples.sort()
print(samples[(len(samples) - 1) // 2])
"""

const FLIGHT_LIVE_PYARROW_CONCURRENT_DOGET_BENCHMARK = raw"""
import json
import pyarrow.flight as fl
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event

host = sys.argv[1]
port = int(sys.argv[2])
concurrent_clients = int(sys.argv[3])
requests_per_client = int(sys.argv[4])
expected_rows = int(sys.argv[5])
expected_payload = sys.argv[6]
lookahead_bytes = int(sys.argv[7])
path = sys.argv[8:]

descriptor = fl.FlightDescriptor.for_path(*path)
ready_barrier = Barrier(concurrent_clients + 1)
start_event = Event()

def run_client(_: int) -> list[int]:
    client = fl.FlightClient(
        f"grpc://{host}:{port}",
        generic_options=[
            ("grpc.http2.lookahead_bytes", lookahead_bytes),
            ("grpc.http2.bdp_probe", 1),
        ],
    )
    info = client.get_flight_info(descriptor)
    assert len(info.endpoints) == 1
    ready_barrier.wait()
    start_event.wait()

    samples = []
    for _ in range(requests_per_client):
        started = time.perf_counter_ns()
        reader = client.do_get(info.endpoints[0].ticket)
        table = reader.read_all()
        finished = time.perf_counter_ns()
        assert table.num_rows == expected_rows
        payload_index = table.schema.get_field_index("payload")
        first_payload_value = table.column(payload_index)[0].as_py()
        assert first_payload_value == expected_payload
        samples.append(finished - started)
    return samples

with ThreadPoolExecutor(max_workers=concurrent_clients) as executor:
    futures = [executor.submit(run_client, worker) for worker in range(concurrent_clients)]
    ready_barrier.wait()
    started = time.perf_counter_ns()
    start_event.set()
    worker_samples = [future.result() for future in futures]
    finished = time.perf_counter_ns()

samples = sorted(sample for worker in worker_samples for sample in worker)
def percentile_index(count: int, numerator: int, denominator: int) -> int:
    if count <= 1:
        return 0
    return min(count - 1, max(0, (count * numerator + denominator - 1) // denominator - 1))

print(json.dumps({
    "concurrent_clients": concurrent_clients,
    "requests_per_client": requests_per_client,
    "total_requests": len(samples),
    "wall_ns": finished - started,
    "request_median_ns": samples[(len(samples) - 1) // 2],
    "request_p95_ns": samples[percentile_index(len(samples), 95, 100)],
    "request_p99_ns": samples[percentile_index(len(samples), 99, 100)],
    "request_max_ns": max(samples),
}))
"""

const FLIGHT_LIVE_PYARROW_CONCURRENT_DOPUT_BENCHMARK = raw"""
import json
import pyarrow as pa
import pyarrow.flight as fl
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event

host = sys.argv[1]
port = int(sys.argv[2])
concurrent_clients = int(sys.argv[3])
requests_per_client = int(sys.argv[4])
batch_count = int(sys.argv[5])
rows_per_batch = int(sys.argv[6])
expected_payload = sys.argv[7]
lookahead_bytes = int(sys.argv[8])
path = sys.argv[9:]

descriptor = fl.FlightDescriptor.for_path(*path)
schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("payload", pa.string()),
])
ready_barrier = Barrier(concurrent_clients + 1)
start_event = Event()

def make_client():
    return fl.FlightClient(
        f"grpc://{host}:{port}",
        generic_options=[
            ("grpc.http2.lookahead_bytes", lookahead_bytes),
            ("grpc.http2.bdp_probe", 1),
        ],
    )

def close_client(client):
    close = getattr(client, "close", None)
    if close is not None:
        close()

def make_batches():
    batches = []
    next_id = 1
    for batch_index in range(batch_count):
        ids = list(range(next_id, next_id + rows_per_batch))
        next_id += rows_per_batch
        batch = pa.record_batch(
            [
                pa.array(ids, type=pa.int64()),
                pa.array([expected_payload] * rows_per_batch, type=pa.string()),
            ],
            schema=schema,
        )
        batches.append((batch, f"perf:{batch_index + 1}".encode("utf-8")))
    return batches

def write_batches(writer, batches):
    for batch, metadata in batches:
        writer.write_with_metadata(batch, metadata)

def run_doput():
    client = make_client()
    try:
        writer, reader = client.do_put(descriptor, schema)
        try:
            write_batches(writer, make_batches())
            writer.done_writing()
            put_result = reader.read()
        finally:
            writer.close()
        assert put_result is not None
        assert put_result.to_pybytes() == b"stored"
    finally:
        close_client(client)

def run_client(_: int) -> list[int]:
    ready_barrier.wait()
    start_event.wait()

    samples = []
    for _ in range(requests_per_client):
        started = time.perf_counter_ns()
        run_doput()
        finished = time.perf_counter_ns()
        samples.append(finished - started)
    return samples

with ThreadPoolExecutor(max_workers=concurrent_clients) as executor:
    futures = [executor.submit(run_client, worker) for worker in range(concurrent_clients)]
    ready_barrier.wait()
    started = time.perf_counter_ns()
    start_event.set()
    worker_samples = [future.result() for future in futures]
    finished = time.perf_counter_ns()

samples = sorted(sample for worker in worker_samples for sample in worker)
def percentile_index(count: int, numerator: int, denominator: int) -> int:
    if count <= 1:
        return 0
    return min(count - 1, max(0, (count * numerator + denominator - 1) // denominator - 1))

print(json.dumps({
    "concurrent_clients": concurrent_clients,
    "requests_per_client": requests_per_client,
    "total_requests": len(samples),
    "wall_ns": finished - started,
    "request_median_ns": samples[(len(samples) - 1) // 2],
    "request_p95_ns": samples[percentile_index(len(samples), 95, 100)],
    "request_p99_ns": samples[percentile_index(len(samples), 99, 100)],
    "request_max_ns": max(samples),
}))
"""

const FLIGHT_LIVE_PYARROW_CONCURRENT_DOEXCHANGE_BENCHMARK = raw"""
import json
import pyarrow as pa
import pyarrow.flight as fl
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event

host = sys.argv[1]
port = int(sys.argv[2])
concurrent_clients = int(sys.argv[3])
requests_per_client = int(sys.argv[4])
batch_count = int(sys.argv[5])
rows_per_batch = int(sys.argv[6])
expected_rows = int(sys.argv[7])
expected_payload = sys.argv[8]
lookahead_bytes = int(sys.argv[9])
path = sys.argv[10:]

descriptor = fl.FlightDescriptor.for_path(*path)
schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("payload", pa.string()),
])
ready_barrier = Barrier(concurrent_clients + 1)
start_event = Event()

def make_client():
    return fl.FlightClient(
        f"grpc://{host}:{port}",
        generic_options=[
            ("grpc.http2.lookahead_bytes", lookahead_bytes),
            ("grpc.http2.bdp_probe", 1),
        ],
    )

def close_client(client):
    close = getattr(client, "close", None)
    if close is not None:
        close()

def make_batches():
    batches = []
    next_id = 1
    for batch_index in range(batch_count):
        ids = list(range(next_id, next_id + rows_per_batch))
        next_id += rows_per_batch
        batch = pa.record_batch(
            [
                pa.array(ids, type=pa.int64()),
                pa.array([expected_payload] * rows_per_batch, type=pa.string()),
            ],
            schema=schema,
        )
        batches.append((batch, f"perf:{batch_index + 1}".encode("utf-8")))
    return batches

def write_batches(writer, batches):
    for batch, metadata in batches:
        writer.write_with_metadata(batch, metadata)

def run_exchange(client):
    writer, reader = client.do_exchange(descriptor)
    try:
        writer.begin(schema)
        write_batches(writer, make_batches())
        writer.done_writing()
        table = reader.read_all()
    finally:
        writer.close()
    assert table.num_rows == expected_rows
    payload_index = table.schema.get_field_index("payload")
    assert table.column(payload_index)[0].as_py() == expected_payload

def run_client(_: int) -> list[int]:
    client = make_client()
    try:
        ready_barrier.wait()
        start_event.wait()

        samples = []
        for _ in range(requests_per_client):
            started = time.perf_counter_ns()
            run_exchange(client)
            finished = time.perf_counter_ns()
            samples.append(finished - started)
        return samples
    finally:
        close_client(client)

with ThreadPoolExecutor(max_workers=concurrent_clients) as executor:
    futures = [executor.submit(run_client, worker) for worker in range(concurrent_clients)]
    ready_barrier.wait()
    started = time.perf_counter_ns()
    start_event.set()
    worker_samples = [future.result() for future in futures]
    finished = time.perf_counter_ns()

samples = sorted(sample for worker in worker_samples for sample in worker)
def percentile_index(count: int, numerator: int, denominator: int) -> int:
    if count <= 1:
        return 0
    return min(count - 1, max(0, (count * numerator + denominator - 1) // denominator - 1))

print(json.dumps({
    "concurrent_clients": concurrent_clients,
    "requests_per_client": requests_per_client,
    "total_requests": len(samples),
    "wall_ns": finished - started,
    "request_median_ns": samples[(len(samples) - 1) // 2],
    "request_p95_ns": samples[percentile_index(len(samples), 95, 100)],
    "request_p99_ns": samples[percentile_index(len(samples), 99, 100)],
    "request_max_ns": max(samples),
}))
"""
