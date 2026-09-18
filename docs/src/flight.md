# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Arrow Flight

Arrow 3 exposes the Flight protocol as `Arrow.Flight`. The protocol messages,
Flight SQL messages, service dispatcher, and IPC conversion layer are part of
Arrow.jl. The supported server transport is the General-registered
[`gRPCServer.jl`](https://github.com/JuliaIO/gRPCServer.jl) package.

```julia
using Arrow
using gRPCServer

service = Arrow.Flight.Service(
    doget=(context, ticket, response) -> begin
        Arrow.Flight.putflightdata!(
            response,
            (id=Int64[1, 2], value=["one", "two"]);
            close=true,
        )
    end,
)

server = Arrow.Flight.grpcserver_flight_server(
    service;
    host="127.0.0.1",
    port=8815,
    max_receive_message_length=64 * 1024 * 1024,
    max_send_message_length=64 * 1024 * 1024,
    max_concurrent_streams=128,
    max_concurrent_requests=128,
    idle_timeout=300.0,
    request_capacity=4,
    response_capacity=4,
    enable_health_check=true,
)
# Arrow.Flight.stop!(server; timeout=30.0)
```

`gRPCServer` is a weak dependency: load it to activate
`ArrowgRPCServerExt`. Arrow.jl does not pin a transport fork or maintain a
second HTTP/2 listener. Lifecycle, HTTP/2, streaming, cancellation, TLS, and
gRPC status handling remain owned by `gRPCServer.jl`.

All keywords other than `request_capacity` and `response_capacity` are passed
to `gRPCServer.GRPCServer`. In particular, configure production TLS with its
`tls` option and set explicit receive/send message limits: the transport's
default message size can be smaller than a legitimate Arrow record batch.
`gRPCServer` validates these options against the selected HTTP/2 backend and
rejects unsupported settings instead of silently ignoring them; for example,
the default HTTP.jl backend does not implement `max_queued_requests` or the
configuration-level `drain_timeout` (use `stop!(server; timeout=...)`).
Channel capacities bound queued decoded messages; increasing them may improve
throughput but increases per-request memory by roughly the size of the queued
record batches.

Every handler receives an `Arrow.Flight.ServerCallContext` containing request
metadata, request ID, method, authority, peer, TLS state, deadline, trace
context, and the transport payload. Long-running handler loops should call
`Arrow.Flight.checkcall(context)` between expensive units of work. This maps
client cancellation and expired deadlines to the standard gRPC status codes;
Julia tasks performing a blocking external operation still need that operation's
own timeout. Handlers can publish response metadata with
`setresponseheader!` and `setresponsetrailer!` before the stream closes.

## IPC conversion

`Arrow.Flight.putflightdata!` drives Arrow 3's incremental IPC writer one
partition at a time and drains each staged IPC publication into `FlightData`
messages. It publishes the schema and each record batch before requesting the
next Tables.jl partition, so a bounded channel or gRPC stream applies
backpressure to encoding as well as transport. The largest in-flight Arrow
allocation is therefore one source partition, rather than the whole response.
`Arrow.Flight.flightdata` uses the same path but collects the messages for
callers that explicitly need a vector.

`Arrow.Flight.stream` incrementally admits each Flight IPC header/body pair to
Arrow 3's message decoder. The Arrow core owns schema validation, immutable
dictionary snapshots and deltas, compression state, semantic validation, and
facade materialization; the Flight layer owns only transport framing and
application metadata. Construction consumes only the schema message. Each
record batch is requested and decoded by the corresponding iterator pull, so
transport backpressure extends through receive-side decoding.
Call `close(stream)` when abandoning iteration early to release decoder codec
state, pending batches, dictionary snapshots, and retained application metadata
deterministically; reaching end of stream performs the same cleanup.

Incoming `FlightData` is consumed in one pass. Iterator and channel inputs are
not collected or rebuilt into a response-sized IPC byte buffer by `stream`.
Already yielded record bodies are rooted by the returned batch values rather
than by the stream. Current dictionary pools remain live for later batches, as
required by IPC semantics. `table` is intentionally the eager convenience API
and `streambytes` intentionally produces a contiguous IPC stream, so those two
retain whole-result behavior. `stream`, `table`, and `streambytes` accept
`limits=Arrow.Limits(...)`; each path uses one cumulative allocation budget for
its framing, verification, decode, retained metadata, and materialization work.

```julia
source = Tables.partitioner(((id=[1, 2],), (id=[3],)))
messages = Arrow.Flight.flightdata(source; app_metadata=("first", "second"))

batches = collect(Arrow.Flight.stream(messages; include_app_metadata=true))
limits = Arrow.Limits(
    max_metadata_bytes=16 * 1024 * 1024,
    max_body_bytes=64 * 1024 * 1024,
    max_total_allocated_bytes=256 * 1024 * 1024,
    max_messages=10_000,
)
table = Arrow.Flight.table(messages; limits=limits)
```

Prefer `putflightdata!` in server handlers. `flightdata` intentionally retains
all encoded messages and is best suited to small responses, tests, and unary
metadata construction. Incremental publication means an error in a later
partition is reported after earlier valid batches may already have reached the
peer; this is the normal gRPC streaming failure model.

The Flight layer owns only Flight framing, descriptors, and application
metadata. Arrow 3 owns schema inference, dictionary encoding, compression,
buffer validation, and public Julia values. Consequently, removed Arrow 2.x
writer keywords such as `dictencode`, `denseunions`, `largelists`, and
`maxdepth` are not Flight options. Use `Arrow.DictEncode` for dictionary
columns.

For partitioned input, pass schema and column metadata explicitly at the
Flight boundary:

```julia
messages = Arrow.Flight.flightdata(
    source;
    metadata=Dict("dataset" => "example"),
    colmetadata=Dict(:id => Dict("role" => "primary-key")),
)
```

The first partition fixes the stream schema, following the Arrow 3 incremental
writer contract. Later partitions must have the same names and compatible
types. Explicit metadata avoids inferring stream-wide schema metadata from an
arbitrary first partition.

## Production performance receipt

The live benchmark uses the official Julia `gRPCServer` listener and a PyArrow
Flight client for `DoGet`, `DoPut`, reused-client `DoPut`, `DoExchange`, and
concurrent-client runs:

```sh
julia --project=test bench/flight.jl
```

It fails rather than silently skipping when PyArrow is unavailable. Runner-
specific throughput gates can be set with
`ARROW_FLIGHT_PYARROW_<OPERATION>_MIN_THROUGHPUT_MIB_PER_SEC`; concurrent gates
use `ARROW_FLIGHT_PYARROW_CONCURRENT_<OPERATION>_MIN_THROUGHPUT_MIB_PER_SEC`.
Record the runner, Julia/Python versions, workload variables, and output before
turning an observed baseline into a CI threshold.

## API reference

```@autodocs
Modules = [Arrow.Flight, Arrow.Flight.SQL]
Private = false
Order = [:type, :function]
```
