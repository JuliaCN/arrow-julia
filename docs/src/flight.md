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

server = Arrow.Flight.grpcserver_flight_server(service; host="127.0.0.1", port=8815)
# Arrow.Flight.stop!(server)
```

`gRPCServer` is a weak dependency: load it to activate
`ArrowgRPCServerExt`. Arrow.jl does not pin a transport fork or maintain a
second HTTP/2 listener. Lifecycle, HTTP/2, streaming, cancellation, TLS, and
gRPC status handling remain owned by `gRPCServer.jl`.

## IPC conversion

`Arrow.Flight.putflightdata!` drives Arrow 3's incremental IPC writer one
partition at a time and drains each staged IPC publication into `FlightData`
messages. It publishes the schema and each record batch before requesting the
next Tables.jl partition, so a bounded channel or gRPC stream applies
backpressure to encoding as well as transport. The largest in-flight Arrow
allocation is therefore one source partition, rather than the whole response.
`Arrow.Flight.flightdata` uses the same path but collects the messages for
callers that explicitly need a vector.

`Arrow.Flight.stream` and `Arrow.Flight.table` rebuild a standard IPC stream
and delegate validation and materialization to `Arrow.Stream` and
`Arrow.Table`. `Arrow.Flight.stream` derives its Tables.jl schema directly
from the decoded IPC schema, so it does not parse and materialize a second
`Arrow.Table` merely to discover column types.

Incoming `FlightData` is consumed in one pass. Iterator and channel inputs are
not first collected into a second message vector: framing, schema detection,
and application-metadata extraction happen while the IPC byte buffer is
rebuilt. Arrow 3's validated reader currently opens that complete IPC buffer,
so receive-side byte storage is still proportional to the response; the Flight
adapter no longer adds another response-sized layer of message retention.

```julia
source = Tables.partitioner(((id=[1, 2],), (id=[3],)))
messages = Arrow.Flight.flightdata(source; app_metadata=("first", "second"))

batches = collect(Arrow.Flight.stream(messages; include_app_metadata=true))
table = Arrow.Flight.table(messages)
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

## API reference

```@autodocs
Modules = [Arrow.Flight, Arrow.Flight.SQL]
Private = false
Order = [:type, :function]
```
