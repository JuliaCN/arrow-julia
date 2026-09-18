# Julia Flight client design

This document defines the intended Julia-native Flight client boundary. It is
deliberately not part of the Flight Server MVP: the server can be merged and
operated without shipping a second, partially validated transport stack.

## Goals and non-goals

The client must expose all core Flight RPC shapes, stream Arrow 3 IPC without
materializing complete datasets, support TLS/authentication/deadlines and make
cancellation observable and deterministic. It must interoperate with PyArrow
and other conforming Flight servers.

The first client release will not own query planning, retry non-idempotent
calls, silently downgrade TLS, or add Arrow 2 compatibility. Flight SQL may be
layered on the same transport after the core client lifecycle is stable.

## Ownership boundaries

The client is split into four layers:

1. `FlightClient` owns endpoint configuration, connection lifecycle, shared
   authentication state and transport capabilities.
2. A narrow transport adapter owns gRPC method invocation, metadata, status,
   deadline and cancellation mapping. Generated protocol messages remain the
   transport contract.
3. Flight stream adapters own descriptors, tickets, `FlightData` framing and
   application metadata.
4. Arrow 3 owns IPC schema admission, decoding, encoding, compression,
   dictionaries and Tables.jl values.

The public API must not expose transport-library stream objects. This keeps a
future transport replacement possible without changing Arrow values or Flight
semantics.

## Proposed public lifecycle

```julia
client = Arrow.Flight.connect(
    "grpc+tls://flight.example.com:443";
    tls=Arrow.Flight.ClientTLS(root_certificates="ca.pem"),
    auth=Arrow.Flight.BasicAuth(user, password),
    default_timeout=30,
)

info = Arrow.Flight.getflightinfo(client, descriptor)
for batch in Arrow.Flight.doget(client, info.endpoint[1].ticket)
    # batch is admitted incrementally by Arrow 3
end

close(client)
```

Every call accepts per-call headers, timeout/deadline and a cancellation token.
Streaming calls return closeable objects whose `close` operation cancels the
RPC and releases transport and Arrow buffers. `DoPut` and `DoExchange` use
explicit writer/reader pairs so half-close is represented rather than inferred.

## Concurrency and memory

A client may serve concurrent RPCs. Individual writer objects are single-
producer; readers are single-consumer. Connection state, authentication token
refresh and call identifiers use independent synchronization rather than a
global per-message lock.

Request and response buffers are bounded. Incoming `FlightData` is decoded as
an iterator, and outgoing Tables.jl partitions are encoded incrementally.
Cancellation must wake blocked readers and writers; it may not depend on a
millisecond polling loop. Backpressure is transport-owned and must propagate to
the Arrow producer.

## Security and authentication

TLS verification is enabled for `grpc+tls` endpoints and cannot be disabled by
an unrelated option. The design supports system roots, explicit root bundles,
hostname override for controlled test environments, optional mTLS, and bearer
or basic authentication. Credentials and authorization headers are redacted
from errors and metrics.

## Errors, retries and observability

gRPC status is mapped to `FlightStatusError` with response headers, trailers
and request ID retained. Automatic retries are initially limited to explicitly
idempotent metadata calls and require a retry policy; streaming writes are
never replayed implicitly.

Client metrics cover active calls, bytes/messages, status classes, cancellation,
deadline expiry and connection setup. Metrics are collected with atomics or
shards on message hot paths.

## Delivery gates

The client is ready only after all of the following are executable:

- core unary, server-streaming, client-streaming and bidirectional calls against
  both the Julia server and PyArrow;
- PyArrow minimum/latest compatibility and plaintext/TLS/mTLS coverage;
- bounded-buffer and large-stream RSS receipts;
- cancellation while blocked on read, write and half-close;
- concurrent p95/p99 and long-running connection-reuse soak;
- negative tests for hostname verification, expired/untrusted certificates,
  authentication failure, deadlines and oversized messages.

Until those gates exist, the supported production surface in Arrow 3 remains
the Flight server and protocol/IPC helpers, not a Julia Flight client.
