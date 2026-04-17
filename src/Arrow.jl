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

"""
    Arrow.jl

A pure Julia implementation of the [apache arrow](https://arrow.apache.org/) memory format specification.

This implementation supports the 1.0 version of the specification, including support for:
  * All primitive data types
  * All nested data types
  * Dictionary encodings, nested dictionary encodings, and messages
  * Extension types
  * Streaming, file, record batch, and replacement and isdelta dictionary messages
  * Buffer compression/decompression via the standard LZ4 frame and Zstd formats

It currently doesn't include support for:
  * Tensors or sparse tensors
  * C data interface

Flight RPC status:
  * Experimental `Arrow.Flight` support is available in-tree
  * Requires Julia `1.12+`
  * Includes generated protocol bindings in the base package while leaving the legacy gRPC-backed `FlightService` client constructors and RPC transport behind an optional `gRPCClient.jl` extension
  * Keeps the top-level Flight module shell thin, with exports and generated-protocol setup split out of `src/flight/Flight.jl`
  * Includes high-level `FlightData <-> Arrow IPC` helpers for `Arrow.Table`, `Arrow.Stream`, and DoPut payload generation
  * Keeps the Flight IPC conversion layer modular under `src/flight/convert/`, with `src/flight/convert.jl` retained as a thin entrypoint
  * Includes transport-neutral client helpers for request headers, binary metadata, and URI parsing in the base package, while the legacy gRPC-backed handshake token reuse, TLS configuration, and remote RPC methods load only when the optional `gRPCClient.jl` extension is present for downstream compatibility
  * Keeps the Flight client compatibility layer modular under `src/flight/client/`, with the base shell retained in `src/flight/client.jl` and the gRPC-backed runtime loaded through `ext/ArrowFlightgRPCClientExt.jl`
  * Includes a transport-agnostic server core (`Service`, `ServerCallContext`, `ServiceDescriptor`, `MethodDescriptor`) for local Flight method dispatch, path lookup, handler testing, packaged backend capability checks through `flight_server_backend_capabilities(...)`, and shared gRPC-over-HTTP/2 framing helpers for lower-level backends
  * Keeps the transport-agnostic server core modular under `src/flight/server/`, with `src/flight/server.jl` retained as a thin entrypoint
  * Includes built-in `PureHTTP2.jl` transport helpers in the Flight server core for package-owned h2c listeners, unary RPCs, client-streaming, server-streaming, and live bidirectional `DoExchange` gRPC-over-HTTP/2 handling through `Flight.purehttp2_flight_server(...)`
  * Treats the built-in `:purehttp2` transport as the only default packaged live Flight backend profile while also exposing an optional weakdep-backed `Nghttp2Wrapper.jl` listener through `Flight.nghttp2_flight_server(...)` for unary plus buffered server-streaming gRPC-over-HTTP/2 proofs
  * Includes package-owned live Python-client coverage for authenticated `ListFlights`, `GetFlightInfo`, `GetSchema`, `DoGet`, `DoPut`, `DoExchange`, `ListActions`, and `DoAction`
  * Keeps targeted Flight verification modular under `test/flight/`, with `test/flight.jl` retained as the shared default entrypoint for generated protocol, server-core, and IPC coverage, and dedicated listener proofs isolated in the PureHTTP2/nghttp2 runner files
  * Includes `test/flight_purehttp2.jl` as the PureHTTP2-first temporary-environment runner for shared Flight interop coverage plus package-owned listener proofs
  * Includes `test/flight_purehttp2_perf.jl` as a focused large-transport runner for package-owned `PureHTTP2` `DoGet` measurement, `test/flight_nghttp2_probe.jl` as a substrate probe for the C-wrapper hook surface, and `test/flight_nghttp2.jl` as the focused weakdep-backed nghttp2 listener plus large-transport comparison runner
  * The current `Nghttp2Wrapper.jl` backend proves package-local unary plus buffered server-streaming Flight methods with trailer-borne `grpc-status`, while `Handshake`, `DoPut`, and `DoExchange` remain explicitly unsupported on that backend
  * `Handshake` token propagation and `PollFlightInfo` currently remain server-core/local proofs because the current external Python client surfaces used in tests do not cover those contracts directly
  * Dedicated CI jobs now exercise the Flight interop suite on stable and nightly Linux through the PureHTTP2-first runner; the built-in PureHTTP2 server substrate is the package-owned live backend direction, including Python-client smoke proofs on the same listener surface

Third-party data formats:
  * csv and parquet support via the existing [CSV.jl](https://github.com/JuliaData/CSV.jl) and [Parquet.jl](https://github.com/JuliaIO/Parquet.jl) packages
  * Other [Tables.jl](https://github.com/JuliaData/Tables.jl)-compatible packages automatically supported ([DataFrames.jl](https://github.com/JuliaData/DataFrames.jl), [JSONTables.jl](https://github.com/JuliaData/JSONTables.jl), [JuliaDB.jl](https://github.com/JuliaData/JuliaDB.jl), [SQLite.jl](https://github.com/JuliaDatabases/SQLite.jl), [MySQL.jl](https://github.com/JuliaDatabases/MySQL.jl), [JDBC.jl](https://github.com/JuliaDatabases/JDBC.jl), [ODBC.jl](https://github.com/JuliaDatabases/ODBC.jl), [XLSX.jl](https://github.com/felipenoris/XLSX.jl), etc.)
  * No current Julia packages support ORC or Avro data formats

See docs for official Arrow.jl API with the [User Manual](@ref) and reference docs for [`Arrow.Table`](@ref), [`Arrow.write`](@ref), and [`Arrow.Stream`](@ref).
"""
module Arrow

using Base.Iterators
using Mmap
import Dates
using DataAPI,
    Tables,
    SentinelArrays,
    PooledArrays,
    JSON3,
    CodecLz4,
    CodecZstd,
    TimeZones,
    BitIntegers,
    ConcurrentUtilities,
    StringViews

export ArrowTypes, Flight

using Base: @propagate_inbounds
import Base: ==

const FILE_FORMAT_MAGIC_BYTES = b"ARROW1"
const CONTINUATION_INDICATOR_BYTES = 0xffffffff
const TENSOR_UNSUPPORTED = "Tensor messages are not supported yet"
const SPARSE_TENSOR_UNSUPPORTED = "SparseTensor messages are not supported yet"

# vendored flatbuffers code for now
include("FlatBuffers/FlatBuffers.jl")
using .FlatBuffers

include("metadata/Flatbuf.jl")
using .Flatbuf
const Meta = Flatbuf

using ArrowTypes
include("utils.jl")
include("logicaltypes.jl")
include("arraytypes/arraytypes.jl")
include("eltypes.jl")
include("logicaltypes_builtin.jl")
include("table.jl")
include("metadata/overlay.jl")
include("write.jl")
include("append.jl")
include("show.jl")
include("flight/Flight.jl")

const ZSTD_COMPRESSOR = Lockable{ZstdCompressor}[]
const ZSTD_DECOMPRESSOR = Lockable{ZstdDecompressor}[]
const LZ4_FRAME_COMPRESSOR = Lockable{LZ4FrameCompressor}[]
const LZ4_FRAME_DECOMPRESSOR = Lockable{LZ4FrameDecompressor}[]

function init_zstd_compressor()
    zstd = ZstdCompressor(; level=3)
    CodecZstd.TranscodingStreams.initialize(zstd)
    return Lockable(zstd)
end

function init_zstd_decompressor()
    zstd = ZstdDecompressor()
    CodecZstd.TranscodingStreams.initialize(zstd)
    return Lockable(zstd)
end

function init_lz4_frame_compressor()
    lz4 = LZ4FrameCompressor(; compressionlevel=4)
    CodecLz4.TranscodingStreams.initialize(lz4)
    return Lockable(lz4)
end

function init_lz4_frame_decompressor()
    lz4 = LZ4FrameDecompressor()
    CodecLz4.TranscodingStreams.initialize(lz4)
    return Lockable(lz4)
end

function access_threaded(f, v::Vector)
    tid = Threads.threadid()
    0 < tid <= length(v) || _length_assert()
    if @inbounds isassigned(v, tid)
        @inbounds x = v[tid]
    else
        x = f()
        @inbounds v[tid] = x
    end
    return x
end
@noinline _length_assert() = @assert false "0 < tid <= v"

zstd_compressor() = access_threaded(init_zstd_compressor, ZSTD_COMPRESSOR)
zstd_decompressor() = access_threaded(init_zstd_decompressor, ZSTD_DECOMPRESSOR)
lz4_frame_compressor() = access_threaded(init_lz4_frame_compressor, LZ4_FRAME_COMPRESSOR)
lz4_frame_decompressor() =
    access_threaded(init_lz4_frame_decompressor, LZ4_FRAME_DECOMPRESSOR)

function __init__()
    nt = @static if isdefined(Base.Threads, :maxthreadid)
        Threads.maxthreadid()
    else
        Threads.nthreads()
    end
    resize!(empty!(LZ4_FRAME_COMPRESSOR), nt)
    resize!(empty!(ZSTD_COMPRESSOR), nt)
    resize!(empty!(LZ4_FRAME_DECOMPRESSOR), nt)
    resize!(empty!(ZSTD_DECOMPRESSOR), nt)
    return
end

end  # module Arrow
