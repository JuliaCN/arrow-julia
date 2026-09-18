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

# Incremental IPC framing for non-seekable IO.  The canonical message decoder
# remains the sole owner of schema, dictionary, compression, and semantic
# state; this adapter only turns an IO byte stream into FramedMessage values.

function _read_ipc_io_exact(
    io::IO,
    size::Int,
    budget::AllocationBudget,
    what::AbstractString;
    allow_clean_eof::Bool=false,
)
    _chargevector!(budget, UInt8, size, what)
    bytes = Vector{UInt8}(undef, size)
    count = readbytes!(io, bytes, size)
    count == size && return bytes
    allow_clean_eof && count == 0 && return nothing
    throw(ValidationError("truncated $what: got $count of $size bytes"))
end

function _read_ipc_io_message(io::IO, limits::Limits, budget::AllocationBudget)
    prefix = _read_ipc_io_exact(io, 8, budget, "IPC prefix"; allow_clean_eof=true)
    prefix === nothing && return nothing
    continuation = ltoh(reinterpret(UInt32, @view(prefix[1:4]))[1])
    continuation == CONTINUATION ||
        throw(ValidationError("missing continuation marker in IPC stream"))
    metalen = Int64(ltoh(reinterpret(Int32, @view(prefix[5:8]))[1]))
    metalen == 0 && return :eos
    0 < metalen <= limits.max_metadata_bytes || throw(
        ValidationError(
            "metadata length $metalen outside (0, $(limits.max_metadata_bytes)]",
        ),
    )
    metalen % 8 == 0 ||
        throw(ValidationError("metadata length $metalen is not 8-byte aligned"))
    metadata = _read_ipc_io_exact(io, Int(metalen), budget, "IPC metadata")
    version, header_type, features, _ =
        _verify_ipc_metadata_budgeted(metadata, limits, budget)
    message = FB.getrootas(Meta.Message, metadata, 0)
    bodylen = Int64(message.bodyLength)
    0 <= bodylen <= limits.max_body_bytes ||
        throw(ValidationError("body length $bodylen outside [0, $(limits.max_body_bytes)]"))
    bodylen % 8 == 0 || throw(ValidationError("body length $bodylen is not 8-byte aligned"))
    body = _read_ipc_io_exact(io, Int(bodylen), budget, "IPC body")
    region = heapregion(body)
    return FramedMessage(
        message,
        BufferSlice(region, 0, region.len),
        version,
        header_type,
        features,
    )
end

mutable struct IncrementalIPCStream <: AC.RecordBatchSource
    io::IO
    decoder::IPCMessageDecoder
    limits::Limits
    budget::AllocationBudget
    exhausted::Bool
    @atomic pulling::Bool
end

function IncrementalIPCStream(io::IO, limits::Limits, budget::AllocationBudget)
    _requirelittleendian()
    first_message = _read_ipc_io_message(io, limits, budget)
    first_message === nothing && throw(ValidationError("empty IPC stream"))
    first_message === :eos && throw(ValidationError("empty IPC stream"))
    decoder = IPCMessageDecoder(first_message, limits, budget)
    return IncrementalIPCStream(io, decoder, limits, budget, false, false)
end

AC.schema(stream::IncrementalIPCStream) = stream.decoder.schema

function Base.close(stream::IncrementalIPCStream)
    stream.exhausted = true
    close(stream.decoder)
    empty!(stream.decoder.batchslots)
    empty!(stream.decoder.pending)
    empty!(stream.decoder.dictionaries)
    empty!(stream.decoder.validated_dictionaries)
    return nothing
end

function AC.nextbatch!(stream::IncrementalIPCStream)
    _, ok = @atomicreplace stream.pulling false => true
    ok || throw(
        Base.ConcurrencyViolationError(
            "IncrementalIPCStream supports only one active nextbatch! call",
        ),
    )
    try
        while true
            ready = takebatch!(stream.decoder)
            ready === nothing || return first(ready)
            stream.exhausted && return nothing
            message = _read_ipc_io_message(stream.io, stream.limits, stream.budget)
            if message === :eos
                bytesavailable(stream.io) == 0 ||
                    throw(ValidationError("trailing bytes after IPC end-of-stream"))
                finish!(stream.decoder)
                stream.exhausted = true
                continue
            elseif message === nothing
                finish!(stream.decoder)
                stream.exhausted = true
                continue
            end
            pushmessage!(stream.decoder, message)
        end
    catch
        close(stream)
        rethrow()
    finally
        @atomic :release stream.pulling = false
    end
end
