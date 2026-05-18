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

# primitives
function build(
    f::Meta.Field,
    ::L,
    batch,
    rb,
    de,
    nodeidx,
    bufferidx,
    varbufferidx,
    convert,
) where {L}
    @debug "building array: L = $L"
    validity = buildbitmap(batch, rb, nodeidx, bufferidx)
    bufferidx += 1
    buffer = rb.buffers[bufferidx]
    meta = buildmetadata(f.custom_metadata)
    # get storage type (non-converted)
    T = juliaeltype(f, nothing, false)
    @debug "storage type for primitive: T = $T"
    bytes, A = reinterp(Base.nonmissingtype(T), batch, buffer, rb.compression)
    len = rb.nodes[nodeidx].length
    T = juliaeltype(f, meta, convert)
    @debug "final julia type for primitive: T = $T"
    return Primitive(T, bytes, validity, A, len, meta),
    nodeidx + 1,
    bufferidx + 1,
    varbufferidx
end

function build(
    f::Meta.Field,
    L::Meta.Bool,
    batch,
    rb,
    de,
    nodeidx,
    bufferidx,
    varbufferidx,
    convert,
)
    @debug "building array: L = $L"
    validity = buildbitmap(batch, rb, nodeidx, bufferidx)
    bufferidx += 1
    buffer = rb.buffers[bufferidx]
    meta = buildmetadata(f.custom_metadata)
    # get storage type (non-converted)
    T = juliaeltype(f, nothing, false)
    @debug "storage type for primitive: T = $T"
    buffer = rb.buffers[bufferidx]
    voff = batch.pos + buffer.offset
    node = rb.nodes[nodeidx]
    if rb.compression === nothing
        decodedbytes = batch.bytes
        pos = voff
        # return ValidityBitmap(batch.bytes, voff, node.length, node.null_count)
    else
        # compressed
        ptr = pointer(batch.bytes, voff)
        _, decodedbytes = uncompress(ptr, buffer, rb.compression)
        pos = 1
        # return ValidityBitmap(decodedbytes, 1, node.length, node.null_count)
    end
    len = rb.nodes[nodeidx].length
    T = juliaeltype(f, meta, convert)
    return BoolVector{T}(decodedbytes, pos, validity, len, meta),
    nodeidx + 1,
    bufferidx + 1,
    varbufferidx
end
