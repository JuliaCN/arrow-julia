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

const ListTypes =
    Union{Meta.Utf8,Meta.LargeUtf8,Meta.Binary,Meta.LargeBinary,Meta.List,Meta.LargeList}
const LargeLists = Union{Meta.LargeUtf8,Meta.LargeBinary,Meta.LargeList,Meta.LargeListView}
const ViewTypes = Union{Meta.Utf8View,Meta.BinaryView}
const ListViewTypes = Union{Meta.ListView,Meta.LargeListView}

function _dictionary_index_as_int(raw_index, name)
    return try
        Int(raw_index)
    catch
        throw(ArgumentError("dictionary column $name has dictionary index too large"))
    end
end

function _assert_dictionary_index_bounds!(
    indices::AbstractVector,
    encoding::DictEncoding,
    validity::ValidityBitmap,
    name::Symbol,
)
    dictionary_len = length(encoding)
    for i in eachindex(indices)
        @inbounds validity[i] || continue
        raw_index = @inbounds indices[i]
        index = _dictionary_index_as_int(raw_index, name)
        index >= 0 ||
            throw(ArgumentError("dictionary column $name has negative dictionary index"))
        index < dictionary_len || throw(
            ArgumentError("dictionary column $name has dictionary index out of bounds"),
        )
    end
    return nothing
end

@inline function _viewbuffercount(validity, views, declared::Integer)
    count = Int(declared)
    for i in eachindex(views)
        validity[i] || continue
        v = @inbounds views[i]
        if !_viewisinline(v.length)
            count = max(count, Int(v.bufindex) + 1)
        end
    end
    return count
end

function _assert_field_node_shape(node, name::Symbol)
    node.length >= 0 || throw(ArgumentError("field $name has negative length"))
    node.null_count >= 0 || throw(ArgumentError("field $name has negative null count"))
    node.null_count <= node.length ||
        throw(ArgumentError("field $name null count exceeds length"))
    return nothing
end

function _assert_value_count(values, len::Integer, name::Symbol)
    length(values) >= len || throw(
        ArgumentError(
            "primitive column $name value buffer length $(length(values)) is shorter than logical length $len",
        ),
    )
    return nothing
end

function _assert_bool_value_bytes(bytes, pos::Integer, len::Integer, name::Symbol)
    required = cld(Int(len), 8)
    available = max(length(bytes) - Int(pos) + 1, 0)
    return _assert_bool_value_byte_count(available, len, name)
end

function _assert_bool_value_byte_count(available::Integer, len::Integer, name::Symbol)
    required = cld(Int(len), 8)
    available >= required || throw(
        ArgumentError(
            "bool column $name value buffer length $available is shorter than required byte length $required",
        ),
    )
    return nothing
end

function _assert_record_batch_buffer_bounds(batch, buffer, bufferidx=nothing)
    label = bufferidx === nothing ? "record batch buffer" : "record batch buffer $bufferidx"
    buffer.offset >= 0 || throw(ArgumentError("$label offset must be non-negative"))
    buffer.length >= 0 || throw(ArgumentError("$label length must be non-negative"))
    body_length = batch.msg.bodyLength
    buffer.offset <= body_length || throw(
        ArgumentError(
            "$label offset $(buffer.offset) exceeds record batch body length $body_length",
        ),
    )
    remaining = body_length - buffer.offset
    buffer.length <= remaining || throw(
        ArgumentError(
            "$label length $(buffer.length) exceeds remaining record batch body bytes $remaining",
        ),
    )
    return nothing
end

function _record_batch_buffer(rb, bufferidx::Integer)
    buffers = rb.buffers
    declared = buffers === nothing ? 0 : length(buffers)
    1 <= bufferidx <= declared || throw(
        ArgumentError(
            "record batch is missing buffer $bufferidx; only $declared buffers are declared",
        ),
    )
    return buffers[bufferidx]
end

function _record_batch_node(rb, nodeidx::Integer)
    nodes = rb.nodes
    declared = nodes === nothing ? 0 : length(nodes)
    1 <= nodeidx <= declared || throw(
        ArgumentError(
            "record batch is missing field node $nodeidx; only $declared field nodes are declared",
        ),
    )
    return nodes[nodeidx]
end

function _record_batch_variadic_count(rb, varbufferidx::Integer)
    counts = rb.variadicBufferCounts
    declared = counts === nothing ? 0 : length(counts)
    1 <= varbufferidx <= declared || throw(
        ArgumentError(
            "record batch is missing variadic buffer count $varbufferidx; only $declared counts are declared",
        ),
    )
    return counts[varbufferidx]
end

function _assert_record_batch_fully_consumed(rb, nodeidx, bufferidx, varbufferidx)
    declared_nodes = rb.nodes === nothing ? 0 : length(rb.nodes)
    consumed_nodes = Int(nodeidx) - 1
    consumed_nodes == declared_nodes || throw(
        ArgumentError(
            "record batch declares $declared_nodes field nodes but schema consumed $consumed_nodes",
        ),
    )

    declared_buffers = rb.buffers === nothing ? 0 : length(rb.buffers)
    consumed_buffers = Int(bufferidx) - 1
    consumed_buffers == declared_buffers || throw(
        ArgumentError(
            "record batch declares $declared_buffers buffers but schema consumed $consumed_buffers",
        ),
    )

    declared_variadic_counts =
        rb.variadicBufferCounts === nothing ? 0 : length(rb.variadicBufferCounts)
    consumed_variadic_counts = Int(varbufferidx) - 1
    consumed_variadic_counts == declared_variadic_counts || throw(
        ArgumentError(
            "record batch declares $declared_variadic_counts variadic buffer counts but schema consumed $consumed_variadic_counts",
        ),
    )
    return nothing
end

function _assert_reinterp_element_width(::Type{T}, len::Integer) where {T}
    width = sizeof(T)
    width == 1 && return nothing
    len % width == 0 || throw(
        ArgumentError(
            "record batch buffer length $len is not a multiple of $width-byte element width for $(T)",
        ),
    )
    return nothing
end

function _build_dictionary_values(field, batch, recordbatch, dictencodings, convert)
    values, nodeidx, bufferidx, varbufferidx = build(
        field,
        field.type,
        batch,
        recordbatch,
        dictencodings,
        Int64(1),
        Int64(1),
        Int64(1),
        convert,
    )
    _assert_record_batch_fully_consumed(recordbatch, nodeidx, bufferidx, varbufferidx)
    return values
end

function _dictionary_encoded_field(dictencoded, id)
    haskey(dictencoded, id) ||
        throw(ArgumentError("dictionary batch id $id has no schema dictionary field"))
    return dictencoded[id]
end

function build(field::Meta.Field, batch, rb, de, nodeidx, bufferidx, varbufferidx, convert)
    name = Symbol(field.name)
    node = _record_batch_node(rb, nodeidx)
    _assert_field_node_shape(node, name)
    d = field.dictionary
    if d !== nothing
        validity = buildbitmap(batch, rb, nodeidx, bufferidx)
        bufferidx += 1
        buffer = _record_batch_buffer(rb, bufferidx)
        S = d.indexType === nothing ? Int32 : juliaeltype(field, d.indexType, false)
        bytes, indices = reinterp(S, batch, buffer, rb.compression)
        @lock de begin
            encoding = de[][d.id]
            _assert_dictionary_index_bounds!(
                indices,
                encoding,
                validity,
                Symbol(field.name),
            )
            A = DictEncoded(
                bytes,
                validity,
                indices,
                encoding,
                buildmetadata(field.custom_metadata),
            )
        end
        nodeidx += 1
        bufferidx += 1
    else
        A, nodeidx, bufferidx, varbufferidx = build(
            field,
            field.type,
            batch,
            rb,
            de,
            nodeidx,
            bufferidx,
            varbufferidx,
            convert,
        )
    end
    return A, nodeidx, bufferidx, varbufferidx
end

function buildbitmap(batch, rb, nodeidx, bufferidx)
    buffer = _record_batch_buffer(rb, bufferidx)
    _assert_record_batch_buffer_bounds(batch, buffer, bufferidx)
    voff = batch.pos + buffer.offset
    node = _record_batch_node(rb, nodeidx)
    if rb.compression === nothing
        return ValidityBitmap(batch.bytes, voff, node.length, node.null_count)
    else
        # compressed
        ptr = pointer(batch.bytes, voff)
        _, decodedbytes = uncompress(ptr, buffer, rb.compression)
        return ValidityBitmap(decodedbytes, 1, node.length, node.null_count)
    end
end

function uncompress(ptr::Ptr{UInt8}, buffer, compression)
    buffer.length < 0 &&
        throw(ArgumentError("compressed arrow buffer length must be non-negative"))
    buffer.length == 0 && return 0, UInt8[]
    buffer.length < 8 && throw(
        ArgumentError(
            "compressed arrow buffer length $(buffer.length) is shorter than the 8-byte uncompressed length header",
        ),
    )
    len = unsafe_load(convert(Ptr{Int64}, ptr))
    len < -1 &&
        throw(ArgumentError("compressed arrow buffer has invalid uncompressed length $len"))
    len == 0 && return 0, UInt8[]
    ptr += 8 # skip past uncompressed length as Int64
    encodedbytes = unsafe_wrap(Array, ptr, buffer.length - 8)
    if len == -1
        # len = -1 means data is not compressed
        # it's unclear why other language implementations allow this
        # but we support to be able to read data produced as such
        return length(encodedbytes), copy(encodedbytes)
    end
    decodedbytes = Vector{UInt8}(undef, len)
    if compression.codec === Meta.CompressionType.LZ4_FRAME
        comp = lz4_frame_decompressor()
        Base.@lock comp begin
            transcode(comp[], encodedbytes, decodedbytes)
        end
    elseif compression.codec === Meta.CompressionType.ZSTD
        comp = zstd_decompressor()
        Base.@lock comp begin
            transcode(comp[], encodedbytes, decodedbytes)
        end
    else
        error(
            "unsupported compression type when reading arrow buffers: $(typeof(compression.codec))",
        )
    end
    return len, decodedbytes
end

function reinterp(::Type{T}, batch, buf, compression) where {T}
    _assert_record_batch_buffer_bounds(batch, buf)
    ptr = pointer(batch.bytes, batch.pos + buf.offset)
    bytes = batch.bytes
    len = buf.length
    if compression !== nothing
        len, bytes = uncompress(ptr, buf, compression)
        ptr = pointer(bytes)
    end
    _assert_reinterp_element_width(T, len)
    alignment = Base.datatype_alignment(T)
    if alignment > 1 && (UInt(ptr) & UInt(alignment - 1)) != 0
        # https://github.com/apache/arrow-julia/issues/345
        # https://github.com/JuliaLang/julia/issues/42326
        # need to ensure that the data/pointers are aligned to T's actual
        # storage requirement before reinterpreting the underlying bytes
        A = Vector{T}(undef, div(len, sizeof(T)))
        unsafe_copyto!(Ptr{UInt8}(pointer(A)), ptr, len)
        return bytes, A
    else
        return bytes, unsafe_wrap(Array, convert(Ptr{T}, ptr), div(len, sizeof(T)))
    end
end
