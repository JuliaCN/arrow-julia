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

const SubVector{T,P} = SubArray{T,1,P,Tuple{UnitRange{Int64}},true}

function build(
    f::Meta.Field,
    L::ListTypes,
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
    ooff = batch.pos + buffer.offset
    OT = L isa LargeLists ? Int64 : Int32
    bytes, offs = reinterp(OT, batch, buffer, rb.compression)
    offsets = Offsets(bytes, offs)
    bufferidx += 1
    len = rb.nodes[nodeidx].length
    nodeidx += 1
    meta = buildmetadata(f.custom_metadata)
    T = juliaeltype(f, meta, convert)
    if L isa Meta.Utf8 ||
       L isa Meta.Utf8View ||
       L isa Meta.LargeUtf8 ||
       L isa Meta.Binary ||
       L isa Meta.BinaryView ||
       L isa Meta.LargeBinary
        buffer = rb.buffers[bufferidx]
        bytes, A = reinterp(UInt8, batch, buffer, rb.compression)
        bufferidx += 1
    else
        bytes = UInt8[]
        A, nodeidx, bufferidx, varbufferidx =
            build(f.children[1], batch, rb, de, nodeidx, bufferidx, varbufferidx, convert)
        # juliaeltype returns Vector for List, translate to SubArray
        S = Base.nonmissingtype(T)
        if S <: Vector
            ST = SubVector{eltype(A),typeof(A)}
            T = S == T ? ST : Union{Missing,ST}
        end
    end
    _assert_offsets_spans(offsets.offsets, len, length(A), "variable-size array")
    if L isa Meta.Utf8 || L isa Meta.LargeUtf8
        _assert_utf8_spans(offsets.offsets, A, validity, len, "UTF-8")
    end
    return List{T,OT,typeof(A)}(bytes, validity, offsets, A, len, meta),
    nodeidx,
    bufferidx,
    varbufferidx
end

function build(
    f::Meta.Field,
    L::ListViewTypes,
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
    OT = L isa Meta.LargeListView ? Int64 : Int32
    offsetbuffer = rb.buffers[bufferidx]
    offsetbytes, offsets = reinterp(OT, batch, offsetbuffer, rb.compression)
    bufferidx += 1
    sizebuffer = rb.buffers[bufferidx]
    sizebytes, sizes = reinterp(OT, batch, sizebuffer, rb.compression)
    bufferidx += 1
    len = rb.nodes[nodeidx].length
    nodeidx += 1
    A, nodeidx, bufferidx, varbufferidx =
        build(f.children[1], batch, rb, de, nodeidx, bufferidx, varbufferidx, convert)
    _assert_list_view_spans(offsets, sizes, A; len)
    meta = buildmetadata(f.custom_metadata)
    T = juliaeltype(f, meta, convert)
    S = Base.nonmissingtype(T)
    if S <: Vector
        ST = SubVector{eltype(A),typeof(A)}
        T = S == T ? ST : Union{Missing,ST}
    end
    return ListView{T,OT,typeof(A)}(
        offsetbytes,
        sizebytes,
        validity,
        offsets,
        sizes,
        A,
        len,
        meta,
    ),
    nodeidx,
    bufferidx,
    varbufferidx
end

function build(
    f::Meta.Field,
    x::Meta.RunEndEncoded,
    batch,
    rb,
    de,
    nodeidx,
    bufferidx,
    varbufferidx,
    convert,
)
    @debug "building array: x = $x"
    len = rb.nodes[nodeidx].length
    nodeidx += 1
    meta = buildmetadata(f.custom_metadata)
    T = juliaeltype(f, meta, convert)
    run_ends, nodeidx, bufferidx, varbufferidx =
        build(f.children[1], batch, rb, de, nodeidx, bufferidx, varbufferidx, false)
    values, nodeidx, bufferidx, varbufferidx =
        build(f.children[2], batch, rb, de, nodeidx, bufferidx, varbufferidx, convert)
    return _makerunendencoded(T, run_ends, values, len, meta),
    nodeidx,
    bufferidx,
    varbufferidx
end

function build(
    f::Meta.Field,
    L::ViewTypes,
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
    _, views = reinterp(ViewElement, batch, buffer, rb.compression)
    inline = reinterpret(UInt8, views)  # reuse the (possibly realigned) memory backing `views`
    bufferidx += 1
    buffers = Vector{UInt8}[]
    nvariadic = _viewbuffercount(validity, views, rb.variadicBufferCounts[varbufferidx])
    for i = 1:nvariadic
        buffer = rb.buffers[bufferidx]
        _, A = reinterp(UInt8, batch, buffer, rb.compression)
        push!(buffers, A)
        bufferidx += 1
    end
    varbufferidx += 1
    len = rb.nodes[nodeidx].length
    nodeidx += 1
    if L isa Meta.Utf8View
        _assert_utf8_view_spans(views, inline, buffers, validity, len, "UTF-8 view")
    else
        _assert_view_spans(views, inline, buffers, validity, len, "binary view")
    end
    meta = buildmetadata(f.custom_metadata)
    T = juliaeltype(f, meta, convert)
    return View{T}(batch.bytes, validity, views, inline, buffers, len, meta),
    nodeidx,
    bufferidx,
    varbufferidx
end

function build(
    f::Meta.Field,
    L::Union{Meta.FixedSizeBinary,Meta.FixedSizeList},
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
    len = rb.nodes[nodeidx].length
    nodeidx += 1
    if L isa Meta.FixedSizeBinary
        buffer = rb.buffers[bufferidx]
        bytes, A = reinterp(UInt8, batch, buffer, rb.compression)
        bufferidx += 1
    else
        bytes = UInt8[]
        A, nodeidx, bufferidx, varbufferidx =
            build(f.children[1], batch, rb, de, nodeidx, bufferidx, varbufferidx, convert)
    end
    meta = buildmetadata(f.custom_metadata)
    T = juliaeltype(f, meta, convert)
    return FixedSizeList{T,typeof(A)}(bytes, validity, A, len, meta),
    nodeidx,
    bufferidx,
    varbufferidx
end

function build(
    f::Meta.Field,
    L::Meta.Map,
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
    ooff = batch.pos + buffer.offset
    OT = Int32
    bytes, offs = reinterp(OT, batch, buffer, rb.compression)
    offsets = Offsets(bytes, offs)
    bufferidx += 1
    len = rb.nodes[nodeidx].length
    nodeidx += 1
    A, nodeidx, bufferidx, varbufferidx =
        build(f.children[1], batch, rb, de, nodeidx, bufferidx, varbufferidx, convert)
    _assert_offsets_spans(offsets.offsets, length(validity), length(A), "map")
    meta = buildmetadata(f.custom_metadata)
    T = juliaeltype(f, meta, convert)
    return Map{T,OT,typeof(A)}(validity, offsets, A, len, meta),
    nodeidx,
    bufferidx,
    varbufferidx
end

function build(
    f::Meta.Field,
    L::Meta.Struct,
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
    len = rb.nodes[nodeidx].length
    vecs = []
    nodeidx += 1
    for child in f.children
        A, nodeidx, bufferidx, varbufferidx =
            build(child, batch, rb, de, nodeidx, bufferidx, varbufferidx, convert)
        push!(vecs, A)
    end
    data = Tuple(vecs)
    meta = buildmetadata(f.custom_metadata)
    T = juliaeltype(f, meta, convert)
    fnames = ntuple(i -> Symbol(f.children[i].name), length(f.children))
    return Struct{T,typeof(data),fnames}(validity, data, len, meta),
    nodeidx,
    bufferidx,
    varbufferidx
end

function build(
    f::Meta.Field,
    L::Meta.Union,
    batch,
    rb,
    de,
    nodeidx,
    bufferidx,
    varbufferidx,
    convert,
)
    @debug "building array: L = $L"
    buffer = rb.buffers[bufferidx]
    bytes, typeIds = reinterp(UInt8, batch, buffer, rb.compression)
    bufferidx += 1
    if L.mode == Meta.UnionMode.Dense
        buffer = rb.buffers[bufferidx]
        bytes2, offsets = reinterp(Int32, batch, buffer, rb.compression)
        bufferidx += 1
    end
    vecs = []
    nodeidx += 1
    for child in f.children
        A, nodeidx, bufferidx, varbufferidx =
            build(child, batch, rb, de, nodeidx, bufferidx, varbufferidx, convert)
        push!(vecs, A)
    end
    data = Tuple(vecs)
    meta = buildmetadata(f.custom_metadata)
    T = juliaeltype(f, meta, convert)
    UT = UnionT(f, convert)
    if L.mode == Meta.UnionMode.Dense
        B = DenseUnion{T,UT,typeof(data)}(bytes, bytes2, typeIds, offsets, data, meta)
    else
        B = SparseUnion{T,UT,typeof(data)}(bytes, typeIds, data, meta)
    end
    return B, nodeidx, bufferidx, varbufferidx
end

function build(
    f::Meta.Field,
    L::Meta.Null,
    batch,
    rb,
    de,
    nodeidx,
    bufferidx,
    varbufferidx,
    convert,
)
    @debug "building array: L = $L"
    meta = buildmetadata(f.custom_metadata)
    T = juliaeltype(f, meta, convert)
    return NullVector{maybemissing(T)}(MissingVector(rb.nodes[nodeidx].length), meta),
    nodeidx + 1,
    bufferidx,
    varbufferidx
end
