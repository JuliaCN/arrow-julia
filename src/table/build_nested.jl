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
    buffer = _record_batch_buffer(rb, bufferidx)
    ooff = batch.pos + buffer.offset
    OT = L isa LargeLists ? Int64 : Int32
    bytes, offs = reinterp(OT, batch, buffer, rb.compression)
    offsets = Offsets(bytes, offs)
    bufferidx += 1
    len = _record_batch_node(rb, nodeidx).length
    nodeidx += 1
    meta = buildmetadata(f.custom_metadata)
    T = juliaeltype(f, meta, convert)
    if L isa Meta.Utf8 ||
       L isa Meta.Utf8View ||
       L isa Meta.LargeUtf8 ||
       L isa Meta.Binary ||
       L isa Meta.BinaryView ||
       L isa Meta.LargeBinary
        buffer = _record_batch_buffer(rb, bufferidx)
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
    offsetbuffer = _record_batch_buffer(rb, bufferidx)
    offsetbytes, offsets = reinterp(OT, batch, offsetbuffer, rb.compression)
    bufferidx += 1
    sizebuffer = _record_batch_buffer(rb, bufferidx)
    sizebytes, sizes = reinterp(OT, batch, sizebuffer, rb.compression)
    bufferidx += 1
    len = _record_batch_node(rb, nodeidx).length
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
    len = _record_batch_node(rb, nodeidx).length
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
    buffer = _record_batch_buffer(rb, bufferidx)
    _, views = reinterp(ViewElement, batch, buffer, rb.compression)
    inline = reinterpret(UInt8, views)  # reuse the (possibly realigned) memory backing `views`
    bufferidx += 1
    buffers = Vector{UInt8}[]
    nvariadic = _viewbuffercount(validity, views, rb.variadicBufferCounts[varbufferidx])
    for i = 1:nvariadic
        buffer = _record_batch_buffer(rb, bufferidx)
        _, A = reinterp(UInt8, batch, buffer, rb.compression)
        push!(buffers, A)
        bufferidx += 1
    end
    varbufferidx += 1
    len = _record_batch_node(rb, nodeidx).length
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
    len = _record_batch_node(rb, nodeidx).length
    nodeidx += 1
    if L isa Meta.FixedSizeBinary
        buffer = _record_batch_buffer(rb, bufferidx)
        bytes, A = reinterp(UInt8, batch, buffer, rb.compression)
        bufferidx += 1
    else
        bytes = UInt8[]
        A, nodeidx, bufferidx, varbufferidx =
            build(f.children[1], batch, rb, de, nodeidx, bufferidx, varbufferidx, convert)
        _assert_fixed_size_list_child_length(A, L.listSize, len, Symbol(f.name))
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
    buffer = _record_batch_buffer(rb, bufferidx)
    ooff = batch.pos + buffer.offset
    OT = Int32
    bytes, offs = reinterp(OT, batch, buffer, rb.compression)
    offsets = Offsets(bytes, offs)
    bufferidx += 1
    len = _record_batch_node(rb, nodeidx).length
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
    len = _record_batch_node(rb, nodeidx).length
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

function _declared_union_type_ids(type_ids, child_count::Integer, name::Symbol)
    if type_ids === nothing
        return collect(0:(Int(child_count) - 1))
    end
    length(type_ids) == child_count ||
        throw(ArgumentError("union column $name type id count must match child count"))
    declared = Int.(type_ids)
    seen = Set{Int}()
    for id in declared
        id >= 0 || throw(ArgumentError("union column $name has negative type id $id"))
        id in seen && throw(ArgumentError("union column $name has duplicate type id $id"))
        push!(seen, id)
    end
    return declared
end

function _union_child_map(declared_type_ids, child_count::Integer, name::Symbol)
    declared = _declared_union_type_ids(declared_type_ids, child_count, name)
    return Dict(id => child_index for (child_index, id) in enumerate(declared))
end

function _assert_union_type_id_buffer!(
    type_ids::AbstractVector{UInt8},
    len::Integer,
    child_by_type_id::AbstractDict{Int,Int},
    name::Symbol,
)
    length(type_ids) == len || throw(
        ArgumentError("union column $name type id buffer length must match row count"),
    )
    for raw_type_id in type_ids
        type_id = Int(raw_type_id)
        haskey(child_by_type_id, type_id) || throw(
            ArgumentError("union column $name references undeclared type id $type_id"),
        )
    end
    return nothing
end

function _assert_dense_union_layout!(
    type_ids::AbstractVector{UInt8},
    offsets::AbstractVector{Int32},
    data::Tuple,
    declared_type_ids,
    len::Integer,
    name::Symbol,
)
    child_by_type_id = _union_child_map(declared_type_ids, length(data), name)
    _assert_union_type_id_buffer!(type_ids, len, child_by_type_id, name)
    length(offsets) == len || throw(
        ArgumentError("dense union column $name offset buffer length must match row count"),
    )
    for i in eachindex(type_ids)
        type_id = Int(@inbounds type_ids[i])
        child_index = child_by_type_id[type_id]
        offset = Int(@inbounds offsets[i])
        offset >= 0 ||
            throw(ArgumentError("dense union column $name has negative child offset"))
        offset < length(data[child_index]) ||
            throw(ArgumentError("dense union column $name has child offset out of bounds"))
    end
    return nothing
end

function _assert_sparse_union_layout!(
    type_ids::AbstractVector{UInt8},
    data::Tuple,
    declared_type_ids,
    len::Integer,
    name::Symbol,
)
    child_by_type_id = _union_child_map(declared_type_ids, length(data), name)
    _assert_union_type_id_buffer!(type_ids, len, child_by_type_id, name)
    for (child_index, child) in enumerate(data)
        child_len = length(child)
        child_len == len || throw(
            ArgumentError(
                "sparse union column $name child $child_index length $child_len must match row count $len",
            ),
        )
    end
    return nothing
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
    buffer = _record_batch_buffer(rb, bufferidx)
    bytes, typeIds = reinterp(UInt8, batch, buffer, rb.compression)
    bufferidx += 1
    if L.mode == Meta.UnionMode.Dense
        buffer = _record_batch_buffer(rb, bufferidx)
        bytes2, offsets = reinterp(Int32, batch, buffer, rb.compression)
        bufferidx += 1
    end
    vecs = []
    len = _record_batch_node(rb, nodeidx).length
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
    name = Symbol(f.name)
    if L.mode == Meta.UnionMode.Dense
        _assert_dense_union_layout!(typeIds, offsets, data, L.typeIds, len, name)
        B = DenseUnion{T,UT,typeof(data)}(bytes, bytes2, typeIds, offsets, data, meta)
    else
        _assert_sparse_union_layout!(typeIds, data, L.typeIds, len, name)
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
    return NullVector{maybemissing(T)}(
        MissingVector(_record_batch_node(rb, nodeidx).length),
        meta,
    ),
    nodeidx + 1,
    bufferidx,
    varbufferidx
end
