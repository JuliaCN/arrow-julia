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

_import_format(schema::ArrowSchema) =
    schema.format == C_NULL ? throw(ArgumentError("ArrowSchema format is C_NULL")) :
    unsafe_string(schema.format)

function _assert_import_live(schema::ArrowSchema, array::ArrowArray)
    schema.release == C_NULL && throw(ArgumentError("cannot import released ArrowSchema"))
    array.release == C_NULL && throw(ArgumentError("cannot import released ArrowArray"))
    return nothing
end

function _assert_import_schema_layout(schema::ArrowSchema, name::Symbol)
    schema.n_children >= 0 ||
        throw(ArgumentError("schema for column $name has negative child count"))
    return nothing
end

function _assert_import_column_layout(array::ArrowArray, name::Symbol)
    array.length >= 0 || throw(ArgumentError("column $name has negative length"))
    array.null_count >= -1 || throw(ArgumentError("column $name has invalid null count"))
    (array.null_count == -1 || array.null_count <= array.length) ||
        throw(ArgumentError("column $name has null count greater than length"))
    array.offset >= 0 || throw(ArgumentError("column $name has negative offset"))
    array.n_buffers >= 0 || throw(ArgumentError("column $name has negative buffer count"))
    array.n_children >= 0 || throw(ArgumentError("column $name has negative child count"))
    return nothing
end

_nullable_field(schema::ArrowSchema, array::ArrowArray) =
    schema.flags & ARROW_FLAG_NULLABLE != 0 || array.null_count != 0

function _assert_binary_like_offsets!(offsets::Vector, name::Symbol, label::AbstractString)
    isempty(offsets) && throw(ArgumentError("$label column $name has empty offsets"))
    previous = Int(first(offsets))
    previous >= 0 || throw(ArgumentError("$label column $name has negative offset"))
    for offset in Iterators.drop(offsets, 1)
        current = Int(offset)
        current >= previous ||
            throw(ArgumentError("$label column $name offsets must be nondecreasing"))
        previous = current
    end
    return previous
end

function _assert_utf8_spans!(
    offsets::Vector,
    data::Vector{UInt8},
    validity::Vector{UInt8},
    len::Integer,
    name::Symbol,
    validity_offset::Int,
)
    for i = 1:Int(len)
        _validity_bit(validity, i, validity_offset) || continue
        start = Int(@inbounds offsets[i]) + 1
        stop = Int(@inbounds offsets[i + 1])
        stop < start && continue
        isvalid(String, @view data[start:stop]) ||
            throw(ArgumentError("UTF-8 column $name value at index $i is not valid UTF-8"))
    end
    return nothing
end

function _assert_view_spans!(
    views::Vector{ViewElement},
    buffers::Vector{Vector{UInt8}},
    validity::Vector{UInt8},
    name::Symbol,
    validity_offset::Int,
)
    for i in eachindex(views)
        _validity_bit(validity, i, validity_offset) || continue
        view = @inbounds views[i]
        len = Int(view.length)
        len >= 0 || throw(ArgumentError("view column $name has negative value length"))
        if _viewisinline(len)
            len <= VIEW_INLINE_BYTES ||
                throw(ArgumentError("view column $name has invalid inline length"))
            continue
        end
        buffer_index = Int(view.bufindex) + 1
        1 <= buffer_index <= length(buffers) ||
            throw(ArgumentError("view column $name references missing variadic buffer"))
        offset = Int(view.offset)
        offset >= 0 || throw(ArgumentError("view column $name has negative data offset"))
        offset + len <= length(buffers[buffer_index]) || throw(
            ArgumentError("view column $name references bytes outside variadic buffer"),
        )
    end
    return nothing
end

function _assert_utf8_view_spans!(
    views::Vector{ViewElement},
    inline::Vector{UInt8},
    buffers::Vector{Vector{UInt8}},
    validity::Vector{UInt8},
    name::Symbol,
    validity_offset::Int,
)
    for i in eachindex(views)
        _validity_bit(validity, i, validity_offset) || continue
        view = @inbounds views[i]
        len = Int(view.length)
        len == 0 && continue
        valid = if _viewisinline(len)
            start = _view_inline_start(i)
            isvalid(String, @view inline[start:(start + len - 1)])
        else
            buffer = @inbounds buffers[Int(view.bufindex) + 1]
            start = Int(view.offset) + 1
            isvalid(String, @view buffer[start:(start + len - 1)])
        end
        valid || throw(
            ArgumentError("UTF-8 view column $name value at index $i is not valid UTF-8"),
        )
    end
    return nothing
end

function _assert_dictionary_indices!(
    indices::Vector,
    dictionary::AbstractVector,
    validity::Vector{UInt8},
    validity_offset::Int,
    name::Symbol,
)
    dictionary_len = length(dictionary)
    for i in eachindex(indices)
        _validity_bit(validity, i, validity_offset) || continue
        raw_index = @inbounds indices[i]
        index = try
            Int(raw_index)
        catch
            throw(ArgumentError("dictionary column $name has dictionary index too large"))
        end
        index >= 0 ||
            throw(ArgumentError("dictionary column $name has negative dictionary index"))
        index < dictionary_len || throw(
            ArgumentError("dictionary column $name has dictionary index out of bounds"),
        )
    end
    return nothing
end

function _assert_list_offsets!(
    offsets::Vector,
    values::AbstractVector,
    name::Symbol,
    label::AbstractString="list",
)
    isempty(offsets) && throw(ArgumentError("$label column $name has empty offsets"))
    previous = Int(first(offsets))
    previous >= 0 || throw(ArgumentError("$label column $name has negative offset"))
    for offset in Iterators.drop(offsets, 1)
        current = Int(offset)
        current >= previous ||
            throw(ArgumentError("$label column $name offsets must be nondecreasing"))
        previous = current
    end
    previous <= length(values) ||
        throw(ArgumentError("$label column $name offsets exceed child value length"))
    return nothing
end

function _assert_list_view_spans!(
    offsets::Vector,
    sizes::Vector,
    values::AbstractVector,
    name::Symbol,
)
    length(offsets) == length(sizes) ||
        throw(ArgumentError("list-view column $name offsets and sizes length mismatch"))
    for i in eachindex(offsets)
        offset = Int(@inbounds offsets[i])
        size = Int(@inbounds sizes[i])
        offset >= 0 || throw(ArgumentError("list-view column $name has negative offset"))
        size >= 0 || throw(ArgumentError("list-view column $name has negative size"))
        offset + size <= length(values) ||
            throw(ArgumentError("list-view column $name span exceeds child value length"))
    end
    return nothing
end

function _assert_union_type_ids!(
    type_ids::Vector{UInt8},
    declared_ids::Vector{UInt8},
    name::Symbol,
)
    declared = Set(declared_ids)
    for type_id in type_ids
        type_id in declared || throw(
            ArgumentError(
                "union column $name references undeclared type id $(Int(type_id))",
            ),
        )
    end
    return nothing
end

function _assert_dense_union_offsets!(
    offsets::Vector{Int32},
    type_ids::Vector{UInt8},
    children::Tuple,
    declared_ids::Vector{UInt8},
    name::Symbol,
)
    id_to_child = _union_type_id_map(declared_ids)
    for (index, type_id) in enumerate(type_ids)
        child_index = id_to_child[type_id]
        offset = Int(@inbounds offsets[index]) + 1
        1 <= offset <= length(children[child_index]) ||
            throw(ArgumentError("dense union column $name has child offset out of bounds"))
    end
    return nothing
end

function _assert_sparse_union_children!(children::Tuple, len::Int, name::Symbol)
    for child in children
        length(child) == len || throw(
            ArgumentError("sparse union column $name child length must match row count"),
        )
    end
    return nothing
end
