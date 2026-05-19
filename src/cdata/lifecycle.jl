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

function _schema_release_callback(schema_ptr::Ptr{ArrowSchema})
    schema_ptr == C_NULL && return nothing
    schema = unsafe_load(schema_ptr)
    schema.release == C_NULL && return nothing
    for i = 1:schema.n_children
        child_ptr = unsafe_load(schema.children, i)
        child_ptr == C_NULL && continue
        child = unsafe_load(child_ptr)
        child.release == C_NULL ||
            ccall(child.release, Cvoid, (Ptr{ArrowSchema},), child_ptr)
    end
    if schema.dictionary != C_NULL
        dictionary = unsafe_load(schema.dictionary)
        dictionary.release == C_NULL ||
            ccall(dictionary.release, Cvoid, (Ptr{ArrowSchema},), schema.dictionary)
    end
    _release_handle!(schema.private_data)
    unsafe_store!(
        schema_ptr,
        ArrowSchema(
            schema.format,
            schema.name,
            schema.metadata,
            schema.flags,
            schema.n_children,
            schema.children,
            schema.dictionary,
            C_NULL,
            C_NULL,
        ),
    )
    return nothing
end

function _array_release_callback(array_ptr::Ptr{ArrowArray})
    array_ptr == C_NULL && return nothing
    array = unsafe_load(array_ptr)
    array.release == C_NULL && return nothing
    for i = 1:array.n_children
        child_ptr = unsafe_load(array.children, i)
        child_ptr == C_NULL && continue
        child = unsafe_load(child_ptr)
        child.release == C_NULL ||
            ccall(child.release, Cvoid, (Ptr{ArrowArray},), child_ptr)
    end
    if array.dictionary != C_NULL
        dictionary = unsafe_load(array.dictionary)
        dictionary.release == C_NULL ||
            ccall(dictionary.release, Cvoid, (Ptr{ArrowArray},), array.dictionary)
    end
    _release_handle!(array.private_data)
    unsafe_store!(
        array_ptr,
        ArrowArray(
            array.length,
            array.null_count,
            array.offset,
            array.n_buffers,
            array.n_children,
            array.buffers,
            array.children,
            array.dictionary,
            C_NULL,
            C_NULL,
        ),
    )
    return nothing
end

const SCHEMA_RELEASE_CALLBACK = Ref{Ptr{Cvoid}}(C_NULL)
const ARRAY_RELEASE_CALLBACK = Ref{Ptr{Cvoid}}(C_NULL)

function _init_release_callbacks!()
    SCHEMA_RELEASE_CALLBACK[] =
        @cfunction(_schema_release_callback, Cvoid, (Ptr{ArrowSchema},))
    ARRAY_RELEASE_CALLBACK[] =
        @cfunction(_array_release_callback, Cvoid, (Ptr{ArrowArray},))
    return nothing
end

function __init__()
    _init_release_callbacks!()
    _init_stream_callbacks!()
    return nothing
end

function _schema_release_callback_ptr()
    SCHEMA_RELEASE_CALLBACK[] == C_NULL && _init_release_callbacks!()
    return SCHEMA_RELEASE_CALLBACK[]
end

function _array_release_callback_ptr()
    ARRAY_RELEASE_CALLBACK[] == C_NULL && _init_release_callbacks!()
    return ARRAY_RELEASE_CALLBACK[]
end

function _children_ptr(child_ptrs::Vector{Ptr{T}}) where {T}
    return isempty(child_ptrs) ? Ptr{Ptr{T}}(C_NULL) : pointer(child_ptrs)
end
