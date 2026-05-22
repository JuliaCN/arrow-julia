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

function _schema_ref_with_children(schema::CData.ArrowSchema, children)
    return Ref(
        CData.ArrowSchema(
            schema.format,
            schema.name,
            schema.metadata,
            schema.flags,
            Int64(length(children)),
            pointer(children),
            schema.dictionary,
            schema.release,
            schema.private_data,
        ),
    )
end

function _array_ref_with_children(array::CData.ArrowArray, children)
    return Ref(
        CData.ArrowArray(
            array.length,
            array.null_count,
            array.offset,
            array.n_buffers,
            Int64(length(children)),
            array.buffers,
            pointer(children),
            array.dictionary,
            array.release,
            array.private_data,
        ),
    )
end

function _schema_ref_with_metadata(schema::CData.ArrowSchema, metadata::Vector{UInt8})
    return Ref(
        CData.ArrowSchema(
            schema.format,
            schema.name,
            Ptr{UInt8}(pointer(metadata)),
            schema.flags,
            schema.n_children,
            schema.children,
            schema.dictionary,
            schema.release,
            schema.private_data,
        ),
    )
end

function _int32_metadata_bytes(values::Int32...)
    return collect(reinterpret(UInt8, collect(values)))
end

function _schema_ref_with_format(schema::CData.ArrowSchema, format::Ptr{UInt8})
    return Ref(
        CData.ArrowSchema(
            format,
            schema.name,
            schema.metadata,
            schema.flags,
            schema.n_children,
            schema.children,
            schema.dictionary,
            schema.release,
            schema.private_data,
        ),
    )
end

function _format_bytes(format::AbstractString)
    bytes = Vector{UInt8}(codeunits(format))
    push!(bytes, 0x00)
    return bytes
end

function _schema_ref_with_release(schema::CData.ArrowSchema, release::Ptr{Cvoid})
    return Ref(
        CData.ArrowSchema(
            schema.format,
            schema.name,
            schema.metadata,
            schema.flags,
            schema.n_children,
            schema.children,
            schema.dictionary,
            release,
            schema.private_data,
        ),
    )
end

function _array_ref_with_release(array::CData.ArrowArray, release::Ptr{Cvoid})
    return Ref(
        CData.ArrowArray(
            array.length,
            array.null_count,
            array.offset,
            array.n_buffers,
            array.n_children,
            array.buffers,
            array.children,
            array.dictionary,
            release,
            array.private_data,
        ),
    )
end
