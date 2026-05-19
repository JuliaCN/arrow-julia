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

mutable struct StreamHandle <: ExportHandle
    source::Table
    emitted::Bool
    last_error::Vector{UInt8}
    private_data::Ptr{Cvoid}
end

struct StreamExport
    ref::Base.RefValue{ArrowArrayStream}
    handle::StreamHandle
end

"""
    Arrow.CData.ExportedStream

Owner object returned by [`Arrow.CData.exportstream`](@ref). Keep this object
alive for as long as a C consumer may access [`stream_ptr`](@ref).
"""
mutable struct ExportedStream
    stream::StreamExport
    stream_base::Ptr{ArrowArrayStream}
end

function ExportedStream(stream::StreamExport)
    return ExportedStream(stream, Base.unsafe_convert(Ptr{ArrowArrayStream}, stream.ref))
end

function _stream_handle(stream_ptr::Ptr{ArrowArrayStream})
    stream_ptr == C_NULL && return nothing
    stream = unsafe_load(stream_ptr)
    stream.private_data == C_NULL && return nothing
    return Base.@lock HANDLE_LOCK begin
        handle = get(LIVE_HANDLES, stream.private_data, nothing)
        handle isa StreamHandle ? handle : nothing
    end
end

function _set_stream_error!(handle::StreamHandle, err)
    io = IOBuffer()
    showerror(io, err)
    handle.last_error = _cstring(String(take!(io)))
    return Cint(1)
end

function _released_array()
    return ArrowArray(
        Int64(0),
        Int64(0),
        Int64(0),
        Int64(0),
        Int64(0),
        Ptr{Ptr{Cvoid}}(C_NULL),
        Ptr{Ptr{ArrowArray}}(C_NULL),
        Ptr{ArrowArray}(C_NULL),
        C_NULL,
        C_NULL,
    )
end

function _stream_get_schema_callback(
    stream_ptr::Ptr{ArrowArrayStream},
    schema_out::Ptr{ArrowSchema},
)
    handle = _stream_handle(stream_ptr)
    handle === nothing && return Cint(1)
    try
        schema_out == C_NULL &&
            throw(ArgumentError("ArrowSchema output pointer must not be C_NULL"))
        schema_export = _schema_export_for_table(handle.source)
        unsafe_store!(schema_out, schema_export.ref[])
        handle.last_error = _cstring("")
        return Cint(0)
    catch err
        return _set_stream_error!(handle, err)
    end
end

function _stream_get_next_callback(
    stream_ptr::Ptr{ArrowArrayStream},
    array_out::Ptr{ArrowArray},
)
    handle = _stream_handle(stream_ptr)
    handle === nothing && return Cint(1)
    try
        array_out == C_NULL &&
            throw(ArgumentError("ArrowArray output pointer must not be C_NULL"))
        if handle.emitted
            unsafe_store!(array_out, _released_array())
            handle.last_error = _cstring("")
            return Cint(0)
        end
        array_export = _array_export_for_table(handle.source)
        unsafe_store!(array_out, array_export.ref[])
        handle.emitted = true
        handle.last_error = _cstring("")
        return Cint(0)
    catch err
        return _set_stream_error!(handle, err)
    end
end

function _stream_get_last_error_callback(stream_ptr::Ptr{ArrowArrayStream})
    handle = _stream_handle(stream_ptr)
    handle === nothing && return Ptr{UInt8}(C_NULL)
    return pointer(handle.last_error)
end

function _stream_release_callback(stream_ptr::Ptr{ArrowArrayStream})
    stream_ptr == C_NULL && return nothing
    stream = unsafe_load(stream_ptr)
    stream.release == C_NULL && return nothing
    _release_handle!(stream.private_data)
    unsafe_store!(stream_ptr, ArrowArrayStream(C_NULL, C_NULL, C_NULL, C_NULL, C_NULL))
    return nothing
end

const STREAM_GET_SCHEMA_CALLBACK = Ref{Ptr{Cvoid}}(C_NULL)
const STREAM_GET_NEXT_CALLBACK = Ref{Ptr{Cvoid}}(C_NULL)
const STREAM_GET_LAST_ERROR_CALLBACK = Ref{Ptr{Cvoid}}(C_NULL)
const STREAM_RELEASE_CALLBACK = Ref{Ptr{Cvoid}}(C_NULL)

function _init_stream_callbacks!()
    STREAM_GET_SCHEMA_CALLBACK[] = @cfunction(
        _stream_get_schema_callback,
        Cint,
        (Ptr{ArrowArrayStream}, Ptr{ArrowSchema}),
    )
    STREAM_GET_NEXT_CALLBACK[] = @cfunction(
        _stream_get_next_callback,
        Cint,
        (Ptr{ArrowArrayStream}, Ptr{ArrowArray}),
    )
    STREAM_GET_LAST_ERROR_CALLBACK[] =
        @cfunction(_stream_get_last_error_callback, Ptr{UInt8}, (Ptr{ArrowArrayStream},),)
    STREAM_RELEASE_CALLBACK[] =
        @cfunction(_stream_release_callback, Cvoid, (Ptr{ArrowArrayStream},))
    return nothing
end

function _stream_get_schema_callback_ptr()
    STREAM_GET_SCHEMA_CALLBACK[] == C_NULL && _init_stream_callbacks!()
    return STREAM_GET_SCHEMA_CALLBACK[]
end

function _stream_get_next_callback_ptr()
    STREAM_GET_NEXT_CALLBACK[] == C_NULL && _init_stream_callbacks!()
    return STREAM_GET_NEXT_CALLBACK[]
end

function _stream_get_last_error_callback_ptr()
    STREAM_GET_LAST_ERROR_CALLBACK[] == C_NULL && _init_stream_callbacks!()
    return STREAM_GET_LAST_ERROR_CALLBACK[]
end

function _stream_release_callback_ptr()
    STREAM_RELEASE_CALLBACK[] == C_NULL && _init_stream_callbacks!()
    return STREAM_RELEASE_CALLBACK[]
end

function _stream_export_for_table(table::Table)
    handle = StreamHandle(table, false, _cstring(""), C_NULL)
    private_data = _retain!(handle)
    handle.private_data = private_data
    stream = ArrowArrayStream(
        _stream_get_schema_callback_ptr(),
        _stream_get_next_callback_ptr(),
        _stream_get_last_error_callback_ptr(),
        _stream_release_callback_ptr(),
        private_data,
    )
    return StreamExport(Ref(stream), handle)
end

"""
    Arrow.CData.exportstream(table::Arrow.Table) -> Arrow.CData.ExportedStream

Export an `Arrow.Table` as a single-batch Arrow C Stream Interface producer.

The stream's `get_schema` callback exports the table schema, `get_next`
exports the table as the first `ArrowArray`, and a later `get_next` call
returns an end-of-stream `ArrowArray` with `release == C_NULL`. Keep the
returned `ExportedStream` alive while a C consumer may access
[`stream_ptr`](@ref), then call [`Arrow.CData.release!`](@ref) when done.
"""
function exportstream(table::Table)
    return ExportedStream(_stream_export_for_table(table))
end

"""
    Arrow.CData.exportstream!(
        stream_out::Ptr{Arrow.CData.ArrowArrayStream},
        table::Arrow.Table,
    ) -> Arrow.CData.ExportedStream

Export an `Arrow.Table` into caller-owned Arrow C Stream Interface storage.
"""
function exportstream!(stream_out::Ptr{ArrowArrayStream}, table::Table)
    stream_out == C_NULL &&
        throw(ArgumentError("ArrowArrayStream output pointer must not be C_NULL"))
    stream_export = _stream_export_for_table(table)
    unsafe_store!(stream_out, stream_export.ref[])
    return ExportedStream(stream_export, stream_out)
end

"""
    Arrow.CData.stream(exported)

Return the current `ArrowArrayStream` value for an exported stream.
"""
stream(exported::ExportedStream) = unsafe_load(stream_ptr(exported))

"""
    Arrow.CData.stream_ptr(exported)

Return a pointer to the exported stream's `ArrowArrayStream` base struct.
"""
stream_ptr(exported::ExportedStream) = exported.stream_base

"""
    Arrow.CData.isreleased(exported::Arrow.CData.ExportedStream)

Return whether an exported C Stream has been released.
"""
isreleased(exported::ExportedStream) = stream(exported).release == C_NULL

"""
    Arrow.CData.release!(exported::Arrow.CData.ExportedStream)

Release the exported stream if it is still live.
"""
function release!(exported::ExportedStream)
    st = stream(exported)
    st.release == C_NULL ||
        ccall(st.release, Cvoid, (Ptr{ArrowArrayStream},), stream_ptr(exported))
    return exported
end
