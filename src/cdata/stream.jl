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

"""
    Arrow.CData.ImportedStream

Borrowed iterator over `ArrowArrayStream` batches. Release it explicitly with
[`Arrow.CData.release!`](@ref).
"""
mutable struct ImportedStream
    stream_base::Ptr{ArrowArrayStream}
end

"""
    Arrow.CData.ImportedStreamBatch

Tables.jl-compatible batch imported from an `ArrowArrayStream`. Release it
explicitly with [`Arrow.CData.release!`](@ref).
"""
mutable struct ImportedStreamBatch
    table::ImportedTable
    schema_ref::Base.RefValue{ArrowSchema}
    array_ref::Base.RefValue{ArrowArray}
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

function _assert_import_stream_layout(stream_ptr::Ptr{ArrowArrayStream})
    stream_ptr == C_NULL &&
        throw(ArgumentError("ArrowArrayStream input pointer must not be C_NULL"))
    stream = unsafe_load(stream_ptr)
    stream.release == C_NULL &&
        throw(ArgumentError("cannot import released ArrowArrayStream"))
    stream.get_schema == C_NULL &&
        throw(ArgumentError("ArrowArrayStream get_schema callback is C_NULL"))
    stream.get_next == C_NULL &&
        throw(ArgumentError("ArrowArrayStream get_next callback is C_NULL"))
    return stream
end

function _stream_last_error(stream::ArrowArrayStream, stream_ptr::Ptr{ArrowArrayStream})
    stream.get_last_error == C_NULL && return ""
    ptr = ccall(stream.get_last_error, Ptr{UInt8}, (Ptr{ArrowArrayStream},), stream_ptr)
    return ptr == C_NULL ? "" : unsafe_string(ptr)
end

function _stream_status_error(
    stream::ArrowArrayStream,
    stream_ptr::Ptr{ArrowArrayStream},
    callback::AbstractString,
)
    detail = _stream_last_error(stream, stream_ptr)
    message =
        isempty(detail) ? "ArrowArrayStream $callback callback failed" :
        "ArrowArrayStream $callback callback failed: $detail"
    return ArgumentError(message)
end

function _get_next_stream_array(stream::ArrowArrayStream, stream_ptr::Ptr{ArrowArrayStream})
    array_ref = Ref{ArrowArray}()
    GC.@preserve array_ref begin
        array_out = Base.unsafe_convert(Ptr{ArrowArray}, array_ref)
        status = ccall(
            stream.get_next,
            Cint,
            (Ptr{ArrowArrayStream}, Ptr{ArrowArray}),
            stream_ptr,
            array_out,
        )
        status == 0 || throw(_stream_status_error(stream, stream_ptr, "get_next"))
        array_ref[].release == C_NULL && return nothing
        return array_ref
    end
end

function _get_stream_schema(
    stream::ArrowArrayStream,
    stream_ptr::Ptr{ArrowArrayStream},
    array_ref::Base.RefValue{ArrowArray},
)
    schema_ref = Ref{ArrowSchema}()
    GC.@preserve schema_ref begin
        schema_out = Base.unsafe_convert(Ptr{ArrowSchema}, schema_ref)
        status = ccall(
            stream.get_schema,
            Cint,
            (Ptr{ArrowArrayStream}, Ptr{ArrowSchema}),
            stream_ptr,
            schema_out,
        )
        if status != 0
            arr = array_ref[]
            array_ptr = Base.unsafe_convert(Ptr{ArrowArray}, array_ref)
            arr.release == C_NULL ||
                ccall(arr.release, Cvoid, (Ptr{ArrowArray},), array_ptr)
            throw(_stream_status_error(stream, stream_ptr, "get_schema"))
        end
        return schema_ref
    end
end

function _import_stream_batch(stream::ArrowArrayStream, stream_ptr::Ptr{ArrowArrayStream})
    array_ref = _get_next_stream_array(stream, stream_ptr)
    array_ref === nothing && return nothing
    schema_ref = _get_stream_schema(stream, stream_ptr, array_ref)
    GC.@preserve schema_ref array_ref begin
        schema_ptr = Base.unsafe_convert(Ptr{ArrowSchema}, schema_ref)
        array_ptr = Base.unsafe_convert(Ptr{ArrowArray}, array_ref)
        table = try
            importtable(schema_ptr, array_ptr)
        catch
            arr = array_ref[]
            arr.release == C_NULL ||
                ccall(arr.release, Cvoid, (Ptr{ArrowArray},), array_ptr)
            sch = schema_ref[]
            sch.release == C_NULL ||
                ccall(sch.release, Cvoid, (Ptr{ArrowSchema},), schema_ptr)
            rethrow()
        end
        return ImportedStreamBatch(table, schema_ref, array_ref)
    end
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
    Arrow.CData.importstream(stream_ptr) -> Arrow.CData.ImportedStream

Borrow an `ArrowArrayStream` as an iterator of
[`Arrow.CData.ImportedStreamBatch`](@ref) table views. Each batch owns the
caller-side `ArrowSchema` and `ArrowArray` storage returned by the stream
callbacks and must be released with [`Arrow.CData.release!`](@ref).
"""
function importstream(stream_ptr::Ptr{ArrowArrayStream})
    _assert_import_stream_layout(stream_ptr)
    return ImportedStream(stream_ptr)
end

"""
    Arrow.CData.stream(exported)

Return the current `ArrowArrayStream` value for an exported stream.
"""
stream(exported::ExportedStream) = unsafe_load(stream_ptr(exported))
stream(imported::ImportedStream) = unsafe_load(stream_ptr(imported))

"""
    Arrow.CData.stream_ptr(exported)

Return a pointer to the exported stream's `ArrowArrayStream` base struct.
"""
stream_ptr(exported::ExportedStream) = exported.stream_base
stream_ptr(imported::ImportedStream) = imported.stream_base

"""
    Arrow.CData.isreleased(exported::Arrow.CData.ExportedStream)

Return whether an exported C Stream has been released.
"""
isreleased(exported::ExportedStream) = stream(exported).release == C_NULL
isreleased(imported::ImportedStream) = stream(imported).release == C_NULL
isreleased(batch::ImportedStreamBatch) = isreleased(batch.table)

schema_ptr(batch::ImportedStreamBatch) = schema_ptr(batch.table)
array_ptr(batch::ImportedStreamBatch) = array_ptr(batch.table)
schema(batch::ImportedStreamBatch) = schema(batch.table)
array(batch::ImportedStreamBatch) = array(batch.table)
getmetadata(batch::ImportedStreamBatch) = getmetadata(batch.table)

Tables.istable(::Type{ImportedStreamBatch}) = true
Tables.columnaccess(::Type{ImportedStreamBatch}) = true
Tables.columns(batch::ImportedStreamBatch) = batch
Tables.columnnames(batch::ImportedStreamBatch) = Tables.columnnames(batch.table)
Tables.schema(batch::ImportedStreamBatch) = Tables.schema(batch.table)
Tables.getcolumn(batch::ImportedStreamBatch, index::Int) =
    Tables.getcolumn(batch.table, index)
Tables.getcolumn(batch::ImportedStreamBatch, name::Symbol) =
    Tables.getcolumn(batch.table, name)

Tables.partitions(stream::ImportedStream) = stream
Base.IteratorSize(::Type{ImportedStream}) = Base.SizeUnknown()
Base.eltype(::Type{ImportedStream}) = ImportedStreamBatch

function Base.iterate(imported::ImportedStream, state=nothing)
    st = _assert_import_stream_layout(stream_ptr(imported))
    batch = _import_stream_batch(st, stream_ptr(imported))
    batch === nothing && return nothing
    return batch, state
end

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

function release!(imported::ImportedStream)
    st = stream(imported)
    st.release == C_NULL ||
        ccall(st.release, Cvoid, (Ptr{ArrowArrayStream},), stream_ptr(imported))
    return imported
end

release!(batch::ImportedStreamBatch) = (release!(batch.table); batch)
