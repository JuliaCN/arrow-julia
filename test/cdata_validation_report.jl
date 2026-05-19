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

using Arrow
using Printf
using Tables

const CData = Arrow.CData

function measure(f)
    GC.gc()
    result = Ref{Any}()
    start = time_ns()
    allocated = @allocated begin
        result[] = f()
    end
    elapsed_ms = (time_ns() - start) / 1_000_000
    return result[], elapsed_ms, allocated
end

function cdata_perf_table(rows::Int)
    ids = collect(Int64(1):Int64(rows))
    scores = Float64.(ids) ./ 10
    flags = isodd.(ids)
    names = ["row_$(i)" for i = 1:rows]
    blobs = [codeunits("blob_$(i)") for i = 1:rows]
    return Arrow.Table(
        Arrow.tobuffer((id=ids, score=scores, flag=flags, name=names, blob=blobs)),
    )
end

function child_array(array::CData.ArrowArray, index::Int)
    return unsafe_load(unsafe_load(array.children, index))
end

child_array(exported, index::Int) = child_array(CData.array(exported), index)

function assert_zero_copy(array::CData.ArrowArray, imported)
    id_array = child_array(array, 1)
    score_array = child_array(array, 2)
    name_array = child_array(array, 4)
    blob_array = child_array(array, 5)

    id = Tables.getcolumn(imported, :id)
    score = Tables.getcolumn(imported, :score)
    name = Tables.getcolumn(imported, :name)
    blob = Tables.getcolumn(imported, :blob)

    @assert pointer(id) == Ptr{Int64}(unsafe_load(id_array.buffers, 2))
    @assert pointer(score) == Ptr{Float64}(unsafe_load(score_array.buffers, 2))
    @assert pointer(name.offsets) == Ptr{Int32}(unsafe_load(name_array.buffers, 2))
    @assert pointer(name.data) == Ptr{UInt8}(unsafe_load(name_array.buffers, 3))
    @assert pointer(blob.offsets) == Ptr{Int32}(unsafe_load(blob_array.buffers, 2))
    @assert pointer(blob.data) == Ptr{UInt8}(unsafe_load(blob_array.buffers, 3))
    return nothing
end

assert_zero_copy(exported, imported) = assert_zero_copy(CData.array(exported), imported)

function checksum(imported)
    ids = Tables.getcolumn(imported, :id)
    names = Tables.getcolumn(imported, :name)
    total = 0
    @inbounds for i in eachindex(ids)
        total += Int(ids[i])
        total += ncodeunits(names[i])
    end
    return total
end

function print_measure(label::AbstractString, elapsed_ms, allocated)
    @printf("%s_ms=%.3f\n", label, elapsed_ms)
    println("$(label)_alloc_bytes=$(allocated)")
    return nothing
end

function optional_int_limit(name::AbstractString)
    value = strip(get(ENV, name, ""))
    isempty(value) && return nothing
    parsed = tryparse(Int, value)
    parsed !== nothing && parsed >= 0 ||
        throw(ArgumentError("$name must be a non-negative integer"))
    return parsed
end

function optional_float_limit(name::AbstractString)
    value = strip(get(ENV, name, ""))
    isempty(value) && return nothing
    parsed = tryparse(Float64, value)
    parsed !== nothing && parsed >= 0 ||
        throw(ArgumentError("$name must be a non-negative number"))
    return parsed
end

function enforce_max(label::AbstractString, value, limit)
    limit === nothing && return nothing
    println("$(label)_max=$(limit)")
    value <= limit || error("$(label)=$(value) exceeded configured max $(limit)")
    return nothing
end

function main()
    rows = parse(Int, get(ENV, "ARROW_CDATA_REPORT_ROWS", "100000"))
    rows > 0 || throw(ArgumentError("ARROW_CDATA_REPORT_ROWS must be positive"))

    table, prepare_ms, prepare_alloc = measure(() -> cdata_perf_table(rows))
    exported, export_ms, export_alloc = measure(() -> CData.exporttable(table))
    imported, import_ms, import_alloc = measure(
        () -> CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported)),
    )

    assert_zero_copy(exported, imported)

    sum_value, scan_ms, scan_alloc = measure(() -> checksum(imported))

    exported_stream, stream_export_ms, stream_export_alloc =
        measure(() -> CData.exportstream(table))
    imported_stream, stream_open_ms, stream_open_alloc =
        measure(() -> CData.importstream(CData.stream_ptr(exported_stream)))
    stream_batches, stream_import_ms, stream_import_alloc =
        measure(() -> collect(Tables.partitions(imported_stream)))
    stream_batch = only(stream_batches)

    assert_zero_copy(CData.array(stream_batch), stream_batch)

    stream_sum_value, stream_scan_ms, stream_scan_alloc =
        measure(() -> checksum(stream_batch))

    println("arrow_cdata_validation_report")
    println("rows=$(rows)")
    print_measure("prepare", prepare_ms, prepare_alloc)
    print_measure("export", export_ms, export_alloc)
    print_measure("import", import_ms, import_alloc)
    print_measure("scan", scan_ms, scan_alloc)
    print_measure("stream_export", stream_export_ms, stream_export_alloc)
    print_measure("stream_open", stream_open_ms, stream_open_alloc)
    print_measure("stream_import", stream_import_ms, stream_import_alloc)
    print_measure("stream_scan", stream_scan_ms, stream_scan_alloc)
    println("zero_copy_checks=passed")
    println("checksum=$(sum_value)")
    println("stream_checksum=$(stream_sum_value)")
    enforce_max(
        "import_alloc_bytes",
        import_alloc,
        optional_int_limit("ARROW_CDATA_MAX_IMPORT_ALLOC_BYTES"),
    )
    enforce_max("import_ms", import_ms, optional_float_limit("ARROW_CDATA_MAX_IMPORT_MS"))
    enforce_max(
        "stream_import_alloc_bytes",
        stream_import_alloc,
        optional_int_limit("ARROW_CDATA_MAX_STREAM_IMPORT_ALLOC_BYTES"),
    )
    enforce_max(
        "stream_import_ms",
        stream_import_ms,
        optional_float_limit("ARROW_CDATA_MAX_STREAM_IMPORT_MS"),
    )

    CData.release!(stream_batch)
    @assert CData.isreleased(stream_batch)
    CData.release!(imported_stream)
    @assert CData.isreleased(imported_stream)
    @assert CData.isreleased(exported_stream)
    CData.release!(imported)
    @assert CData.isreleased(imported)
    @assert CData.isreleased(exported)
    return nothing
end

main()
