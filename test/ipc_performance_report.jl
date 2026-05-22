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

function ipc_perf_table(rows::Int)
    ids = collect(Int64(1):Int64(rows))
    scores = Float64.(ids) ./ 10
    flags = isodd.(ids)
    names = ["row_$(i)" for i = 1:rows]
    blobs = [codeunits("blob_$(i)") for i = 1:rows]
    return (id=ids, score=scores, flag=flags, name=names, blob=blobs)
end

function write_ipc_bytes(source; file::Bool)
    io = IOBuffer()
    Arrow.write(io, source; file=file)
    return take!(io)
end

function direct_tobuffer_bytes(source)
    io = Arrow.tobuffer(source; file=false)
    return take!(io)
end

function stream_to_batches(bytes::Vector{UInt8})
    return collect(Arrow.Stream(bytes; convert=false))
end

function table_checksum(table)
    ids = Tables.getcolumn(table, :id)
    scores = Tables.getcolumn(table, :score)
    flags = Tables.getcolumn(table, :flag)
    names = Tables.getcolumn(table, :name)
    blobs = Tables.getcolumn(table, :blob)
    return table_checksum_columns(ids, scores, flags, names, blobs)
end

function table_checksum_columns(ids, scores, flags, names, blobs)
    total = 0
    @inbounds for i in eachindex(ids)
        total += Int(ids[i])
        total += round(Int, scores[i] * 10)
        total += flags[i] ? 1 : 0
        total += ncodeunits(names[i])
        total += length(blobs[i])
    end
    return total
end

function list_payload_widths(column)
    offsets = column.offsets.offsets
    total = 0
    @inbounds for i = 1:(length(offsets) - 1)
        total += Int(offsets[i + 1]) - Int(offsets[i])
    end
    return total
end

function table_physical_checksum(table)
    ids = Tables.getcolumn(table, :id)
    scores = Tables.getcolumn(table, :score)
    flags = Tables.getcolumn(table, :flag)
    names = Tables.getcolumn(table, :name)
    blobs = Tables.getcolumn(table, :blob)
    return table_physical_checksum_columns(ids, scores, flags, names, blobs)
end

function table_physical_checksum_columns(ids, scores, flags, names, blobs)
    total = 0
    @inbounds for i in eachindex(ids)
        total += Int(ids[i])
        total += round(Int, scores[i] * 10)
        total += flags[i] ? 1 : 0
    end
    total += list_payload_widths(names)
    total += list_payload_widths(blobs)
    return total
end

function checksum(batches::AbstractVector)
    total = 0
    for batch in batches
        total += table_checksum(batch)
    end
    return total
end

checksum(table) = table_checksum(table)

function physical_checksum(batches::AbstractVector)
    total = 0
    for batch in batches
        total += table_physical_checksum(batch)
    end
    return total
end

physical_checksum(table) = table_physical_checksum(table)

function print_measure(label::AbstractString, elapsed_ms, allocated)
    @printf("%s_ms=%.3f\n", label, elapsed_ms)
    println("$(label)_alloc_bytes=$(allocated)")
    return nothing
end

function print_throughput(label::AbstractString, bytes::Integer, elapsed_ms)
    mib_per_sec = (bytes / max(elapsed_ms, eps(Float64)) * 1_000) / 1024.0^2
    @printf("%s_throughput_mib_per_sec=%.3f\n", label, mib_per_sec)
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

function row_count(batches::AbstractVector)
    total = 0
    for batch in batches
        total += length(Tables.getcolumn(batch, :id))
    end
    return total
end

row_count(table) = length(Tables.getcolumn(table, :id))

function assert_row_count(table, rows::Int, label::AbstractString)
    actual = length(Tables.getcolumn(table, :id))
    actual == rows || error("$(label)_rows=$(actual) did not match expected rows=$(rows)")
    return nothing
end

function assert_row_count(batches::AbstractVector, rows::Int, label::AbstractString)
    actual = row_count(batches)
    actual == rows || error("$(label)_rows=$(actual) did not match expected rows=$(rows)")
    return nothing
end

function warmup_ipc()
    source = ipc_perf_table(16)
    direct_source = (id=source.id, score=source.score, flag=source.flag)
    stream_bytes = write_ipc_bytes(source; file=false)
    file_bytes = write_ipc_bytes(source; file=true)
    direct_tobuffer_bytes(direct_source)
    stream_batches = stream_to_batches(stream_bytes)
    file_table = Arrow.Table(file_bytes; convert=false)
    physical_checksum(stream_batches)
    physical_checksum(file_table)
    checksum(stream_batches)
    checksum(file_table)
    return nothing
end

function main()
    rows = parse(Int, get(ENV, "ARROW_IPC_REPORT_ROWS", "50000"))
    rows > 0 || throw(ArgumentError("ARROW_IPC_REPORT_ROWS must be positive"))

    warmup_ipc()

    source, prepare_ms, prepare_alloc = measure(() -> ipc_perf_table(rows))
    expected_checksum = checksum(source)
    direct_source = (id=source.id, score=source.score, flag=source.flag)

    stream_bytes, stream_write_ms, stream_write_alloc =
        measure(() -> write_ipc_bytes(source; file=false))
    file_bytes, file_write_ms, file_write_alloc =
        measure(() -> write_ipc_bytes(source; file=true))
    direct_bytes, direct_tobuffer_ms, direct_tobuffer_alloc =
        measure(() -> direct_tobuffer_bytes(direct_source))

    stream_batches, stream_read_ms, stream_read_alloc =
        measure(() -> stream_to_batches(stream_bytes))
    file_table, file_read_ms, file_read_alloc =
        measure(() -> Arrow.Table(file_bytes; convert=false))
    direct_table = Arrow.Table(direct_bytes; convert=false)

    assert_row_count(stream_batches, rows, "stream_read")
    assert_row_count(file_table, rows, "file_read")
    assert_row_count(direct_table, rows, "direct_tobuffer_read")

    stream_physical_checksum, stream_physical_scan_ms, stream_physical_scan_alloc =
        measure(() -> physical_checksum(stream_batches))
    file_physical_checksum, file_physical_scan_ms, file_physical_scan_alloc =
        measure(() -> physical_checksum(file_table))
    stream_checksum, stream_scan_ms, stream_scan_alloc =
        measure(() -> checksum(stream_batches))
    file_checksum, file_scan_ms, file_scan_alloc = measure(() -> checksum(file_table))
    stream_physical_checksum == expected_checksum || error(
        "stream_physical_checksum=$(stream_physical_checksum) did not match $(expected_checksum)",
    )
    file_physical_checksum == expected_checksum || error(
        "file_physical_checksum=$(file_physical_checksum) did not match $(expected_checksum)",
    )
    stream_checksum == expected_checksum ||
        error("stream_checksum=$(stream_checksum) did not match $(expected_checksum)")
    file_checksum == expected_checksum ||
        error("file_checksum=$(file_checksum) did not match $(expected_checksum)")

    println("arrow_ipc_performance_report")
    println("rows=$(rows)")
    println("stream_bytes=$(length(stream_bytes))")
    println("file_bytes=$(length(file_bytes))")
    println("direct_tobuffer_bytes=$(length(direct_bytes))")
    println("expected_checksum=$(expected_checksum)")
    println("stream_physical_checksum=$(stream_physical_checksum)")
    println("file_physical_checksum=$(file_physical_checksum)")
    println("stream_checksum=$(stream_checksum)")
    println("file_checksum=$(file_checksum)")
    print_measure("prepare", prepare_ms, prepare_alloc)
    print_measure("stream_write", stream_write_ms, stream_write_alloc)
    print_throughput("stream_write", length(stream_bytes), stream_write_ms)
    print_measure("file_write", file_write_ms, file_write_alloc)
    print_throughput("file_write", length(file_bytes), file_write_ms)
    print_measure("direct_tobuffer", direct_tobuffer_ms, direct_tobuffer_alloc)
    print_throughput("direct_tobuffer", length(direct_bytes), direct_tobuffer_ms)
    print_measure("stream_read", stream_read_ms, stream_read_alloc)
    print_throughput("stream_read", length(stream_bytes), stream_read_ms)
    print_measure("file_read", file_read_ms, file_read_alloc)
    print_throughput("file_read", length(file_bytes), file_read_ms)
    print_measure(
        "stream_physical_scan",
        stream_physical_scan_ms,
        stream_physical_scan_alloc,
    )
    print_throughput("stream_physical_scan", length(stream_bytes), stream_physical_scan_ms)
    print_measure("file_physical_scan", file_physical_scan_ms, file_physical_scan_alloc)
    print_throughput("file_physical_scan", length(file_bytes), file_physical_scan_ms)
    print_measure("stream_scan", stream_scan_ms, stream_scan_alloc)
    print_throughput("stream_scan", length(stream_bytes), stream_scan_ms)
    print_measure("file_scan", file_scan_ms, file_scan_alloc)
    print_throughput("file_scan", length(file_bytes), file_scan_ms)

    enforce_max(
        "stream_write_alloc_bytes",
        stream_write_alloc,
        optional_int_limit("ARROW_IPC_MAX_STREAM_WRITE_ALLOC_BYTES"),
    )
    enforce_max(
        "file_write_alloc_bytes",
        file_write_alloc,
        optional_int_limit("ARROW_IPC_MAX_FILE_WRITE_ALLOC_BYTES"),
    )
    enforce_max(
        "direct_tobuffer_alloc_bytes",
        direct_tobuffer_alloc,
        optional_int_limit("ARROW_IPC_MAX_DIRECT_TOBUFFER_ALLOC_BYTES"),
    )
    enforce_max(
        "stream_read_alloc_bytes",
        stream_read_alloc,
        optional_int_limit("ARROW_IPC_MAX_STREAM_READ_ALLOC_BYTES"),
    )
    enforce_max(
        "file_read_alloc_bytes",
        file_read_alloc,
        optional_int_limit("ARROW_IPC_MAX_FILE_READ_ALLOC_BYTES"),
    )
    enforce_max(
        "stream_physical_scan_alloc_bytes",
        stream_physical_scan_alloc,
        optional_int_limit("ARROW_IPC_MAX_STREAM_PHYSICAL_SCAN_ALLOC_BYTES"),
    )
    enforce_max(
        "file_physical_scan_alloc_bytes",
        file_physical_scan_alloc,
        optional_int_limit("ARROW_IPC_MAX_FILE_PHYSICAL_SCAN_ALLOC_BYTES"),
    )
    enforce_max(
        "stream_scan_alloc_bytes",
        stream_scan_alloc,
        optional_int_limit("ARROW_IPC_MAX_STREAM_SCAN_ALLOC_BYTES"),
    )
    enforce_max(
        "file_scan_alloc_bytes",
        file_scan_alloc,
        optional_int_limit("ARROW_IPC_MAX_FILE_SCAN_ALLOC_BYTES"),
    )
    enforce_max(
        "stream_write_ms",
        stream_write_ms,
        optional_float_limit("ARROW_IPC_MAX_STREAM_WRITE_MS"),
    )
    enforce_max(
        "file_write_ms",
        file_write_ms,
        optional_float_limit("ARROW_IPC_MAX_FILE_WRITE_MS"),
    )
    enforce_max(
        "direct_tobuffer_ms",
        direct_tobuffer_ms,
        optional_float_limit("ARROW_IPC_MAX_DIRECT_TOBUFFER_MS"),
    )
    enforce_max(
        "stream_read_ms",
        stream_read_ms,
        optional_float_limit("ARROW_IPC_MAX_STREAM_READ_MS"),
    )
    enforce_max(
        "file_read_ms",
        file_read_ms,
        optional_float_limit("ARROW_IPC_MAX_FILE_READ_MS"),
    )
    enforce_max(
        "stream_physical_scan_ms",
        stream_physical_scan_ms,
        optional_float_limit("ARROW_IPC_MAX_STREAM_PHYSICAL_SCAN_MS"),
    )
    enforce_max(
        "file_physical_scan_ms",
        file_physical_scan_ms,
        optional_float_limit("ARROW_IPC_MAX_FILE_PHYSICAL_SCAN_MS"),
    )
    enforce_max(
        "stream_scan_ms",
        stream_scan_ms,
        optional_float_limit("ARROW_IPC_MAX_STREAM_SCAN_MS"),
    )
    enforce_max(
        "file_scan_ms",
        file_scan_ms,
        optional_float_limit("ARROW_IPC_MAX_FILE_SCAN_MS"),
    )
    return nothing
end

main()
