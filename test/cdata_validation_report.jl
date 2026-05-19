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
    return (value=result[], elapsed_ms=elapsed_ms, allocated=allocated)
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
    return checksum_columns(
        Tables.getcolumn(imported, :id),
        Tables.getcolumn(imported, :name),
    )
end

function checksum_columns(ids::AbstractVector, names)
    offsets = names.offsets
    total = 0
    @inbounds for i in eachindex(ids)
        total += Int(ids[i])
        total += Int(offsets[i + 1]) - Int(offsets[i])
    end
    return total
end

noop_cleanup(_) = nothing

function release_exported_table!(exported)
    CData.release!(exported)
    @assert CData.isreleased(exported)
    return nothing
end

function release_exported_stream!(exported_stream)
    CData.release!(exported_stream)
    @assert CData.isreleased(exported_stream)
    return nothing
end

function cleanup_base_import!(sample)
    CData.release!(sample.imported)
    @assert CData.isreleased(sample.imported)
    @assert CData.isreleased(sample.exported)
    return nothing
end

function cleanup_stream_open!(sample)
    CData.release!(sample.imported_stream)
    @assert CData.isreleased(sample.imported_stream)
    @assert CData.isreleased(sample.exported_stream)
    return nothing
end

function cleanup_stream_batch!(sample)
    CData.release!(sample.batch)
    @assert CData.isreleased(sample.batch)
    CData.release!(sample.imported_stream)
    @assert CData.isreleased(sample.imported_stream)
    @assert CData.isreleased(sample.exported_stream)
    return nothing
end

function cleanup_stream_batches!(sample)
    foreach(CData.release!, sample.batches)
    @assert all(CData.isreleased, sample.batches)
    CData.release!(sample.imported_stream)
    @assert CData.isreleased(sample.imported_stream)
    @assert CData.isreleased(sample.exported_stream)
    return nothing
end

function pull_stream_batch(imported_stream)
    next = iterate(Tables.partitions(imported_stream))
    next === nothing && error("expected one C Stream batch")
    batch, _ = next
    return batch
end

function operation_sample(value, measured)
    return (value=value, elapsed_ms=measured.elapsed_ms, allocated=measured.allocated)
end

function measure_base_import(table)
    exported = CData.exporttable(table)
    measured = measure(
        () -> CData.importtable(CData.schema_ptr(exported), CData.array_ptr(exported)),
    )
    return operation_sample((exported=exported, imported=measured.value), measured)
end

function measure_stream_open(table)
    exported_stream = CData.exportstream(table)
    measured = measure(() -> CData.importstream(CData.stream_ptr(exported_stream)))
    return operation_sample(
        (exported_stream=exported_stream, imported_stream=measured.value),
        measured,
    )
end

function measure_stream_batch(table)
    exported_stream = CData.exportstream(table)
    imported_stream = CData.importstream(CData.stream_ptr(exported_stream))
    measured = measure(() -> pull_stream_batch(imported_stream))
    return operation_sample(
        (
            exported_stream=exported_stream,
            imported_stream=imported_stream,
            batch=measured.value,
        ),
        measured,
    )
end

function measure_stream_collect(table)
    exported_stream = CData.exportstream(table)
    imported_stream = CData.importstream(CData.stream_ptr(exported_stream))
    measured = measure(() -> collect(Tables.partitions(imported_stream)))
    return operation_sample(
        (
            exported_stream=exported_stream,
            imported_stream=imported_stream,
            batches=measured.value,
        ),
        measured,
    )
end

function better_sample(candidate, current)
    current === nothing && return true
    candidate.allocated < current.allocated && return true
    return candidate.allocated == current.allocated &&
           candidate.elapsed_ms < current.elapsed_ms
end

function steady_measure(operation; cleanup=noop_cleanup, warmups::Int=2, samples::Int=5)
    for _ = 1:warmups
        sample = operation()
        cleanup(sample.value)
    end
    best = nothing
    for _ = 1:samples
        sample = operation()
        if better_sample(sample, best)
            best === nothing || cleanup(best.value)
            best = sample
        else
            cleanup(sample.value)
        end
    end
    return best
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
    warmups = parse(Int, get(ENV, "ARROW_CDATA_REPORT_WARMUPS", "2"))
    warmups >= 0 || throw(ArgumentError("ARROW_CDATA_REPORT_WARMUPS must be non-negative"))
    samples = parse(Int, get(ENV, "ARROW_CDATA_REPORT_SAMPLES", "5"))
    samples > 0 || throw(ArgumentError("ARROW_CDATA_REPORT_SAMPLES must be positive"))

    prepared = measure(() -> cdata_perf_table(rows))
    table = prepared.value
    exported = steady_measure(
        () -> measure(() -> CData.exporttable(table));
        cleanup=(release_exported_table!),
        warmups=warmups,
        samples=samples,
    )
    imported = steady_measure(
        () -> measure_base_import(table);
        cleanup=(cleanup_base_import!),
        warmups=warmups,
        samples=samples,
    )

    assert_zero_copy(imported.value.exported, imported.value.imported)

    scanned = steady_measure(
        () -> measure(() -> checksum(imported.value.imported));
        warmups=warmups,
        samples=samples,
    )

    exported_stream = steady_measure(
        () -> measure(() -> CData.exportstream(table));
        cleanup=(release_exported_stream!),
        warmups=warmups,
        samples=samples,
    )
    opened_stream = steady_measure(
        () -> measure_stream_open(table);
        cleanup=(cleanup_stream_open!),
        warmups=warmups,
        samples=samples,
    )
    stream_batch = steady_measure(
        () -> measure_stream_batch(table);
        cleanup=(cleanup_stream_batch!),
        warmups=warmups,
        samples=samples,
    )
    stream_collect = steady_measure(
        () -> measure_stream_collect(table);
        cleanup=(cleanup_stream_batches!),
        warmups=warmups,
        samples=samples,
    )

    assert_zero_copy(CData.array(stream_batch.value.batch), stream_batch.value.batch)

    stream_scanned = steady_measure(
        () -> measure(() -> checksum(stream_batch.value.batch));
        warmups=warmups,
        samples=samples,
    )

    println("arrow_cdata_validation_report")
    println("rows=$(rows)")
    println("warmups=$(warmups)")
    println("samples=$(samples)")
    print_measure("prepare", prepared.elapsed_ms, prepared.allocated)
    print_measure("export", exported.elapsed_ms, exported.allocated)
    print_measure("import", imported.elapsed_ms, imported.allocated)
    print_measure("scan", scanned.elapsed_ms, scanned.allocated)
    print_measure("stream_export", exported_stream.elapsed_ms, exported_stream.allocated)
    print_measure("stream_open", opened_stream.elapsed_ms, opened_stream.allocated)
    print_measure("stream_import", stream_batch.elapsed_ms, stream_batch.allocated)
    print_measure("stream_collect", stream_collect.elapsed_ms, stream_collect.allocated)
    print_measure("stream_scan", stream_scanned.elapsed_ms, stream_scanned.allocated)
    println("zero_copy_checks=passed")
    println("checksum=$(scanned.value)")
    println("stream_checksum=$(stream_scanned.value)")
    enforce_max(
        "import_alloc_bytes",
        imported.allocated,
        optional_int_limit("ARROW_CDATA_MAX_IMPORT_ALLOC_BYTES"),
    )
    enforce_max(
        "import_ms",
        imported.elapsed_ms,
        optional_float_limit("ARROW_CDATA_MAX_IMPORT_MS"),
    )
    enforce_max(
        "stream_import_alloc_bytes",
        stream_batch.allocated,
        optional_int_limit("ARROW_CDATA_MAX_STREAM_IMPORT_ALLOC_BYTES"),
    )
    enforce_max(
        "stream_import_ms",
        stream_batch.elapsed_ms,
        optional_float_limit("ARROW_CDATA_MAX_STREAM_IMPORT_MS"),
    )

    cleanup_stream_batches!(stream_collect.value)
    cleanup_stream_batch!(stream_batch.value)
    cleanup_stream_open!(opened_stream.value)
    release_exported_stream!(exported_stream.value)
    cleanup_base_import!(imported.value)
    release_exported_table!(exported.value)
    return nothing
end

main()
