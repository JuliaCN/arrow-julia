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

const FlightSQL = Arrow.Flight.SQL
const FlightSQLGenerated = Arrow.Flight.SQL.Generated

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

function flight_sql_command_fixtures()
    return (
        FlightSQLGenerated.CommandGetCatalogs(),
        FlightSQLGenerated.CommandGetSqlInfo(UInt32[0, 4, 8]),
        FlightSQLGenerated.CommandGetTables(
            "catalog",
            "schema%",
            "table%",
            ["BASE TABLE", "VIEW"],
            true,
        ),
        FlightSQLGenerated.CommandStatementQuery("select * from t", UInt8[0x01]),
        FlightSQLGenerated.CommandStatementUpdate("update t set x = 1", UInt8[]),
        FlightSQLGenerated.CommandPreparedStatementQuery(UInt8[0x10, 0x20]),
        FlightSQLGenerated.CommandPreparedStatementUpdate(UInt8[0x30, 0x40]),
        FlightSQLGenerated.CommandStatementIngest(
            nothing,
            "target_table",
            "target_schema",
            "target_catalog",
            true,
            UInt8[0x55],
            Dict("mode" => "append"),
        ),
    )
end

function flight_sql_action_fixtures()
    return (
        FlightSQLGenerated.ActionCreatePreparedStatementRequest("select ?", UInt8[0x01]),
        FlightSQLGenerated.ActionClosePreparedStatementRequest(UInt8[0x02]),
        FlightSQLGenerated.ActionBeginTransactionRequest(),
        FlightSQLGenerated.ActionBeginSavepointRequest(UInt8[0x03], "savepoint_1"),
        FlightSQLGenerated.ActionCancelQueryRequest(UInt8[0x04]),
    )
end

function pack_commands(commands)
    checksum = 0
    for message in commands
        descriptor = FlightSQL.commanddescriptor(message)
        checksum += length(descriptor.cmd)
    end
    return checksum
end

function command_descriptors(commands)
    return map(FlightSQL.commanddescriptor, commands)
end

function decode_commands(descriptors)
    checksum = 0
    for descriptor in descriptors
        any = FlightSQL.decodeany(descriptor.cmd)
        checksum += ncodeunits(any.type_url)
        checksum += length(any.value)
    end
    return checksum
end

function pack_actions(actions)
    checksum = 0
    for message in actions
        action = FlightSQL.action(message)
        checksum += ncodeunits(getfield(action, Symbol("#type")))
        checksum += length(action.body)
    end
    return checksum
end

function action_messages(actions)
    return map(FlightSQL.action, actions)
end

function decode_actions(actions)
    checksum = 0
    for action in actions
        any = FlightSQL.decodeany(action.body)
        checksum += ncodeunits(any.type_url)
        checksum += length(any.value)
    end
    return checksum
end

function put_result_metadata()
    unknown = FlightSQL.doputupdateresult(-1)
    updated = FlightSQL.doputupdateresult(42)
    prepared = FlightSQL.doputpreparedstatementresult(UInt8[0xaa, 0xbb])
    return FlightSQL.doputupdatecount(unknown) +
           FlightSQL.doputupdatecount(updated) +
           length(FlightSQL.doputpreparedstatementhandle(prepared))
end

function better_sample(candidate, current)
    current === nothing && return true
    candidate.allocated < current.allocated && return true
    return candidate.allocated == current.allocated &&
           candidate.elapsed_ms < current.elapsed_ms
end

function steady_measure(operation; warmups::Int=3, samples::Int=7)
    for _ = 1:warmups
        operation()
    end
    best = nothing
    for _ = 1:samples
        sample = measure(operation)
        better_sample(sample, best) && (best = sample)
    end
    return best
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

function print_measure(label::AbstractString, sample)
    @printf("%s_ms=%.3f\n", label, sample.elapsed_ms)
    println("$(label)_alloc_bytes=$(sample.allocated)")
    println("$(label)_checksum=$(sample.value)")
    return nothing
end

function main()
    warmups = parse(Int, get(ENV, "ARROW_FLIGHT_SQL_REPORT_WARMUPS", "3"))
    samples = parse(Int, get(ENV, "ARROW_FLIGHT_SQL_REPORT_SAMPLES", "7"))
    warmups >= 0 ||
        throw(ArgumentError("ARROW_FLIGHT_SQL_REPORT_WARMUPS must be non-negative"))
    samples > 0 || throw(ArgumentError("ARROW_FLIGHT_SQL_REPORT_SAMPLES must be positive"))

    commands = flight_sql_command_fixtures()
    actions = flight_sql_action_fixtures()
    descriptors = command_descriptors(commands)
    action_payloads = action_messages(actions)

    command_pack =
        steady_measure(() -> pack_commands(commands); warmups=warmups, samples=samples)
    command_decode =
        steady_measure(() -> decode_commands(descriptors); warmups=warmups, samples=samples)
    action_pack =
        steady_measure(() -> pack_actions(actions); warmups=warmups, samples=samples)
    action_decode = steady_measure(
        () -> decode_actions(action_payloads);
        warmups=warmups,
        samples=samples,
    )
    put_results = steady_measure(put_result_metadata; warmups=warmups, samples=samples)

    println("arrow_flight_sql_performance_report")
    println("command_count=$(length(commands))")
    println("action_count=$(length(actions))")
    println("julia_num_threads=$(Threads.nthreads())")
    println("warmups=$(warmups)")
    println("samples=$(samples)")
    print_measure("flight_sql_command_pack", command_pack)
    print_measure("flight_sql_command_decode", command_decode)
    print_measure("flight_sql_action_pack", action_pack)
    print_measure("flight_sql_action_decode", action_decode)
    print_measure("flight_sql_put_result", put_results)

    enforce_max(
        "flight_sql_command_pack_alloc_bytes",
        command_pack.allocated,
        optional_int_limit("ARROW_FLIGHT_SQL_MAX_COMMAND_PACK_ALLOC_BYTES"),
    )
    enforce_max(
        "flight_sql_command_decode_alloc_bytes",
        command_decode.allocated,
        optional_int_limit("ARROW_FLIGHT_SQL_MAX_COMMAND_DECODE_ALLOC_BYTES"),
    )
    enforce_max(
        "flight_sql_action_pack_alloc_bytes",
        action_pack.allocated,
        optional_int_limit("ARROW_FLIGHT_SQL_MAX_ACTION_PACK_ALLOC_BYTES"),
    )
    enforce_max(
        "flight_sql_action_decode_alloc_bytes",
        action_decode.allocated,
        optional_int_limit("ARROW_FLIGHT_SQL_MAX_ACTION_DECODE_ALLOC_BYTES"),
    )
    enforce_max(
        "flight_sql_put_result_alloc_bytes",
        put_results.allocated,
        optional_int_limit("ARROW_FLIGHT_SQL_MAX_PUT_RESULT_ALLOC_BYTES"),
    )
    enforce_max(
        "flight_sql_command_pack_ms",
        command_pack.elapsed_ms,
        optional_float_limit("ARROW_FLIGHT_SQL_MAX_COMMAND_PACK_MS"),
    )
    enforce_max(
        "flight_sql_action_pack_ms",
        action_pack.elapsed_ms,
        optional_float_limit("ARROW_FLIGHT_SQL_MAX_ACTION_PACK_MS"),
    )
    return nothing
end

main()
