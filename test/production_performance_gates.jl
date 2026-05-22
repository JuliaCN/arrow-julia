# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

const TEST_ROOT = @__DIR__

const REPORT_SCRIPTS = (
    ipc="ipc_performance_report.jl",
    cdata="cdata_validation_report.jl",
    flightwire="flight_wire_performance_report.jl",
    flight="flight_purehttp2_perf.jl",
    flightsql="flight_sql_performance_report.jl",
    flightsqlendpoint="flight_sql_endpoint_report.jl",
)

function selected_reports()
    raw = get(ENV, "ARROW_PRODUCTION_PERFORMANCE_REPORTS", "ipc,cdata,flightwire,flight")
    names = Symbol[]
    for item in split(raw, ',')
        name = Symbol(strip(item))
        haskey(REPORT_SCRIPTS, name) || error(
            "Unknown production performance report '$item'; expected one of $(keys(REPORT_SCRIPTS))",
        )
        push!(names, name)
    end
    return names
end

function julia_report_cmd(script::AbstractString)
    return Cmd(
        vcat(
            Base.julia_cmd().exec,
            ["--project=$(TEST_ROOT)", joinpath(TEST_ROOT, script)],
        ),
    )
end

function run_report(name::Symbol)
    script = REPORT_SCRIPTS[name]
    println("production_performance_report=$(name) script=$(script)")
    flush(stdout)
    run(julia_report_cmd(script))
    return nothing
end

function main()
    names = selected_reports()
    isempty(names) &&
        error("ARROW_PRODUCTION_PERFORMANCE_REPORTS must select at least one report")
    println("production_performance_reports=$(join(String.(names), ','))")
    for name in names
        run_report(name)
    end
    println("production_performance_status=passed")
    return nothing
end

main()
