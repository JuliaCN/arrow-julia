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

using Arrow, JSON3, Tables, Test

include(joinpath(dirname(pathof(Arrow)), "../test/arrowjson.jl"))
# using .ArrowJSON

struct IntegrationCLIOptions
    jsonname::String
    arrowname::String
    inputname::String
    outputname::String
    mode::String
    verbose::Bool
    integration::Bool
end

function _normalizemode(mode)
    normalized = uppercase(replace(mode, "-" => "_"))
    normalized in
    ("ARROW_TO_JSON", "JSON_TO_ARROW", "VALIDATE", "FILE_TO_STREAM", "STREAM_TO_FILE") ||
        error("unknown integration test mode: $mode")
    return normalized
end

function _splitoption(arg)
    startswith(arg, "--") || return arg, nothing
    parts = split(arg, "="; limit=2)
    return parts[1], length(parts) == 2 ? parts[2] : nothing
end

function _requirednext(args, i, flag)
    i < length(args) || error("missing value for $flag")
    value = args[i + 1]
    startswith(value, "-") && error("missing value for $flag")
    return value
end

function parseintegrationargs(args)
    jsonname = ""
    arrowname = ""
    inputname = ""
    outputname = ""
    mode = "VALIDATE"
    verbose = false
    integration = false
    i = firstindex(args)
    while i <= lastindex(args)
        arg = args[i]
        if arg in ("--integration", "-i")
            integration = true
        elseif arg in ("--verbose", "-v")
            verbose = true
        elseif arg in ("--json", "-j")
            jsonname = _requirednext(args, i, arg)
            i += 1
        elseif arg in ("--arrow", "-a")
            arrowname = _requirednext(args, i, arg)
            i += 1
        elseif arg in ("--input", "--in")
            inputname = _requirednext(args, i, arg)
            i += 1
        elseif arg in ("--output", "--out", "-o")
            outputname = _requirednext(args, i, arg)
            i += 1
        elseif arg in ("--mode", "-m")
            mode = _requirednext(args, i, arg)
            i += 1
        else
            key, value = _splitoption(arg)
            if key == "--json" && value !== nothing
                jsonname = value
            elseif key == "--arrow" && value !== nothing
                arrowname = value
            elseif key in ("--input", "--in") && value !== nothing
                inputname = value
            elseif key in ("--output", "--out") && value !== nothing
                outputname = value
            elseif key == "--mode" && value !== nothing
                mode = value
            else
                error("unknown integration test argument: $arg")
            end
        end
        i += 1
    end
    return IntegrationCLIOptions(
        jsonname,
        arrowname,
        inputname,
        outputname,
        _normalizemode(mode),
        verbose,
        integration,
    )
end

function runcommand(options::IntegrationCLIOptions)
    return runcommand(
        options.jsonname,
        options.arrowname,
        options.mode,
        options.verbose;
        inputname=options.inputname,
        outputname=options.outputname,
    )
end

function _requirepath(path, label)
    path == "" && error("must provide $label")
    return path
end

function _filetostream(inputname, outputname)
    tbl = Arrow.Table(inputname)
    open(outputname, "w") do io
        Arrow.write(io, tbl; file=false)
    end
    return
end

function _streamtofile(inputname, outputname)
    tbl = Arrow.Table(inputname)
    Arrow.write(outputname, tbl)
    return
end

function runcommand(jsonname, arrowname, mode, verbose; inputname="", outputname="")
    mode = _normalizemode(mode)
    if mode == "ARROW_TO_JSON"
        _requirepath(jsonname, "json file name")
        _requirepath(arrowname, "arrow file name")
        verbose && println(stderr, "Converting Arrow IPC file $arrowname to JSON $jsonname")
        tbl = Arrow.Table(arrowname)
        df = ArrowJSON.DataFile(tbl)
        open(jsonname, "w") do io
            JSON3.write(io, df)
        end
    elseif mode == "JSON_TO_ARROW"
        _requirepath(jsonname, "json file name")
        _requirepath(arrowname, "arrow file name")
        verbose && println(stderr, "Converting JSON $jsonname to Arrow IPC file $arrowname")
        df = ArrowJSON.parsefile(jsonname)
        Arrow.write(arrowname, df)
    elseif mode == "VALIDATE"
        _requirepath(jsonname, "json file name")
        _requirepath(arrowname, "arrow file name")
        verbose &&
            println(stderr, "Validating Arrow IPC file $arrowname against JSON $jsonname")
        df = ArrowJSON.parsefile(jsonname)
        tbl = Arrow.Table(arrowname)
        @test isequal(df, tbl)
    elseif mode == "FILE_TO_STREAM"
        _requirepath(inputname, "input file name")
        _requirepath(outputname, "output stream file name")
        verbose &&
            println(stderr, "Converting Arrow IPC file $inputname to stream $outputname")
        _filetostream(inputname, outputname)
    elseif mode == "STREAM_TO_FILE"
        _requirepath(inputname, "input stream file name")
        _requirepath(outputname, "output file name")
        verbose &&
            println(stderr, "Converting Arrow IPC stream $inputname to file $outputname")
        _streamtofile(inputname, outputname)
    end
    return
end

function main(args=ARGS)
    return runcommand(parseintegrationargs(args))
end

if abspath(PROGRAM_FILE) == abspath(@__FILE__)
    try
        main()
    catch err
        showerror(stderr, err)
        println(stderr)
        exit(1)
    end
end
