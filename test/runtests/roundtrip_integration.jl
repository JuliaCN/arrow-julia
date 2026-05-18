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

@testset "table roundtrips" begin
    for case in testtables
        testtable(case...)
    end
end # @testset "table roundtrips"

@testset "table append" begin
    # skip windows since file locking prevents mktemp cleanup
    if !Sys.iswindows()
        for case in testtables
            testappend(case...)
        end

        testappend_partitions()

        for compression_option in (:lz4, :zstd)
            testappend_compression(compression_option)
        end
    end
end # @testset "table append"

@testset "arrow json integration tests" begin
    for file in readdir(joinpath(dirname(pathof(Arrow)), "../test/arrowjson"))
        jsonfile = joinpath(joinpath(dirname(pathof(Arrow)), "../test/arrowjson"), file)
        @testset "integration test for $jsonfile" begin
            df = ArrowJSON.parsefile(jsonfile)
            io = Arrow.tobuffer(df)
            tbl = Arrow.Table(io; convert=false)
            @test isequal(df, tbl)
        end
    end
end # @testset "arrow json integration tests"

@testset "Archery-style integration CLI" begin
    arrowjsondir = joinpath(dirname(pathof(Arrow)), "../test/arrowjson")
    sourcejson = joinpath(arrowjsondir, "primitive-empty.json")
    mktempdir() do dir
        arrowfile = joinpath(dir, "primitive.arrow")
        streamfile = joinpath(dir, "primitive.stream")
        convertedfile = joinpath(dir, "primitive-converted.arrow")
        generatedjson = joinpath(dir, "primitive-roundtrip.json")
        options = parseintegrationargs([
            "--integration",
            "--json=$sourcejson",
            "--arrow",
            arrowfile,
            "--mode=json-to-arrow",
        ])
        @test options.integration
        @test options.jsonname == sourcejson
        @test options.arrowname == arrowfile
        @test options.mode == "JSON_TO_ARROW"
        runcommand(options)
        runcommand(sourcejson, arrowfile, "VALIDATE", false)
        runcommand(generatedjson, arrowfile, "ARROW_TO_JSON", false)
        runcommand(generatedjson, arrowfile, "VALIDATE", false)
        streamoptions = parseintegrationargs([
            "--integration",
            "--input=$arrowfile",
            "--output",
            streamfile,
            "--mode=file-to-stream",
        ])
        @test streamoptions.mode == "FILE_TO_STREAM"
        @test streamoptions.inputname == arrowfile
        @test streamoptions.outputname == streamfile
        runcommand(streamoptions)
        runcommand(sourcejson, streamfile, "VALIDATE", false)
        fileoptions = parseintegrationargs([
            "--integration",
            "--input",
            streamfile,
            "--output=$convertedfile",
            "--mode=stream-to-file",
        ])
        @test fileoptions.mode == "STREAM_TO_FILE"
        runcommand(fileoptions)
        runcommand(sourcejson, convertedfile, "VALIDATE", false)
    end
    @test_throws ErrorException parseintegrationargs(["--json"])
    @test_throws ErrorException parseintegrationargs(["--mode=UNKNOWN"])
end

@testset "integration JSON preserves physical layouts" begin
    arrowjsondir = joinpath(dirname(pathof(Arrow)), "../test/arrowjson")
    modern = ArrowJSON.parsefile(joinpath(arrowjsondir, "modern-layouts.json"))
    modernbytes = take!(Arrow.tobuffer(modern))
    modernstream = Arrow.Stream(modernbytes; convert=false)
    Tables.schema(modernstream)
    modernfields = Dict(
        String(field.name) => field for field in getfield(modernstream, :schema).fields
    )
    @test modernfields["utf8_view_nullable"].type isa Arrow.Meta.Utf8View
    @test modernfields["binary_view_nullable"].type isa Arrow.Meta.BinaryView
    @test modernfields["list_view_nullable"].type isa Arrow.Meta.ListView
    @test modernfields["large_list_view_nullable"].type isa Arrow.Meta.LargeListView
    moderntable = Arrow.Table(modernbytes; convert=false)
    @test Tables.getcolumn(moderntable, :utf8_view_nullable) isa Arrow.View
    @test Tables.getcolumn(moderntable, :binary_view_nullable) isa Arrow.View
    @test Tables.getcolumn(moderntable, :list_view_nullable) isa Arrow.ListView
    @test Tables.getcolumn(moderntable, :large_list_view_nullable) isa Arrow.ListView
    for codec in (:lz4, :zstd)
        compressedbytes = take!(Arrow.tobuffer(modern; compress=codec))
        compressedstream = Arrow.Stream(compressedbytes; convert=false)
        Tables.schema(compressedstream)
        compressedfields = Dict(
            String(field.name) => field for
            field in getfield(compressedstream, :schema).fields
        )
        @test compressedfields["utf8_view_nullable"].type isa Arrow.Meta.Utf8View
        @test compressedfields["binary_view_nullable"].type isa Arrow.Meta.BinaryView
        @test compressedfields["list_view_nullable"].type isa Arrow.Meta.ListView
        @test compressedfields["large_list_view_nullable"].type isa Arrow.Meta.LargeListView
        @test isequal(modern, Arrow.Table(compressedbytes; convert=false))
    end

    ree = ArrowJSON.parsefile(joinpath(arrowjsondir, "run-end-encoded.json"))
    reebytes = take!(Arrow.tobuffer(ree))
    reestream = Arrow.Stream(reebytes; convert=false)
    Tables.schema(reestream)
    @test only(getfield(reestream, :schema).fields).type isa Arrow.Meta.RunEndEncoded
    reetable = Arrow.Table(reebytes; convert=false)
    @test Tables.getcolumn(reetable, :run_end_encoded_nullable) isa Arrow.RunEndEncoded
end

@testset "abstract path" begin
    # Make a custom path type that simulates how AWSS3.jl's S3Path works
    struct CustomPath <: AbstractPath
        path::PosixPath
    end

    Base.read(p::CustomPath) = read(p.path)

    io = Arrow.tobuffer((col=[0],))
    tt = Arrow.Table(io)

    mktempdir() do dir
        p = Path(joinpath(dir, "test.arrow"))
        Arrow.write(p, tt)
        @test isfile(p)

        # skip windows since file locking prevents mktemp cleanup
        if !Sys.iswindows()
            tt2 = Arrow.Table(p)
            @test values(tt) == values(tt2)

            tt3 = Arrow.Table(CustomPath(p))
            @test values(tt) == values(tt3)
        end
    end
end # @testset "abstract path"
