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

@testset "DataAPI.metadata" begin
    df = DataFrame(a=1, b=2, c=3)
    for i = 1:2
        io = IOBuffer()
        if i == 1 # skip writing metadata in the first iteration
            Arrow.write(io, df)
        else
            Arrow.write(io, df, metadata=metadata(df), colmetadata=colmetadata(df))
        end
        seekstart(io)
        tbl = Arrow.Table(io)

        @test DataAPI.metadatasupport(typeof(tbl)) == (read=true, write=false)
        @test metadata(tbl) == metadata(df)
        @test metadata(tbl; style=true) == metadata(df; style=true)
        @test_throws Exception metadata(tbl, "xyz")
        @test metadata(tbl, "xyz", "something") == "something"
        @test metadata(tbl, "xyz", "something"; style=true) == ("something", :default)
        @test metadatakeys(tbl) == metadatakeys(df)

        @test DataAPI.colmetadatasupport(typeof(tbl)) == (read=true, write=false)
        @test colmetadata(tbl) == colmetadata(df)
        @test colmetadata(tbl; style=true) == colmetadata(df; style=true)
        @test_throws MethodError colmetadata(tbl, "xyz")
        @test_throws KeyError colmetadata(tbl, :xyz)
        @test colmetadata(tbl, :b) == colmetadata(df, :b)
        @test_throws MethodError colmetadata(tbl, :b, "xyz")
        @test colmetadata(tbl, :b, "xyz", "something") == "something"
        @test colmetadata(tbl, :b, "xyz", "something"; style=true) ==
              ("something", :default)
        @test Set(colmetadatakeys(tbl)) == Set(colmetadatakeys(df))

        # add metadata for the second iteration
        metadata!(df, "tkey", "tvalue")
        metadata!(df, "tkey2", "tvalue2")
        colmetadata!(df, :a, "ackey", "acvalue")
        colmetadata!(df, :a, "ackey2", "acvalue2")
        colmetadata!(df, :c, "cckey", "ccvalue")
    end
end # @testset "DataAPI.metadata"

@testset "DataAPI.colmetadata partitioned read" begin
    source = Tables.partitioner(((a=[1, 2],), (a=[3],)))
    colmeta = Dict(:a => Dict("test:metadata" => "partitioned"))

    io = IOBuffer()
    Arrow.write(io, source; colmetadata=colmeta)
    seekstart(io)
    tbl = Arrow.Table(io)

    @test tbl.a == [1, 2, 3]
    @test DataAPI.colmetadata(tbl, :a, "test:metadata") == "partitioned"

    parts = collect(Tables.partitions(tbl))
    @test length(parts) == 2
    @test parts[1].a == [1, 2]
    @test parts[2].a == [3]
    @test DataAPI.colmetadata(parts[1], :a, "test:metadata") == "partitioned"
    @test DataAPI.colmetadata(parts[2], :a, "test:metadata") == "partitioned"
end
