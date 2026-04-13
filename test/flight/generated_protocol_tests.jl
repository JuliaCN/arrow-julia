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

function flight_test_generated_protocol_formatter_surface()
    generated_path = joinpath(
        dirname(pathof(Arrow)),
        "flight",
        "generated",
        "arrow",
        "flight",
        "protocol",
        "Flight_pb.jl",
    )
    source = read(generated_path, String)

    @test !occursin("::Type{<:var\"", source)
    @test occursin("where {T<:var\"SessionOptionValue.StringListValue\"}", source)
    @test occursin("where {T<:var\"SetSessionOptionsResult.Error\"}", source)
    @test !occursin("import gRPCClient", source)
    @test !occursin("# gRPCClient.jl BEGIN", source)
end
