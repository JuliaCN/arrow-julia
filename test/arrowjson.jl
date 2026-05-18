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

module ArrowJSON

using Mmap
using StructTypes, JSON3, Tables, SentinelArrays, Arrow

# read json files as "table"
# write to arrow stream/file
# read arrow stream/file back

include(joinpath(@__DIR__, "arrowjson_support", "types.jl"))
include(joinpath(@__DIR__, "arrowjson_support", "metadata_schema.jl"))
include(joinpath(@__DIR__, "arrowjson_support", "table_interface.jl"))
include(joinpath(@__DIR__, "arrowjson_support", "physical_columns.jl"))
include(joinpath(@__DIR__, "arrowjson_support", "arrow_array.jl"))
include(joinpath(@__DIR__, "arrowjson_support", "datafile_roundtrip.jl"))

end
