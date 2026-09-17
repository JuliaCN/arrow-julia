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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

using ProtoBuf

const ROOT = normpath(joinpath(@__DIR__, ".."))
const PROTO_ROOT = joinpath(ROOT, "src", "flight", "proto")
const GENERATED_ROOT = joinpath(ROOT, "src", "flight", "generated")
const LICENSE_HEADER = """# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# \"License\"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""

mktempdir() do generated
    ProtoBuf.protojl(
        ["Flight.proto", "FlightSql.proto"],
        PROTO_ROOT,
        generated;
        common_abstract_type=true,
    )
    for (directory, _, files) in walkdir(generated), file in files
        endswith(file, ".jl") || continue
        source = joinpath(directory, file)
        relative = relpath(source, generated)
        destination = joinpath(GENERATED_ROOT, relative)
        mkpath(dirname(destination))
        write(destination, LICENSE_HEADER, read(source, String))
    end
end
