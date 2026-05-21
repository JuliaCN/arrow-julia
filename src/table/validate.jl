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

"""
    Arrow.validate(input, pos=1, len=nothing; convert::Bool=false)
    Arrow.validate(inputs::Vector; convert::Bool=false)

Validate an Arrow IPC stream or file by running the same reader-side structural
checks as [`Arrow.Table`](@ref). Return `nothing` when validation succeeds and
throw the reader's `ArgumentError` when malformed IPC metadata or buffers are
detected.

The default `convert=false` keeps validation focused on Arrow physical layout
checks instead of Julia semantic type conversion. Pass `convert=true` to also
exercise the normal converted reader path.
"""
function validate(input, pos::Integer=1, len=nothing; convert::Bool=false)
    Table(input, pos, len; convert=convert)
    return nothing
end

function validate(input::Vector{UInt8}, pos::Integer=1, len=nothing; convert::Bool=false)
    Table(input, pos, len; convert=convert)
    return nothing
end

function validate(inputs::Vector; convert::Bool=false)
    Table(inputs; convert=convert)
    return nothing
end
