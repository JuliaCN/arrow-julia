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

_normalized_descriptor_path(path::AbstractVector{<:AbstractString}) =
    [String(segment) for segment in path]
_normalized_descriptor_path(path::Tuple) = [String(segment) for segment in path]

"""
    Arrow.Flight.pathdescriptor(path)

Build a Flight `PATH` descriptor from a tuple or vector of path segments
without manually constructing the generated protobuf type.
"""
function pathdescriptor(path)
    descriptor_type = Protocol.var"FlightDescriptor.DescriptorType"
    return Protocol.FlightDescriptor(
        descriptor_type.PATH,
        UInt8[],
        _normalized_descriptor_path(path),
    )
end
