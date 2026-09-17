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

"""
    grpcserver_flight_server(service::Service; kwargs...)

Start an Arrow Flight server on the packaged `gRPCServer.jl` backend.
Load `gRPCServer` in the active Julia session to activate the extension.
"""
function grpcserver_flight_server(args...; kwargs...)
    throw(
        ArgumentError(
            "Arrow Flight packaged listener backend requires loading gRPCServer.jl so the ArrowgRPCServerExt extension can activate",
        ),
    )
end

"""
    stop!(server; force = false)

Stop one Arrow Flight listener backend instance.
"""
function stop!(server; force::Bool=false)
    throw(MethodError(stop!, (server,)))
end

grpcserver_extension_loaded() =
    !isnothing(Base.get_extension(ArrowParent, :ArrowgRPCServerExt))
