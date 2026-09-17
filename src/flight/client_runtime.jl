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
Capability summary for Arrow.jl's package-owned Julia Flight client runtime.
"""
struct FlightClientRuntimeCapabilities
    supported::Bool
    blockers::Vector{String}
end

"""
    flight_client_runtime_capabilities()

Return the current package-owned Julia Flight client-runtime contract.
"""
function flight_client_runtime_capabilities()
    return FlightClientRuntimeCapabilities(
        false,
        String[
            "Arrow.jl currently owns Flight protocol, IPC conversion, server runtime, and external Python-client interop proofs, not an in-package Julia Flight gRPC client runtime",
            "A Julia Flight client runtime requires a dedicated dependency/API design and live client test matrix before it can be exposed as package-owned support",
        ],
    )
end

"""
    flight_client_runtime_supported()

Return whether Arrow.jl currently ships a package-owned Julia Flight client
runtime.
"""
function flight_client_runtime_supported()
    return flight_client_runtime_capabilities().supported
end
