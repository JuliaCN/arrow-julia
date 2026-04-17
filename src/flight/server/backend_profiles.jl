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
Capability summary for one packaged Arrow Flight server backend profile.
"""
struct FlightServerBackendCapabilities
    backend::Symbol
    request_streaming::Bool
    response_streaming::Bool
    response_trailers::Bool
    bidirectional_doexchange::Bool
    blockers::Vector{String}
end

"""
    flight_server_backend_capabilities(backend::Symbol = :purehttp2)

Return the packaged capability contract for one Arrow Flight server backend.
"""
function flight_server_backend_capabilities(backend::Symbol=:purehttp2)
    if backend == :purehttp2
        return FlightServerBackendCapabilities(:purehttp2, true, true, true, true, String[])
    elseif backend == :grpcserver
        throw(
            ArgumentError(
                "Arrow Flight server backend :grpcserver has been retired; use :purehttp2",
            ),
        )
    elseif backend == :nghttp2
        if !nghttp2_extension_loaded()
            return FlightServerBackendCapabilities(
                :nghttp2,
                false,
                false,
                false,
                false,
                String[
                    "Arrow.jl ships the nghttp2 backend behind the optional Nghttp2Wrapper.jl extension; load Nghttp2Wrapper to activate it",
                    "PureHTTP2 remains the only default packaged Flight listener backend",
                ],
            )
        end

        return FlightServerBackendCapabilities(
            :nghttp2,
            false,
            true,
            true,
            false,
            String[
                "Arrow Flight request-streaming methods Handshake, DoPut, and DoExchange are still unsupported on the nghttp2 backend",
                "The current nghttp2 backend is not the default packaged backend; PureHTTP2 remains the package-owned CI and product lane",
                "Future work still needs request-streaming and bidirectional proof before :nghttp2 can satisfy the full Flight server contract",
            ],
        )
    end

    throw(
        ArgumentError(
            "Unsupported Arrow Flight server backend :$(backend); expected one of :purehttp2 or :nghttp2",
        ),
    )
end

"""
    flight_server_backend_supported(backend::Symbol = :purehttp2)

Return whether one backend satisfies the packaged Arrow Flight server contract.
"""
function flight_server_backend_supported(backend::Symbol=:purehttp2)
    capabilities = flight_server_backend_capabilities(backend)
    return capabilities.request_streaming &&
           capabilities.response_streaming &&
           capabilities.response_trailers &&
           capabilities.bidirectional_doexchange
end

"""
    require_flight_server_backend(backend::Symbol = :purehttp2; subject = "Arrow Flight server")

Throw an `ArgumentError` when the requested backend does not satisfy the
packaged Arrow Flight server contract.
"""
function require_flight_server_backend(
    backend::Symbol=:purehttp2;
    subject::AbstractString="Arrow Flight server",
)
    flight_server_backend_supported(backend) && return nothing

    capabilities = flight_server_backend_capabilities(backend)
    blocker_message = join(capabilities.blockers, "; ")
    throw(
        ArgumentError(
            "$(subject) does not support backend :$(backend): $(blocker_message)",
        ),
    )
end
