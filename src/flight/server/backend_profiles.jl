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
    flight_server_backend_capabilities(backend::Symbol = :grpcserver)

Return the packaged capability contract for one Arrow Flight server backend.
"""
function flight_server_backend_capabilities(backend::Symbol=:grpcserver)
    if backend == :grpcserver
        if !grpcserver_extension_loaded()
            return FlightServerBackendCapabilities(
                :grpcserver,
                false,
                false,
                false,
                false,
                String[
                    "Arrow.jl ships the packaged Flight listener backend behind the optional gRPCServer.jl extension; load gRPCServer to activate it",
                    "The legacy Arrow-owned PureHTTP2 listener surface has been retired; gRPCServer.jl now owns the packaged HTTP/2 transport",
                ],
            )
        end

        return FlightServerBackendCapabilities(
            :grpcserver,
            true,
            true,
            true,
            true,
            String[],
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
                    "The packaged live Flight listener backend now lives behind gRPCServer.jl",
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
                "The current nghttp2 backend is not the packaged live backend; gRPCServer.jl owns the supported Flight listener path",
                "Future work still needs request-streaming and bidirectional proof before :nghttp2 can satisfy the full Flight server contract",
            ],
        )
    end

    throw(
        ArgumentError(
            "Unsupported Arrow Flight server backend :$(backend); expected one of :grpcserver or :nghttp2",
        ),
    )
end

"""
    flight_server_backend_supported(backend::Symbol = :grpcserver)

Return whether one backend satisfies the packaged Arrow Flight server contract.
"""
function flight_server_backend_supported(backend::Symbol=:grpcserver)
    capabilities = flight_server_backend_capabilities(backend)
    return capabilities.request_streaming &&
           capabilities.response_streaming &&
           capabilities.response_trailers &&
           capabilities.bidirectional_doexchange
end

"""
    require_flight_server_backend(backend::Symbol = :grpcserver; subject = "Arrow Flight server")

Throw an `ArgumentError` when the requested backend does not satisfy the
packaged Arrow Flight server contract.
"""
function require_flight_server_backend(
    backend::Symbol=:grpcserver;
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
