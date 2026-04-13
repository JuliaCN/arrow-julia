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

Flight.listflights(
    client::Client,
    criteria::Protocol.Criteria,
    response::Channel{Protocol.FlightInfo};
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
) = _grpc_async_request(
    client,
    Flight._listflights_client(client; kwargs...),
    criteria,
    response,
    headers=Flight._merge_headers(client, headers),
)

function Flight.listflights(
    client::Client,
    criteria::Protocol.Criteria=Protocol.Criteria(UInt8[]);
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
)
    response = Channel{Protocol.FlightInfo}(response_capacity)
    req = Flight.listflights(client, criteria, response; headers=headers, kwargs...)
    return req, response
end

function Flight.getflightinfo(
    client::Client,
    descriptor::Protocol.FlightDescriptor;
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
)
    return _grpc_sync_request(
        client,
        Flight._getflightinfo_client(client; kwargs...),
        descriptor;
        headers=Flight._merge_headers(client, headers),
    )
end

function Flight.pollflightinfo(
    client::Client,
    descriptor::Protocol.FlightDescriptor;
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
)
    return _grpc_sync_request(
        client,
        Flight._pollflightinfo_client(client; kwargs...),
        descriptor;
        headers=Flight._merge_headers(client, headers),
    )
end

function Flight.getschema(
    client::Client,
    descriptor::Protocol.FlightDescriptor;
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
)
    return _grpc_sync_request(
        client,
        Flight._getschema_client(client; kwargs...),
        descriptor;
        headers=Flight._merge_headers(client, headers),
    )
end
