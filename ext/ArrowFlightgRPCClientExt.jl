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

module ArrowFlightgRPCClientExt

using Arrow
using gRPCClient

const Flight = Arrow.Flight
const Client = Flight.Client
const Protocol = Flight.Protocol
const HeaderPair = Flight.HeaderPair
const DEFAULT_STREAM_BUFFER = Flight.DEFAULT_STREAM_BUFFER
const DEFAULT_MAX_MESSAGE_LENGTH = Flight.DEFAULT_MAX_MESSAGE_LENGTH
const DEFAULT_IPC_ALIGNMENT = Flight.DEFAULT_IPC_ALIGNMENT
const ArrowParent = Flight.ArrowParent
const _merge_headers = Flight._merge_headers
const _start_flight_producer = Flight._start_flight_producer
const _emitflightdata! = Flight._emitflightdata!
const putflightdata! = Flight.putflightdata!

include(joinpath(dirname(pathof(Arrow)), "flight", "client", "protocol_clients.jl"))
include(joinpath(dirname(pathof(Arrow)), "flight", "client", "transport.jl"))
include(joinpath(dirname(pathof(Arrow)), "flight", "client", "auth.jl"))
include(joinpath(dirname(pathof(Arrow)), "flight", "client", "rpc_methods.jl"))

end # module ArrowFlightgRPCClientExt
