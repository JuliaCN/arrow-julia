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

const FLIGHT_TEST_TRANSIENT_GRPC_STATUSES = Set((
    gRPCClient.GRPC_DEADLINE_EXCEEDED,
    gRPCClient.GRPC_UNAVAILABLE,
))

is_transient_flight_startup_error(err) =
    err isa gRPCClient.gRPCServiceCallException &&
    err.grpc_status in FLIGHT_TEST_TRANSIENT_GRPC_STATUSES

function with_transient_flight_startup_retry(
    f::F;
    attempts::Integer=3,
    base_delay_secs::Real=0.5,
) where {F}
    last_error = nothing
    for attempt in 1:attempts
        try
            return f()
        catch err
            if !is_transient_flight_startup_error(err) || attempt == attempts
                rethrow()
            end
            last_error = err
            sleep(Float64(base_delay_secs) * attempt)
        end
    end
    throw(last_error)
end

function with_test_grpc_handle(f::F) where {F}
    grpc = gRPCClient.gRPCCURL()
    gRPCClient.grpc_init(grpc)
    try
        return f(grpc)
    finally
        gRPCClient.grpc_shutdown(grpc)
    end
end

function load_grpcserver()
    isnothing(Base.find_package("gRPCServer")) && return nothing
    return Base.require(
        Base.PkgId(Base.UUID("608c6337-0d7d-447f-bb69-0f5674ee3959"), "gRPCServer"),
    )
end
