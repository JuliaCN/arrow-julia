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

function _rethrow_flight_status_error(error::Flight.FlightStatusError)
    throw(gRPCServer.GRPCError(gRPCServer.StatusCode.T(error.code), error.message))
end

function _configured_service(service::Flight.Service)
    return GRPCServerFlightService(
        service,
        STREAM_BUFFER_SIZE,
        STREAM_BUFFER_SIZE,
        GRPCServerRequestGate(DEFAULT_MAX_ACTIVE_REQUESTS),
    )
end

_configured_service(service::GRPCServerFlightService) = service

function _transport_method(method::Flight.MethodDescriptor)
    return Flight.TransportMethodDescriptor(method)
end

function _with_request_gate(handler::Function, request_gate::GRPCServerRequestGate)
    return function(args...)
        _try_acquire_request!(request_gate) || _throw_request_limit_error(request_gate)
        try
            return handler(args...)
        finally
            _release_request!(request_gate)
        end
    end
end

function _unary_handler(service, method::Flight.MethodDescriptor)
    configured = _configured_service(service)
    transport_method = _transport_method(method)
    return _with_request_gate(configured.request_gate) do context, request
        return Flight.transport_unary_call(
            configured.service,
            _call_context(context),
            transport_method,
            request;
            on_status_error=_rethrow_flight_status_error,
        )
    end
end

function _server_streaming_handler(service, method::Flight.MethodDescriptor)
    configured = _configured_service(service)
    transport_method = _transport_method(method)
    return _with_request_gate(configured.request_gate) do context, request, stream
        Flight.transport_server_streaming_call(
            configured.service,
            _call_context(context),
            transport_method,
            request,
            message -> gRPCServer.send!(stream, message);
            response_capacity=configured.response_capacity,
            on_status_error=_rethrow_flight_status_error,
        )
        gRPCServer.close!(stream)
        return nothing
    end
end

function _client_streaming_handler(service, method::Flight.MethodDescriptor)
    configured = _configured_service(service)
    transport_method = _transport_method(method)
    return _with_request_gate(configured.request_gate) do context, stream
        return Flight.transport_client_streaming_call(
            configured.service,
            _call_context(context),
            transport_method,
            stream;
            request_capacity=configured.request_capacity,
            on_status_error=_rethrow_flight_status_error,
        )
    end
end

function _bidi_streaming_handler(service, method::Flight.MethodDescriptor)
    configured = _configured_service(service)
    transport_method = _transport_method(method)
    return _with_request_gate(configured.request_gate) do context, stream
        Flight.transport_bidi_streaming_call(
            configured.service,
            _call_context(context),
            transport_method,
            stream,
            message -> gRPCServer.send!(stream, message);
            request_capacity=configured.request_capacity,
            response_capacity=configured.response_capacity,
            on_status_error=_rethrow_flight_status_error,
        )
        gRPCServer.close!(stream)
        return nothing
    end
end

function _handler(service, method::Flight.MethodDescriptor)
    if !method.request_streaming && !method.response_streaming
        return _unary_handler(service, method)
    elseif !method.request_streaming && method.response_streaming
        return _server_streaming_handler(service, method)
    elseif method.request_streaming && !method.response_streaming
        return _client_streaming_handler(service, method)
    end
    return _bidi_streaming_handler(service, method)
end
