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

using Sockets

function grpcserver_extension_live_port()
    socket = Sockets.listen(parse(Sockets.IPv4, "127.0.0.1"), 0)
    _, port = getsockname(socket)
    close(socket)
    return Int(port)
end

function wait_for_grpcserver_extension_live_server(host::AbstractString, port::Integer)
    deadline = time() + 5.0
    last_error = nothing

    while time() < deadline
        try
            socket = Sockets.connect(parse(Sockets.IPv4, host), port)
            close(socket)
            return
        catch err
            last_error = err
        end
        sleep(0.05)
    end

    detail =
        isnothing(last_error) ? "unknown readiness failure" : sprint(showerror, last_error)
    error(
        "gRPCServer Flight test server did not become ready on $(host):$(port): $(detail)",
    )
end

function with_grpcserver_extension_live_server(f::F, grpcserver, service) where {F}
    host = "127.0.0.1"
    port = grpcserver_extension_live_port()
    server = grpcserver.GRPCServer(host, port)
    grpcserver.register!(server, service)
    grpcserver.start!(server)

    try
        wait_for_grpcserver_extension_live_server(host, port)
        return f(server, host, port)
    finally
        grpcserver.stop!(server; force=true)
    end
end
