# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

using Test
using Arrow
using Dates
using Libdl
using Tables

const CData = Arrow.CData
const CDataTestRoot = dirname(@__DIR__)

_test_fixture(name::AbstractString) = joinpath(CDataTestRoot, name)

_cstring(ptr::Ptr{UInt8}) = ptr == C_NULL ? nothing : unsafe_string(ptr)

function _child_schema(schema::CData.ArrowSchema, index::Integer)
    return unsafe_load(unsafe_load(schema.children, index))
end

function _child_array(array::CData.ArrowArray, index::Integer)
    return unsafe_load(unsafe_load(array.children, index))
end

function _child_array_ptr(array::CData.ArrowArray, index::Integer)
    return unsafe_load(array.children, index)
end

function _set_array_layout!(
    array_ptr::Ptr{CData.ArrowArray};
    length=nothing,
    null_count=nothing,
    offset=nothing,
    buffers=nothing,
)
    array = unsafe_load(array_ptr)
    updated = CData.ArrowArray(
        length === nothing ? array.length : Int64(length),
        null_count === nothing ? array.null_count : Int64(null_count),
        offset === nothing ? array.offset : Int64(offset),
        array.n_buffers,
        array.n_children,
        buffers === nothing ? array.buffers : pointer(buffers),
        array.children,
        array.dictionary,
        array.release,
        array.private_data,
    )
    unsafe_store!(array_ptr, updated)
    return updated
end

function _metadata_dict(ptr::Ptr{UInt8})
    ptr == C_NULL && return Dict{String,String}()
    offset = 0
    function read_int32()
        value = unsafe_load(Ptr{Int32}(ptr + offset))
        offset += 4
        return Int(value)
    end
    function read_string()
        len = read_int32()
        value = len == 0 ? "" : unsafe_string(ptr + offset, len)
        offset += len
        return value
    end
    count = read_int32()
    metadata = Dict{String,String}()
    for _ = 1:count
        key = read_string()
        value = read_string()
        metadata[key] = value
    end
    return metadata
end

function _compile_cdata_smoke()
    cc = get(ENV, "CC", "cc")
    source = _test_fixture("cdata_smoke.c")
    output = joinpath(mktempdir(), "libarrow_julia_cdata_smoke.$(Libdl.dlext)")
    shared_flag = Sys.isapple() ? "-dynamiclib" : "-shared"
    include_dir = dirname(CData.header_path())
    run(`$cc $shared_flag -fPIC -I$include_dir $source -o $output`)
    return output
end

_julia_include_dir() = abspath(Sys.BINDIR, Base.INCLUDEDIR)
_julia_lib_dir() = abspath(Sys.BINDIR, Base.LIBDIR)

function _macos_deployment_target_flags(libjulia)
    Sys.isapple() || return String[]
    try
        output = read(`otool -l $libjulia`, String)
        build_version = match(r"LC_BUILD_VERSION.*?minos\s+([0-9.]+)"s, output)
        build_version === nothing && return String[]
        return ["-mmacosx-version-min=$(build_version.captures[1])"]
    catch
        return String[]
    end
end

function _compile_cdata_embed_smoke()
    cc = get(ENV, "CC", "cc")
    source = _test_fixture("cdata_embed_smoke.c")
    output = joinpath(mktempdir(), "arrow_julia_cdata_embed_smoke")
    include_dir = dirname(CData.header_path())
    julia_include = _julia_include_dir()
    julia_include_private = joinpath(julia_include, "julia")
    julia_lib = _julia_lib_dir()
    julia_private_lib = joinpath(julia_lib, "julia")
    deployment_flags =
        _macos_deployment_target_flags(joinpath(julia_lib, "libjulia.$(Libdl.dlext)"))
    rpath_julia = "-Wl,-rpath,$julia_lib"
    rpath_private = "-Wl,-rpath,$julia_private_lib"
    run(
        `$cc $deployment_flags -I$include_dir -I$julia_include -I$julia_include_private $source -L$julia_lib -ljulia $rpath_julia $rpath_private -o $output`,
    )
    return output
end

function _active_project_dir()
    project = Base.active_project()
    return project === nothing ? CDataTestRoot : dirname(project)
end

function _current_load_path()
    separator = Sys.iswindows() ? ";" : ":"
    return join(Base.LOAD_PATH, separator)
end

function _run_cdata_embed_smoke(executable)
    julia_lib = _julia_lib_dir()
    julia_private_lib = joinpath(julia_lib, "julia")
    lib_path_var = Sys.isapple() ? "DYLD_LIBRARY_PATH" : "LD_LIBRARY_PATH"
    existing = get(ENV, lib_path_var, "")
    lib_path = join(
        isempty(existing) ? (julia_lib, julia_private_lib) :
        (julia_lib, julia_private_lib, existing),
        Sys.iswindows() ? ";" : ":",
    )
    withenv(
        "JULIA_PROJECT" => _active_project_dir(),
        "JULIA_LOAD_PATH" => _current_load_path(),
        lib_path_var => lib_path,
    ) do
        run(`$executable`)
    end
    return true
end
