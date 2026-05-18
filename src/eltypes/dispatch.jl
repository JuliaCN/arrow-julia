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

function arrowtype end

arrowtype(b, col::AbstractVector{T}) where {T} = arrowtype(b, maybemissing(T))
arrowtype(b, col::DictEncoded) = arrowtype(b, col.encoding.data)
arrowtype(b, col::Compressed) = arrowtype(b, col.data)

function juliaeltype(f::Meta.Field, ::Nothing, convert::Bool)
    T = juliaeltype(f, convert)
    return convert ? finaljuliatype(T) : T
end

function juliaeltype(f::Meta.Field, meta::AbstractDict{String,String}, convert::Bool)
    TT = juliaeltype(f, convert)
    spec = _extensionspec(meta)
    if spec !== nothing
        _validatebuiltinextension(spec, f)
        !convert && return TT
        T = finaljuliatype(TT)
        storageT =
            spec.name === TIMESTAMP_WITH_OFFSET_SYMBOL ?
            maybemissing(juliaeltype(f, false)) : maybemissing(TT)
        JT = _resolveextensionjuliatype(spec, storageT)
        if JT !== nothing
            return f.nullable ? Union{JT,Missing} : JT
        else
            typename = _extensiontypename(spec)
            @warn "unsupported $(EXTENSION_NAME_KEY) type: \"$typename\", arrow type = $TT" maxlog =
                1 _id = hash((:juliaeltype, typename, TT))
        end
    end
    !convert && return TT
    T = finaljuliatype(TT)
    return something(TT, T)
end

function juliaeltype(f::Meta.Field, convert::Bool)
    T = juliaeltype(f, f.type, convert)
    return f.nullable ? Union{T,Missing} : T
end

juliaeltype(f::Meta.Field, ::Meta.Null, convert) = Missing

function arrowtype(b, ::Type{Missing})
    Meta.nullStart(b)
    return Meta.Null, Meta.nullEnd(b), nothing
end

function juliaeltype(f::Meta.Field, int::Meta.Int, convert)
    if int.is_signed
        if int.bitWidth == 8
            Int8
        elseif int.bitWidth == 16
            Int16
        elseif int.bitWidth == 32
            Int32
        elseif int.bitWidth == 64
            Int64
        elseif int.bitWidth == 128
            Int128
        else
            error("$int is not valid arrow type metadata")
        end
    else
        if int.bitWidth == 8
            UInt8
        elseif int.bitWidth == 16
            UInt16
        elseif int.bitWidth == 32
            UInt32
        elseif int.bitWidth == 64
            UInt64
        elseif int.bitWidth == 128
            UInt128
        else
            error("$int is not valid arrow type metadata")
        end
    end
end

function arrowtype(b, ::Type{T}) where {T<:Integer}
    Meta.intStart(b)
    Meta.intAddBitWidth(b, Int32(8 * sizeof(T)))
    Meta.intAddIsSigned(b, T <: Signed)
    return Meta.Int, Meta.intEnd(b), nothing
end
