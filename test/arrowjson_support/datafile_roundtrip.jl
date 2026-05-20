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

# take any Tables.jl source and write out arrow json datafile
function DataFile(source)
    fields = Field[]
    metadata = _metadata_pairs(Arrow.getmetadata(source))
    batches = RecordBatch[]
    dictionaries = DictionaryBatch[]
    dictencodings = Dict{String,Tuple{Base.Type,DictEncoding}}()
    dictid = Ref(0)
    sch = nothing
    for (i, tbl1) in enumerate(Tables.partitions(source))
        tbl = Arrow.toarrowtable(
            Tables.Columns(tbl1),
            Dict{Int64,Any}(),
            false,
            nothing,
            true,
            false,
            false,
            Arrow.DEFAULT_MAX_DEPTH,
            nothing,
            nothing,
        )
        if i == 1
            sch = Tables.schema(tbl)
            for (column_index, nm) in enumerate(sch.names)
                T = sch.types[column_index]
                col = Tables.getcolumn(tbl, column_index)
                if col isa Arrow.DictEncode
                    id = dictid[]
                    dictid[] += 1
                    codes = DataAPI.refarray(col.data)
                    if codes !== col.data
                        IT = Type(eltype(codes))
                    else
                        IT = Type(Arrow.encodingtype(length(unique(col))))
                    end
                    dictencodings[String(nm)] = (T, DictEncoding(id, IT, false))
                end
                field = Field(String(nm), T, dictencodings, col)
                field.metadata = _metadata_pairs(Arrow.getmetadata(col))
                push!(fields, field)
            end
        end
        # build record batch
        len = Tables.rowcount(tbl)
        columns = FieldData[]
        for (column_index, nm) in enumerate(sch.names)
            T = sch.types[column_index]
            col = Tables.getcolumn(tbl, column_index)
            push!(columns, FieldData(String(nm), T, col, dictencodings))
        end
        push!(batches, RecordBatch(len, columns))
        # build dictionaries
        for (nm, (T, dictencoding)) in dictencodings
            column = FieldData(nm, T, Tables.getcolumn(tbl, nm), nothing)
            recordbatch = RecordBatch(len, [column])
            push!(dictionaries, DictionaryBatch(dictencoding.id, recordbatch))
        end
    end
    schema = Schema(fields, metadata)
    return DataFile(schema, batches, dictionaries)
end

function _schema_type_members(::Base.Type{T}) where {T}
    T === Missing && return Base.Type[Missing]
    S = Base.nonmissingtype(T)
    if S <: Arrow.UnionT
        members = Base.Type[]
        for child in fieldtypes(eltype(S))
            append!(members, _schema_type_members(child))
        end
        T !== S && push!(members, Missing)
        return unique(members)
    elseif T isa Union
        members = Base.Type[]
        for child in Base.uniontypes(T)
            append!(members, _schema_type_members(child))
        end
        return unique(members)
    else
        return Base.Type[T]
    end
end

function _schema_type_equal(expected::Base.Type, actual::Base.Type)
    expected == actual && return true
    expected_members = _schema_type_members(expected)
    actual_members = _schema_type_members(actual)
    length(expected_members) == length(actual_members) || return false
    return all(member -> any(==(member), actual_members), expected_members)
end

function _schema_equal(expected::Tables.Schema, actual::Tables.Schema)
    expected.names == actual.names || return false
    length(expected.types) == length(actual.types) || return false
    return all(
        _schema_type_equal(expected.types[i], actual.types[i]) for
        i = 1:length(expected.types)
    )
end

function Base.isequal(df::DataFile, tbl::Arrow.Table)
    expected_schema = Tables.schema(df)
    actual_schema = Tables.schema(tbl)
    (
        Arrow.is_equivalent_schema(expected_schema, actual_schema) ||
        _schema_equal(expected_schema, actual_schema)
    ) || return false
    _metadata_equal(df.schema.metadata, Arrow.getmetadata(tbl)) || return false
    for (column_index, field) in enumerate(df.schema.fields)
        _metadata_equal(
            field.metadata,
            Arrow.getmetadata(Tables.getcolumn(tbl, column_index)),
        ) || return false
    end
    i = 1
    for (col1, col2) in zip(Tables.Columns(df), Tables.Columns(tbl))
        if !isequal(col1, col2)
            @show i
            return false
        end
        i += 1
    end
    return true
end
