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

"""
    Arrow.CData

Low-level Apache Arrow C Data Interface bindings for in-process sharing of
Arrow buffers with C-compatible consumers.

The first supported producer surface is [`Arrow.CData.exporttable`](@ref),
which exports an `Arrow.Table` as a top-level struct array when all columns are
null, primitive, boolean, string/list, fixed-size, map, struct, or dictionary
encoded Arrow vectors, including binary/UTF-8 view, dense/sparse union, and
run-end encoded layouts. Logical scalar primitive storage uses standard C Data
format strings. The first import surface is [`Arrow.CData.importtable`](@ref),
which borrows top-level struct arrays with matching supported column layouts.
"""
module CData

import Tables

import ..Arrow:
    ArrowTypes,
    ArrowVector,
    BoolVector,
    Date,
    Decimal,
    DenseUnion,
    DictEncoded,
    Duration,
    FixedSizeList,
    Int256,
    Interval,
    List,
    Map,
    Meta,
    MetadataVector,
    NullVector,
    Primitive,
    RunEndEncoded,
    SparseUnion,
    Struct,
    Table,
    Time,
    Timestamp,
    UnionT,
    VIEW_ELEMENT_BYTES,
    VIEW_INLINE_BYTES,
    VIEW_LENGTH_BYTES,
    View,
    ViewElement,
    _metadatavectordata,
    _viewisinline,
    columns,
    getmetadata,
    liststringtype,
    names,
    nullcount,
    validitybitmap

export ArrowArray,
    ArrowSchema,
    ExportedTable,
    ImportedBinaryVector,
    ImportedBoolVector,
    ImportedBinaryViewVector,
    ImportedDenseUnionVector,
    ImportedDictionaryVector,
    ImportedFixedSizeBinaryVector,
    ImportedFixedSizeListVector,
    ImportedListVector,
    ImportedListViewVector,
    ImportedMapVector,
    ImportedNullablePrimitiveVector,
    ImportedNullableStringVector,
    ImportedRunEndEncodedVector,
    ImportedSparseUnionVector,
    ImportedStringVector,
    ImportedStringViewVector,
    ImportedStructVector,
    ImportedTable,
    array,
    array_ptr,
    exporttable,
    exporttable!,
    header_path,
    importtable,
    isreleased,
    release!,
    schema,
    schema_ptr

const ARROW_FLAG_DICTIONARY_ORDERED = Int64(1)
const ARROW_FLAG_NULLABLE = Int64(2)
const ARROW_FLAG_MAP_KEYS_SORTED = Int64(4)

include("cdata/core.jl")
include("cdata/formats.jl")
include("cdata/lifecycle.jl")
include("cdata/export.jl")
include("cdata/imported_vectors.jl")
include("cdata/import.jl")

end # module CData
