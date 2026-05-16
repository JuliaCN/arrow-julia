/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "arrow_julia_cdata.h"

#include <julia/julia.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

JULIA_DEFINE_FAST_TLS

static int eval_ok(const char *code)
{
    jl_value_t *value = jl_eval_string(code);
    if (jl_exception_occurred()) {
        return 0;
    }
    return value != 0;
}

static int validate_exported_table(struct ArrowSchema *schema, struct ArrowArray *array)
{
    if (schema == 0 || array == 0) {
        return 1;
    }
    if (schema->format == 0 || strcmp(schema->format, "+s") != 0) {
        return 2;
    }
    if (schema->n_children != 2 || array->n_children != 2 || array->length != 2) {
        return 3;
    }
    if (schema->children == 0 || array->children == 0) {
        return 4;
    }
    if (schema->children[0] == 0 || schema->children[1] == 0) {
        return 5;
    }
    if (array->children[0] == 0 || array->children[1] == 0) {
        return 6;
    }
    if (strcmp(schema->children[0]->format, "i") != 0 ||
        strcmp(schema->children[0]->name, "id") != 0) {
        return 7;
    }
    if (strcmp(schema->children[1]->format, "u") != 0 ||
        strcmp(schema->children[1]->name, "name") != 0) {
        return 8;
    }
    if (array->children[0]->n_buffers != 2 || array->children[1]->n_buffers != 3) {
        return 9;
    }
    if (array->children[0]->buffers == 0 || array->children[1]->buffers == 0) {
        return 10;
    }
    if (array->children[0]->buffers[1] == 0 || array->children[1]->buffers[2] == 0) {
        return 11;
    }
    return 0;
}

int main(void)
{
    struct ArrowSchema schema;
    struct ArrowArray array;
    char export_expr[4096];
    int validation;

    jl_init();

    if (!eval_ok("using Arrow")) {
        jl_atexit_hook(1);
        return 10;
    }

    snprintf(
        export_expr,
        sizeof(export_expr),
        "begin\n"
        "schema = Ptr{Arrow.CData.ArrowSchema}(UInt(%llu))\n"
        "array = Ptr{Arrow.CData.ArrowArray}(UInt(%llu))\n"
        "table = Arrow.Table(Arrow.tobuffer((id=Int32[1, 2], name=[\"a\", \"b\"])))\n"
        "global __arrow_julia_cdata_owner = Arrow.CData.exporttable!(schema, array, table)\n"
        "nothing\n"
        "end",
        (unsigned long long)(uintptr_t)&schema,
        (unsigned long long)(uintptr_t)&array);

    if (!eval_ok(export_expr)) {
        jl_atexit_hook(1);
        return 11;
    }

    validation = validate_exported_table(&schema, &array);
    if (validation != 0) {
        arrow_julia_cdata_release_pair(&schema, &array);
        jl_atexit_hook(1);
        return 20 + validation;
    }

    arrow_julia_cdata_release_pair(&schema, &array);
    if (!arrow_julia_cdata_schema_is_released(&schema) ||
        !arrow_julia_cdata_array_is_released(&array)) {
        jl_atexit_hook(1);
        return 40;
    }

    if (!eval_ok("Arrow.CData.isreleased(__arrow_julia_cdata_owner) || error(\"not released\")")) {
        jl_atexit_hook(1);
        return 41;
    }

    jl_atexit_hook(0);
    return 0;
}
