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

#include <string.h>

int arrow_julia_cdata_smoke_validate(struct ArrowSchema *schema, struct ArrowArray *array)
{
    if (schema == 0 || array == 0) {
        return 1;
    }
    if (arrow_julia_cdata_schema_is_released(schema) ||
        arrow_julia_cdata_array_is_released(array)) {
        return 2;
    }
    if (schema->format == 0 || strcmp(schema->format, "+s") != 0) {
        return 3;
    }
    if (schema->n_children != 2 || array->n_children != 2) {
        return 4;
    }
    if (array->length != 2 || array->n_buffers != 1 || array->buffers == 0) {
        return 5;
    }
    if (array->buffers[0] != 0) {
        return 6;
    }
    if (schema->children == 0 || array->children == 0) {
        return 7;
    }
    if (schema->children[0] == 0 || schema->children[1] == 0) {
        return 8;
    }
    if (array->children[0] == 0 || array->children[1] == 0) {
        return 9;
    }
    if (strcmp(schema->children[0]->name, "id") != 0 ||
        strcmp(schema->children[0]->format, "i") != 0) {
        return 10;
    }
    if (strcmp(schema->children[1]->name, "name") != 0 ||
        strcmp(schema->children[1]->format, "u") != 0) {
        return 11;
    }
    if (array->children[0]->n_buffers != 2 || array->children[1]->n_buffers != 3) {
        return 12;
    }
    if (array->children[0]->buffers == 0 || array->children[1]->buffers == 0) {
        return 13;
    }
    if (array->children[0]->buffers[1] == 0 || array->children[1]->buffers[2] == 0) {
        return 14;
    }
    return 0;
}

int arrow_julia_cdata_smoke_release_array(struct ArrowArray *array)
{
    if (array == 0) {
        return 1;
    }
    arrow_julia_cdata_release_array(array);
    return arrow_julia_cdata_array_is_released(array) ? 0 : 2;
}

int arrow_julia_cdata_smoke_release_schema(struct ArrowSchema *schema)
{
    if (schema == 0) {
        return 1;
    }
    arrow_julia_cdata_release_schema(schema);
    return arrow_julia_cdata_schema_is_released(schema) ? 0 : 2;
}
