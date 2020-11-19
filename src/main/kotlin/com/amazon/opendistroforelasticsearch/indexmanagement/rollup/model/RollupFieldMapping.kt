/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup.Companion.DIMENSIONS_FIELD
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup.Companion.METRICS_FIELD

data class RollupFieldMapping(val fieldType: FieldType, val fieldName: String, val mappingType: String, var sourceType: String? = null) {

    fun sourceType(type: String?) {
        this.sourceType = type
    }

    override fun toString(): String {
        return "$fieldName.$mappingType"
    }

    fun toIssue(isFieldMissing: Boolean = false): String {
        return if (isFieldMissing || mappingType == UNKNOWN_MAPPING) return "missing field $fieldName"
        else when (fieldType) {
            FieldType.METRIC -> "missing $mappingType aggregation on $fieldName"
            else -> "missing $mappingType grouping on $fieldName"
        }
    }

    companion object {
        const val UNKNOWN_MAPPING = "unknown"
        enum class FieldType(val type: String) {
            DIMENSION(DIMENSIONS_FIELD),
            METRIC(METRICS_FIELD);

            override fun toString(): String {
                return type
            }
        }
    }
}
