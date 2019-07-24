/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

@file:Suppress("TopLevelPropertyNaming")

package com.amazon.opendistroforelasticsearch.indexstatemanagement.util

import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentFragment
import org.elasticsearch.common.xcontent.XContentBuilder

const val _DOC = "_doc"
const val _ID = "_id"
const val _VERSION = "_version"
const val _SEQ_NO = "_seq_no"
const val IF_SEQ_NO = "if_seq_no"
const val _PRIMARY_TERM = "_primary_term"
const val IF_PRIMARY_TERM = "if_primary_term"
const val REFRESH = "refresh"

const val WITH_TYPE = "with_type"
val XCONTENT_WITHOUT_TYPE = ToXContent.MapParams(mapOf(WITH_TYPE to "false"))

const val SCHEMA_VERSION = "schema_version"
const val NO_SCHEMA_VERSION = 0

const val FAILURES = "failures"
const val FAILED_INDICES = "failed_indices"
const val UPDATED_INDICES = "updated_indices"

fun buildInvalidIndexResponse(builder: XContentBuilder, failedIndices: MutableList<FailedIndex>) {
    if (failedIndices.isNotEmpty()) {
        builder.field(FAILURES, true)
        builder.startArray(FAILED_INDICES)
        for (failedIndex in failedIndices) {
            failedIndex.toXContent(builder, ToXContent.EMPTY_PARAMS)
        }
        builder.endArray()
    } else {
        builder.field(FAILURES, false)
        builder.startArray(FAILED_INDICES).endArray()
    }
}

data class FailedIndex(val name: String, val uuid: String, val reason: String) : ToXContentFragment {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(INDEX_NAME_FIELD, name)
            .field(INDEX_UUID_FIELD, uuid)
            .field(REASON_FIELD, reason)
        return builder.endObject()
    }

    companion object {
        const val INDEX_NAME_FIELD = "index_name"
        const val INDEX_UUID_FIELD = "index_uuid"
        const val REASON_FIELD = "reason"
    }
}