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

@file:Suppress("TopLevelPropertyNaming", "MatchingDeclarationName")
package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.optionalTimeField
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentFragment
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import java.time.Instant

const val WITH_TYPE = "with_type"
val XCONTENT_WITHOUT_TYPE = ToXContent.MapParams(mapOf(WITH_TYPE to "false"))

const val FAILURES = "failures"
const val FAILED_INDICES = "failed_indices"
const val UPDATED_INDICES = "updated_indices"

fun buildInvalidIndexResponse(builder: XContentBuilder, failedIndices: List<FailedIndex>) {
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

data class FailedIndex(val name: String, val uuid: String, val reason: String) : Writeable, ToXContentFragment {

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

    constructor(sin: StreamInput) : this(
        name = sin.readString(),
        uuid = sin.readString(),
        reason = sin.readString()
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(name)
        out.writeString(uuid)
        out.writeString(reason)
    }
}

/**
 * Gets the XContentBuilder for partially updating a [ManagedIndexConfig]'s ChangePolicy
 */
fun getPartialChangePolicyBuilder(
    changePolicy: ChangePolicy?
): XContentBuilder {
    return XContentFactory.jsonBuilder()
        .startObject()
        .startObject(ManagedIndexConfig.MANAGED_INDEX_TYPE)
        .optionalTimeField(ManagedIndexConfig.LAST_UPDATED_TIME_FIELD, Instant.now())
        .field(ManagedIndexConfig.CHANGE_POLICY_FIELD, changePolicy)
        .endObject()
        .endObject()
}
