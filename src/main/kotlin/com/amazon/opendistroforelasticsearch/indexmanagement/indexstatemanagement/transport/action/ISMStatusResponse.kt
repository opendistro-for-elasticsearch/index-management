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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.FailedIndex
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.UPDATED_INDICES
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.buildInvalidIndexResponse
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import java.io.IOException

open class ISMStatusResponse : ActionResponse, ToXContentObject {

    val updated: Int
    val failedIndices: List<FailedIndex>

    constructor(
        updated: Int,
        failedIndices: List<FailedIndex>
    ) : super() {
        this.updated = updated
        this.failedIndices = failedIndices
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        updated = sin.readInt(),
        failedIndices = sin.readList(::FailedIndex)
    )

    override fun writeTo(out: StreamOutput) {
        out.writeInt(updated)
        out.writeCollection(failedIndices)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field(UPDATED_INDICES, updated)
        buildInvalidIndexResponse(builder, failedIndices)
        return builder.endObject()
    }
}
