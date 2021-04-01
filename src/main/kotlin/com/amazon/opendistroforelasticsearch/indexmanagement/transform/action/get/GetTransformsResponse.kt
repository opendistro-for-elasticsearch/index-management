/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform.Companion.TRANSFORM_TYPE
import com.amazon.opendistroforelasticsearch.indexmanagement.util._ID
import com.amazon.opendistroforelasticsearch.indexmanagement.util._PRIMARY_TERM
import com.amazon.opendistroforelasticsearch.indexmanagement.util._SEQ_NO
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.rest.RestStatus
import java.io.IOException

class GetTransformsResponse(
    val transforms: List<Transform>,
    val totalTransforms: Int,
    val status: RestStatus
) : ActionResponse(), ToXContentObject {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        transforms = sin.readList(::Transform),
        totalTransforms = sin.readInt(),
        status = sin.readEnum(RestStatus::class.java)
    )

    override fun writeTo(out: StreamOutput) {
        out.writeCollection(transforms)
        out.writeInt(totalTransforms)
        out.writeEnum(status)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field("total_transforms", totalTransforms)
            .startArray("transforms")
            .apply {
                for (transform in transforms) {
                    this.startObject()
                        .field(_ID, transform.id)
                        .field(_SEQ_NO, transform.seqNo)
                        .field(_PRIMARY_TERM, transform.primaryTerm)
                        .field(TRANSFORM_TYPE, transform, XCONTENT_WITHOUT_TYPE)
                        .endObject()
                }
            }
            .endArray()
            .endObject()
    }
}
