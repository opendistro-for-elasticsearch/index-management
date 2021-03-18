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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.preview

import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.rest.RestStatus

class PreviewTransformResponse(
    val documents: List<Map<String, Any>>,
    val status: RestStatus
) : ActionResponse(), ToXContentObject {

    constructor(sin: StreamInput) : this(
        documents = sin.let {
            val documentList = mutableListOf<Map<String, Any>>()
            val size = it.readVInt()
            for (i in 0 until size) {
                documentList.add(sin.readMap()!!)
            }
            documentList.toList()
        },
        status = sin.readEnum(RestStatus::class.java)
    )

    override fun writeTo(out: StreamOutput) {
        out.writeVInt(documents.size)
        for (document in documents) {
            out.writeMap(document)
        }
        out.writeEnum(status)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field("documents", documents)
            .endObject()
    }
}
