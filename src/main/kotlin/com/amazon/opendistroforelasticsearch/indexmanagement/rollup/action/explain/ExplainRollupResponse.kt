/*
 *
 *  * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License").
 *  * You may not use this file except in compliance with the License.
 *  * A copy of the License is located at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * or in the "license" file accompanying this file. This file is distributed
 *  * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  * express or implied. See the License for the specific language governing
 *  * permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.explain

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import java.io.IOException

class ExplainRollupResponse : ActionResponse, ToXContentObject {
    val idsToMetadata: Map<String, RollupMetadata?>

    constructor(idsToMetadata: Map<String, RollupMetadata?>) : super() {
        this.idsToMetadata = idsToMetadata
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        idsToMetadata = sin.let {
            val idsToMetadata = mutableMapOf<String, RollupMetadata?>()
            val size = it.readVInt()
            for (i in 0 until size) {
                idsToMetadata[it.readString()] = if (sin.readBoolean()) RollupMetadata(it) else null
            }
            idsToMetadata.toMap()
        }
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeVInt(idsToMetadata.size)
        idsToMetadata.entries.forEach { (id, metadata) ->
            out.writeString(id)
            out.writeBoolean(metadata != null)
            metadata?.writeTo(out)
        }
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        idsToMetadata.entries.forEach { (id, metadata) ->
            builder.field(id, metadata)
        }
        return builder.endObject()
    }
}
