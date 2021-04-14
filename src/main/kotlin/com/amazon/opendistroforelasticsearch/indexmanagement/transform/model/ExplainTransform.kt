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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform.model

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import java.io.IOException

data class ExplainTransform(
    val metadataID: String? = null,
    val metadata: TransformMetadata? = null
) : ToXContentObject, Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        metadataID = sin.readOptionalString(),
        metadata = if (sin.readBoolean()) TransformMetadata(sin) else null
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeOptionalString(metadataID)
        out.writeBoolean(metadata != null)
        metadata?.writeTo(out)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(Transform.METADATA_ID_FIELD, metadataID)
            .field(TransformMetadata.TRANSFORM_METADATA_TYPE, metadata, XCONTENT_WITHOUT_TYPE)
            .endObject()
    }
}
