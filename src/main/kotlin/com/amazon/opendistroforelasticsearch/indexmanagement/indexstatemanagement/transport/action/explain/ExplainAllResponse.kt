/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.explain

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import java.io.IOException

class ExplainAllResponse : ExplainResponse, ToXContentObject {

    val totalManagedIndices: Int
    val enabledState: Map<String, Boolean>

    constructor(
        indexNames: List<String>,
        indexPolicyIDs: List<String?>,
        indexMetadatas: List<ManagedIndexMetaData?>,
        totalManagedIndices: Int,
        enabledState: Map<String, Boolean>
    ) : super(indexNames, indexPolicyIDs, indexMetadatas) {
        this.totalManagedIndices = totalManagedIndices
        this.enabledState = enabledState
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        indexNames = sin.readStringList(),
        indexPolicyIDs = sin.readStringList(),
        indexMetadatas = sin.readList { ManagedIndexMetaData.fromStreamInput(it) },
        totalManagedIndices = sin.readInt(),
        enabledState = sin.readMap() as Map<String, Boolean>
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeInt(totalManagedIndices)
        out.writeMap(enabledState)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        indexNames.forEachIndexed { ind, name ->
            builder.startObject(name)
            builder.field(ManagedIndexSettings.POLICY_ID.key, indexPolicyIDs[ind])
            indexMetadatas[ind]?.toXContent(builder, ToXContent.EMPTY_PARAMS)
            builder.field("enabled", enabledState[name])
            builder.endObject()
        }
        builder.field("total_managed_indices", totalManagedIndices)
        return builder.endObject()
    }
}
