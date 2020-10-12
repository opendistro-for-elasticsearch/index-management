/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import java.io.IOException

class ExplainResponse : ActionResponse, ToXContentObject {

    // TODO refactor this de-coupled lists usage to map maybe
    val indexNames: List<String>
    val indexPolicyIDs: List<String?>
    val indexMetadatas: List<ManagedIndexMetaData?>
    val totalManagedIndices: Int

    constructor(
        indexNames: List<String>,
        indexPolicyIDs: List<String?>,
        indexMetadatas: List<ManagedIndexMetaData?>,
        totalManagedIndices: Int
    ) : super() {
        this.indexNames = indexNames
        this.indexPolicyIDs = indexPolicyIDs
        this.indexMetadatas = indexMetadatas
        this.totalManagedIndices = totalManagedIndices
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        indexNames = sin.readStringList(),
        indexPolicyIDs = sin.readStringList(),
        indexMetadatas = sin.readList { ManagedIndexMetaData.fromStreamInput(it) },
        totalManagedIndices = sin.readInt()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringCollection(indexNames)
        out.writeStringCollection(indexPolicyIDs)
        out.writeCollection(indexMetadatas)
        out.writeInt(totalManagedIndices)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        indexNames.forEachIndexed { ind, name ->
            builder.startObject(name)
            builder.field(ManagedIndexSettings.POLICY_ID.key, indexPolicyIDs[ind])
            indexMetadatas[ind]?.toXContent(builder, ToXContent.EMPTY_PARAMS)
            builder.endObject()
        }
        builder.field("totalManagedIndices", totalManagedIndices)
        return builder.endObject()
    }
}
