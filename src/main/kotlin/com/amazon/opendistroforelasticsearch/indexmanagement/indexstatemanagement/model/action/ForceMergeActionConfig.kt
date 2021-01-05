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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.action.Action
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.action.ForceMergeAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.script.ScriptService
import java.io.IOException

data class ForceMergeActionConfig(
    val maxNumSegments: Int,
    val index: Int
) : ToXContentObject, ActionConfig(ActionType.FORCE_MERGE, index) {

    init {
        require(maxNumSegments > 0) { "Force merge {$MAX_NUM_SEGMENTS_FIELD} must be greater than 0" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        super.toXContent(builder, params)
            .startObject(ActionType.FORCE_MERGE.type)
                .field(MAX_NUM_SEGMENTS_FIELD, maxNumSegments)
            .endObject()
        return builder.endObject()
    }

    override fun isFragment(): Boolean = super<ToXContentObject>.isFragment()

    override fun toAction(
        clusterService: ClusterService,
        scriptService: ScriptService,
        client: Client,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action = ForceMergeAction(clusterService, client, managedIndexMetaData, this)

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        maxNumSegments = sin.readInt(),
        index = sin.readInt()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeInt(maxNumSegments)
        out.writeInt(index)
    }

    companion object {
        const val MAX_NUM_SEGMENTS_FIELD = "max_num_segments"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, index: Int): ForceMergeActionConfig {
            var maxNumSegments: Int? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    MAX_NUM_SEGMENTS_FIELD -> maxNumSegments = xcp.intValue()
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ForceMergeActionConfig.")
                }
            }

            return ForceMergeActionConfig(
                requireNotNull(maxNumSegments) { "ForceMergeActionConfig maxNumSegments is null" },
                index
            )
        }
    }
}
