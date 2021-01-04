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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.action.Action
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.action.RollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.ISMRollup
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import org.elasticsearch.script.ScriptService
import java.io.IOException

class RollupActionConfig(
    val ismRollup: ISMRollup,
    val index: Int
) : ToXContentObject, ActionConfig(ActionType.ROLLUP, index) {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        super.toXContent(builder, params)
                .startObject(ActionType.ROLLUP.type)
                .field(ISM_ROLLUP_FIELD, ismRollup)
                .endObject()
                .endObject()
        return builder
    }

    override fun toAction(
        clusterService: ClusterService,
        scriptService: ScriptService,
        client: Client,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action = RollupAction(clusterService, client, ismRollup, managedIndexMetaData, this)

    override fun isFragment(): Boolean = super<ToXContentObject>.isFragment()

    @Throws(IOException::class)
    constructor(sin: StreamInput): this(ismRollup = ISMRollup(sin), index = sin.readInt())

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        ismRollup.writeTo(out)
        out.writeInt(actionIndex)
    }

    companion object {
        const val ISM_ROLLUP_FIELD = "ism_rollup"
        var ismRollup: ISMRollup? = null

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, actionIndex: Int): RollupActionConfig {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ISM_ROLLUP_FIELD -> ismRollup = ISMRollup.parse(xcp)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in RollupActionConfig.")
                }
            }

            return RollupActionConfig(
                    ismRollup = requireNotNull(ismRollup) { "RollupActionConfig rollup is null" },
                    index = actionIndex)
        }
    }
}
