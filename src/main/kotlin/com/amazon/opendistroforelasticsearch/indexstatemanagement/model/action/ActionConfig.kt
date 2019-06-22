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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action

import com.amazon.opendistroforelasticsearch.indexstatemanagement.action.Action
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.ActionType
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentFragment
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

abstract class ActionConfig(
    val type: ActionType,
    val configTimeout: ActionTimeout?,
    val configRetry: ActionRetry?
) : ToXContentFragment {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        configTimeout?.toXContent(builder, params)
        configRetry?.toXContent(builder, params)
        return builder
    }

    abstract fun toActionClass(
        clusterService: ClusterService,
        client: Client,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ActionConfig {
            var actionConfig: ActionConfig? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    DeleteActionConfig.DELETE_ACTION_TYPE.type -> actionConfig = DeleteActionConfig.parse(xcp)
                    else -> throw IllegalArgumentException("Invalid field: [fieldName] found in State action.")
                }
            }

            return requireNotNull(actionConfig) { "ActionConfig inside state is null" }
        }
    }
}
