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
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentFragment
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.script.ScriptService
import java.io.IOException

abstract class ActionConfig(
    val type: ActionType,
    val actionIndex: Int
) : ToXContentFragment {

    var configTimeout: ActionTimeout? = null
        private set
    var configRetry: ActionRetry? = null
        private set

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        configTimeout?.toXContent(builder, params)
        configRetry?.toXContent(builder, params)
        return builder
    }

    abstract fun toAction(
        clusterService: ClusterService,
        scriptService: ScriptService,
        client: Client,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action

    enum class ActionType(val type: String) {
        DELETE("delete"),
        TRANSITION("transition"),
        ROLLOVER("rollover"),
        CLOSE("close"),
        OPEN("open"),
        READ_ONLY("read_only"),
        READ_WRITE("read_write"),
        REPLICA_COUNT("replica_count"),
        FORCE_MERGE("force_merge"),
        NOTIFICATION("notification");

        override fun toString(): String {
            return type
        }
    }

    companion object {
        @Suppress("ComplexMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, index: Int): ActionConfig {
            var actionConfig: ActionConfig? = null
            var timeout: ActionTimeout? = null
            var retry: ActionRetry? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ActionTimeout.TIMEOUT_FIELD -> timeout = ActionTimeout.parse(xcp)
                    ActionRetry.RETRY_FIELD -> retry = ActionRetry.parse(xcp)
                    ActionType.DELETE.type -> actionConfig = DeleteActionConfig.parse(xcp, index)
                    ActionType.ROLLOVER.type -> actionConfig = RolloverActionConfig.parse(xcp, index)
                    ActionType.OPEN.type -> actionConfig = OpenActionConfig.parse(xcp, index)
                    ActionType.CLOSE.type -> actionConfig = CloseActionConfig.parse(xcp, index)
                    ActionType.READ_ONLY.type -> actionConfig = ReadOnlyActionConfig.parse(xcp, index)
                    ActionType.READ_WRITE.type -> actionConfig = ReadWriteActionConfig.parse(xcp, index)
                    ActionType.REPLICA_COUNT.type -> actionConfig = ReplicaCountActionConfig.parse(xcp, index)
                    ActionType.FORCE_MERGE.type -> actionConfig = ForceMergeActionConfig.parse(xcp, index)
                    ActionType.NOTIFICATION.type -> actionConfig = NotificationActionConfig.parse(xcp, index)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in Action.")
                }
            }

            requireNotNull(actionConfig) { "ActionConfig inside state is null" }

            actionConfig.configTimeout = timeout
            actionConfig.configRetry = retry

            return actionConfig
        }
    }
}
