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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.model.actions

import com.amazon.opendistroforelasticsearch.indexstatemanagement.actions.Action
import com.amazon.opendistroforelasticsearch.indexstatemanagement.actions.DeleteAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class DeleteActionConfig(
    val timeout: ActionTimeout?,
    val retry: ActionRetry?
) : ToXContentObject, ActionConfig(DELETE_ACTION_TYPE, timeout, retry) {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject().startObject(DELETE_ACTION_TYPE)
        super.toXContent(builder, params)
        return builder.endObject().endObject()
    }

    override fun isFragment(): Boolean = false

    override fun toActionClass(
        clusterService: ClusterService,
        client: Client,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action = DeleteAction(clusterService, client, managedIndexMetaData, this)

    companion object {
        const val DELETE_ACTION_TYPE = "delete"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): DeleteActionConfig {
            var timeout: ActionTimeout? = null
            var retry: ActionRetry? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ActionTimeout.TIMEOUT_FIELD -> timeout = ActionTimeout.parse(xcp)
                    ActionRetry.RETRY_FIELD -> retry = ActionRetry.parse(xcp)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in DeleteActionConfig.")
                }
            }

            return DeleteActionConfig(timeout, retry)
        }
    }
}
