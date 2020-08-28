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
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.action.NotificationAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.destination.Destination
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptService
import java.io.IOException

data class NotificationActionConfig(
    val destination: Destination,
    val messageTemplate: Script,
    val index: Int
) : ToXContentObject, ActionConfig(ActionType.NOTIFICATION, index) {

    init {
        require(messageTemplate.lang == MUSTACHE) { "Notification message template must be a mustache script" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        super.toXContent(builder, params).startObject(ActionType.NOTIFICATION.type)
        builder.field(DESTINATION_FIELD, destination)
                .field(MESSAGE_TEMPLATE_FIELD, messageTemplate)
                .endObject()
                .endObject()
        return builder
    }

    override fun isFragment(): Boolean = super<ToXContentObject>.isFragment()

    override fun toAction(
        clusterService: ClusterService,
        scriptService: ScriptService,
        client: Client,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action = NotificationAction(clusterService, scriptService, client, managedIndexMetaData, this)

    companion object {
        const val DESTINATION_FIELD = "destination"
        const val MESSAGE_TEMPLATE_FIELD = "message_template"
        const val MUSTACHE = "mustache"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, index: Int): NotificationActionConfig {
            var destination: Destination? = null
            var messageTemplate: Script? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    DESTINATION_FIELD -> destination = Destination.parse(xcp)
                    MESSAGE_TEMPLATE_FIELD -> messageTemplate = Script.parse(xcp, Script.DEFAULT_TEMPLATE_LANG)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in NotificationActionConfig.")
                }
            }

            return NotificationActionConfig(
                destination = requireNotNull(destination) { "NotificationActionConfig destination is null" },
                messageTemplate = requireNotNull(messageTemplate) { "NotificationActionConfig message template is null" },
                index = index
            )
        }
    }
}
