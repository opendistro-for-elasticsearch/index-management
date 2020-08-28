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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.destination.Destination
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.script.Script
import java.io.IOException

data class ErrorNotification(
    val destination: Destination,
    val messageTemplate: Script
) : ToXContentObject {

    init {
        require(messageTemplate.lang == MUSTACHE) { "ErrorNotification message template must be a mustache script" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
                .field(DESTINATION_FIELD, destination)
                .field(MESSAGE_TEMPLATE_FIELD, messageTemplate)
                .endObject()
    }

    companion object {
        const val DESTINATION_FIELD = "destination"
        const val MESSAGE_TEMPLATE_FIELD = "message_template"
        const val MUSTACHE = "mustache"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ErrorNotification {
            var destination: Destination? = null
            var messageTemplate: Script? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    DESTINATION_FIELD -> destination = Destination.parse(xcp)
                    MESSAGE_TEMPLATE_FIELD -> messageTemplate = Script.parse(xcp, Script.DEFAULT_TEMPLATE_LANG)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ErrorNotification.")
                }
            }

            return ErrorNotification(
                destination = requireNotNull(destination) { "ErrorNotification destination is null" },
                messageTemplate = requireNotNull(messageTemplate) { "ErrorNotification message template is null" }
            )
        }
    }
}
