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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.destination

import com.amazon.opendistroforelasticsearch.alerting.destination.Notification
import com.amazon.opendistroforelasticsearch.alerting.destination.message.BaseMessage
import com.amazon.opendistroforelasticsearch.alerting.destination.message.ChimeMessage
import com.amazon.opendistroforelasticsearch.alerting.destination.message.CustomWebhookMessage
import com.amazon.opendistroforelasticsearch.alerting.destination.message.SlackMessage
import com.amazon.opendistroforelasticsearch.alerting.destination.response.DestinationResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.convertToMap
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.isHostInDenylist
import org.apache.logging.log4j.LogManager
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

enum class DestinationType(val value: String) {
    CHIME("chime"),
    SLACK("slack"),
    CUSTOM_WEBHOOK("custom_webhook"),
}

data class Destination(
    val type: DestinationType,
    val chime: Chime?,
    val slack: Slack?,
    val customWebhook: CustomWebhook?
) : ToXContentObject, Writeable {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(type.value, constructResponseForDestinationType(type))
            .endObject()
        return builder
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readEnum(DestinationType::class.java),
        sin.readOptionalWriteable(::Chime),
        sin.readOptionalWriteable(::Slack),
        sin.readOptionalWriteable(::CustomWebhook)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeEnum(type)
        out.writeOptionalWriteable(chime)
        out.writeOptionalWriteable(slack)
        out.writeOptionalWriteable(customWebhook)
    }

    companion object {
        const val CHIME = "chime"
        const val SLACK = "slack"
        const val CUSTOMWEBHOOK = "custom_webhook"

        private val logger = LogManager.getLogger(Destination::class.java)

        @Suppress("ComplexMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Destination {
            var slack: Slack? = null
            var chime: Chime? = null
            var customWebhook: CustomWebhook? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    CHIME -> chime = Chime.parse(xcp)
                    SLACK -> slack = Slack.parse(xcp)
                    CUSTOMWEBHOOK -> customWebhook = CustomWebhook.parse(xcp)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in Destination.")
                }
            }

            val type = when {
                chime != null -> DestinationType.CHIME
                slack != null -> DestinationType.SLACK
                customWebhook != null -> DestinationType.CUSTOM_WEBHOOK
                else -> throw IllegalArgumentException("Must specify a destination type")
            }

            return Destination(
                type,
                chime,
                slack,
                customWebhook
            )
        }
    }

    @Throws(IOException::class)
    fun publish(compiledSubject: String?, compiledMessage: String, denyHostRanges: List<String>): DestinationResponse {
        val destinationMessage: BaseMessage
        when (type) {
            DestinationType.CHIME -> {
                val messageContent = chime?.constructMessageContent(compiledSubject, compiledMessage)
                destinationMessage = ChimeMessage.Builder("chime_message")
                        .withUrl(chime?.url)
                        .withMessage(messageContent)
                        .build()
            }
            DestinationType.SLACK -> {
                val messageContent = slack?.constructMessageContent(compiledSubject, compiledMessage)
                destinationMessage = SlackMessage.Builder("slack_message")
                        .withUrl(slack?.url)
                        .withMessage(messageContent)
                        .build()
            }
            DestinationType.CUSTOM_WEBHOOK -> {
                destinationMessage = CustomWebhookMessage.Builder("custom_webhook")
                        .withUrl(customWebhook?.url)
                        .withScheme(customWebhook?.scheme)
                        .withHost(customWebhook?.host)
                        .withPort(customWebhook?.port)
                        .withPath(customWebhook?.path)
                        .withQueryParams(customWebhook?.queryParams)
                        .withHeaderParams(customWebhook?.headerParams)
                        .withMessage(compiledMessage).build()
            }
        }
        validateDestinationUri(destinationMessage, denyHostRanges)
        val response = Notification.publish(destinationMessage) as DestinationResponse
        logger.info("Message published for action type: $type, messageid: ${response.responseContent}, statuscode: ${response.statusCode}")
        return response
    }

    fun constructResponseForDestinationType(type: DestinationType): Any {
        var content: Any? = null
        when (type) {
            DestinationType.CHIME -> content = chime?.convertToMap()?.get(type.value)
            DestinationType.SLACK -> content = slack?.convertToMap()?.get(type.value)
            DestinationType.CUSTOM_WEBHOOK -> content = customWebhook?.convertToMap()?.get(type.value)
        }
        if (content == null) {
            throw IllegalArgumentException("Content is NULL for destination type ${type.value}")
        }
        return content
    }

    private fun validateDestinationUri(destinationMessage: BaseMessage, denyHostRanges: List<String>) {
        if (destinationMessage.isHostInDenylist(denyHostRanges)) {
            throw IllegalArgumentException("The destination address is invalid.")
        }
    }
}
