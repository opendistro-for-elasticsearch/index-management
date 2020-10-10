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

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.string
import org.elasticsearch.common.Strings
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.common.xcontent.XContentType
import java.io.IOException
import java.lang.IllegalStateException

/**
 * A value object that represents a Slack message. Slack message will be
 * submitted to the Slack destination
 *
 * Temporary import from alerting, this will be removed once we pull notifications out of
 * alerting so all plugins can consume and use.
 */
data class Slack(val url: String?) : ToXContent, Writeable {

    init {
        require(!Strings.isNullOrEmpty(url)) { "URL is null or empty" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject(TYPE)
                .field(URL, url)
                .endObject()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(url)
    }

    companion object {
        const val URL = "url"
        const val TYPE = "slack"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Slack {
            lateinit var url: String

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    URL -> url = xcp.text()
                    else -> {
                        throw IllegalStateException("Unexpected field: $fieldName, while parsing Slack destination")
                    }
                }
            }
            return Slack(url)
        }
    }

    fun constructMessageContent(subject: String?, message: String): String {
        val messageContent: String? = if (Strings.isNullOrEmpty(subject)) message else "$subject \n\n $message"
        val builder = XContentFactory.contentBuilder(XContentType.JSON)
        builder.startObject()
                .field("text", messageContent)
                .endObject()
        return builder.string()
    }
}
