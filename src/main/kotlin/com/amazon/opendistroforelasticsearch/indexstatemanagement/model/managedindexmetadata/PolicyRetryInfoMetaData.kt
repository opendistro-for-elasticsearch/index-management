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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata

import org.elasticsearch.common.Strings
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentFragment
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.common.xcontent.XContentType
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

data class PolicyRetryInfoMetaData(
    val failed: Boolean,
    val consumedRetries: Int
) : Writeable, ToXContentFragment {

    override fun writeTo(out: StreamOutput) {
        out.writeBoolean(failed)
        out.writeInt(consumedRetries)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder
            .field(FAILED, failed)
            .field(CONSUMED_RETRIES, consumedRetries)
    }

    fun getMapValueString(): String = Strings.toString(this, false, false)

    fun asTemplateArg(): Map<String, Any> = mapOf(FAILED to failed, CONSUMED_RETRIES to consumedRetries)

    companion object {
        const val RETRY_INFO = "retry_info"
        const val FAILED = "failed"
        const val CONSUMED_RETRIES = "consumed_retries"

        fun fromStreamInput(si: StreamInput): PolicyRetryInfoMetaData {
            val failed: Boolean? = si.readBoolean()
            val consumedRetries: Int? = si.readInt()

            return PolicyRetryInfoMetaData(
                requireNotNull(failed) { "$FAILED is null" },
                requireNotNull(consumedRetries) { "$CONSUMED_RETRIES is null" }
            )
        }

        fun fromManagedIndexMetaDataMap(map: Map<String, String?>): PolicyRetryInfoMetaData? {
            val stateJsonString = map[RETRY_INFO]
            return if (stateJsonString != null) {
                val inputStream = ByteArrayInputStream(stateJsonString.toByteArray(StandardCharsets.UTF_8))
                val parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, inputStream)
                parser.nextToken()
                parse(parser)
            } else {
                null
            }
        }

        fun parse(xcp: XContentParser): PolicyRetryInfoMetaData {
            var failed: Boolean? = null
            var consumedRetries: Int? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    FAILED -> failed = xcp.booleanValue()
                    CONSUMED_RETRIES -> consumedRetries = xcp.intValue()
                }
            }

            return PolicyRetryInfoMetaData(
                requireNotNull(failed) { "$FAILED is null" },
                requireNotNull(consumedRetries) { "$CONSUMED_RETRIES is null" }
            )
        }
    }
}
