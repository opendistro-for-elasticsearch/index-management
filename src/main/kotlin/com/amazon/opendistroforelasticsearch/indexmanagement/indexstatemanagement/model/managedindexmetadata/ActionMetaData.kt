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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData.Companion.NAME
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData.Companion.START_TIME
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

data class ActionMetaData(
    val name: String,
    val startTime: Long?,
    val index: Int,
    val failed: Boolean,
    val consumedRetries: Int,
    val lastRetryTime: Long?,
    val actionProperties: ActionProperties?
) : Writeable, ToXContentFragment {

    override fun writeTo(out: StreamOutput) {
        out.writeString(name)
        out.writeOptionalLong(startTime)
        out.writeInt(index)
        out.writeBoolean(failed)
        out.writeInt(consumedRetries)
        out.writeOptionalLong(lastRetryTime)

        out.writeOptionalWriteable(actionProperties)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .field(NAME, name)
            .field(START_TIME, startTime)
            .field(INDEX, index)
            .field(FAILED, failed)
            .field(CONSUMED_RETRIES, consumedRetries)
            .field(LAST_RETRY_TIME, lastRetryTime)

        if (actionProperties != null) {
            builder.startObject(ActionProperties.ACTION_PROPERTIES)
            actionProperties.toXContent(builder, params)
            builder.endObject()
        }

        return builder
    }

    fun getMapValueString(): String {
        return Strings.toString(this, false, false)
    }

    companion object {
        const val ACTION = "action"
        const val INDEX = "index"
        const val FAILED = "failed"
        const val CONSUMED_RETRIES = "consumed_retries"
        const val LAST_RETRY_TIME = "last_retry_time"

        fun fromStreamInput(si: StreamInput): ActionMetaData {
            val name: String? = si.readString()
            val startTime: Long? = si.readOptionalLong()
            val index: Int? = si.readInt()
            val failed: Boolean? = si.readBoolean()
            val consumedRetries: Int? = si.readInt()
            val lastRetryTime: Long? = si.readOptionalLong()

            val actionProperties: ActionProperties? = si.readOptionalWriteable { ActionProperties.fromStreamInput(it) }

            return ActionMetaData(
                requireNotNull(name) { "$NAME is null" },
                startTime,
                requireNotNull(index) { "$INDEX is null" },
                requireNotNull(failed) { "$FAILED is null" },
                requireNotNull(consumedRetries) { "$CONSUMED_RETRIES is null" },
                lastRetryTime,
                actionProperties
            )
        }

        fun fromManagedIndexMetaDataMap(map: Map<String, String?>): ActionMetaData? {
            val stateJsonString = map[ACTION]
            return if (stateJsonString != null) {
                val inputStream = ByteArrayInputStream(stateJsonString.toByteArray(StandardCharsets.UTF_8))
                val parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, inputStream)
                parser.nextToken()
                parse(parser)
            } else {
                null
            }
        }

        @Suppress("ComplexMethod")
        fun parse(xcp: XContentParser): ActionMetaData {
            var name: String? = null
            var startTime: Long? = null
            var index: Int? = null
            var failed: Boolean? = null
            var consumedRetries: Int? = null
            var lastRetryTime: Long? = null
            var actionProperties: ActionProperties? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    NAME -> name = xcp.text()
                    START_TIME -> startTime = if (xcp.currentToken() == Token.VALUE_NULL) null else xcp.longValue()
                    INDEX -> index = xcp.intValue()
                    FAILED -> failed = xcp.booleanValue()
                    CONSUMED_RETRIES -> consumedRetries = xcp.intValue()
                    LAST_RETRY_TIME -> lastRetryTime = if (xcp.currentToken() == Token.VALUE_NULL) null else xcp.longValue()
                    ActionProperties.ACTION_PROPERTIES ->
                        actionProperties = if (xcp.currentToken() == Token.VALUE_NULL) null else ActionProperties.parse(xcp)
                }
            }

            return ActionMetaData(
                requireNotNull(name) { "$NAME is null" },
                startTime,
                requireNotNull(index) { "$INDEX is null" },
                requireNotNull(failed) { "$FAILED is null" },
                requireNotNull(consumedRetries) { "$CONSUMED_RETRIES is null" },
                lastRetryTime,
                actionProperties
            )
        }
    }
}
