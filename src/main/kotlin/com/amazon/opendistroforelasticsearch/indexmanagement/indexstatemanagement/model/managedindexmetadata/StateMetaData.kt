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

data class StateMetaData(
    val name: String,
    val startTime: Long
) : Writeable, ToXContentFragment {

    override fun writeTo(out: StreamOutput) {
        out.writeString(name)
        out.writeLong(startTime)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder
            .field(NAME, name)
            .field(START_TIME, startTime)
    }

    fun getMapValueString(): String = Strings.toString(this, false, false)

    companion object {
        const val STATE = "state"

        fun fromStreamInput(si: StreamInput): StateMetaData {
            val name: String? = si.readString()
            val startTime: Long? = si.readLong()

            return StateMetaData(
                requireNotNull(name) { "$NAME is null" },
                requireNotNull(startTime) { "$START_TIME is null" }
            )
        }

        fun fromManagedIndexMetaDataMap(map: Map<String, String?>): StateMetaData? {
            val stateJsonString = map[STATE]
            return if (stateJsonString != null) {
                val inputStream = ByteArrayInputStream(stateJsonString.toByteArray(StandardCharsets.UTF_8))
                val parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, inputStream)
                parser.nextToken()
                parse(parser)
            } else {
                null
            }
        }

        fun parse(xcp: XContentParser): StateMetaData {
            var name: String? = null
            var startTime: Long? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    NAME -> name = xcp.text()
                    START_TIME -> startTime = xcp.longValue()
                }
            }

            return StateMetaData(
                requireNotNull(name) { "$NAME is null" },
                requireNotNull(startTime) { "$START_TIME is null" }
            )
        }
    }
}
