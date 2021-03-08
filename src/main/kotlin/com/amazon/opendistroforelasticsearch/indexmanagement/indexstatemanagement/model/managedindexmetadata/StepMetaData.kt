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
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
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
import java.util.Locale

data class StepMetaData(
    val name: String,
    val startTime: Long,
    val stepStatus: Step.StepStatus
) : Writeable, ToXContentFragment {

    override fun writeTo(out: StreamOutput) {
        out.writeString(name)
        out.writeLong(startTime)
        stepStatus.writeTo(out)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .field(NAME, name)
            .field(START_TIME, startTime)
            .field(STEP_STATUS, stepStatus.toString())

        return builder
    }

    fun getMapValueString(): String {
        return Strings.toString(this, false, false)
    }

    companion object {
        const val STEP = "step"
        const val STEP_STATUS = "step_status"

        fun fromStreamInput(si: StreamInput): StepMetaData {
            val name: String? = si.readString()
            val startTime: Long? = si.readLong()
            val stepStatus: Step.StepStatus? = Step.StepStatus.read(si)

            return StepMetaData(
                requireNotNull(name) { "$NAME is null" },
                requireNotNull(startTime) { "$START_TIME is null" },
                requireNotNull(stepStatus) { "$STEP_STATUS is null" }
            )
        }

        fun fromManagedIndexMetaDataMap(map: Map<String, String?>): StepMetaData? {
            val stepJsonString = map[STEP]
            return if (stepJsonString != null) {
                val inputStream = ByteArrayInputStream(stepJsonString.toByteArray(StandardCharsets.UTF_8))
                val parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, inputStream)
                parser.nextToken()
                parse(parser)
            } else {
                null
            }
        }

        fun parse(xcp: XContentParser): StepMetaData {
            var name: String? = null
            var startTime: Long? = null
            var stepStatus: Step.StepStatus? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    NAME -> name = xcp.text()
                    START_TIME -> startTime = xcp.longValue()
                    STEP_STATUS -> stepStatus = Step.StepStatus.valueOf(xcp.text().toUpperCase(Locale.ROOT))
                }
            }

            return StepMetaData(
                requireNotNull(name) { "$NAME is null" },
                requireNotNull(startTime) { "$START_TIME is null" },
                requireNotNull(stepStatus) { "$STEP_STATUS is null" }
            )
        }
    }
}
