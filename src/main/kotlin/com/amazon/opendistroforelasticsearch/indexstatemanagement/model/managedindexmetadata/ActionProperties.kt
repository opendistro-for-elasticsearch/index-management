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

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentFragment
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken

/** Properties that will persist across steps of a single Action. Will be stored in the [ActionMetaData]. */
data class ActionProperties(
    val wasReadOnly: Boolean? = null,
    val maxNumSegments: Int? = null
) : Writeable, ToXContentFragment {

    override fun writeTo(out: StreamOutput) {
        out.writeOptionalBoolean(wasReadOnly)
        out.writeOptionalInt(maxNumSegments)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        if (wasReadOnly != null) builder.field(WAS_READ_ONLY, wasReadOnly)
        if (maxNumSegments != null) builder.field(MAX_NUM_SEGMENTS, maxNumSegments)

        return builder
    }

    companion object {
        const val ACTION_PROPERTIES = "action_properties"
        const val WAS_READ_ONLY = "was_read_only"
        const val MAX_NUM_SEGMENTS = "max_num_segments"

        fun fromStreamInput(si: StreamInput): ActionProperties {
            val wasReadOnly: Boolean? = si.readOptionalBoolean()
            val maxNumSegments: Int? = si.readOptionalInt()

            return ActionProperties(wasReadOnly, maxNumSegments)
        }

        fun parse(xcp: XContentParser): ActionProperties {
            var wasReadOnly: Boolean? = null
            var maxNumSegments: Int? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    WAS_READ_ONLY -> wasReadOnly = xcp.booleanValue()
                    MAX_NUM_SEGMENTS -> maxNumSegments = xcp.intValue()
                }
            }

            return ActionProperties(wasReadOnly, maxNumSegments)
        }
    }
}
