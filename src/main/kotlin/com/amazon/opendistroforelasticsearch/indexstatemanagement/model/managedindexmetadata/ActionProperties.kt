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
    val maxNumSegments: Int? = null,
    val snapshotName: String? = null
) : Writeable, ToXContentFragment {

    override fun writeTo(out: StreamOutput) {
        out.writeOptionalInt(maxNumSegments)
        out.writeOptionalString(snapshotName)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        if (maxNumSegments != null) builder.field(Properties.MAX_NUM_SEGMENTS.key, maxNumSegments)
        if (snapshotName != null) builder.field(Properties.SNAPSHOT_NAME.key, snapshotName)
        return builder
    }

    companion object {
        const val ACTION_PROPERTIES = "action_properties"

        fun fromStreamInput(si: StreamInput): ActionProperties {
            val maxNumSegments: Int? = si.readOptionalInt()
            val snapshotName: String? = si.readOptionalString()

            return ActionProperties(maxNumSegments, snapshotName)
        }

        fun parse(xcp: XContentParser): ActionProperties {
            var maxNumSegments: Int? = null
            var snapshotName: String? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    Properties.MAX_NUM_SEGMENTS.key -> maxNumSegments = xcp.intValue()
                    Properties.SNAPSHOT_NAME.key -> snapshotName = xcp.text()
                }
            }

            return ActionProperties(maxNumSegments, snapshotName)
        }
    }

    enum class Properties(val key: String) { MAX_NUM_SEGMENTS("max_num_segments"), SNAPSHOT_NAME("snapshot_name") }
}
