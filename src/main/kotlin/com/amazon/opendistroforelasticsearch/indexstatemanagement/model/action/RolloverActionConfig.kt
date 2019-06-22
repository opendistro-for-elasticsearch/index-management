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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action

import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class RolloverActionConfig(
    val minSize: ByteSizeValue?,
    val minDocs: Long?,
    val minAge: TimeValue?,
    val timeout: ActionTimeout?,
    val retry: ActionRetry?
) : ToXContentObject {

    init {
        if (minSize != null) require(minSize.bytes > 0) { "RolloverActionConfig minSize value must be greater than 0" }

        if (minDocs != null) require(minDocs > 0) { "RolloverActionConfig minDocs value must be greater than 0" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject().startObject(ROLLOVER_ACTION_TYPE)
        timeout?.toXContent(builder, params)
        retry?.toXContent(builder, params)
        if (minSize != null) builder.field(MIN_SIZE_FIELD, minSize.stringRep)
        if (minDocs != null) builder.field(MIN_DOCS_FIELD, minDocs)
        if (minAge != null) builder.field(MIN_AGE_FIELD, minAge.stringRep)
        return builder.endObject().endObject()
    }

    companion object {
        const val ROLLOVER_ACTION_TYPE = "rollover"

        const val MIN_SIZE_FIELD = "min_size"
        const val MIN_DOCS_FIELD = "min_docs"
        const val MIN_AGE_FIELD = "min_age"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): RolloverActionConfig {
            var minSize: ByteSizeValue? = null
            var minDocs: Long? = null
            var minAge: TimeValue? = null
            var timeout: ActionTimeout? = null
            var retry: ActionRetry? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ActionTimeout.TIMEOUT_FIELD -> timeout = ActionTimeout.parse(xcp)
                    ActionRetry.RETRY_FIELD -> retry = ActionRetry.parse(xcp)
                    MIN_SIZE_FIELD -> minSize = ByteSizeValue.parseBytesSizeValue(xcp.text(), MIN_SIZE_FIELD)
                    MIN_DOCS_FIELD -> minDocs = xcp.longValue()
                    MIN_AGE_FIELD -> minAge = TimeValue.parseTimeValue(xcp.text(), MIN_AGE_FIELD)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in RolloverActionConfig.")
                }
            }

            return RolloverActionConfig(
                minSize = minSize,
                minDocs = minDocs,
                minAge = minAge,
                timeout = timeout,
                retry = retry
            )
        }
    }
}
