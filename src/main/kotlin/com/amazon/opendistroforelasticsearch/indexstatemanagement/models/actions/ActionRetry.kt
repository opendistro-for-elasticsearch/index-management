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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.models.actions

import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentFragment
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class ActionRetry(
    val count: Long,
    val backoff: String = DEFAULT_BACKOFF,
    val delay: TimeValue = TimeValue.timeValueMinutes(1)
) : ToXContentFragment {

    init { require(count > 0) { "Count for ActionRetry must be greater than 0" } }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .startObject(RETRY_FIELD)
                .field(COUNT_FIELD, count)
                .field(BACKOFF_FIELD, backoff)
                .field(DELAY_FIELD, delay.stringRep)
            .endObject()
        return builder
    }

    companion object {
        const val DEFAULT_BACKOFF = "exponential"

        const val RETRY_FIELD = "retry"
        const val COUNT_FIELD = "count"
        const val BACKOFF_FIELD = "backoff"
        const val DELAY_FIELD = "delay"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ActionRetry {
            var count: Long? = null
            var backoff: String = DEFAULT_BACKOFF
            var delay: TimeValue = TimeValue.timeValueMinutes(1)

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    COUNT_FIELD -> count = xcp.longValue()
                    BACKOFF_FIELD -> backoff = xcp.text()
                    DELAY_FIELD -> delay = TimeValue.parseTimeValue(xcp.text(), DELAY_FIELD)
                }
            }

            return ActionRetry(
                count = requireNotNull(count) { "ActionRetry count is null" },
                backoff = backoff,
                delay = delay
            )
        }
    }
}
