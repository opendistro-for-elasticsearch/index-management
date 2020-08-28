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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import org.apache.logging.log4j.LogManager
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentFragment
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException
import java.time.Instant
import java.util.Locale
import kotlin.math.pow

data class ActionRetry(
    val count: Long,
    val backoff: Backoff = Backoff.EXPONENTIAL,
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
        const val RETRY_FIELD = "retry"
        const val COUNT_FIELD = "count"
        const val BACKOFF_FIELD = "backoff"
        const val DELAY_FIELD = "delay"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ActionRetry {
            var count: Long? = null
            var backoff: Backoff = Backoff.EXPONENTIAL
            var delay: TimeValue = TimeValue.timeValueMinutes(1)

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    COUNT_FIELD -> count = xcp.longValue()
                    BACKOFF_FIELD -> backoff = Backoff.valueOf(xcp.text().toUpperCase(Locale.ROOT))
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

    enum class Backoff(val type: String, val getNextRetryTime: (consumedRetries: Int, timeValue: TimeValue) -> Long) {
        EXPONENTIAL("exponential", { consumedRetries, timeValue ->
            (2.0.pow(consumedRetries - 1)).toLong() * timeValue.millis
        }),
        CONSTANT("constant", { _, timeValue ->
            timeValue.millis
        }),
        LINEAR("linear", { consumedRetries, timeValue ->
            consumedRetries * timeValue.millis
        });

        private val logger = LogManager.getLogger(javaClass)

        override fun toString(): String {
            return type
        }

        @Suppress("ReturnCount")
        fun shouldBackoff(actionMetaData: ActionMetaData?, actionRetry: ActionRetry?): Pair<Boolean, Long?> {
            if (actionMetaData == null || actionRetry == null) {
                logger.debug("There is no actionMetaData and ActionRetry we don't need to backoff")
                return Pair(false, null)
            }

            if (actionMetaData.consumedRetries > 0) {
                if (actionMetaData.lastRetryTime != null) {
                    val remainingTime = getNextRetryTime(actionMetaData.consumedRetries, actionRetry.delay) -
                        (Instant.now().toEpochMilli() - actionMetaData.lastRetryTime)

                    return Pair(remainingTime > 0, remainingTime)
                }
            }

            return Pair(false, null)
        }
    }
}
