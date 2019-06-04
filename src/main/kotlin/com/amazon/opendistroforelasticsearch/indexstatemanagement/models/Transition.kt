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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.models

import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.CronSchedule
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.ScheduleParser
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class Transition(
    val stateName: String,
    val indexAge: String?,
    val docCount: Long?,
    val size: String?,
    val cron: CronSchedule?
) : ToXContent {

    init {
        var conditionsSet = 0
        if (indexAge != null) conditionsSet++
        if (docCount != null) conditionsSet++
        if (size != null) conditionsSet++
        if (cron != null) conditionsSet++

        require(conditionsSet < 2) { "Cannot provide more than one Transition condition" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .startObject()
                .field(STATE_FIELD, stateName)
                .field(INDEX_AGE_FIELD, indexAge)
                .field(DOC_COUNT_FIELD, docCount)
                .field(SIZE_FIELD, size)
                .field(CRON_FIELD, cron)
            .endObject()
        return builder
    }

    companion object {
        const val STATE_FIELD = "state"
        const val INDEX_AGE_FIELD = "index_age"
        const val DOC_COUNT_FIELD = "doc_count"
        const val SIZE_FIELD = "size"
        const val CRON_FIELD = "cron"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Transition {
            lateinit var name: String
            var indexAge: String? = null
            var docCount: Long? = null
            var size: String? = null
            var cron: CronSchedule? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    STATE_FIELD -> name = xcp.text()
                    INDEX_AGE_FIELD -> indexAge = xcp.text()
                    DOC_COUNT_FIELD -> docCount = xcp.longValue()
                    SIZE_FIELD -> size = xcp.text()
                    CRON_FIELD -> cron = ScheduleParser.parse(xcp) as? CronSchedule
                }
            }

            return Transition(
                stateName = requireNotNull(name) { "Transition state name is null" },
                indexAge = indexAge,
                docCount = docCount,
                size = size,
                cron = cron
            )
        }
    }
}
