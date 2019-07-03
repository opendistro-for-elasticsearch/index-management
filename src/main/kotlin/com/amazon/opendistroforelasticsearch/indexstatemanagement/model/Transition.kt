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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.model

import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.CronSchedule
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.ScheduleParser
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class Transition(
    val stateName: String,
    val conditions: Conditions?
) : ToXContentObject {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(STATE_NAME_FIELD, stateName)
        if (conditions != null) builder.field(CONDITIONS_FIELD, conditions)
        return builder.endObject()
    }

    companion object {
        const val STATE_NAME_FIELD = "state_name"
        const val CONDITIONS_FIELD = "conditions"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Transition {
            var name: String? = null
            var conditions: Conditions? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    STATE_NAME_FIELD -> name = xcp.text()
                    CONDITIONS_FIELD -> conditions = Conditions.parse(xcp)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in Transition.")
                }
            }

            return Transition(
                stateName = requireNotNull(name) { "Transition state name is null" },
                conditions = conditions
            )
        }
    }
}

data class Conditions(
    val indexAge: TimeValue? = null,
    val docCount: Long? = null,
    val size: ByteSizeValue? = null,
    val cron: CronSchedule? = null
) : ToXContentObject {

    init {
        val conditionsList = listOf(indexAge, docCount, size, cron)
        require(conditionsList.filterNotNull().size == 1) { "Cannot provide more than one Transition condition" }

        // Validate doc count condition
        if (docCount != null) require(docCount > 0) { "Transition doc count condition must be greater than 0" }

        // Validate size condition
        if (size != null) require(size.bytes > 0) { "Transition size condition must be greater than 0" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (indexAge != null) builder.field(INDEX_AGE_FIELD, indexAge.stringRep)
        if (docCount != null) builder.field(DOC_COUNT_FIELD, docCount)
        if (size != null) builder.field(SIZE_FIELD, size.stringRep)
        if (cron != null) builder.field(CRON_FIELD, cron)
        return builder.endObject()
    }

    // TODO: field names are different from rollover field names but refer to same concept, should make the same
    companion object {
        const val INDEX_AGE_FIELD = "index_age"
        const val DOC_COUNT_FIELD = "doc_count"
        const val SIZE_FIELD = "size"
        const val CRON_FIELD = "cron"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Conditions {
            var indexAge: TimeValue? = null
            var docCount: Long? = null
            var size: ByteSizeValue? = null
            var cron: CronSchedule? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    INDEX_AGE_FIELD -> indexAge = TimeValue.parseTimeValue(xcp.text(), INDEX_AGE_FIELD)
                    DOC_COUNT_FIELD -> docCount = xcp.longValue()
                    SIZE_FIELD -> size = ByteSizeValue.parseBytesSizeValue(xcp.text(), SIZE_FIELD)
                    CRON_FIELD -> cron = ScheduleParser.parse(xcp) as? CronSchedule
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in Conditions.")
                }
            }

            return Conditions(indexAge, docCount, size, cron)
        }
    }
}
