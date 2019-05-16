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

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.instant
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.optionalTimeField
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.Schedule
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.ScheduleParser
import org.apache.logging.log4j.LogManager
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException
import java.time.Instant

// TODO: This is an unfinished ManagedIndex data class; just needed for the jobParser

data class ManagedIndex(
    val id: String = NO_ID,
    val version: Long = NO_VERSION,
    val jobName: String,
    val index: String,
    val enabled: Boolean,
    val jobSchedule: Schedule,
    val jobLastUpdateTime: Instant,
    val jobEnabledTime: Instant?
) : ScheduledJobParameter {

    private val type =
            MANAGED_INDEX_TYPE

    override fun isEnabled() = enabled

    override fun getName() = jobName

    override fun getEnabledTime() = jobEnabledTime

    override fun getSchedule() = jobSchedule

    override fun getLastUpdateTime() = jobLastUpdateTime

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
                .startObject(MANAGED_INDEX_TYPE)
                .field(TYPE_FIELD, type)
                .field(NAME_FIELD, jobName)
                .field(ENABLED_FIELD, enabled)
                .field(INDEX_FIELD, index)
                .field(SCHEDULE_FIELD, jobSchedule)
                .optionalTimeField(LAST_UPDATE_TIME_FIELD, jobLastUpdateTime)
                .optionalTimeField(ENABLED_TIME_FIELD, jobEnabledTime)
                .endObject()
                .endObject()
        return builder
    }

    companion object {
        const val MANAGED_INDEX_TYPE = "managed_index"
        const val NO_ID = ""
        const val NO_VERSION = 1L
        const val NAME_FIELD = "name"
        const val ENABLED_FIELD = "enabled"
        const val SCHEDULE_FIELD = "schedule"
        const val LAST_UPDATE_TIME_FIELD = "last_update_time"
        const val ENABLED_TIME_FIELD = "enabled_time"
        const val TYPE_FIELD = "type"
        const val INDEX_FIELD = "index"

        val logger = LogManager.getLogger(ManagedIndex::class.java)

        @JvmStatic
        @JvmOverloads
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, id: String = NO_ID, version: Long = NO_VERSION): ScheduledJobParameter {
            lateinit var name: String
            lateinit var index: String
            lateinit var schedule: Schedule
            var lastUpdateTime: Instant? = null
            var enabledTime: Instant? = null
            var enabled = true

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    NAME_FIELD -> name = xcp.text()
                    INDEX_FIELD -> index = xcp.text()
                    ENABLED_FIELD -> enabled = xcp.booleanValue()
                    SCHEDULE_FIELD -> schedule = ScheduleParser.parse(xcp)
                    ENABLED_TIME_FIELD -> enabledTime = xcp.instant()
                    LAST_UPDATE_TIME_FIELD -> lastUpdateTime = xcp.instant()
                    else -> {
                        xcp.skipChildren()
                    }
                }
            }

            if (enabled && enabledTime == null) {
                enabledTime = Instant.now()
            } else if (!enabled) {
                enabledTime = null
            }

            return ManagedIndex(
                    id,
                    version,
                    index = requireNotNull(index) { "Managed Index index is null" },
                    jobName = requireNotNull(name) { "Managed Index name is null" },
                    enabled = enabled,
                    jobSchedule = requireNotNull(schedule) { "Managed Index schedule is null" },
                    jobLastUpdateTime = requireNotNull(lastUpdateTime) { "Managed Index Last update time is null" },
                    jobEnabledTime = enabledTime
            )
        }
    }
}
