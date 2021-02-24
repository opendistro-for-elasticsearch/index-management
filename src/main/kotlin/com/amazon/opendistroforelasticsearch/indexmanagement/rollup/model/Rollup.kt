/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.instant
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.optionalTimeField
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.WITH_TYPE
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.DateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Dimension
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Histogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Terms
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils
import com.amazon.opendistroforelasticsearch.indexmanagement.util._ID
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.CronSchedule
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.Schedule
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.ScheduleParser
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.index.seqno.SequenceNumbers
import java.io.IOException
import java.time.Instant

data class Rollup(
    val id: String = NO_ID,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
    val enabled: Boolean,
    val schemaVersion: Long,
    val jobSchedule: Schedule,
    val jobLastUpdatedTime: Instant,
    val jobEnabledTime: Instant?,
    val description: String,
    val sourceIndex: String,
    val targetIndex: String,
    val metadataID: String?,
    val roles: List<String>,
    val pageSize: Int,
    val delay: Long?,
    val continuous: Boolean,
    val dimensions: List<Dimension>,
    val metrics: List<RollupMetrics>
) : ScheduledJobParameter, Writeable {

    init {
        if (enabled) {
            requireNotNull(jobEnabledTime) { "Job enabled time must be present if the job is enabled" }
        } else {
            require(jobEnabledTime == null) { "Job enabled time must not be present if the job is disabled" }
        }
        when (jobSchedule) {
            is CronSchedule -> {
                // Job scheduler already correctly throws errors for this
            }
            is IntervalSchedule -> {
                require(jobSchedule.interval >= MINIMUM_JOB_INTERVAL) { "Rollup job schedule interval must be greater than 0" }
            }
        }
        require(sourceIndex != targetIndex) { "Your source and target index cannot be the same" }
        require(dimensions.filter { it.type == Dimension.Type.DATE_HISTOGRAM }.size == 1) {
            "Must specify precisely one date histogram dimension" // this covers empty dimensions case too
        }
        require(dimensions.first().type == Dimension.Type.DATE_HISTOGRAM) { "The first dimension must be a date histogram" }
        require(pageSize in MINIMUM_PAGE_SIZE..MAXIMUM_PAGE_SIZE) { "Page size must be between 1 and 10,000" }
        if (delay != null) require(delay >= MINIMUM_DELAY) { "Delay must be non-negative if set" }
    }

    override fun isEnabled() = enabled

    override fun getName() = id // the id is user chosen and represents the rollup's name

    override fun getEnabledTime() = jobEnabledTime

    override fun getSchedule() = jobSchedule

    override fun getLastUpdateTime() = jobLastUpdatedTime

    override fun getLockDurationSeconds(): Long = ROLLUP_LOCK_DURATION_SECONDS

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
        enabled = sin.readBoolean(),
        schemaVersion = sin.readLong(),
        jobSchedule = sin.let {
            when (requireNotNull(sin.readEnum(ScheduleType::class.java)) { "ScheduleType cannot be null" }) {
                ScheduleType.CRON -> CronSchedule(sin)
                ScheduleType.INTERVAL -> IntervalSchedule(sin)
            }
        },
        jobLastUpdatedTime = sin.readInstant(),
        jobEnabledTime = sin.readOptionalInstant(),
        description = sin.readString(),
        sourceIndex = sin.readString(),
        targetIndex = sin.readString(),
        metadataID = sin.readOptionalString(),
        roles = sin.readStringArray().toList(),
        pageSize = sin.readInt(),
        delay = sin.readOptionalLong(),
        continuous = sin.readBoolean(),
        dimensions = sin.let {
            val dimensionsList = mutableListOf<Dimension>()
            val size = it.readVInt()
            for (i in 0 until size) {
                val type = it.readEnum(Dimension.Type::class.java)
                dimensionsList.add(
                    when (requireNotNull(type) { "Dimension type cannot be null" }) {
                        Dimension.Type.DATE_HISTOGRAM -> DateHistogram(sin)
                        Dimension.Type.TERMS -> Terms(sin)
                        Dimension.Type.HISTOGRAM -> Histogram(sin)
                    }
                )
            }
            dimensionsList.toList()
        },
        metrics = sin.readList(::RollupMetrics)
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.startObject(ROLLUP_TYPE)
        builder.field(ROLLUP_ID_FIELD, id)
            .field(ENABLED_FIELD, enabled)
            .field(SCHEDULE_FIELD, jobSchedule)
            .optionalTimeField(LAST_UPDATED_TIME_FIELD, jobLastUpdatedTime)
            .optionalTimeField(ENABLED_TIME_FIELD, jobEnabledTime)
            .field(DESCRIPTION_FIELD, description)
            .field(SCHEMA_VERSION_FIELD, schemaVersion)
            .field(SOURCE_INDEX_FIELD, sourceIndex)
            .field(TARGET_INDEX_FIELD, targetIndex)
            .field(METADATA_ID_FIELD, metadataID)
            .field(ROLES_FIELD, roles.toTypedArray())
            .field(PAGE_SIZE_FIELD, pageSize)
            .field(DELAY_FIELD, delay)
            .field(CONTINUOUS_FIELD, continuous)
            .field(DIMENSIONS_FIELD, dimensions.toTypedArray())
            .field(RollupMetrics.METRICS_FIELD, metrics.toTypedArray())
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.endObject()
        builder.endObject()
        return builder
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        out.writeBoolean(enabled)
        out.writeLong(schemaVersion)
        if (schedule is CronSchedule) {
            out.writeEnum(ScheduleType.CRON)
        } else {
            out.writeEnum(ScheduleType.INTERVAL)
        }
        schedule.writeTo(out)
        out.writeInstant(jobLastUpdatedTime)
        out.writeOptionalInstant(jobEnabledTime)
        out.writeString(description)
        out.writeString(sourceIndex)
        out.writeString(targetIndex)
        out.writeOptionalString(metadataID)
        out.writeStringArray(roles.toTypedArray())
        out.writeInt(pageSize)
        out.writeOptionalLong(delay)
        out.writeBoolean(continuous)
        out.writeVInt(dimensions.size)
        for (dimension in dimensions) {
            out.writeEnum(dimension.type)
            when (dimension) {
                is DateHistogram -> dimension.writeTo(out)
                is Terms -> dimension.writeTo(out)
                is Histogram -> dimension.writeTo(out)
            }
        }
        out.writeCollection(metrics)
    }

    companion object {
        // TODO: Move this enum to Job Scheduler plugin
        enum class ScheduleType {
            CRON, INTERVAL;
        }
        const val ROLLUP_LOCK_DURATION_SECONDS = 1800L // 30 minutes
        const val ROLLUP_TYPE = "rollup"
        const val ROLLUP_ID_FIELD = "rollup_id"
        const val NO_ID = ""
        const val ENABLED_FIELD = "enabled"
        const val SCHEMA_VERSION_FIELD = "schema_version"
        const val SCHEDULE_FIELD = "schedule"
        const val LAST_UPDATED_TIME_FIELD = "last_updated_time"
        const val ENABLED_TIME_FIELD = "enabled_time"
        const val DESCRIPTION_FIELD = "description"
        const val SOURCE_INDEX_FIELD = "source_index"
        const val TARGET_INDEX_FIELD = "target_index"
        const val METADATA_ID_FIELD = "metadata_id"
        const val ROLES_FIELD = "roles"
        const val PAGE_SIZE_FIELD = "page_size"
        const val DELAY_FIELD = "delay"
        const val CONTINUOUS_FIELD = "continuous"
        const val DIMENSIONS_FIELD = "dimensions"
        const val METRICS_FIELD = "metrics"
        const val MINIMUM_JOB_INTERVAL = 1
        const val MINIMUM_DELAY = 0
        const val MINIMUM_PAGE_SIZE = 1
        const val MAXIMUM_PAGE_SIZE = 10_000
        const val ROLLUP_DOC_ID_FIELD = "$ROLLUP_TYPE.$_ID"
        const val ROLLUP_DOC_COUNT_FIELD = "$ROLLUP_TYPE._doc_count"
        const val ROLLUP_DOC_SCHEMA_VERSION_FIELD = "$ROLLUP_TYPE._$SCHEMA_VERSION_FIELD"

        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @JvmOverloads
        @Throws(IOException::class)
        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): Rollup {
            var schedule: Schedule? = null
            var schemaVersion: Long = IndexUtils.DEFAULT_SCHEMA_VERSION
            var lastUpdatedTime: Instant? = null
            var enabledTime: Instant? = null
            var enabled = true
            var description: String? = null
            var sourceIndex: String? = null
            var targetIndex: String? = null
            var metadataID: String? = null
            val roles = mutableListOf<String>()
            var pageSize: Int? = null
            var delay: Long? = null
            var continuous = false
            val dimensions = mutableListOf<Dimension>()
            val metrics = mutableListOf<RollupMetrics>()

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)

            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ROLLUP_ID_FIELD -> { requireNotNull(xcp.text()) { "The rollup_id field is null" } /* Just used for searching */ }
                    ENABLED_FIELD -> enabled = xcp.booleanValue()
                    SCHEDULE_FIELD -> schedule = ScheduleParser.parse(xcp)
                    SCHEMA_VERSION_FIELD -> schemaVersion = xcp.longValue()
                    ENABLED_TIME_FIELD -> enabledTime = xcp.instant()
                    LAST_UPDATED_TIME_FIELD -> lastUpdatedTime = xcp.instant()
                    DESCRIPTION_FIELD -> description = xcp.text()
                    SOURCE_INDEX_FIELD -> sourceIndex = xcp.text()
                    TARGET_INDEX_FIELD -> targetIndex = xcp.text()
                    METADATA_ID_FIELD -> metadataID = xcp.textOrNull()
                    ROLES_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            roles.add(xcp.text())
                        }
                    }
                    PAGE_SIZE_FIELD -> pageSize = xcp.intValue()
                    DELAY_FIELD -> delay = if (xcp.currentToken() == Token.VALUE_NULL) null else xcp.longValue()
                    CONTINUOUS_FIELD -> continuous = xcp.booleanValue()
                    DIMENSIONS_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            dimensions.add(Dimension.parse(xcp))
                        }
                    }
                    METRICS_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            metrics.add(RollupMetrics.parse(xcp))
                        }
                    }
                    else -> throw IllegalArgumentException("Invalid field [$fieldName] found in Rollup.")
                }
            }

            if (enabled && enabledTime == null) {
                enabledTime = Instant.now()
            } else if (!enabled) {
                enabledTime = null
            }

            // If the seqNo/primaryTerm are unassigned this job hasn't been created yet so we instantiate the startTime
            // TODO: Make startTime public in Job Scheduler so we can just directly check the value
            if (seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || primaryTerm == SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
                if (schedule is IntervalSchedule) {
                    schedule = IntervalSchedule(Instant.now(), schedule.interval, schedule.unit)
                }
            }
            return Rollup(
                id = id,
                seqNo = seqNo,
                primaryTerm = primaryTerm,
                enabled = enabled,
                schemaVersion = schemaVersion,
                jobSchedule = requireNotNull(schedule) { "Rollup schedule is null" },
                jobLastUpdatedTime = lastUpdatedTime ?: Instant.now(),
                jobEnabledTime = enabledTime,
                description = requireNotNull(description) { "Rollup description is null" },
                sourceIndex = requireNotNull(sourceIndex) { "Rollup source index is null" },
                targetIndex = requireNotNull(targetIndex) { "Rollup target index is null" },
                metadataID = metadataID,
                roles = roles.toList(),
                pageSize = requireNotNull(pageSize) { "Rollup page size is null" },
                delay = delay,
                continuous = continuous,
                dimensions = dimensions,
                metrics = metrics
            )
        }
    }
}

sealed class RollupJobValidationResult {
    object Valid : RollupJobValidationResult()
    data class Invalid(val reason: String) : RollupJobValidationResult()
    data class Failure(val message: String, val e: Exception? = null) : RollupJobValidationResult()
}
