package com.amazon.opendistroforelasticsearch.indexmanagement.transform.model

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.instant
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.optionalTimeField
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.WITH_TYPE
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.DateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Dimension
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Histogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Terms
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.CronSchedule
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.Schedule
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.ScheduleParser
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.AbstractQueryBuilder
import org.elasticsearch.index.query.MatchAllQueryBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.seqno.SequenceNumbers
import org.elasticsearch.search.aggregations.AggregatorFactories
import java.io.IOException
import java.time.Instant

data class Transform(
    val id: String = NO_ID,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
    override val schemaVersion: Long,
    override val jobSchedule: Schedule,
    val metadataId: String?,
    val updatedAt: Instant,
    override val enabled: Boolean,
    override val enabledAt: Instant?,
    override val description: String,
    val sourceIndex: String,
    val dataSelectionQuery: QueryBuilder = MatchAllQueryBuilder(),
    val targetIndex: String,
    val roles: List<String>,
    val pageSize: Int,
    val groups: List<Dimension>,
    val aggregations: AggregatorFactories.Builder
) : ScheduleJob(schemaVersion = schemaVersion, jobSchedule = jobSchedule, enabled = enabled, enabledAt = enabledAt, description = description),
    ScheduledJobParameter,
    Writeable {

    init {
        aggregations.aggregatorFactories.forEach {
            require(supportedAggregations.contains(it.type)) { "Unsupported aggregation [${it.type}]" }
        }
        when (jobSchedule) {
            is CronSchedule -> {}
            is IntervalSchedule -> {
                require(jobSchedule.interval >= MINIMUM_JOB_INTERVAL) { "Transform job schedule interval must be greater than 0" }
            }
        }
        require(groups.isNotEmpty()) { "Groupings are Empty" }
        require(sourceIndex != targetIndex) { "Source and target indices cannot be the same" }
        require(pageSize in MINIMUM_PAGE_SIZE..MAXIMUM_PAGE_SIZE) { "Page size must be between 1 and 10,000" }
    }

    override fun getName() = id

    override fun getSchedule() = jobSchedule

    override fun getLastUpdateTime() = updatedAt

    override fun getEnabledTime() = enabledAt

    override fun isEnabled() = enabled

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.startObject(TRANSFORM_TYPE)
        builder.field(TRANSFORM_ID_FIELD, id)
            .field(SCHEMA_VERSION_FIELD, schemaVersion)
            .field(JOB_SCHEDULE_FIELD, schedule)
            .field(METADATA_ID_FIELD, metadataId)
            .optionalTimeField(UPDATED_AT_FIELD, updatedAt)
            .field(ENABLED_FIELD, enabled)
            .optionalTimeField(ENABLED_AT_FIELD, enabledAt)
            .field(DESCRIPTION_FIELD, description)
            .field(SOURCE_INDEX_FIELD, sourceIndex)
            .field(DATA_SELECTION_QUERY_FIELD, dataSelectionQuery)
            .field(TARGET_INDEX_FIELD, targetIndex)
            .field(ROLES_FIELD, roles.toTypedArray())
            .field(PAGE_SIZE_FIELD, pageSize)
            .field(GROUPS_FIELD, groups.toTypedArray())
            .field(AGGREGATIONS_FIELD, aggregations)
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.endObject()
        builder.endObject()
        return builder
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        out.writeLong(schemaVersion)
        if (schedule is CronSchedule) {
            out.writeEnum(ScheduleType.CRON)
        } else {
            out.writeEnum(ScheduleType.INTERVAL)
        }
        schedule.writeTo(out)
        out.writeOptionalString(metadataId)
        out.writeInstant(updatedAt)
        out.writeBoolean(enabled)
        out.writeOptionalInstant(enabledAt)
        out.writeString(description)
        out.writeString(sourceIndex)
        out.writeOptionalNamedWriteable(dataSelectionQuery)
        out.writeString(targetIndex)
        out.writeStringArray(roles.toTypedArray())
        out.writeInt(pageSize)
        out.writeVInt(groups.size)
        for (group in groups) {
            out.writeEnum(group.type)
            when (group) {
                is DateHistogram -> group.writeTo(out)
                is Terms -> group.writeTo(out)
                is Histogram -> group.writeTo(out)
            }
        }
        out.writeOptionalWriteable(aggregations)
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
        schemaVersion = sin.readLong(),
        jobSchedule = sin.let {
            when (requireNotNull(sin.readEnum(ScheduleType::class.java)) { "ScheduleType cannot be null" }) {
                ScheduleType.CRON -> CronSchedule(sin)
                ScheduleType.INTERVAL -> IntervalSchedule(sin)
            }
        },
        metadataId = sin.readOptionalString(),
        updatedAt = sin.readInstant(),
        enabled = sin.readBoolean(),
        enabledAt = sin.readOptionalInstant(),
        description = sin.readString(),
        sourceIndex = sin.readString(),
        dataSelectionQuery = requireNotNull(sin.readNamedWriteable(QueryBuilder::class.java)) { "Query cannot be null" },
        targetIndex = sin.readString(),
        roles = sin.readStringArray().toList(),
        pageSize = sin.readInt(),
        groups = sin.let {
            val dimensionList = mutableListOf<Dimension>()
            val size = it.readVInt()
            for (i in 0 until size) {
                val type = it.readEnum(Dimension.Type::class.java)
                dimensionList.add(
                    when (requireNotNull(type) { "Dimension type cannot be null" }) {
                        Dimension.Type.DATE_HISTOGRAM -> DateHistogram(sin)
                        Dimension.Type.TERMS -> Terms(sin)
                        Dimension.Type.HISTOGRAM -> Histogram(sin)
                    }
                )
            }
            dimensionList.toList()
        },
        aggregations = sin.readOptionalWriteable { AggregatorFactories.Builder(it) }!!
    )

    companion object {
        enum class ScheduleType {
            CRON, INTERVAL;
        }

        val supportedAggregations = listOf("sum", "max", "min", "value_count", "avg", "scripted_metric")
        const val NO_ID = ""
        const val TRANSFORM_TYPE = "transform"
        const val TRANSFORM_ID_FIELD = "transform_id"
        const val ENABLED_FIELD = "enabled"
        const val UPDATED_AT_FIELD = "updated_at"
        const val ENABLED_AT_FIELD = "enabled_at"
        const val SOURCE_INDEX_FIELD = "source_index"
        const val TARGET_INDEX_FIELD = "target_index"
        const val DESCRIPTION_FIELD = "description"
        const val DATA_SELECTION_QUERY_FIELD = "data_selection_query"
        const val METADATA_ID_FIELD = "metadata_id"
        const val PAGE_SIZE_FIELD = "page_size"
        const val ROLES_FIELD = "roles"
        const val JOB_SCHEDULE_FIELD = "schedule"
        const val GROUPS_FIELD = "groups"
        const val AGGREGATIONS_FIELD = "aggregations"
        const val SCHEMA_VERSION_FIELD = "schema_version"
        const val MINIMUM_PAGE_SIZE = 1
        const val MAXIMUM_PAGE_SIZE = 10_000
        const val MINIMUM_JOB_INTERVAL = 1

        @JvmStatic
        @JvmOverloads
        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): Transform {
            var schedule: Schedule? = null
            var schemaVersion: Long = IndexUtils.DEFAULT_SCHEMA_VERSION
            var updatedAt: Instant? = null
            var enabledAt: Instant? = null
            var enabled = true
            var description: String? = null
            var sourceIndex: String? = null
            var dataSelectionQuery: QueryBuilder = MatchAllQueryBuilder()
            var targetIndex: String? = null
            var metadataId: String? = null
            val roles = mutableListOf<String>()
            var pageSize: Int? = null
            val groups = mutableListOf<Dimension>()
            var aggregations: AggregatorFactories.Builder? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)

            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    TRANSFORM_ID_FIELD -> {
                        requireNotNull(xcp.text()) { "The transform_id field is null" }
                    }
                    JOB_SCHEDULE_FIELD -> schedule = ScheduleParser.parse(xcp)
                    SCHEMA_VERSION_FIELD -> schemaVersion = xcp.longValue()
                    UPDATED_AT_FIELD -> updatedAt = xcp.instant()
                    ENABLED_AT_FIELD -> enabledAt = xcp.instant()
                    ENABLED_FIELD -> enabled = xcp.booleanValue()
                    DESCRIPTION_FIELD -> description = xcp.text()
                    SOURCE_INDEX_FIELD -> sourceIndex = xcp.text()
                    DATA_SELECTION_QUERY_FIELD -> {
                        // dataSelectionQuery = AbstractQueryBuilder.parseInnerQueryBuilder(xcp)
                        val registry = xcp.xContentRegistry
                        val source = xcp.mapOrdered()
                        val xContentBuilder = XContentFactory.jsonBuilder().map(source)
                        val sourceParser = XContentType.JSON.xContent().createParser(registry, LoggingDeprecationHandler.INSTANCE, BytesReference
                            .bytes(xContentBuilder).streamInput())
                        dataSelectionQuery = AbstractQueryBuilder.parseInnerQueryBuilder(sourceParser)
                    }
                    TARGET_INDEX_FIELD -> targetIndex = xcp.text()
                    METADATA_ID_FIELD -> metadataId = xcp.textOrNull()
                    ROLES_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            roles.add(xcp.text())
                        }
                    }
                    PAGE_SIZE_FIELD -> pageSize = xcp.intValue()
                    GROUPS_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            groups.add(Dimension.parse(xcp))
                        }
                    }
                    AGGREGATIONS_FIELD -> aggregations = AggregatorFactories.parseAggregators(xcp)
                    else -> throw IllegalArgumentException("Invalid field [$fieldName] found in Transforms.")
                }
            }

            if (enabled && enabledAt == null) {
                enabledAt = Instant.now()
            } else if (!enabled) {
                enabledAt = null
            }

            // If the seqNo/primaryTerm are unassigned this job hasn't been created yet so we instantiate the startTime
            if (seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || primaryTerm == SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
                if (schedule is IntervalSchedule) {
                    schedule = IntervalSchedule(Instant.now(), schedule.interval, schedule.unit)
                }
            }

            return Transform(
                id = id,
                seqNo = seqNo,
                primaryTerm = primaryTerm,
                schemaVersion = schemaVersion,
                jobSchedule = requireNotNull(schedule) { "Transform schedule is null" },
                metadataId = metadataId,
                updatedAt = updatedAt ?: Instant.now(),
                enabled = enabled,
                enabledAt = enabledAt,
                description = requireNotNull(description) { "Transform description is null" },
                sourceIndex = requireNotNull(sourceIndex) { "Transform source index is null" },
                dataSelectionQuery = dataSelectionQuery,
                targetIndex = requireNotNull(targetIndex) { "Transform target index is null" },
                roles = roles,
                pageSize = requireNotNull(pageSize) { "Transform page size is null" },
                groups = groups,
                aggregations = requireNotNull(aggregations) { "Transform aggregation is null" }
            )
        }
    }
}

abstract class ScheduleJob(
    open val jobSchedule: Schedule,
    open val schemaVersion: Long,
    open val enabled: Boolean,
    open val enabledAt: Instant?,
    open val description: String
)