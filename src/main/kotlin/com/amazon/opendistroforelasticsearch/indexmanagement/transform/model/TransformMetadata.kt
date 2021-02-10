package com.amazon.opendistroforelasticsearch.indexmanagement.transform.model

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.instant
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.optionalTimeField
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.WITH_TYPE
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.index.seqno.SequenceNumbers
import java.io.IOException
import java.time.Instant
import java.util.Locale

data class TransformMetadata(
    val id: String = NO_ID,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
    val transformId: String,
    val afterKey: Map<String, Any>? = null,
    val lastUpdatedAt: Instant,
    val status: Status,
    val failureReason: String? = null,
    val stats: TransformStats
) : ToXContentObject, Writeable {

    enum class Status(val type: String) {
        INIT("init"),
        STARTED("started"),
        STOPPED("stopped"),
        FINISHED("finished"),
        FAILED("failed");

        override fun toString(): String {
            return type
        }
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
        transformId = sin.readString(),
        afterKey = if (sin.readBoolean()) sin.readMap() else null,
        lastUpdatedAt = sin.readInstant(),
        status = sin.readEnum(Status::class.java),
        failureReason = sin.readOptionalString(),
        stats = TransformStats(sin)
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.startObject(TRANSFORM_METADATA_TYPE)

        builder.field(TRANSFORM_ID_FIELD, transformId)
        if (afterKey != null) builder.field(AFTER_KEY_FIELD, afterKey)
        builder.optionalTimeField(LAST_UPDATED_AT_FIELD, lastUpdatedAt)
        builder.field(STATUS_FIELD, status.type)
        builder.field(FAILURE_REASON, failureReason)
        builder.field(STATS_FIELD, stats)

        if (params.paramAsBoolean(WITH_TYPE, true)) builder.endObject()
        return builder.endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        out.writeString(transformId)
        out.writeBoolean(afterKey != null)
        afterKey?.let { out.writeMap(it) }
        out.writeInstant(lastUpdatedAt)
        out.writeEnum(status)
        out.writeOptionalString(failureReason)
        stats.writeTo(out)
    }

    companion object {
        const val NO_ID = ""
        const val TRANSFORM_METADATA_TYPE = "transform_metadata"
        const val TRANSFORM_ID_FIELD = "transform_id"
        const val AFTER_KEY_FIELD = "after_key"
        const val LAST_UPDATED_AT_FIELD = "last_updated_at"
        const val STATUS_FIELD = "status"
        const val STATS_FIELD = "stats"
        const val FAILURE_REASON = "failure_reason"

        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): TransformMetadata {
            var transformId: String? = null
            var afterkey: Map<String, Any>? = null
            var lastUpdatedAt: Instant? = null
            var status: Status? = null
            var failureReason: String? = null
            var stats: TransformStats? = null

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    TRANSFORM_ID_FIELD -> transformId = xcp.text()
                    AFTER_KEY_FIELD -> afterkey = xcp.map()
                    LAST_UPDATED_AT_FIELD -> lastUpdatedAt = xcp.instant()
                    STATUS_FIELD -> status = Status.valueOf(xcp.text().toUpperCase(Locale.ROOT))
                    FAILURE_REASON -> failureReason = xcp.textOrNull()
                    STATS_FIELD -> stats = TransformStats.parse(xcp)
                }
            }

            return TransformMetadata(
                id,
                seqNo,
                primaryTerm,
                transformId = requireNotNull(transformId) { "TransformId must not be null" },
                afterKey = afterkey,
                lastUpdatedAt = requireNotNull(lastUpdatedAt) { "Last updated time must not be null" },
                status = requireNotNull(status) { "Status must not be null" },
                failureReason = failureReason,
                stats = requireNotNull(stats) { "Stats must not be null" }
            )
        }
    }
}

data class TransformStats(
    val pagesProcessed: Long,
    val documentsProcessed: Long,
    val documentsIndexed: Long,
    val indexTimeInMillis: Long,
    val searchTimeInMillis: Long
) : ToXContentObject, Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        pagesProcessed = sin.readLong(),
        documentsProcessed = sin.readLong(),
        documentsIndexed = sin.readLong(),
        indexTimeInMillis = sin.readLong(),
        searchTimeInMillis = sin.readLong()
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(PAGES_PROCESSED_FIELD, pagesProcessed)
            .field(DOCUMENTS_PROCESSED_FIELD, documentsProcessed)
            .field(DOCUMENTS_INDEXED_FIELD, documentsIndexed)
            .field(INDEX_TIME_IN_MILLIS_FIELD, indexTimeInMillis)
            .field(SEARCH_TIME_IN_MILLIS_FIELD, searchTimeInMillis)
            .endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeLong(pagesProcessed)
        out.writeLong(documentsProcessed)
        out.writeLong(documentsIndexed)
        out.writeLong(indexTimeInMillis)
        out.writeLong(searchTimeInMillis)
    }

    companion object {
        private const val PAGES_PROCESSED_FIELD = "pages_processed"
        private const val DOCUMENTS_PROCESSED_FIELD = "documents_processed"
        private const val DOCUMENTS_INDEXED_FIELD = "documents_indexed"
        private const val INDEX_TIME_IN_MILLIS_FIELD = "index_time_in_millis"
        private const val SEARCH_TIME_IN_MILLIS_FIELD = "search_time_in_millis"

        @Suppress("ComplexMethod, LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): TransformStats {
            var pagesProcessed: Long? = null
            var documentsProcessed: Long? = null
            var documentsIndexed: Long? = null
            var indexTimeInMillis: Long? = null
            var searchTimeInMillis: Long? = null

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    PAGES_PROCESSED_FIELD -> pagesProcessed = xcp.longValue()
                    DOCUMENTS_PROCESSED_FIELD -> documentsProcessed = xcp.longValue()
                    DOCUMENTS_INDEXED_FIELD -> documentsIndexed = xcp.longValue()
                    INDEX_TIME_IN_MILLIS_FIELD -> indexTimeInMillis = xcp.longValue()
                    SEARCH_TIME_IN_MILLIS_FIELD -> searchTimeInMillis = xcp.longValue()
                }
            }

            return TransformStats(
                pagesProcessed = requireNotNull(pagesProcessed) { "" },
                documentsProcessed = requireNotNull(documentsProcessed) { "" },
                documentsIndexed = requireNotNull(documentsIndexed) { "" },
                indexTimeInMillis = requireNotNull(indexTimeInMillis) { "" },
                searchTimeInMillis = requireNotNull(searchTimeInMillis) { "" }
            )
        }
    }
}
