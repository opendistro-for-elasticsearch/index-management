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
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.index.seqno.SequenceNumbers
import java.io.IOException
import java.time.Instant
import java.util.Locale

data class ContinuousMetadata(
    val nextWindowStartTime: Instant,
    val nextWindowEndTime: Instant
) : ToXContentObject, Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        nextWindowStartTime = sin.readInstant(),
        nextWindowEndTime = sin.readInstant()
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .timeField(NEXT_WINDOW_START_TIME_FIELD, NEXT_WINDOW_START_TIME_FIELD, nextWindowStartTime.toEpochMilli())
            .timeField(NEXT_WINDOW_END_TIME_FIELD, NEXT_WINDOW_END_TIME_FIELD, nextWindowEndTime.toEpochMilli())
            .endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeInstant(nextWindowStartTime)
        out.writeInstant(nextWindowEndTime)
    }

    companion object {
        private const val NEXT_WINDOW_START_TIME_FIELD = "next_window_start_time"
        private const val NEXT_WINDOW_END_TIME_FIELD = "next_window_end_time"

        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ContinuousMetadata {
            var windowStartTime: Instant? = null
            var windowEndTime: Instant? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    NEXT_WINDOW_START_TIME_FIELD -> windowStartTime = xcp.instant()
                    NEXT_WINDOW_END_TIME_FIELD -> windowEndTime = xcp.instant()
                }
            }

            return ContinuousMetadata(
                nextWindowStartTime = requireNotNull(windowStartTime) { "Next window start time must not be null for a continuous job" },
                nextWindowEndTime = requireNotNull(windowEndTime) { "Next window end time must not be null for a continuous job" }
            )
        }
    }
}

data class RollupMetadata(
    val id: String = NO_ID,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
    val rollupID: String,
    val afterKey: Map<String, Any>? = null,
    val lastUpdatedTime: Instant,
    val continuous: ContinuousMetadata? = null,
    val status: Status,
    val failureReason: String? = null
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
        rollupID = sin.readString(),
        afterKey = if (sin.readBoolean()) sin.readMap() else null,
        lastUpdatedTime = sin.readInstant(),
        continuous = if (sin.readBoolean()) ContinuousMetadata(sin) else null,
        status = sin.readEnum(Status::class.java),
        failureReason = sin.readOptionalString()
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(ROLLUP_ID_FIELD, rollupID)
        if (afterKey != null) builder.field(AFTER_KEY_FIELD, afterKey)
        builder.optionalTimeField(LAST_UPDATED_FIELD, lastUpdatedTime)
        if (continuous != null) builder.field(CONTINUOUS_FIELD, continuous)
        builder.field(STATUS_FIELD, status.type)
        if (failureReason != null) builder.field(FAILURE_REASON, failureReason)
        return builder.endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        out.writeString(rollupID)
        out.writeBoolean(afterKey != null)
        afterKey?.let { out.writeMap(it) }
        out.writeInstant(lastUpdatedTime)
        out.writeBoolean(continuous != null)
        continuous?.writeTo(out)
        out.writeEnum(status)
        out.writeOptionalString(failureReason)
    }

    companion object {
        const val ROLLUP_METADATA_TYPE = "rollup_metadata"
        const val NO_ID = ""
        const val ROLLUP_ID_FIELD = "rollup_id"
        const val AFTER_KEY_FIELD = "after_key"
        const val LAST_UPDATED_FIELD = "last_updated_time"
        const val CONTINUOUS_FIELD = "continuous"
        const val STATUS_FIELD = "status"
        const val FAILURE_REASON = "failure_reason"
        const val STATS_FIELD = "stats" // TODO

        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): RollupMetadata {
            var rollupID: String? = null
            var afterKey: Map<String, Any>? = null
            var lastUpdatedTime: Instant? = null
            var continuous: ContinuousMetadata? = null
            var status: Status? = null
            var failureReason: String? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ROLLUP_ID_FIELD -> rollupID = xcp.text()
                    AFTER_KEY_FIELD -> afterKey = xcp.map()
                    LAST_UPDATED_FIELD -> lastUpdatedTime = xcp.instant()
                    CONTINUOUS_FIELD -> continuous = ContinuousMetadata.parse(xcp)
                    STATUS_FIELD -> status = Status.valueOf(xcp.text().toUpperCase(Locale.ROOT))
                    FAILURE_REASON -> failureReason = xcp.text()
                }
            }

            return RollupMetadata(
                id,
                seqNo,
                primaryTerm,
                rollupID = requireNotNull(rollupID) { "RollupID must not be null" },
                afterKey = afterKey,
                lastUpdatedTime = requireNotNull(lastUpdatedTime) { "Last updated time must not be null" },
                continuous = continuous,
                status = requireNotNull(status) { "Status must not be null" },
                failureReason = failureReason
            )
        }

        // TODO generic helper with generic type for parsing with type
        @JvmStatic
        @JvmOverloads
        @Throws(IOException::class)
        fun parseWithType(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): RollupMetadata {
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp::getTokenLocation)
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            val metadata = parse(xcp, id, seqNo, primaryTerm)
            ensureExpectedToken(Token.END_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            return metadata
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput) = RollupMetadata(sin)
    }
}
