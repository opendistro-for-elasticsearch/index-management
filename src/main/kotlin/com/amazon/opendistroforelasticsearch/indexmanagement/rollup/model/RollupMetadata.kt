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

data class RollupMetadata(
    val id: String = NO_ID,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
    val rollupID: String,
    val afterKey: Map<String, Any>? = null,
    val lastUpdatedTime: Instant,
    val nextWindowStartTime: Instant? = null,
    val nextWindowEndTime: Instant? = null,
    val status: Status,
    val failureReason: String? = null
): ToXContentObject, Writeable {

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
    constructor(sin: StreamInput): this(
        id = sin.readString(),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
        rollupID = sin.readString(),
        afterKey = if (sin.readBoolean()) sin.readMap() else null,
        lastUpdatedTime = sin.readInstant(),
        nextWindowStartTime = sin.readOptionalInstant(),
        nextWindowEndTime = sin.readOptionalInstant(),
        status = sin.readEnum(Status::class.java),
        failureReason = sin.readOptionalString()
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(ROLLUP_ID_FIELD, rollupID)
        if (afterKey != null) builder.field(AFTER_KEY_FIELD, afterKey)
        builder
            .optionalTimeField(LAST_UPDATED_FIELD, lastUpdatedTime)
            .optionalTimeField(WINDOW_START_TIME_FIELD, nextWindowStartTime)
            .optionalTimeField(WINDOW_END_TIME_FIELD, nextWindowEndTime)
            .field(STATUS_FIELD, status.type)
        if (failureReason != null) builder.field(FAILURE_REASON, failureReason)
        return builder.endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        out.writeString(rollupID)
        if (afterKey == null) {
            out.writeBoolean(false)
        } else {
            out.writeBoolean(true)
            out.writeMap(afterKey)
        }
        out.writeInstant(lastUpdatedTime)
        out.writeInstant(nextWindowStartTime)
        out.writeInstant(nextWindowEndTime)
        out.writeEnum(status)
        out.writeOptionalString(failureReason)
    }

    companion object {
        const val ROLLUP_METADATA_TYPE = "rollup_metadata"
        const val NO_ID = ""
        const val ROLLUP_ID_FIELD = "rollup_id"
        const val AFTER_KEY_FIELD = "after_key"
        const val LAST_UPDATED_FIELD = "last_updated_time"
        const val WINDOW_START_TIME_FIELD = "window_start_time"
        const val WINDOW_END_TIME_FIELD = "window_end_time"
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
            var windowStartTime: Instant? = null
            var windowEndTime: Instant? = null
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
                    WINDOW_START_TIME_FIELD -> windowStartTime = xcp.instant()
                    WINDOW_END_TIME_FIELD -> windowEndTime = xcp.instant()
                    STATUS_FIELD -> status = Status.valueOf(xcp.text().toUpperCase(Locale.ROOT))
                    FAILURE_REASON -> failureReason = xcp.text()
                }
            }

            // TODO: These should not be null if job is continous but should be if noncontinous
            //nextWindowStartTime = requireNotNull(windowStartTime) { "Window start time must not be null" },
            //nextWindowEndTime = requireNotNull(windowEndTime) { "Window end time must not be null" },

            return RollupMetadata(
                id,
                seqNo,
                primaryTerm,
                rollupID = requireNotNull(rollupID) { "RollupID must not be null" },
                afterKey = afterKey,
                lastUpdatedTime = requireNotNull(lastUpdatedTime) { "Last updated time must not be null" },
                nextWindowStartTime = windowStartTime,
                nextWindowEndTime = windowEndTime,
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
