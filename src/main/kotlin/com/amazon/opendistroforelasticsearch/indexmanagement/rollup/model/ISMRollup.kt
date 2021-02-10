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

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.DateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Dimension
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Histogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Terms
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import com.amazon.opendistroforelasticsearch.notification.repackage.org.apache.commons.codec.digest.DigestUtils
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import org.elasticsearch.index.seqno.SequenceNumbers
import java.io.IOException
import java.time.Instant
import java.time.temporal.ChronoUnit

data class ISMRollup(
    val description: String,
    val targetIndex: String,
    val pageSize: Int,
    val dimensions: List<Dimension>,
    val metrics: List<RollupMetrics>
) : ToXContentObject, Writeable {

    // TODO: This can be moved to a common place, since this is shared between Rollup and ISMRollup
    init {
        require(pageSize in Rollup.MINIMUM_PAGE_SIZE..Rollup.MAXIMUM_PAGE_SIZE) { "Page size must be between ${Rollup.MINIMUM_PAGE_SIZE} " +
                "and ${Rollup.MAXIMUM_PAGE_SIZE}" }
        require(description.isNotEmpty()) { "Description cannot be empty" }
        require(targetIndex.isNotEmpty()) { "Target Index cannot be empty" }
        require(dimensions.filter { it.type == Dimension.Type.DATE_HISTOGRAM }.size == 1) {
            "Must specify precisely one date histogram dimension"
        }
        require(dimensions.first().type == Dimension.Type.DATE_HISTOGRAM) { "The first dimension must be a date histogram" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
                .field(Rollup.DESCRIPTION_FIELD, description)
                .field(Rollup.TARGET_INDEX_FIELD, targetIndex)
                .field(Rollup.PAGE_SIZE_FIELD, pageSize)
                .field(Rollup.DIMENSIONS_FIELD, dimensions)
                .field(Rollup.METRICS_FIELD, metrics)
        builder.endObject()
        return builder
    }

    fun toRollup(sourceIndex: String, roles: List<String> = listOf()): Rollup {
        val id = sourceIndex + toString()
        val currentTime = Instant.now()
        return Rollup(
            id = DigestUtils.sha1Hex(id),
            seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            enabled = true,
            schemaVersion = IndexUtils.DEFAULT_SCHEMA_VERSION,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = currentTime,
            jobEnabledTime = currentTime,
            description = this.description,
            sourceIndex = sourceIndex,
            targetIndex = this.targetIndex,
            metadataID = null,
            pageSize = pageSize,
            delay = null,
            continuous = false,
            dimensions = dimensions,
            metrics = metrics,
            user = null
        )
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
            description = sin.readString(),
            targetIndex = sin.readString(),
            pageSize = sin.readInt(),
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

    override fun toString(): String {
        val sb = StringBuffer()
        sb.append(targetIndex)
        sb.append(pageSize)
        dimensions.forEach {
            sb.append(it.type)
            sb.append(it.sourceField)
        }
        metrics.forEach {
            sb.append(it.sourceField)
            it.metrics.forEach {
                metric -> sb.append(metric.type)
            }
        }

        return sb.toString()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(description)
        out.writeString(targetIndex)
        out.writeInt(pageSize)
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
        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(
            xcp: XContentParser
        ): ISMRollup {
            var description = ""
            var targetIndex = ""
            var pageSize = 0
            val dimensions = mutableListOf<Dimension>()
            val metrics = mutableListOf<RollupMetrics>()

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)

            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    Rollup.DESCRIPTION_FIELD -> description = xcp.text()
                    Rollup.TARGET_INDEX_FIELD -> targetIndex = xcp.text()
                    Rollup.PAGE_SIZE_FIELD -> pageSize = xcp.intValue()
                    Rollup.DIMENSIONS_FIELD -> {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            dimensions.add(Dimension.parse(xcp))
                        }
                    }
                    Rollup.METRICS_FIELD -> {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            metrics.add(RollupMetrics.parse(xcp))
                        }
                    }
                    else -> throw IllegalArgumentException("Invalid field, [$fieldName] not supported in ISM Rollup.")
                }
            }

            return ISMRollup(
                    description = description,
                    pageSize = pageSize,
                    dimensions = dimensions,
                    metrics = metrics,
                    targetIndex = targetIndex
            )
        }
    }
}
