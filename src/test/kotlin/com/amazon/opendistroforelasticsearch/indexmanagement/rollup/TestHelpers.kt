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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.string
import com.amazon.opendistroforelasticsearch.indexmanagement.randomInstant
import com.amazon.opendistroforelasticsearch.indexmanagement.randomSchedule
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.ContinuousMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.ExplainRollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.ISMRollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetrics
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupStats
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.DateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Dimension
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Histogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Terms
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Average
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Max
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Metric
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Min
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Sum
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.ValueCount
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.index.query.TermQueryBuilder
import org.elasticsearch.test.rest.ESRestTestCase
import java.util.Locale

fun randomInterval(): String = if (ESRestTestCase.randomBoolean()) randomFixedInterval() else randomCalendarInterval()

fun randomCalendarInterval(): String = "1d"

fun randomFixedInterval(): String = "30m"

fun randomFixedDateHistogram(): DateHistogram = ESRestTestCase.randomAlphaOfLength(10).let {
    DateHistogram(sourceField = it, targetField = it, fixedInterval = randomFixedInterval(), calendarInterval = null, timezone = ESRestTestCase.randomZone())
}
fun randomCalendarDateHistogram(): DateHistogram = ESRestTestCase.randomAlphaOfLength(10).let {
    DateHistogram(sourceField = it, targetField = it, fixedInterval = null, calendarInterval = randomCalendarInterval(), timezone = ESRestTestCase.randomZone())
}

fun randomDateHistogram(): DateHistogram = if (ESRestTestCase.randomBoolean()) randomFixedDateHistogram() else randomCalendarDateHistogram()

fun randomHistogram(): Histogram = ESRestTestCase.randomAlphaOfLength(10).let {
    Histogram(
        sourceField = it,
        targetField = it,
        interval = ESRestTestCase.randomDoubleBetween(0.0, Double.MAX_VALUE, false) // start, end, lowerInclusive
    )
}

fun randomTerms(): Terms = ESRestTestCase.randomAlphaOfLength(10).let { Terms(sourceField = it, targetField = it) }

fun randomAverage(): Average = Average()

fun randomMax(): Max = Max()

fun randomMin(): Min = Min()

fun randomSum(): Sum = Sum()

fun randomValueCount(): ValueCount = ValueCount()

val metrics = listOf(randomAverage(), randomMax(), randomMin(), randomSum(), randomValueCount())

fun randomMetric(): Metric =
    ESRestTestCase.randomSubsetOf(1, metrics).first()

fun randomMetrics(): List<Metric> =
    ESRestTestCase.randomList(1, metrics.size, ::randomMetric).distinctBy { it.type }

fun randomRollupMetrics(): RollupMetrics = ESRestTestCase.randomAlphaOfLength(10).let {
    RollupMetrics(sourceField = it, targetField = it, metrics = randomMetrics())
}

fun randomRollupDimensions(): List<Dimension> {
    val dimensions = mutableListOf<Dimension>(randomDateHistogram())
    for (i in 0..ESRestTestCase.randomInt(10)) {
        dimensions.add(if (ESRestTestCase.randomBoolean()) randomTerms() else randomHistogram())
    }
    return dimensions.toList()
}

fun randomRollup(): Rollup {
    val enabled = ESRestTestCase.randomBoolean()
    return Rollup(
        id = ESRestTestCase.randomAlphaOfLength(10),
        seqNo = ESRestTestCase.randomNonNegativeLong(),
        primaryTerm = ESRestTestCase.randomNonNegativeLong(),
        enabled = enabled,
        schemaVersion = ESRestTestCase.randomLongBetween(1, 1000),
        jobSchedule = randomSchedule(),
        jobLastUpdatedTime = randomInstant(),
        jobEnabledTime = if (enabled) randomInstant() else null,
        description = ESRestTestCase.randomAlphaOfLength(10),
        sourceIndex = ESRestTestCase.randomAlphaOfLength(10).toLowerCase(Locale.ROOT),
        targetIndex = ESRestTestCase.randomAlphaOfLength(10).toLowerCase(Locale.ROOT),
        metadataID = if (ESRestTestCase.randomBoolean()) null else ESRestTestCase.randomAlphaOfLength(10),
        roles = ESRestTestCase.randomList(10) { ESRestTestCase.randomAlphaOfLength(10) },
        pageSize = ESRestTestCase.randomIntBetween(1, 10000),
        delay = ESRestTestCase.randomNonNegativeLong(),
        continuous = ESRestTestCase.randomBoolean(),
        dimensions = randomRollupDimensions(),
        metrics = ESRestTestCase.randomList(20, ::randomRollupMetrics).distinctBy { it.targetField }
    )
}

fun randomRollupStats(): RollupStats {
    return RollupStats(
        pagesProcessed = ESRestTestCase.randomNonNegativeLong(),
        documentsProcessed = ESRestTestCase.randomNonNegativeLong(),
        rollupsIndexed = ESRestTestCase.randomNonNegativeLong(),
        indexTimeInMillis = ESRestTestCase.randomNonNegativeLong(),
        searchTimeInMillis = ESRestTestCase.randomNonNegativeLong()
    )
}

fun randomRollupMetadataStatus(): RollupMetadata.Status {
    return ESRestTestCase.randomFrom(RollupMetadata.Status.values().toList())
}

fun randomContinuousMetadata(): ContinuousMetadata {
    val one = randomInstant()
    val two = randomInstant()
    return ContinuousMetadata(
        nextWindowEndTime = if (one.isAfter(two)) one else two,
        nextWindowStartTime = if (one.isAfter(two)) two else one
    )
}

fun randomAfterKey(): Map<String, Any>? {
    return if (ESRestTestCase.randomBoolean()) {
        null
    } else {
        mapOf("test" to 17)
    }
}

fun randomRollupMetadata(): RollupMetadata {
    val status = randomRollupMetadataStatus()
    return RollupMetadata(
        id = ESRestTestCase.randomAlphaOfLength(10),
        seqNo = ESRestTestCase.randomNonNegativeLong(),
        primaryTerm = ESRestTestCase.randomNonNegativeLong(),
        rollupID = ESRestTestCase.randomAlphaOfLength(10),
        afterKey = randomAfterKey(),
        lastUpdatedTime = randomInstant(),
        continuous = randomContinuousMetadata(),
        status = status,
        failureReason = if (status == RollupMetadata.Status.FAILED) ESRestTestCase.randomAlphaOfLength(10) else null,
        stats = randomRollupStats()
    )
}

fun randomExplainRollup(): ExplainRollup {
    val metadata = randomRollupMetadata()
    return ExplainRollup(metadataID = metadata.id, metadata = metadata)
}

fun randomISMRollup(): ISMRollup {
    return ISMRollup(
        description = ESRestTestCase.randomAlphaOfLength(10),
        targetIndex = ESRestTestCase.randomAlphaOfLength(10).toLowerCase(Locale.ROOT),
        pageSize = ESRestTestCase.randomIntBetween(1, 10000),
        dimensions = randomRollupDimensions(),
        metrics = ESRestTestCase.randomList(20, ::randomRollupMetrics).distinctBy { it.targetField }
    )
}

fun randomDimension(): Dimension {
    val dimensions = listOf(randomTerms(), randomHistogram(), randomDateHistogram())
    return ESRestTestCase.randomSubsetOf(1, dimensions).first()
}

fun randomTermQuery(): TermQueryBuilder { return TermQueryBuilder(ESRestTestCase.randomAlphaOfLength(5), ESRestTestCase.randomAlphaOfLength(5)) }

fun DateHistogram.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun Histogram.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun Terms.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun Average.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun Max.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun Min.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun Sum.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun ValueCount.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun RollupMetrics.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun Rollup.toJsonString(params: ToXContent.Params = ToXContent.EMPTY_PARAMS): String = this.toXContent(XContentFactory.jsonBuilder(), params).string()

fun RollupMetadata.toJsonString(params: ToXContent.Params = ToXContent.EMPTY_PARAMS): String = this.toXContent(XContentFactory.jsonBuilder(), params).string()

fun ISMRollup.toJsonString(params: ToXContent.Params = ToXContent.EMPTY_PARAMS): String = this.toXContent(XContentFactory.jsonBuilder(), params).string()