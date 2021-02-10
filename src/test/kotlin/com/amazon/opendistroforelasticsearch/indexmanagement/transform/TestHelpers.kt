package com.amazon.opendistroforelasticsearch.indexmanagement.transform

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.string
import com.amazon.opendistroforelasticsearch.indexmanagement.randomInstant
import com.amazon.opendistroforelasticsearch.indexmanagement.randomSchedule
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Dimension
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomAfterKey
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomDimension
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.TransformMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.TransformStats
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.AggregatorFactories
import org.elasticsearch.test.rest.ESRestTestCase

fun randomGroups(): List<Dimension> {
    val dimensions = mutableListOf<Dimension>()
    for (i in 0..ESRestTestCase.randomIntBetween(1, 10)) {
        dimensions.add(randomDimension())
    }
    return dimensions
}

fun sumAggregation(): AggregationBuilder = AggregationBuilders.sum(ESRestTestCase.randomAlphaOfLength(10)).field(ESRestTestCase.randomAlphaOfLength(10))
fun maxAggregation(): AggregationBuilder = AggregationBuilders.max(ESRestTestCase.randomAlphaOfLength(10)).field(ESRestTestCase.randomAlphaOfLength(10))
fun minAggregation(): AggregationBuilder = AggregationBuilders.min(ESRestTestCase.randomAlphaOfLength(10)).field(ESRestTestCase.randomAlphaOfLength(10))
fun valueCountAggregation(): AggregationBuilder = AggregationBuilders.count(ESRestTestCase.randomAlphaOfLength(10)).field(ESRestTestCase.randomAlphaOfLength(10))
fun avgAggregation(): AggregationBuilder = AggregationBuilders.avg(ESRestTestCase.randomAlphaOfLength(10)).field(ESRestTestCase.randomAlphaOfLength(10))

fun randomAggregationBuilder(): AggregationBuilder {
    val aggregations = listOf(sumAggregation(), maxAggregation(), minAggregation(), valueCountAggregation(), avgAggregation())
    return ESRestTestCase.randomSubsetOf(1, aggregations).first()
}

fun randomAggregationFactories(): AggregatorFactories.Builder {
    val factories = AggregatorFactories.builder()
    for (i in 1..ESRestTestCase.randomIntBetween(1, 10)) {
        factories.addAggregator(randomAggregationBuilder())
    }
    return factories
}

fun randomTransform(): Transform {
    val enabled = ESRestTestCase.randomBoolean()
    return Transform(
        id = ESRestTestCase.randomAlphaOfLength(10),
        seqNo = ESRestTestCase.randomNonNegativeLong(),
        primaryTerm = ESRestTestCase.randomNonNegativeLong(),
        schemaVersion = ESRestTestCase.randomLongBetween(1, 1000),
        jobSchedule = randomSchedule(),
        metadataId = if (ESRestTestCase.randomBoolean()) null else ESRestTestCase.randomAlphaOfLength(10),
        updatedAt = randomInstant(),
        enabled = enabled,
        enabledAt = if (enabled) randomInstant() else null,
        description = ESRestTestCase.randomAlphaOfLength(10),
        sourceIndex = ESRestTestCase.randomAlphaOfLength(10),
        targetIndex = ESRestTestCase.randomAlphaOfLength(10),
        roles = ESRestTestCase.randomList(10) { ESRestTestCase.randomAlphaOfLength(10) },
        pageSize = ESRestTestCase.randomIntBetween(1, 10000),
        groups = randomGroups(),
        aggregations = randomAggregationFactories()
    )
}

fun randomTransformMetadata(): TransformMetadata {
    val status = randomTransformMetadataStatus()
    return TransformMetadata(
        id = ESRestTestCase.randomAlphaOfLength(10),
        seqNo = ESRestTestCase.randomNonNegativeLong(),
        primaryTerm = ESRestTestCase.randomNonNegativeLong(),
        transformId = ESRestTestCase.randomAlphaOfLength(10),
        afterKey = randomAfterKey(),
        lastUpdatedAt = randomInstant(),
        status = status,
        failureReason = if (status == TransformMetadata.Status.FAILED) ESRestTestCase.randomAlphaOfLength(10) else null,
        stats = randomTransformStats()
    )
}

fun randomTransformStats(): TransformStats {
    return TransformStats(
        pagesProcessed = ESRestTestCase.randomNonNegativeLong(),
        documentsProcessed = ESRestTestCase.randomNonNegativeLong(),
        documentsIndexed = ESRestTestCase.randomNonNegativeLong(),
        indexTimeInMillis = ESRestTestCase.randomNonNegativeLong(),
        searchTimeInMillis = ESRestTestCase.randomNonNegativeLong()
    )
}

fun randomTransformMetadataStatus(): TransformMetadata.Status {
    return ESRestTestCase.randomFrom(TransformMetadata.Status.values().toList())
}

fun Transform.toJsonString(params: ToXContent.Params = ToXContent.EMPTY_PARAMS): String = this.toXContent(XContentFactory.jsonBuilder(), params).string()

fun TransformMetadata.toJsonString(params: ToXContent.Params = ToXContent.EMPTY_PARAMS): String = this.toXContent(XContentFactory.jsonBuilder(), params)
    .string()
