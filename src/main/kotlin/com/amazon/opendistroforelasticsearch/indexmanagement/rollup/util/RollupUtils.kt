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

@file:Suppress("TooManyFunctions")

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.util

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.DateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Histogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Terms
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Average
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Max
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Min
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Sum
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.ValueCount
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.index.query.MatchAllQueryBuilder
import org.elasticsearch.index.query.RangeQueryBuilder
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder
import org.elasticsearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder

fun Rollup.getRollupSearchRequest(metadata: RollupMetadata): SearchRequest {
    val query = if (metadata.continuous != null) {
        RangeQueryBuilder(this.getDateHistogram().sourceField)
            .from(metadata.continuous.nextWindowStartTime, true)
            .to(metadata.continuous.nextWindowEndTime, false)
    } else {
        MatchAllQueryBuilder()
    }
    val searchSourceBuilder = SearchSourceBuilder()
        .trackTotalHits(false)
        .size(0)
        .aggregation(this.getCompositeAggregationBuilder(metadata.afterKey))
        .query(query)
    return SearchRequest(this.sourceIndex)
        .source(searchSourceBuilder)
        .allowPartialSearchResults(false)
}

@Suppress("ComplexMethod", "NestedBlockDepth")
fun Rollup.getCompositeAggregationBuilder(afterKey: Map<String, Any>?): CompositeAggregationBuilder {
    val sources = mutableListOf<CompositeValuesSourceBuilder<*>>()
    this.dimensions.forEach { dimension ->
        when (dimension) {
            is DateHistogram -> {
                DateHistogramValuesSourceBuilder(dimension.targetField + ".date_histogram")
                    .missingBucket(true) // TODO: Should this always be true or be user-defined?
                    .apply {
                        this.field(dimension.sourceField)
                        this.timeZone(dimension.timezone)
                        dimension.calendarInterval?.let { it ->
                            this.calendarInterval(DateHistogramInterval(it))
                        }
                        dimension.fixedInterval?.let { it ->
                            this.fixedInterval(DateHistogramInterval(it))
                        }
                    }.also { sources.add(it) }
            }
            is Terms -> {
                TermsValuesSourceBuilder(dimension.targetField + ".terms")
                    .missingBucket(true)
                    .apply {
                        this.field(dimension.sourceField)
                    }.also { sources.add(it) }
            }
            is Histogram -> {
                HistogramValuesSourceBuilder(dimension.targetField + ".histogram")
                    .missingBucket(true)
                    .apply {
                        this.field(dimension.sourceField)
                        this.interval(dimension.interval)
                    }.also { sources.add(it) }
            }
        }
    }
    return CompositeAggregationBuilder(this.id, sources).size(this.pageSize).also { compositeAgg ->
        afterKey?.let { compositeAgg.aggregateAfter(it) }
        this.metrics.forEach { metric ->
            val subAggs = metric.metrics.flatMap { agg ->
                when (agg) {
                    is Average -> {
                        listOf(
                            SumAggregationBuilder(metric.targetFieldWithType(agg) + ".sum").field(metric.sourceField),
                            ValueCountAggregationBuilder(metric.targetFieldWithType(agg) + ".value_count").field(metric.sourceField)
                        )
                    }
                    is Sum -> listOf(SumAggregationBuilder(metric.targetFieldWithType(agg)).field(metric.sourceField))
                    is Max -> listOf(MaxAggregationBuilder(metric.targetFieldWithType(agg)).field(metric.sourceField))
                    is Min -> listOf(MinAggregationBuilder(metric.targetFieldWithType(agg)).field(metric.sourceField))
                    is ValueCount -> listOf(ValueCountAggregationBuilder(metric.targetFieldWithType(agg)).field(metric.sourceField))
                    // This shouldn't be possible as rollup will fail to initialize with an unsupported metric
                    else -> throw IllegalArgumentException("Found unsupported metric aggregation ${agg.type.type}")
                }
            }
            subAggs.forEach { compositeAgg.subAggregation(it) }
        }
    }
}

// There can only be one date histogram in Rollup and it should always be in the first position of dimensions
// This is validated in the rollup init itself, but need to redo it here to correctly return date histogram
fun Rollup.getDateHistogram(): DateHistogram {
    val dimension = this.dimensions.first()
    require(dimension is DateHistogram) { "The first dimension in rollup must be a date histogram" }
    return dimension
}

fun Rollup.getInitialDocValues(docCount: Long): MutableMap<String, Any?> =
    mutableMapOf(
        Rollup.ROLLUP_DOC_ID_FIELD to this.id,
        Rollup.ROLLUP_DOC_COUNT_FIELD to docCount,
        Rollup.ROLLUP_DOC_SCHEMA_VERSION_FIELD to this.schemaVersion
    )
