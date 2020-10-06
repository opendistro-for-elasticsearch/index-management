/*
 *
 *  * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License").
 *  * You may not use this file except in compliance with the License.
 *  * A copy of the License is located at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * or in the "license" file accompanying this file. This file is distributed
 *  * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  * express or implied. See the License for the specific language governing
 *  * permissions and limitations under the License.
 *
 */

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
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder

fun Rollup.getRollupSearchRequest(metadata: RollupMetadata): SearchRequest {
    val query = if (metadata.nextWindowEndTime != null && metadata.nextWindowStartTime != null) { // TODO: Clean this up, what about adding a continous: { start, end } to metadata that is nullable?
        RangeQueryBuilder(this.dimensions.find { dim -> dim is DateHistogram }!!.sourceField)
            .from(metadata.nextWindowStartTime, true)
            .to(metadata.nextWindowEndTime, false)
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
        .allowPartialSearchResults(false) // TODO check the behavior one this
}

fun Rollup.getCompositeAggregationBuilder(afterKey: Map<String, Any>?): CompositeAggregationBuilder {
    val sources = mutableListOf<CompositeValuesSourceBuilder<*>>()
    this.dimensions.forEach { dimension ->
        when (dimension) {
            is DateHistogram -> {
                DateHistogramValuesSourceBuilder(dimension.targetField + ".date_histogram").apply {
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
                TermsValuesSourceBuilder(dimension.targetField + ".terms").apply {
                    this.field(dimension.sourceField)
                }.also { sources.add(it) }
            }
            is Histogram -> {
                HistogramValuesSourceBuilder(dimension.targetField + ".histogram").apply {
                    this.field(dimension.sourceField)
                    this.interval(dimension.interval)
                }.also { sources.add(it) }
            }
        }
    }
    return CompositeAggregationBuilder(this.id, sources).size(this.pageSize).also { compositeAgg ->
        afterKey?.let { compositeAgg.aggregateAfter(it) }
        this.metrics.forEach { metric ->
            metric.metrics.forEach { agg ->
                compositeAgg.subAggregation(
                    when (agg) {
                        is Average -> AvgAggregationBuilder(metric.aggregationName(agg)).field(metric.sourceField)
                        is Sum -> SumAggregationBuilder(metric.aggregationName(agg)).field(metric.sourceField)
                        is Max -> MaxAggregationBuilder(metric.aggregationName(agg)).field(metric.sourceField)
                        is Min -> MinAggregationBuilder(metric.aggregationName(agg)).field(metric.sourceField)
                        is ValueCount -> ValueCountAggregationBuilder(metric.aggregationName(agg)).field(metric.sourceField)
                        else -> throw IllegalArgumentException("Found unsupported metric aggregation ${agg.type.type}") // TODO: This needs to cancel the rollup
                    }
                )
            }
        }
    }
}
