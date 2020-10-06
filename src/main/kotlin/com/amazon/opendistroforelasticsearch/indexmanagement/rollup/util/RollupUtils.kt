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

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupMapperService.Companion.ROLLUPS
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetrics
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.DateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Dimension
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Histogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Terms
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Average
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Max
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Min
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Sum
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.ValueCount
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils.Companion._META
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.MatchAllQueryBuilder
import org.elasticsearch.index.query.RangeQueryBuilder
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.aggregations.AggregatorFactories
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder
import org.elasticsearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.WeightedAvgAggregationBuilder
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig
import org.elasticsearch.search.builder.SearchSourceBuilder

fun Rollup.getRollupSearchRequest(metadata: RollupMetadata): SearchRequest {
    val query = if (metadata.nextWindowEndTime != null && metadata.nextWindowStartTime != null) { // TODO: Clean this up, what about adding a continuous: { start, end } to metadata that is nullable?
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

fun Rollup.findMatchingDimension(field: String, type: Dimension.Type): Dimension? =
    this.dimensions.find { dimension -> dimension.sourceField == field && dimension.type == type }

inline fun <reified T> Rollup.findMatchingMetric(field: String): RollupMetrics? =
    this.metrics.find { metric -> metric.sourceField == field && metric.metrics.any { m -> m is T } }

fun IndexMetadata.getRollupJobs(): List<Rollup>? {
    val rollupJobs = mutableListOf<Rollup>()
    val source = this.mapping()?.source() ?: return null
    val xcp = XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, source.compressedReference(), XContentType.JSON)
    ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation) // start of block
    ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp::getTokenLocation) // _doc
    ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation) // start of _doc block
    while (xcp.nextToken() != Token.END_OBJECT) {
        val fieldName = xcp.currentName()
        xcp.nextToken()

        when (fieldName) {
            _META -> {
                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val metaField = xcp.currentName()
                    xcp.nextToken()

                    when (metaField) {
                        ROLLUPS -> {
                            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
                            while (xcp.nextToken() != Token.END_OBJECT) {
                                val rollupID = xcp.currentName()
                                xcp.nextToken()
                                rollupJobs.add(Rollup.parse(xcp, rollupID))
                            }
                        }
                        else -> xcp.skipChildren()
                    }
                }
            }
            else -> xcp.skipChildren()
        }
    }
    return rollupJobs
}

// TODO: If we have to set this manually for each aggregation builder then it means we could miss new ones settings in the future
fun Rollup.rewriteAggregationBuilder(aggregationBuilder: AggregationBuilder): AggregationBuilder {
    val aggFactory = AggregatorFactories.builder().also { factories ->
        aggregationBuilder.subAggregations.forEach {
            factories.addAggregator(this.rewriteAggregationBuilder(it))
        }
    }

    return when (aggregationBuilder) {
        is TermsAggregationBuilder -> {
            val dim = this.findMatchingDimension(aggregationBuilder.field(), Dimension.Type.TERMS) as Terms
            dim.getRewrittenAggregation(aggregationBuilder, aggFactory)
        }
        is DateHistogramAggregationBuilder -> {
            val dim = this.findMatchingDimension(aggregationBuilder.field(), Dimension.Type.DATE_HISTOGRAM) as DateHistogram
            dim.getRewrittenAggregation(aggregationBuilder, aggFactory)
        }
        is HistogramAggregationBuilder -> {
            val dim = this.findMatchingDimension(aggregationBuilder.field(), Dimension.Type.HISTOGRAM) as Histogram
            dim.getRewrittenAggregation(aggregationBuilder, aggFactory)
        }
        is SumAggregationBuilder -> {
            val metric = this.findMatchingMetric<Sum>(aggregationBuilder.field())!! // TODO: !! here and below
            SumAggregationBuilder(aggregationBuilder.name)
                .field(metric.targetField + ".sum") // TODO: hardcoded here and below
        }
        is AvgAggregationBuilder -> {
            val metric = this.findMatchingMetric<Average>(aggregationBuilder.field())!!
            WeightedAvgAggregationBuilder(aggregationBuilder.name)
                .value(MultiValuesSourceFieldConfig.Builder().setFieldName(metric.targetField + ".avg").build())
                .weight(MultiValuesSourceFieldConfig.Builder().setFieldName("rollup.doc_count").build()) // TODO
                // TODO: .setMetadata()?
                // TODO: .subAggregations()?
                // TODO: .format()? and anything else?
        }
        is MaxAggregationBuilder -> {
            val metric = this.findMatchingMetric<Max>(aggregationBuilder.field())!!
            MaxAggregationBuilder(aggregationBuilder.name)
                .field(metric.targetField + ".max")
        }
        is MinAggregationBuilder -> {
            val metric = this.findMatchingMetric<Min>(aggregationBuilder.field())!!
            MinAggregationBuilder(aggregationBuilder.name)
                .field(metric.targetField + ".min")
        }
        is ValueCountAggregationBuilder -> {
            val metric = this.findMatchingMetric<ValueCount>(aggregationBuilder.field())!!
            ValueCountAggregationBuilder(aggregationBuilder.name)
                .field(metric.targetField + ".value_count")
        }
        // TODO: this won't really throw an exception and stop the query.. just silently logs, we need to rewrite the parsed query to a failed with specific message
        else -> throw UnsupportedOperationException("The ${aggregationBuilder.type} aggregation is not currently supported in rollups")
    }
}

// TODO: Not a fan of this.. but I can't find a way to overwrite the aggregations on the shallow copy or original
//  so we need to instantiate a new one so we can add the rewritten aggregation builders
fun SearchSourceBuilder.rewriteSearchSourceBuilder(job: Rollup): SearchSourceBuilder {
    val ssb = SearchSourceBuilder()
    this.aggregations()?.aggregatorFactories?.forEach { ssb.aggregation(job.rewriteAggregationBuilder(it)) }
    if (this.explain() != null) ssb.explain(this.explain())
    if (this.ext() != null) ssb.ext(this.ext())
    ssb.fetchSource(this.fetchSource())
    this.docValueFields()?.forEach { ssb.docValueField(it.field, it.format) }
    ssb.storedFields(this.storedFields())
    if (this.from() >= 0) ssb.from(this.from())
    ssb.highlighter(this.highlighter())
    this.indexBoosts()?.forEach { ssb.indexBoost(it.index, it.boost) }
    if (this.minScore() != null) ssb.minScore(this.minScore())
    if (this.postFilter() != null) ssb.postFilter(this.postFilter())
    ssb.profile(this.profile())
    if (this.query() != null) ssb.query(this.query())
    this.rescores()?.forEach { ssb.addRescorer(it) }
    this.scriptFields()?.forEach { ssb.scriptField(it.fieldName(), it.script(), it.ignoreFailure()) }
    if (this.searchAfter() != null) ssb.searchAfter(this.searchAfter())
    if (this.slice() != null) ssb.slice(this.slice())
    if (this.size() >= 0) ssb.size(this.size())
    this.sorts()?.forEach { ssb.sort(it) }
    if (this.stats() != null) ssb.stats(this.stats())
    if (this.suggest() != null) ssb.suggest(this.suggest())
    if (this.terminateAfter() >= 0) ssb.terminateAfter(this.terminateAfter())
    if (this.timeout() != null) ssb.timeout(this.timeout())
    ssb.trackScores(this.trackScores())
    this.trackTotalHitsUpTo()?.let { ssb.trackTotalHitsUpTo(it) }
    if (this.version() != null) ssb.version(this.version())
    if (this.seqNoAndPrimaryTerm() != null) ssb.seqNoAndPrimaryTerm(this.seqNoAndPrimaryTerm())
    if (this.collapse() != null) ssb.collapse(this.collapse())
    return ssb
}