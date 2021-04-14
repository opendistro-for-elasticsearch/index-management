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

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupMapperService
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupFieldMapping

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.common.model.dimension.DateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.common.model.dimension.Dimension
import com.amazon.opendistroforelasticsearch.indexmanagement.common.model.dimension.Histogram
import com.amazon.opendistroforelasticsearch.indexmanagement.common.model.dimension.Terms
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Average
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Max
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Min
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Sum
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.ValueCount
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.BoostingQueryBuilder
import org.elasticsearch.index.query.ConstantScoreQueryBuilder
import org.elasticsearch.index.query.DisMaxQueryBuilder
import org.elasticsearch.index.query.MatchAllQueryBuilder
import org.elasticsearch.index.query.MatchPhraseQueryBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.RangeQueryBuilder
import org.elasticsearch.index.query.TermQueryBuilder
import org.elasticsearch.index.query.TermsQueryBuilder
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptType
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.aggregations.AggregatorFactories
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder

const val DATE_FIELD_EPOCH_MILLIS_FORMAT = "epoch_millis"

fun Rollup.getRollupSearchRequest(metadata: RollupMetadata): SearchRequest {
    val query = if (metadata.continuous != null) {
        RangeQueryBuilder(this.getDateHistogram().sourceField)
            .from(metadata.continuous.nextWindowStartTime.toEpochMilli(), true)
            .to(metadata.continuous.nextWindowEndTime.toEpochMilli(), false)
            .format(DATE_FIELD_EPOCH_MILLIS_FORMAT)
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
    this.dimensions.forEach { dimension -> sources.add(dimension.toSourceBuilder(appendType = true)) }
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

fun Rollup.findMatchingDimension(field: String, type: Dimension.Type): Dimension? =
        this.dimensions.find { dimension -> dimension.sourceField == field && dimension.type == type }

// This method is only to be used after its confirmed the search/aggs is valid and these exist
@Suppress("NestedBlockDepth")
inline fun <reified T> Rollup.findMatchingMetricField(field: String): String {
    for (rollupMetrics in this.metrics) {
        if (rollupMetrics.sourceField == field) {
            for (metric in rollupMetrics.metrics) {
                if (metric is T) {
                    return rollupMetrics.targetFieldWithType(metric)
                }
            }
        }
    }
    throw IllegalStateException("Did not find matching rollup metric")
}

@Suppress("NestedBlockDepth", "ComplexMethod")
fun IndexMetadata.getRollupJobs(): List<Rollup>? {
    val rollupJobs = mutableListOf<Rollup>()
    val source = this.mapping()?.source() ?: return null
    val xcp = XContentHelper
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, source.compressedReference(), XContentType.JSON)
    ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp) // start of block
    ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp) // _doc
    ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp) // start of _doc block
    while (xcp.nextToken() != Token.END_OBJECT) {
        val fieldName = xcp.currentName()
        xcp.nextToken()

        when (fieldName) {
            IndexUtils._META -> {
                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val metaField = xcp.currentName()
                    xcp.nextToken()

                    when (metaField) {
                        RollupMapperService.ROLLUPS -> {
                            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
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
    // This method is the authoritative source for getting the rollupJobs for an index and if there are none found this method should return null
    return if (rollupJobs.size > 0) rollupJobs else null
}

// TODO: If we have to set this manually for each aggregation builder then it means we could miss new ones settings in the future
@Suppress("ComplexMethod")
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
            SumAggregationBuilder(aggregationBuilder.name)
                    .field(this.findMatchingMetricField<Sum>(aggregationBuilder.field()))
        }
        is AvgAggregationBuilder -> {
            ScriptedMetricAggregationBuilder(aggregationBuilder.name)
                    .initScript(Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "state.sums = 0; state.counts = 0;", emptyMap()))
                    .mapScript(Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                            "state.sums += doc[\"${this.findMatchingMetricField<Average>(aggregationBuilder.field()) + ".sum"}\"].value; " +
                                    "state.counts += doc[\"${this.findMatchingMetricField<Average>(aggregationBuilder.field()) + ".value_count"}\"" +
                                    "].value", emptyMap()))
                    .combineScript(Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                            "def d = new long[2]; d[0] = state.sums; d[1] = state.counts; return d", emptyMap()))
                    .reduceScript(Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                            "double sum = 0; double count = 0; for (a in states) { sum += a[0]; count += a[1]; } return sum/count", emptyMap()))
        }
        is MaxAggregationBuilder -> {
            MaxAggregationBuilder(aggregationBuilder.name)
                    .field(this.findMatchingMetricField<Max>(aggregationBuilder.field()))
        }
        is MinAggregationBuilder -> {
            MinAggregationBuilder(aggregationBuilder.name)
                    .field(this.findMatchingMetricField<Min>(aggregationBuilder.field()))
        }
        is ValueCountAggregationBuilder -> {
            /*
            * A value count aggs of a pre-computed value count is incorrect as it just returns the number of
            * pre-computed value counts instead of their sum. Unfortunately can't just use the sum aggregation
            * because I was not able to find a way to cast the result of that to a long (instead of the returned float)
            * and the 3893 vs 3893.0 was bothering me.. so this is the next best I can think of. Hopefully there is a better
            * way and we can use that in the future.
            * */
            ScriptedMetricAggregationBuilder(aggregationBuilder.name)
                    .initScript(Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "state.valueCounts = []", emptyMap()))
                    .mapScript(Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                            "state.valueCounts.add(doc[\"${this.findMatchingMetricField<ValueCount>(aggregationBuilder.field())}\"].value)",
                            emptyMap()))
                    .combineScript(Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                            "long valueCount = 0; for (vc in state.valueCounts) { valueCount += vc } return valueCount", emptyMap()))
                    .reduceScript(Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                            "long valueCount = 0; for (vc in states) { valueCount += vc } return valueCount", emptyMap()))
        }
        // We do nothing otherwise, the validation logic should have already verified so not throwing an exception
        else -> aggregationBuilder
    }
}

@Suppress("ComplexMethod", "LongMethod")
fun Rollup.rewriteQueryBuilder(queryBuilder: QueryBuilder, fieldNameMappingTypeMap: Map<String, String>): QueryBuilder {
    return when (queryBuilder) {
        is TermQueryBuilder -> {
            val updatedFieldName = queryBuilder.fieldName() + "." + Dimension.Type.TERMS.type
            val updatedTermQueryBuilder = TermQueryBuilder(updatedFieldName, queryBuilder.value())
            updatedTermQueryBuilder.boost(queryBuilder.boost())
            updatedTermQueryBuilder.queryName(queryBuilder.queryName())
        }
        is TermsQueryBuilder -> {
            val updatedFieldName = queryBuilder.fieldName() + "." + Dimension.Type.TERMS.type
            val updatedTermsQueryBuilder = TermsQueryBuilder(updatedFieldName, queryBuilder.values())
            updatedTermsQueryBuilder.boost(queryBuilder.boost())
            updatedTermsQueryBuilder.queryName(queryBuilder.queryName())
        }
        is RangeQueryBuilder -> {
            val updatedFieldName = queryBuilder.fieldName() + "." + fieldNameMappingTypeMap.getValue(queryBuilder.fieldName())
            val updatedRangeQueryBuilder = RangeQueryBuilder(updatedFieldName)
            updatedRangeQueryBuilder.includeLower(queryBuilder.includeLower())
            updatedRangeQueryBuilder.includeUpper(queryBuilder.includeUpper())
            updatedRangeQueryBuilder.from(queryBuilder.from())
            updatedRangeQueryBuilder.to(queryBuilder.to())
            if (queryBuilder.timeZone() != null) updatedRangeQueryBuilder.timeZone(queryBuilder.timeZone())
            if (queryBuilder.format() != null) updatedRangeQueryBuilder.format(queryBuilder.format())
            if (queryBuilder.relation()?.relationName != null) updatedRangeQueryBuilder.relation(queryBuilder.relation().relationName)
            updatedRangeQueryBuilder.queryName(queryBuilder.queryName())
            updatedRangeQueryBuilder.boost(queryBuilder.boost())
        }
        is BoolQueryBuilder -> {
            val newBoolQueryBuilder = BoolQueryBuilder()
            queryBuilder.must()?.forEach {
                val newMustQueryBuilder = this.rewriteQueryBuilder(it, fieldNameMappingTypeMap)
                newBoolQueryBuilder.must(newMustQueryBuilder)
            }
            queryBuilder.mustNot()?.forEach {
                val newMustNotQueryBuilder = this.rewriteQueryBuilder(it, fieldNameMappingTypeMap)
                newBoolQueryBuilder.mustNot(newMustNotQueryBuilder)
            }
            queryBuilder.should()?.forEach {
                val newShouldQueryBuilder = this.rewriteQueryBuilder(it, fieldNameMappingTypeMap)
                newBoolQueryBuilder.should(newShouldQueryBuilder)
            }
            queryBuilder.filter()?.forEach {
                val newFilterQueryBuilder = this.rewriteQueryBuilder(it, fieldNameMappingTypeMap)
                newBoolQueryBuilder.filter(newFilterQueryBuilder)
            }
            newBoolQueryBuilder.minimumShouldMatch(queryBuilder.minimumShouldMatch())
            newBoolQueryBuilder.adjustPureNegative(queryBuilder.adjustPureNegative())
            newBoolQueryBuilder.queryName(queryBuilder.queryName())
            newBoolQueryBuilder.boost(queryBuilder.boost())
        }
        is BoostingQueryBuilder -> {
            val newPositiveQueryBuilder = this.rewriteQueryBuilder(queryBuilder.positiveQuery(), fieldNameMappingTypeMap)
            val newNegativeQueryBuilder = this.rewriteQueryBuilder(queryBuilder.negativeQuery(), fieldNameMappingTypeMap)
            val newBoostingQueryBuilder = BoostingQueryBuilder(newPositiveQueryBuilder, newNegativeQueryBuilder)
            if (queryBuilder.negativeBoost() >= 0) newBoostingQueryBuilder.negativeBoost(queryBuilder.negativeBoost())
            newBoostingQueryBuilder.queryName(queryBuilder.queryName())
            newBoostingQueryBuilder.boost(queryBuilder.boost())
        }
        is ConstantScoreQueryBuilder -> {
            val newInnerQueryBuilder = this.rewriteQueryBuilder(queryBuilder.innerQuery(), fieldNameMappingTypeMap)
            val newConstantScoreQueryBuilder = ConstantScoreQueryBuilder(newInnerQueryBuilder)
            newConstantScoreQueryBuilder.boost(queryBuilder.boost())
            newConstantScoreQueryBuilder.queryName(queryBuilder.queryName())
        }
        is DisMaxQueryBuilder -> {
            val newDisMaxQueryBuilder = DisMaxQueryBuilder()
            queryBuilder.innerQueries().forEach { newDisMaxQueryBuilder.add(this.rewriteQueryBuilder(it, fieldNameMappingTypeMap)) }
            newDisMaxQueryBuilder.tieBreaker(queryBuilder.tieBreaker())
            newDisMaxQueryBuilder.queryName(queryBuilder.queryName())
            newDisMaxQueryBuilder.boost(queryBuilder.boost())
        }
        is MatchPhraseQueryBuilder -> {
            val newFieldName = queryBuilder.fieldName() + "." + Dimension.Type.TERMS.type
            val newMatchPhraseQueryBuilder = MatchPhraseQueryBuilder(newFieldName, queryBuilder.value())
            newMatchPhraseQueryBuilder.queryName(queryBuilder.queryName())
            newMatchPhraseQueryBuilder.boost(queryBuilder.boost())
        }
        // We do nothing otherwise, the validation logic should have already verified so not throwing an exception
        else -> queryBuilder
    }
}

fun Rollup.buildRollupQuery(fieldNameMappingTypeMap: Map<String, String>, oldQuery: QueryBuilder): QueryBuilder {
    val wrappedQueryBuilder = BoolQueryBuilder()
    wrappedQueryBuilder.must(this.rewriteQueryBuilder(oldQuery, fieldNameMappingTypeMap))
    wrappedQueryBuilder.filter(TermQueryBuilder("rollup._id", this.id))
    return wrappedQueryBuilder
}

fun Rollup.populateFieldMappings(): Set<RollupFieldMapping> {
    val fieldMappings = mutableSetOf<RollupFieldMapping>()
    this.dimensions.forEach {
        fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, it.sourceField, it.type.type))
    }
    this.metrics.forEach { rollupMetric ->
        rollupMetric.metrics.forEach { metric ->
            fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, rollupMetric.sourceField, metric.type.type))
        }
    }
    return fieldMappings
}

// TODO: Not a fan of this.. but I can't find a way to overwrite the aggregations on the shallow copy or original
//  so we need to instantiate a new one so we can add the rewritten aggregation builders
@Suppress("ComplexMethod")
fun SearchSourceBuilder.rewriteSearchSourceBuilder(job: Rollup, fieldNameMappingTypeMap: Map<String, String>): SearchSourceBuilder {
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
    if (this.query() != null) ssb.query(job.buildRollupQuery(fieldNameMappingTypeMap, this.query()))
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

fun Rollup.getInitialDocValues(docCount: Long): MutableMap<String, Any?> =
    mutableMapOf(
        Rollup.ROLLUP_DOC_ID_FIELD to this.id,
        Rollup.ROLLUP_DOC_COUNT_FIELD to docCount,
        Rollup.ROLLUP_DOC_SCHEMA_VERSION_FIELD to this.schemaVersion
    )
