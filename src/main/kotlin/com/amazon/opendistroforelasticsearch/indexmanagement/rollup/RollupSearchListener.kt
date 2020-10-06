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

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.DateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Histogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Terms
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings.Companion.ROLLUP_SEARCH_ENABLED
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.util.getRollupJobs
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.util.rewriteSearchSourceBuilder
import org.apache.logging.log4j.LogManager
import org.apache.lucene.index.IndexReader
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.Query
import org.apache.lucene.search.QueryVisitor
import org.apache.lucene.search.ScoreMode
import org.apache.lucene.search.Weight
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.query.ParsedQuery
import org.elasticsearch.index.shard.SearchOperationListener
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.aggregations.SearchContextAggregations
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder
import org.elasticsearch.search.internal.SearchContext

// TODO: Move this
class RollupExceptionQuery(val message: String) : Query() {
    override fun createWeight(searcher: IndexSearcher, scoreMode: ScoreMode, boost: Float): Weight = throw UnsupportedOperationException(message)
    override fun rewrite(reader: IndexReader): Query = throw UnsupportedOperationException(message)
    override fun visit(visitor: QueryVisitor) = throw UnsupportedOperationException(message)
    override fun hashCode(): Int = -1
    override fun equals(other: Any?): Boolean = false
    override fun toString(field: String?): String = "rollup_exception_query"
}

class RollupSearchListener(
    private val clusterService: ClusterService,
    settings: Settings,
    private val indexNameExpressionResolver: IndexNameExpressionResolver
) : SearchOperationListener {

    private val logger = LogManager.getLogger(javaClass)
    @Volatile private var searchEnabled = ROLLUP_SEARCH_ENABLED.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ROLLUP_SEARCH_ENABLED) {
            searchEnabled = it
        }
    }

    // TODO: What is the expected behavior when you are searching an incomplete non-continuous rollup job?
    //  And how does it affect the rollup job matching process? i.e. you have two jobs 100% but one isn't done yet.
    // TODO: Reject all queries that do not have size = 0 as we do not return rollup documents
    //   they can still use queries, but only for filtering the result set
    // TODO: Reject script in searches because we don't parse and rewrite scripts?
    override fun onPreQueryPhase(searchContext: SearchContext) {
        if (!searchEnabled) return

        val indices = searchContext.request().indices().map { it.toString() }.toTypedArray()
        val concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterService.state(), searchContext.request().indicesOptions(), *indices)

        val hasNonRollupIndex = concreteIndices.any {
            val isRollupIndex = RollupSettings.ROLLUP_INDEX.get(clusterService.state().metadata.index(it).settings)
            if (!isRollupIndex) logger.warn("A non-rollup index cannot be searched with a rollup index [index=$it]")
            !isRollupIndex
        }

        if (hasNonRollupIndex) {
            searchContext.parsedQuery(ParsedQuery(RollupExceptionQuery("Cannot query rollup and normal indices in the same request")))
            return
        }

        val rollupJobs = searchContext.indexShard().indexSettings().indexMetadata.getRollupJobs()
        if (rollupJobs == null) {
            searchContext.parsedQuery(ParsedQuery(RollupExceptionQuery("Could not find the mapping source for the index")))
            return
        }

        val (dimensionTypesToFields, metricFieldsToTypes) = getAggregationMetadata(searchContext.request().source().aggregations()?.aggregatorFactories)

        // TODO: How does this job matching work with roles/security?
        // TODO: Move to helper class or ext fn - this is veeeeery inefficient, but it works for development/testing
        // The goal here is to find all the matching rollup jobs from the ones that exist on this rollup index
        // A matching rollup job is one that has all the fields used in the aggregations in its own dimensions/metrics
        val matchingRollupJobs = rollupJobs.filter { job ->
            // We take the provided dimensionsTypesToFields and confirm for each entry that the given type (dimension type) and set (source fields)
            // exists in the rollup job itself to verify that the query can be answered with the data from this rollup job
            val hasAllDimensions = dimensionTypesToFields.entries.all { (type, set) ->
                // The filteredDimensionsFields are all the source fields of a specific dimension type on this job
                val filteredDimensionsFields = job.dimensions.filter { it.type.type == type }.map {
                    when (it) {
                        is DateHistogram -> it.sourceField
                        is Histogram -> it.sourceField
                        is Terms -> it.sourceField
                        // TODO: can't throw - have to return early after overwriting parsedquery
                        else -> throw IllegalArgumentException("Found unsupported Dimension during search transformation [${it.type.type}]")
                    }
                }
                // Confirm that the list of source fields on the rollup job's dimensions contains all of the fields in the user's aggregation
                filteredDimensionsFields.containsAll(set)
            }
            // We take the provided metricFieldsToTypes and confirm for each entry that the given field (source field) and set (metric types)
            // exists in the rollup job itself to verify that the query can be answered with the data from this rollup job
            val hasAllMetrics = metricFieldsToTypes.entries.all { (field, set) ->
                // The filteredMetrics are all the metric types that were computed for the given source field on this job
                val filteredMetrics = job.metrics.find { it.sourceField == field }?.metrics?.map { it.type.type } ?: emptyList()
                // Confirm that the list of metric aggregation types in the rollup job's metrics contains all of the metric types
                // in the user's aggregation for this given source field
                filteredMetrics.containsAll(set)
            }

            hasAllDimensions && hasAllMetrics
        }

        if (matchingRollupJobs.isEmpty()) {
            // TODO: This needs to be more helpful and say what fields were missing
            searchContext.parsedQuery(ParsedQuery(RollupExceptionQuery("Could not find a rollup job that can answer this query")))
            return
        }

        // Very simple resolution to start: just take all the matching jobs that can answer the query and use the newest one
        val matchedRollup = matchingRollupJobs.reduce { matched, new ->
            if (matched.lastUpdateTime.isAfter(new.lastUpdateTime)) matched
            else new
        }

        val aggregationBuilders = searchContext.request().source().aggregations()?.aggregatorFactories
        if (aggregationBuilders != null) {
            val newSourceBuilder = searchContext.request().source().rewriteSearchSourceBuilder(matchedRollup)
            searchContext.request().source(newSourceBuilder)
            val factories = searchContext.request().source().aggregations().build(searchContext.queryShardContext, null)
            searchContext.aggregations(SearchContextAggregations(factories, searchContext.aggregations().multiBucketConsumer()))
        }
    }

    @Throws(UnsupportedOperationException::class)
    private fun getAggregationMetadata(
        aggregationBuilders: Collection<AggregationBuilder>?,
        dimensionTypesToFields: MutableMap<String, MutableSet<String>> = mutableMapOf(),
        metricFieldsToTypes: MutableMap<String, MutableSet<String>> = mutableMapOf()
    ): Pair<Map<String, Set<String>>, Map<String, Set<String>>> {
        aggregationBuilders?.forEach {
            when (it) {
                is TermsAggregationBuilder -> {
                    dimensionTypesToFields.computeIfAbsent(it.type) { mutableSetOf() }.add(it.field())
                }
                is DateHistogramAggregationBuilder -> {
                    dimensionTypesToFields.computeIfAbsent(it.type) { mutableSetOf() }.add(it.field())
                }
                is HistogramAggregationBuilder -> {
                    dimensionTypesToFields.computeIfAbsent(it.type) { mutableSetOf() }.add(it.field())
                }
                is SumAggregationBuilder -> {
                    metricFieldsToTypes.computeIfAbsent(it.field()) { mutableSetOf() }.add(it.type)
                }
                is AvgAggregationBuilder -> {
                    metricFieldsToTypes.computeIfAbsent(it.field()) { mutableSetOf() }.add(it.type)
                }
                is MaxAggregationBuilder -> {
                    metricFieldsToTypes.computeIfAbsent(it.field()) { mutableSetOf() }.add(it.type)
                }
                is MinAggregationBuilder -> {
                    metricFieldsToTypes.computeIfAbsent(it.field()) { mutableSetOf() }.add(it.type)
                }
                is ValueCountAggregationBuilder -> {
                    metricFieldsToTypes.computeIfAbsent(it.field()) { mutableSetOf() }.add(it.type)
                }
            }
            if (it.subAggregations?.isNotEmpty() == true) {
                getAggregationMetadata(it.subAggregations, dimensionTypesToFields, metricFieldsToTypes)
            }
        }
        return dimensionTypesToFields to metricFieldsToTypes
    }
}
