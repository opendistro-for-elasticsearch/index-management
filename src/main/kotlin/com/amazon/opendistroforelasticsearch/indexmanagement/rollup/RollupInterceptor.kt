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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupFieldMapping
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupFieldMapping.Companion.RANGE_MAPPING
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Dimension
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.util.getRollupJobs
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.util.populateFieldMappings
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.util.rewriteSearchSourceBuilder
import org.apache.logging.log4j.LogManager
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.BoostingQueryBuilder
import org.elasticsearch.index.query.ConstantScoreQueryBuilder
import org.elasticsearch.index.query.DisMaxQueryBuilder
import org.elasticsearch.index.query.MatchAllQueryBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.RangeQueryBuilder
import org.elasticsearch.index.query.TermQueryBuilder
import org.elasticsearch.index.query.TermsQueryBuilder
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder
import org.elasticsearch.search.internal.ShardSearchRequest
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportChannel
import org.elasticsearch.transport.TransportInterceptor
import org.elasticsearch.transport.TransportRequest
import org.elasticsearch.transport.TransportRequestHandler

class RollupInterceptor(
    val clusterService: ClusterService,
    val indexNameExpressionResolver: IndexNameExpressionResolver
) : TransportInterceptor {

    private val logger = LogManager.getLogger(javaClass)

    @Suppress("ComplexMethod", "SpreadOperator", "NestedBlockDepth", "LongMethod")
    override fun <T : TransportRequest> interceptHandler(
        action: String,
        executor: String,
        forceExecution: Boolean,
        actualHandler: TransportRequestHandler<T>
    ): TransportRequestHandler<T> {
        return object : TransportRequestHandler<T> {
            override fun messageReceived(request: T, channel: TransportChannel, task: Task) {
                if (request is ShardSearchRequest) {
                    val index = request.shardId().indexName
                    val isRollupIndex = RollupSettings.ROLLUP_INDEX.get(clusterService.state().metadata.index(index).settings)
                    if (isRollupIndex) {
                        val indices = request.indices().map { it.toString() }.toTypedArray()
                        val concreteIndices = indexNameExpressionResolver
                                .concreteIndexNames(clusterService.state(), request.indicesOptions(), *indices)

                        val hasNonRollupIndex = concreteIndices.any {
                            val isNonRollupIndex = !RollupSettings.ROLLUP_INDEX.get(clusterService.state().metadata.index(it).settings)
                            if (isNonRollupIndex) {
                                logger.warn("A non-rollup index cannot be searched with a rollup index [non-rollup-index=$it] [rollup-index=$index]")
                            }
                            isNonRollupIndex
                        }

                        if (hasNonRollupIndex) {
                            throw IllegalArgumentException("Cannot query rollup and normal indices in the same request")
                        }

                        val rollupJobs = clusterService.state().metadata.index(index).getRollupJobs()
                                ?: throw IllegalArgumentException("Could not find the mapping source for the index")

                        val queryFieldMappings = getQueryMetadata(request.source().query())
                        val aggregationFieldMappings = getAggregationMetadata(request.source().aggregations()?.aggregatorFactories)
                        val fieldMappings = queryFieldMappings + aggregationFieldMappings

                        val (matchingRollupJobs, issues) = findMatchingRollupJobs(fieldMappings, rollupJobs)

                        if (matchingRollupJobs.isEmpty()) {
                            throw IllegalArgumentException("Could not find a rollup job that can answer this query because $issues")
                        }

                        // Very simple resolution to start: just take all the matching jobs that can answer the query and use the newest one
                        val matchedRollup = matchingRollupJobs.reduce { matched, new ->
                            if (matched.lastUpdateTime.isAfter(new.lastUpdateTime)) matched
                            else new
                        }

                        // only rebuild if there is necessity to rebuild
                        if (fieldMappings.isNotEmpty()) {
                            request.source(request.source().rewriteSearchSourceBuilder(matchedRollup))
                        }
                    }
                }
                actualHandler.messageReceived(request, channel, task)
            }
        }
    }

    @Suppress("ComplexMethod")
    private fun getAggregationMetadata(
        aggregationBuilders: Collection<AggregationBuilder>?,
        fieldMappings: MutableSet<RollupFieldMapping> = mutableSetOf()
    ): Set<RollupFieldMapping> {
        aggregationBuilders?.forEach {
            when (it) {
                is TermsAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, it.field(), it.type))
                }
                is DateHistogramAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, it.field(), it.type))
                }
                is HistogramAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, it.field(), it.type))
                }
                is SumAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, it.field(), it.type))
                }
                is AvgAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, it.field(), it.type))
                }
                is MaxAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, it.field(), it.type))
                }
                is MinAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, it.field(), it.type))
                }
                is ValueCountAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, it.field(), it.type))
                }
                else -> throw UnsupportedOperationException("The ${it.type} aggregation is not currently supported in rollups")
            }
            if (it.subAggregations?.isNotEmpty() == true) {
                getAggregationMetadata(it.subAggregations, fieldMappings)
            }
        }
        return fieldMappings
    }

    @Suppress("ComplexMethod")
    private fun getQueryMetadata(
        query: QueryBuilder?,
        fieldMappings: MutableSet<RollupFieldMapping> = mutableSetOf()
    ): Set<RollupFieldMapping> {
        if (query == null) {
            return fieldMappings
        }

        when (query) {
            is TermQueryBuilder -> {
                fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, query.fieldName(), Dimension.Type.TERMS.type))
            }
            is TermsQueryBuilder -> {
                fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, query.fieldName(), Dimension.Type.TERMS.type))
            }
            is RangeQueryBuilder -> {
                fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, query.fieldName(), RANGE_MAPPING))
            }
            is MatchAllQueryBuilder -> {
                // do nothing
            }
            is BoolQueryBuilder -> {
                query.must()?.forEach { this.getQueryMetadata(it, fieldMappings) }
                query.mustNot()?.forEach { this.getQueryMetadata(it, fieldMappings) }
                query.should()?.forEach { this.getQueryMetadata(it, fieldMappings) }
                query.filter()?.forEach { this.getQueryMetadata(it, fieldMappings) }
            }
            is BoostingQueryBuilder -> {
                query.positiveQuery()?.also { this.getQueryMetadata(it, fieldMappings) }
                query.negativeQuery()?.also { this.getQueryMetadata(it, fieldMappings) }
            }
            is ConstantScoreQueryBuilder -> {
                query.innerQuery()?.also { this.getQueryMetadata(it, fieldMappings) }
            }
            is DisMaxQueryBuilder -> {
                query.innerQueries().forEach { this.getQueryMetadata(it, fieldMappings) }
            }
            is FunctionScoreQueryBuilder -> {
                query.query().also { this.getQueryMetadata(it, fieldMappings) }
                query.filterFunctionBuilders().forEach { this.getQueryMetadata(it.filter, fieldMappings) }
            }
            else -> {
                throw UnsupportedOperationException("The ${query.name} query is currently not supported in rollups")
            }
        }

        return fieldMappings
    }

    // TODO: can be potentially more efficient
    private fun findMatchingRollupJobs(
        fieldMappings: Set<RollupFieldMapping>,
        rollupJobs: List<Rollup>
    ): Pair<Set<Rollup>, Set<String>> {
        val rollupFieldMappings = rollupJobs.map { rollup ->
            rollup to rollup.populateFieldMappings()
        }.toMap()

        val nonRangeFieldMappings = mutableSetOf<RollupFieldMapping>()
        val rangeFieldMappings = mutableSetOf<RollupFieldMapping>()

        fieldMappings.forEach {
            if (it.mappingType == RANGE_MAPPING) rangeFieldMappings.add(it)
            else nonRangeFieldMappings.add(it)
        }

        val potentialRollupFieldMappings = rollupFieldMappings.filterValues { it.containsAll(nonRangeFieldMappings) }

        // TODO: not sure what to do yet on nonRangeFieldMappings

        val issues = mutableSetOf<String>()
        val allFieldMappings = mutableSetOf<RollupFieldMapping>()
        rollupFieldMappings.values.forEach { allFieldMappings.addAll(it) }
        nonRangeFieldMappings.forEach {
            if (!allFieldMappings.contains(it)) issues.add(it.toIssue())
        }

        return potentialRollupFieldMappings.keys to issues
    }
}
