/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.retry
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.exceptions.TransformSearchServiceException
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.TransformSearchResult
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.TransformStats
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.settings.TransformSettings.Companion.TRANSFORM_JOB_SEARCH_BACKOFF_COUNT
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.settings.TransformSettings.Companion.TRANSFORM_JOB_SEARCH_BACKOFF_MILLIS
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils.Companion.ODFE_MAGIC_NULL
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils.Companion.hashToFixedSize
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.bulk.BackoffPolicy
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.search.aggregations.Aggregation
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder
import org.elasticsearch.search.aggregations.metrics.InternalAvg
import org.elasticsearch.search.aggregations.metrics.InternalMax
import org.elasticsearch.search.aggregations.metrics.InternalMin
import org.elasticsearch.search.aggregations.metrics.InternalSum
import org.elasticsearch.search.aggregations.metrics.InternalValueCount
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation
import org.elasticsearch.search.aggregations.metrics.Percentiles
import org.elasticsearch.search.aggregations.metrics.ScriptedMetric
import org.elasticsearch.search.builder.SearchSourceBuilder
import kotlin.math.max
import kotlin.math.pow
import org.elasticsearch.index.query.QueryBuilder

class TransformSearchService(
    val settings: Settings,
    val clusterService: ClusterService,
    private val esClient: Client
) {

    private var logger = LogManager.getLogger(javaClass)

    @Volatile private var backoffPolicy =
        BackoffPolicy.constantBackoff(TRANSFORM_JOB_SEARCH_BACKOFF_MILLIS.get(settings), TRANSFORM_JOB_SEARCH_BACKOFF_COUNT.get(settings))

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(TRANSFORM_JOB_SEARCH_BACKOFF_MILLIS, TRANSFORM_JOB_SEARCH_BACKOFF_COUNT) {
            millis, count -> backoffPolicy = BackoffPolicy.constantBackoff(millis, count)
        }
    }

    suspend fun executeCompositeSearch(transform: Transform, afterKey: Map<String, Any>? = null): TransformSearchResult {
        val errorMessage = "Failed to search data in source indices in transform job ${transform.id}"
        try {
            var retryAttempt = 0
            val searchResponse = backoffPolicy.retry(logger) {
                // TODO: Should we store the value of the past successful page size (?)
                val pageSizeDecay = 2f.pow(retryAttempt++)
                esClient.suspendUntil { listener: ActionListener<SearchResponse> ->
                    val pageSize = max(1, transform.pageSize.div(pageSizeDecay.toInt()))
                    if (retryAttempt > 1) {
                        logger.debug(
                            "Attempt [${retryAttempt - 1}] of composite search failed for transform [${transform.id}]. Attempting " +
                                "again with reduced page size [$pageSize]"
                        )
                    }
                    val aggregationBuilder = getAggregationBuilder(transform, afterKey, pageSize)
                    val request = getSearchServiceRequest(transform.sourceIndex, transform.dataSelectionQuery, aggregationBuilder)
                    search(request, listener)
                }
            }
            return convertResponse(transform, searchResponse)
        } catch (e: TransformSearchServiceException) {
            logger.error(errorMessage)
            throw e
        } catch (e: Exception) {
            logger.error(errorMessage)
            throw TransformSearchServiceException(errorMessage, e)
        }
    }

    private fun getSearchServiceRequest(index: String, query: QueryBuilder, aggregationBuilder: CompositeAggregationBuilder): SearchRequest {
        val searchSourceBuilder = SearchSourceBuilder()
            .trackTotalHits(false)
            .size(0)
            .aggregation(aggregationBuilder)
            .query(query)
        return SearchRequest(index)
            .source(searchSourceBuilder)
            .allowPartialSearchResults(false)
    }

    private fun getAggregationBuilder(transform: Transform, afterKey: Map<String, Any>?, pageSize: Int): CompositeAggregationBuilder {
        val sources = mutableListOf<CompositeValuesSourceBuilder<*>>()
        transform.groups.forEach { group -> sources.add(group.toSourceBuilder()) }
        return CompositeAggregationBuilder(transform.id, sources)
            .size(pageSize)
            .subAggregations(transform.aggregations)
            .apply { afterKey?.let { this.aggregateAfter(it) } }
    }

    private fun convertResponse(transform: Transform, searchResponse: SearchResponse): TransformSearchResult {
        val aggs = searchResponse.aggregations.get(transform.id) as CompositeAggregation
        val documentsProcessed = aggs.buckets.fold(0L) { sum, it -> sum + it.docCount }
        val pagesProcessed = 1L
        val searchTime = searchResponse.took.millis
        val stats = TransformStats(pagesProcessed, documentsProcessed, 0, 0, searchTime)
        val afterKey = aggs.afterKey()
        val docsToIndex = mutableListOf<DocWriteRequest<*>>()
        aggs.buckets.forEach { aggregatedBucket ->
            val id = transform.id + "#" + aggregatedBucket.key.entries.joinToString(":") { bucket -> bucket.value?.toString() ?: ODFE_MAGIC_NULL }
            val hashedId = hashToFixedSize(id)

            val document = transform.convertToDoc(aggregatedBucket.docCount)
            aggregatedBucket.key.entries.forEach { bucket -> document[bucket.key] = bucket.value }
            aggregatedBucket.aggregations.forEach { aggregation -> document[aggregation.name] = getAggregationValue(aggregation) }

            val indexRequest = IndexRequest(transform.targetIndex)
                .id(hashedId)
                .source(document, XContentType.JSON)
            docsToIndex.add(indexRequest)
        }

        return TransformSearchResult(stats, docsToIndex, afterKey)
    }

    private fun getAggregationValue(aggregation: Aggregation): Any {
        return when (aggregation) {
            is InternalSum, is InternalMin, is InternalMax, is InternalAvg, is InternalValueCount -> {
                val agg = aggregation as NumericMetricsAggregation.SingleValue
                agg.value()
            }
            is Percentiles -> {
                val percentiles = mutableMapOf<String, Double>()
                aggregation.forEach { percentile ->
                    percentiles[percentile.percent.toString()] = percentile.value
                }
                percentiles
            }
            is ScriptedMetric -> {
                aggregation.aggregation()
            }
            else -> throw TransformSearchServiceException(
                "Found aggregation [${aggregation.name}] of type [${aggregation.type}] in composite result that is not currently supported"
            )
        }
    }
}
