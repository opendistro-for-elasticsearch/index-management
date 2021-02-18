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
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupIndexer
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.TransformMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.TransformStats
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.settings.TransformSettings.Companion.TRANSFORM_JOB_SEARCH_BACKOFF_COUNT
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.settings.TransformSettings.Companion.TRANSFORM_JOB_SEARCH_BACKOFF_MILLIS
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.bulk.BackoffPolicy
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.hash.MurmurHash3
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
import java.lang.IllegalStateException
import java.nio.ByteBuffer
import java.util.Base64
import kotlin.math.max
import kotlin.math.pow

class TransformSearchService(
    val transform: Transform,
    val metadata: TransformMetadata,
    val settings: Settings,
    val clusterService: ClusterService,
    private val esClient: Client
) {

    private var logger = LogManager.getLogger(javaClass)
    private var retryPolicy =
        BackoffPolicy.constantBackoff(TRANSFORM_JOB_SEARCH_BACKOFF_MILLIS.get(settings), TRANSFORM_JOB_SEARCH_BACKOFF_COUNT.get(settings))

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(TRANSFORM_JOB_SEARCH_BACKOFF_MILLIS, TRANSFORM_JOB_SEARCH_BACKOFF_COUNT) {
            millis, count -> retryPolicy = BackoffPolicy.constantBackoff(millis, count)
        }
    }

    suspend fun executeCompositeSearch(): Pair<Pair<TransformStats, Map<String, Any>?>, List<DocWriteRequest<*>>> {
        try {
            var retryAttempt = 0
            val searchResponse = retryPolicy.retry(logger) {
                // TODO: Should we store the value of the past successful page size (?)
                val pageSizeDecay = 2f.pow(retryAttempt++)
                esClient.suspendUntil { listener: ActionListener<SearchResponse> ->
                    val pageSize = max(1, transform.pageSize.div(pageSizeDecay.toInt()))
                    if (retryAttempt > 1) {
                        logger.warn("Attempt [${retryAttempt - 1}] of composite search failed for transform [${transform.id}]. Attempting " +
                            "again with reduced page size [$pageSize]")
                        search(getSearchServiceRequest(pageSize), listener)
                    }
                }
            }
            return convertResponse(searchResponse)
        } catch (e: Exception) {
            logger.error("Failed to execute the internal search for the ")
            throw e
        }
    }

    private fun getSearchServiceRequest(pageSize: Int): SearchRequest {
        val query = transform.dataSelectionQuery
        val searchSourceBuilder = SearchSourceBuilder()
            .trackTotalHits(false)
            .size(0)
            .aggregation(getAggregationBuilder(pageSize))
            .query(query)
        return SearchRequest(transform.sourceIndex)
            .source(searchSourceBuilder)
            .allowPartialSearchResults(false)
    }

    private fun getAggregationBuilder(pageSize: Int): CompositeAggregationBuilder {
        val sources = mutableListOf<CompositeValuesSourceBuilder<*>>()
        transform.groups.forEach { group -> sources.add(group.toSourceBuilder()) }
        return CompositeAggregationBuilder(transform.id, sources)
            .size(pageSize)
            .subAggregations(transform.aggregations)
            .apply {
                this@TransformSearchService.metadata.afterKey?.let { this.aggregateAfter(it) }
            }
    }

    private fun convertResponse(searchResponse: SearchResponse): Pair<Pair<TransformStats, Map<String, Any>?>, List<DocWriteRequest<*>>> {
        val aggs = searchResponse.aggregations.get(transform.id) as CompositeAggregation
        val documentsProcessed = aggs.buckets.fold(0L) { sum, it -> sum + it.docCount }
        val pagesProcessed = 1L
        val searchTime = searchResponse.took.millis
        val stats = TransformStats(pagesProcessed, documentsProcessed, 0, 0, searchTime)
        val afterKey = aggs.afterKey()
        val metadata = Pair(stats, afterKey)
        val docsToIndex = mutableListOf<DocWriteRequest<*>>()
        aggs.buckets.forEach {
            val id = transform.id + "#" + it.key.entries.joinToString(":") { it.value?.toString() ?: "ODFE-NULL-ODFE" }
            val docByteArray = id.toByteArray()
            val hash = MurmurHash3.hash128(docByteArray, 0, docByteArray.size, RollupIndexer.DOCUMENT_ID_SEED, MurmurHash3.Hash128())
            val byteArray = ByteBuffer.allocate(RollupIndexer.BYTE_ARRAY_SIZE).putLong(hash.h1).putLong(hash.h2).array()
            val hashedId = Base64.getUrlEncoder().withoutPadding().encodeToString(byteArray)

            val document = transform.convertToDoc(it.docCount)
            it.key.entries.forEach { document[it.key] = it.value }
            it.aggregations.forEach { document[it.name] = getAggregationValue(it) }

            val indexRequest = IndexRequest(transform.targetIndex)
                .id(hashedId)
                .source(document, XContentType.JSON)
            docsToIndex.add(indexRequest)
        }

        return Pair(metadata, docsToIndex)
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
            else -> throw IllegalStateException("Found aggregation [${aggregation.name}] of type [${aggregation.type}] in composite result " +
                "that is not currently supported")
        }
    }
}
