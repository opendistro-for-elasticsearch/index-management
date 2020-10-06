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

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.retry
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Dimension
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings.Companion.ROLLUP_INGEST_BACKOFF_COUNT
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings.Companion.ROLLUP_INGEST_BACKOFF_MILLIS
import com.amazon.opendistroforelasticsearch.indexmanagement.util._ID
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.bulk.BackoffPolicy
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite
import org.elasticsearch.search.aggregations.metrics.InternalAvg
import org.elasticsearch.search.aggregations.metrics.InternalMax
import org.elasticsearch.search.aggregations.metrics.InternalMin
import org.elasticsearch.search.aggregations.metrics.InternalSum
import org.elasticsearch.search.aggregations.metrics.InternalValueCount

class RollupIndexer(
    settings: Settings,
    clusterService: ClusterService,
    private val client: Client
) {
    private val logger = LogManager.getLogger(javaClass)

    @Volatile private var retryIngestPolicy =
        BackoffPolicy.constantBackoff(ROLLUP_INGEST_BACKOFF_MILLIS.get(settings), ROLLUP_INGEST_BACKOFF_COUNT.get(settings))

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ROLLUP_INGEST_BACKOFF_MILLIS, ROLLUP_INGEST_BACKOFF_COUNT) {
            millis, count -> retryIngestPolicy = BackoffPolicy.constantBackoff(millis, count)
        }
    }

    /*
    * TODO: Need to handle the situation where we retried but still had failed bulk items
    *  In that case we can't proceed, we need to allow the job to try and re-rollup this window in the future
    *  But does "in the future" mean the next execution or just the the next loop in the current execution?
    * TODO: Can someone set a really high backoff that causes us to go over the lock duration?
    * */
    suspend fun indexRollups(rollup: Rollup, internalComposite: InternalComposite) {
        var requestsToRetry = convertResponseToRequests(rollup, internalComposite)
        if (requestsToRetry.isNotEmpty()) {
            retryIngestPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
                val bulkRequest = BulkRequest().add(requestsToRetry)
                val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
                val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }
                requestsToRetry = failedResponses.filter { it.status() == RestStatus.TOO_MANY_REQUESTS }
                    .map { bulkRequest.requests()[it.itemId] as IndexRequest }

                if (requestsToRetry.isNotEmpty()) {
                    val retryCause = failedResponses.first { it.status() == RestStatus.TOO_MANY_REQUESTS }.failure.cause
                    throw ExceptionsHelper.convertToElastic(retryCause)
                }
            }
        }
    }

    // TODO: Doc counts for aggregations are showing the doc counts of the rollup docs and not the raw data which is expected...
    //  Elastic has a PR for a _doc_count mapping which we might be able to use but its in PR and they could change it
    //  Is there a way we can overwrite doc_count? On request/response? https://github.com/elastic/elasticsearch/pull/58339
    //  Perhaps try to save it in what will most likely be the correct way for that PR so we can reuse in the future?

    // TODO: Clean this up
    @Suppress("ComplexMethod")
    fun convertResponseToRequests(job: Rollup, internalComposite: InternalComposite): List<DocWriteRequest<*>> {
        val requests = mutableListOf<DocWriteRequest<*>>()
        internalComposite.buckets.forEach {
            // TODO: Come up with way to handle documentID - needs to be deterministic and unique for all rollup documents per rollup job
            //  For now just use the sorted keys for development
            val documentId = it.key.entries.sortedBy { it.key }.joinToString { it.value.toString() }
            // TODO: Move these somewhere else to be reused
            val mapOfKeyValues = mutableMapOf(
                "${Rollup.ROLLUP_TYPE}.$_ID" to job.id,
                "${Rollup.ROLLUP_TYPE}.doc_count" to it.docCount,
                "${Rollup.ROLLUP_TYPE}.${Rollup.SCHEMA_VERSION_FIELD}" to job.schemaVersion
            )
            val aggResults = mutableMapOf<String, MutableMap<String, Any>>()
            // TODO: Should we store more information about date_histogram and histogram on the rollup document or rely on it being on the rollup job?
            it.key.entries.forEach {
                val type = (job.dimensions.find { dim -> dim.targetField == it.key } as Dimension).type.type
                aggResults.computeIfAbsent(it.key) { mutableMapOf() }[type] = it.value
            }
            it.aggregations.forEach {
                when (it) {
                    // TODO: Clean up suffixes
                    is InternalSum -> aggResults.computeIfAbsent(it.name.removeSuffix((".sum"))) { mutableMapOf() }[it.type] = it.value
                    is InternalMax -> aggResults.computeIfAbsent(it.name.removeSuffix(".max")) { mutableMapOf() }[it.type] = it.value
                    is InternalMin -> aggResults.computeIfAbsent(it.name.removeSuffix(".min")) { mutableMapOf() }[it.type] = it.value
                    is InternalValueCount -> aggResults.computeIfAbsent(it.name.removeSuffix(".value_count")) { mutableMapOf() }[it.type] = it.value
                    is InternalAvg -> aggResults.computeIfAbsent(it.name.removeSuffix(".avg")) { mutableMapOf() }[it.type] = it.value
                    else -> logger.info("Unsupported aggregation") // TODO: error
                }
            }
            mapOfKeyValues.putAll(aggResults)
            val indexRequest = IndexRequest(job.targetIndex)
                .id(documentId)
                .source(mapOfKeyValues, XContentType.JSON)
            requests.add(indexRequest)
        }
        return requests
    }
}
