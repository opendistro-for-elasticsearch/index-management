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
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupStats
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings.Companion.ROLLUP_INGEST_BACKOFF_COUNT
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings.Companion.ROLLUP_INGEST_BACKOFF_MILLIS
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.util.getInitialDocValues
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils.Companion.ODFE_MAGIC_NULL
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils.Companion.hashToFixedSize
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
import org.elasticsearch.transport.RemoteTransportException

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

    @Suppress("ReturnCount")
    suspend fun indexRollups(rollup: Rollup, internalComposite: InternalComposite): RollupIndexResult {
        try {
            var requestsToRetry = convertResponseToRequests(rollup, internalComposite)
            var stats = RollupStats(0, 0, requestsToRetry.size.toLong(), 0, 0)
            if (requestsToRetry.isNotEmpty()) {
                retryIngestPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
                    if (it.seconds >= (Rollup.ROLLUP_LOCK_DURATION_SECONDS / 2)) {
                        throw ExceptionsHelper.convertToElastic(
                            IllegalStateException("Cannot retry ingestion with a delay more than half of the rollup lock TTL")
                        )
                    }
                    val bulkRequest = BulkRequest().add(requestsToRetry)
                    val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
                    stats = stats.copy(indexTimeInMillis = stats.indexTimeInMillis + bulkResponse.took.millis)
                    val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }
                    requestsToRetry = failedResponses.filter { it.status() == RestStatus.TOO_MANY_REQUESTS }
                        .map { bulkRequest.requests()[it.itemId] as IndexRequest }

                    if (requestsToRetry.isNotEmpty()) {
                        val retryCause = failedResponses.first { it.status() == RestStatus.TOO_MANY_REQUESTS }.failure.cause
                        throw ExceptionsHelper.convertToElastic(retryCause)
                    }
                }
            }
            return RollupIndexResult.Success(stats)
        } catch (e: RemoteTransportException) {
            logger.error(e.message, e.cause)
            return RollupIndexResult.Failure(cause = ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: Exception) {
            logger.error(e.message, e.cause)
            return RollupIndexResult.Failure(cause = e)
        }
    }

    // TODO: Doc counts for aggregations are showing the doc counts of the rollup docs and not the raw data which is expected...
    //  Elastic has a PR for a _doc_count mapping which we might be able to use but its in PR and they could change it
    //  Is there a way we can overwrite doc_count? On request/response? https://github.com/elastic/elasticsearch/pull/58339
    //  Perhaps try to save it in what will most likely be the correct way for that PR so we can reuse in the future?
    @Suppress("ComplexMethod")
    fun convertResponseToRequests(job: Rollup, internalComposite: InternalComposite): List<DocWriteRequest<*>> {
        val requests = mutableListOf<DocWriteRequest<*>>()
        internalComposite.buckets.forEach {
            val docId = job.id + "#" + it.key.entries.joinToString("#") { it.value?.toString() ?: ODFE_MAGIC_NULL }
            val documentId = hashToFixedSize(docId)

            val mapOfKeyValues = job.getInitialDocValues(it.docCount)
            val aggResults = mutableMapOf<String, Any?>()
            it.key.entries.forEach { aggResults[it.key] = it.value }
            it.aggregations.forEach {
                when (it) {
                    is InternalSum -> aggResults[it.name] = it.value
                    is InternalMax -> aggResults[it.name] = it.value
                    is InternalMin -> aggResults[it.name] = it.value
                    is InternalValueCount -> aggResults[it.name] = it.value
                    is InternalAvg -> aggResults[it.name] = it.value
                    else -> throw IllegalStateException("Found aggregation in composite result that is not supported [${it.type} - ${it.name}]")
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

sealed class RollupIndexResult {
    data class Success(val stats: RollupStats) : RollupIndexResult()
    data class Failure(
        val message: String = "An error occurred while indexing to the rollup target index",
        val cause: Exception
    ) : RollupIndexResult()
}
