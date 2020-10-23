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
import org.elasticsearch.common.UUIDs
import org.elasticsearch.common.hash.MurmurHash3
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite
import org.elasticsearch.search.aggregations.metrics.InternalAvg
import org.elasticsearch.search.aggregations.metrics.InternalMax
import org.elasticsearch.search.aggregations.metrics.InternalMin
import org.elasticsearch.search.aggregations.metrics.InternalSum
import org.elasticsearch.search.aggregations.metrics.InternalValueCount
import java.util.Random

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
    suspend fun indexRollups(rollup: Rollup, internalComposite: InternalComposite): RollupIndexResult {
        var requestsToRetry = convertResponseToRequests(rollup, internalComposite)
        try {
            var stats = RollupStats(0, 0, requestsToRetry.size.toLong(), 0, 0)
            if (requestsToRetry.isNotEmpty()) {
                retryIngestPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
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
        } catch (e: Exception) { // TODO: other exceptions
            logger.error(e.message, e.cause)
            return RollupIndexResult.Failure(cause = e)
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
            // TODO: We're hashing and then providing as a seed into random which seems like
            //  unneeded work, look into the randomBase64 to see if we can skip the random part - has to be before initial release
            val docId = job.id + "#" + it.key.entries.joinToString("#") { it.value?.toString() ?: "#ODFE-MAGIC-NULL-MAGIC-ODFE#" }
            val docByteArray = docId.toByteArray()
            val hash = MurmurHash3.hash128(docByteArray, 0, docByteArray.size, DOCUMENT_ID_SEED, MurmurHash3.Hash128())

            val uuid1 = UUIDs.randomBase64UUID(Random(hash.h1))
            val uuid2 = UUIDs.randomBase64UUID(Random(hash.h2))
            val documentId = "${job.id}#$uuid1#$uuid2"
            // TODO: Move these somewhere else to be reused
            val mapOfKeyValues = mutableMapOf<String, Any?>(
                "${Rollup.ROLLUP_TYPE}.$_ID" to job.id,
                "${Rollup.ROLLUP_TYPE}.doc_count" to it.docCount,
                "${Rollup.ROLLUP_TYPE}.${Rollup.SCHEMA_VERSION_FIELD}" to job.schemaVersion
            )
            val aggResults = mutableMapOf<String, Any?>()
            // TODO: Should we store more information about date_histogram and histogram on the rollup document or rely on it being on the rollup job?
            it.key.entries.forEach { aggResults[it.key] = it.value }
            it.aggregations.forEach {
                when (it) {
                    // TODO: Clean up suffixes
                    is InternalSum -> aggResults[it.name] = it.value
                    is InternalMax -> aggResults[it.name] = it.value
                    is InternalMin -> aggResults[it.name] = it.value
                    is InternalValueCount -> aggResults[it.name] = it.value
                    is InternalAvg -> aggResults[it.name] = it.value
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

    companion object {
        const val DOCUMENT_ID_SEED = 72390L
    }
}

sealed class RollupIndexResult {
    data class Success(val stats: RollupStats) : RollupIndexResult()
    data class Failure(
        val message: String = "An error occurred while indexing to the rollup target index",
        val cause: Exception
    ) : RollupIndexResult()
}
