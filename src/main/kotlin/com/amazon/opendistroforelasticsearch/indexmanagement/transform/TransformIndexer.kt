package com.amazon.opendistroforelasticsearch.indexmanagement.transform

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.retry
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.exceptions.TransformIndexException
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.settings.TransformSettings
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
import org.elasticsearch.rest.RestStatus

class TransformIndexer(
    settings: Settings,
    clusterService: ClusterService,
    private val esClient: Client
) {

    private val logger = LogManager.getLogger(javaClass)

    @Volatile private var backoffPolicy = BackoffPolicy.constantBackoff(
            TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_MILLIS.get(settings),
            TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_COUNT.get(settings)
    )

    init {
        // To update the retry policy with updated settings
        clusterService.clusterSettings.addSettingsUpdateConsumer(
            TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_MILLIS,
            TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_COUNT
        ) {
            millis, count -> backoffPolicy = BackoffPolicy.constantBackoff(millis, count)
        }
    }

    suspend fun index(docsToIndex: List<DocWriteRequest<*>>): Long {
        var requests = docsToIndex
        var indexTimeInMillis = 0L
        try {
            if (requests.isNotEmpty()) {
                backoffPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
                    val bulkRequest = BulkRequest().add(docsToIndex)
                    val bulkResponse: BulkResponse = esClient.suspendUntil { bulk(bulkRequest, it) }
                    indexTimeInMillis += bulkResponse.took.millis
                    val failed = (bulkResponse.items ?: arrayOf()).filter { item -> item.isFailed }
                    requests = failed.filter { itemResponse ->
                        itemResponse.status() == RestStatus.TOO_MANY_REQUESTS
                    }.map { itemResponse ->
                        bulkRequest.requests()[itemResponse.itemId] as IndexRequest
                    }
                    if (requests.isNotEmpty()) {
                        val retryCause = failed.first { it.status() == RestStatus.TOO_MANY_REQUESTS }.failure.cause
                        throw ExceptionsHelper.convertToElastic(retryCause)
                    }
                }
            }
            return indexTimeInMillis
        } catch (e: Exception) {
            throw TransformIndexException("Failed to index the documents", e)
        }
    }
}
