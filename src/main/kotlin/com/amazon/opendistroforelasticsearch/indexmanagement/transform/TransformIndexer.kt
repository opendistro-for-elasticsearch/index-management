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
import org.elasticsearch.transport.RemoteTransportException

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
        var updatableDocsToIndex = docsToIndex
        var indexTimeInMillis = 0L
        try {
            if (updatableDocsToIndex.isNotEmpty()) {
                logger.debug("Attempting to index ${updatableDocsToIndex.size} documents to ${updatableDocsToIndex.first().index()}")
                backoffPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
                    val bulkRequest = BulkRequest().add(updatableDocsToIndex)
                    val bulkResponse: BulkResponse = esClient.suspendUntil { bulk(bulkRequest, it) }
                    indexTimeInMillis += bulkResponse.took.millis

                    val failed = (bulkResponse.items ?: arrayOf()).filter { item -> item.isFailed }

                    updatableDocsToIndex = failed.map { itemResponse ->
                        bulkRequest.requests()[itemResponse.itemId] as IndexRequest
                    }
                    if (updatableDocsToIndex.isNotEmpty()) {
                        val retryCause = failed.first().failure.cause
                        throw ExceptionsHelper.convertToElastic(retryCause)
                    }
                }
            }
            return indexTimeInMillis
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            throw TransformIndexException("Failed to index the documents", unwrappedException)
        } catch (e: Exception) {
            throw TransformIndexException("Failed to index the documents", e)
        }
    }
}
