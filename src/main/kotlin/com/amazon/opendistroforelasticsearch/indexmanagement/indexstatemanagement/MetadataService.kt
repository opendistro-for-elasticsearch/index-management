/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.retry
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getManagedIndexMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.managedIndexMetadataIndexRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.revertManagedIndexMetadataID
import com.amazon.opendistroforelasticsearch.indexmanagement.util.OpenForTesting
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.bulk.BackoffPolicy
import org.elasticsearch.action.bulk.BulkItemResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.Index
import org.elasticsearch.rest.RestStatus
import java.lang.Exception

/**
 * When all nodes have same version IM plugin (CDI/DDI finished)
 * MetadataService starts to move metadata from cluster state to config index
 */
@OpenForTesting
class MetadataService(
    private val client: Client,
    private val clusterService: ClusterService,
    private val skipExecution: SkipExecution,
    private val imIndices: IndexManagementIndices
) {
    private val logger = LogManager.getLogger(javaClass)

    @Volatile private var runningLock = false // in case 2 moveMetadata() process running

    private val successfullyIndexedIndices = mutableSetOf<metadataDocID>()
    private var failedToIndexIndices = mutableMapOf<metadataDocID, BulkItemResponse.Failure>()
    private var failedToCleanIndices = mutableSetOf<Index>()

    private var counter = 0

    // used in coordinator sweep to cancel scheduled process
    @Volatile final var finishFlag = false
        private set

    @Suppress("MagicNumber")
    @Volatile private var retryPolicy =
        BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(50), 3)

    @Suppress("ReturnCount", "LongMethod", "ComplexMethod")
    suspend fun moveMetadata() {
        if (runningLock) {
            logger.info("There is a move metadata process running...")
            return
        } else if (finishFlag) {
            logger.info("Move metadata has finished.")
            return
        }
        try {
            runningLock = true

            if (skipExecution.flag) {
                logger.info("Cluster still has nodes running old version of ISM plugin, skip ping execution on new nodes until all nodes upgraded")
                runningLock = false
                return
            }

            val indicesMetadata = clusterService.state().metadata.indices
            var clusterStateManagedIndexMetadata = indicesMetadata.map {
                it.key to it.value.getManagedIndexMetadata()
            }.filter { it.second != null }.distinct().toMap()
            // filter out previous failedToClean indices which already been indexed
            clusterStateManagedIndexMetadata =
                clusterStateManagedIndexMetadata.filter { it.key !in failedToCleanIndices.map { index -> index.name } }
            val indexUuidMap = clusterStateManagedIndexMetadata.map { indicesMetadata[it.key].indexUUID to it.key }.toMap()

            if (clusterStateManagedIndexMetadata.isEmpty()) {
                if (failedToCleanIndices.isNotEmpty()) {
                    logger.info("Failed to clean indices: $failedToCleanIndices. Only clean cluster state metadata in this run.")
                    cleanMetadatas(failedToCleanIndices.toList())
                    finishFlag = false; runningLock = false
                    return
                }
                if (counter++ > 2) {
                    logger.info("Move Metadata succeed, set finish flag to true. Indices failed to get indexed: $failedToIndexIndices")
                    finishFlag = true; runningLock = false
                    return
                }
            } else {
                counter = 0; finishFlag = false // index metadata for indices which metadata hasn't been indexed
                val bulkIndexReq =
                    clusterStateManagedIndexMetadata.mapNotNull { it.value }.map {
                        managedIndexMetadataIndexRequest(
                            it,
                            waitRefresh = false, // should be set at bulk request level
                            create = true // restrict this as create operation
                        )
                    }
                // remove the part which gonna be indexed from last time failedToIndex
                failedToIndexIndices = failedToIndexIndices.filterKeys { it !in indexUuidMap.keys }.toMutableMap()
                successfullyIndexedIndices.clear()
                indexMetadatas(bulkIndexReq)

                logger.debug("success indexed: ${successfullyIndexedIndices.map { indexUuidMap[it] }}")
                logger.debug(
                    "failed indexed: ${failedToIndexIndices.map { indexUuidMap[it.key] }};" +
                            "failed reason: ${failedToIndexIndices.values.distinct()}"
                )
            }

            // clean metadata for indices which metadata already been indexed
            val indicesToCleanMetadata =
                indexUuidMap.filter { it.key in successfullyIndexedIndices }.map { Index(it.value, it.key) }
                    .toList() + failedToCleanIndices

            cleanMetadatas(indicesToCleanMetadata)
            logger.debug("Failed to clean cluster metadata for: ${failedToCleanIndices.map { it.name }}")
        } finally {
            runningLock = false
        }
    }

    private suspend fun indexMetadatas(requests: List<DocWriteRequest<*>>) {
        if (requests.isEmpty()) return
        var requestsToRetry = requests

        // when we try to index sth to config index
        // we need to make sure the schema is up to date
        if (!imIndices.attemptUpdateConfigIndexMapping()) {
            logger.error("Failed to update config index mapping.")
            return
        }

        retryPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
            val bulkRequest = BulkRequest().add(requestsToRetry)
            val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
            val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }

            val retryIndexUuids = mutableListOf<Int>()
            bulkResponse.items.forEach {
                val indexUuid = revertManagedIndexMetadataID(it.id)
                if (it.isFailed) {
                    if (it.status() == RestStatus.TOO_MANY_REQUESTS) {
                        retryIndexUuids.add(it.itemId)
                    } else {
                        logger.error("failed reason: ${it.failure}, ${it.failureMessage}")
                        failedToIndexIndices[indexUuid] = it.failure
                    }
                } else {
                    successfullyIndexedIndices.add(indexUuid)
                    failedToIndexIndices.remove(indexUuid)
                }
            }
            requestsToRetry = retryIndexUuids.map { bulkRequest.requests()[it] }

            if (requestsToRetry.isNotEmpty()) {
                val retryCause = failedResponses.first { it.status() == RestStatus.TOO_MANY_REQUESTS }.failure.cause
                throw ExceptionsHelper.convertToElastic(retryCause)
            }
        }
    }

    private suspend fun cleanMetadatas(indices: List<Index>) {
        if (indices.isEmpty()) return

        val request = UpdateManagedIndexMetaDataRequest(indicesToRemoveManagedIndexMetaDataFrom = indices)
        try {
            retryPolicy.retry(logger) {
                val response: AcknowledgedResponse =
                    client.suspendUntil { execute(UpdateManagedIndexMetaDataAction.INSTANCE, request, it) }
                if (response.isAcknowledged) {
                    failedToCleanIndices.removeAll(indices)
                } else {
                    logger.error("Failed to clean cluster state metadata for indices: [$indices].")
                    failedToCleanIndices.addAll(indices)
                }
            }
        } catch (e: Exception) {
            logger.error("Failed to clean cluster state metadata for indices: [$indices].", e)
            failedToCleanIndices.addAll(indices)
        }
    }
}

typealias metadataDocID = String
