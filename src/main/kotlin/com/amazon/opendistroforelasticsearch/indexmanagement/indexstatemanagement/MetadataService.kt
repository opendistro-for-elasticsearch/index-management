package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.retry
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.managedIndexMetadataIndexRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.revertISMMetadataID
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
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.Index
import org.elasticsearch.rest.RestStatus
import java.lang.Exception

/**
 * When all nodes have same version IM plugin, MetadataService starts to move
 * metadata from cluster state to metadata index
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

    private val successIndexedIndices = mutableSetOf<metadataDocID>()
    private var failedToIndexIndices = mutableMapOf<metadataDocID, BulkItemResponse.Failure>()
    private var failedToCleanIndices = mutableListOf<Index>()

    private var counter = 0
    private var finishFlag = false // used in coordinator to cancel scheduled process
    fun isFinished() = finishFlag

    @Volatile
    private var retryPolicy =
        BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(50), 3)

    // just implement the business logic here, no need to do those listen to settings
    // schedule a runnable
    suspend fun moveMetadata() {
        if (runningLock) {
            logger.info("There is a move metadata process running...")
            return
        }
        runningLock = true

        if (skipExecution.flag) {
            logger.info("Cluster still has nodes running old version ISM plugin, skip execution on new nodes until all nodes upgraded")
            runningLock = false
            return
        }

        val indicesMetadata = clusterService.state().metadata.indices
        val clusterStateMetadata = indicesMetadata.map {
            it.key to it.value.getManagedIndexMetaData()
        }.filter { it.second != null }.distinct().toMap()
        // filter out previous failedToClean indices which already been indexed
        clusterStateMetadata.filter { it.key !in failedToCleanIndices.map { index -> index.name } }
        val indexUuidMap = clusterStateMetadata.map { it.key to indicesMetadata[it.key].indexUUID }.toMap()

        logger.info("cluster state metadata: ${clusterStateMetadata.keys}")

        // index metadata for indices which metadata hasn't been indexed
        val bulkIndexReq =
            clusterStateMetadata.mapNotNull { it.value }.map { managedIndexMetadataIndexRequest(it, false) }
        // remove the part which gonna be indexed from last time failedToIndex
        failedToIndexIndices = failedToIndexIndices.filterKeys { it !in indexUuidMap.values }.toMutableMap()
        if (bulkIndexReq.isEmpty()) {
            logger.info("Index metadata request is empty.")
            if (counter++ > 2) {
                logger.info("Move Metadata succeed, set finish flag to true. Failed to index indices: $failedToIndexIndices")
                finishFlag = true
                runningLock = false
                return
            }
        } else {
            counter = 0; finishFlag = false
        }

        successIndexedIndices.clear()
        indexMetadatas(bulkIndexReq)

        logger.info("index uuid map: $indexUuidMap")
        logger.info("success indexed: $successIndexedIndices")
        logger.info("failed indexed: $failedToIndexIndices")

        // clean metadata for indices which metadata already been indexed
        val indicesToCleanMetadata =
            indexUuidMap.filter { it.value in successIndexedIndices }.map { Index(it.key, it.value) }
                .toList() + failedToCleanIndices

        logger.info("clean metadata for indices: $indicesToCleanMetadata")
        cleanMetadatas(indicesToCleanMetadata.distinct())

        logger.info("failed clean: ${failedToCleanIndices.map { it.name }}")

        runningLock = false
    }

    suspend fun indexMetadatas(requests: List<DocWriteRequest<*>>) {
        if (requests.isEmpty()) return
        var requestsToRetry = requests

        logger.info("update config index mapping")
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
                val indexUuid = revertISMMetadataID(it.id)
                if (it.isFailed) {
                    if (it.status() == RestStatus.TOO_MANY_REQUESTS) {
                        retryIndexUuids.add(it.itemId)
                    } else {
                        logger.error("failed reason: ${it.failure}, ${it.failureMessage}")
                        failedToIndexIndices[indexUuid] = it.failure
                    }
                } else {
                    successIndexedIndices.add(indexUuid)
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
        } catch (e: ClusterBlockException) {
            logger.error(
                "Failed to clean cluster state metadata for indices: [$indices], due to cluster block exception.",
                e
            )
            failedToCleanIndices.addAll(indices)
        } catch (e: Exception) {
            logger.error("Failed to clean cluster state metadata for indices: [$indices].", e)
            failedToCleanIndices.addAll(indices)
        }
    }
}

typealias metadataDocID = String
