package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getPolicyID
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.FailedIndex
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.isFailed
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.updateEnableManagedIndexRequest
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.index.Index
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

private val log = LogManager.getLogger(TransportRetryFailedManagedIndexAction::class.java)

class TransportRetryFailedManagedIndexAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters
) : HandledTransportAction<RetryFailedManagedIndexRequest, RetryFailedManagedIndexResponse>(
        RetryFailedManagedIndexAction.NAME, transportService, actionFilters, ::RetryFailedManagedIndexRequest
) {
    override fun doExecute(task: Task, request: RetryFailedManagedIndexRequest, listener: ActionListener<RetryFailedManagedIndexResponse>) {
        RetryFailedManagedIndexHandler(client, listener, request).start()
    }

    inner class RetryFailedManagedIndexHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<RetryFailedManagedIndexResponse>,
        private val request: RetryFailedManagedIndexRequest
    ) {
        private val failedIndices: MutableList<FailedIndex> = mutableListOf()
        private val listOfIndexMetaDataBulk: MutableList<ManagedIndexMetaData> = mutableListOf()
        private val listOfIndexMetaData: MutableList<Pair<Index, ManagedIndexMetaData>> = mutableListOf()
        private var updated: Int = 0

        @Suppress("SpreadOperator")
        fun start() {
            val strictExpandIndicesOptions = IndicesOptions.strictExpand()

            val clusterStateRequest = ClusterStateRequest()
            clusterStateRequest.masterNodeTimeout()
            clusterStateRequest.clear()
                    .indices(*request.indices.toTypedArray())
                    .metadata(true)
                    .local(false)
                    .indicesOptions(strictExpandIndicesOptions)

            client.admin()
                .cluster()
                .state(clusterStateRequest, object : ActionListener<ClusterStateResponse> {
                    override fun onResponse(response: ClusterStateResponse) {
                        processResponse(response)
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(t)
                    }
                })
        }

        fun processResponse(clusterStateResponse: ClusterStateResponse) {
            val state = clusterStateResponse.state
            populateList(state)

            if (listOfIndexMetaDataBulk.isNotEmpty()) {
                updateBulkRequest(listOfIndexMetaDataBulk.map { it.indexUuid })
            } else {
                updated = 0
                actionListener.onResponse(RetryFailedManagedIndexResponse(updated, failedIndices))
                return
            }
        }

        private fun populateList(state: ClusterState) {
            for (indexMetaDataEntry in state.metadata.indices) {
                val indexMetaData = indexMetaDataEntry.value
                val managedIndexMetaData = indexMetaData.getManagedIndexMetaData()
                when {
                    indexMetaData.getPolicyID() == null ->
                        failedIndices.add(FailedIndex(indexMetaData.index.name, indexMetaData.index.uuid, "This index is not being managed."))
                    managedIndexMetaData == null ->
                        failedIndices.add(FailedIndex(indexMetaData.index.name, indexMetaData.index.uuid, "There is no IndexMetaData information"))
                    !managedIndexMetaData.isFailed ->
                        failedIndices.add(FailedIndex(indexMetaData.index.name, indexMetaData.index.uuid, "This index is not in failed state."))
                    else ->
                        listOfIndexMetaDataBulk.add(managedIndexMetaData)
                }
            }
        }

        private fun updateBulkRequest(documentIds: List<String>) {
            val requestsToRetry = createEnableBulkRequest(documentIds)
            val bulkRequest = BulkRequest().add(requestsToRetry)

            client.bulk(bulkRequest, ActionListener.wrap(::onBulkResponse, ::onFailure))
        }

        private fun onBulkResponse(bulkResponse: BulkResponse) {
            for (bulkItemResponse in bulkResponse) {
                val managedIndexMetaData = listOfIndexMetaDataBulk.first { it.indexUuid == bulkItemResponse.id }
                if (bulkItemResponse.isFailed) {
                    failedIndices.add(FailedIndex(managedIndexMetaData.index, managedIndexMetaData.indexUuid, bulkItemResponse.failureMessage))
                } else {
                    listOfIndexMetaData.add(
                            Pair(Index(managedIndexMetaData.index, managedIndexMetaData.indexUuid), managedIndexMetaData.copy(
                                    stepMetaData = null,
                                    policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
                                    actionMetaData = managedIndexMetaData.actionMetaData?.copy(
                                            failed = false,
                                            consumedRetries = 0,
                                            lastRetryTime = null,
                                            startTime = null
                                    ),
                                    transitionTo = request.startState,
                                    info = mapOf("message" to "Attempting to retry")
                            ))
                    )
                }
            }

            if (listOfIndexMetaData.isNotEmpty()) {
                val updateManagedIndexMetaDataRequest =
                        UpdateManagedIndexMetaDataRequest(indicesToAddManagedIndexMetaDataTo = listOfIndexMetaData)
                client.execute(
                        UpdateManagedIndexMetaDataAction.INSTANCE,
                        updateManagedIndexMetaDataRequest,
                        ActionListener.wrap(::onUpdateManagedIndexMetaDataActionResponse, ::onFailure)
                )
            } else {
                updated = 0
                actionListener.onResponse(RetryFailedManagedIndexResponse(updated, failedIndices))
                return
            }
        }

        private fun createEnableBulkRequest(documentIds: List<String>): List<DocWriteRequest<*>> {
            return documentIds.map { updateEnableManagedIndexRequest(it) }
        }

        private fun onUpdateManagedIndexMetaDataActionResponse(response: AcknowledgedResponse) {
            if (response.isAcknowledged) {
                updated = listOfIndexMetaData.size
            } else {
                updated = 0
                failedIndices.addAll(listOfIndexMetaData.map {
                    FailedIndex(it.first.name, it.first.uuid, "failed to update IndexMetaData")
                })
            }
            actionListener.onResponse(RetryFailedManagedIndexResponse(updated, failedIndices))
            return
        }

        fun onFailure(e: Exception) {
            try {
                if (e is ClusterBlockException) {
                    failedIndices.addAll(listOfIndexMetaData.map {
                        FailedIndex(it.first.name, it.first.uuid, "failed to update with ClusterBlockException. ${e.message}")
                    })
                }

                updated = 0
                actionListener.onResponse(RetryFailedManagedIndexResponse(updated, failedIndices))
                return
            } catch (inner: Exception) {
                inner.addSuppressed(e)
                log.error("failed to send failure response", inner)
            }
        }
    }
}
