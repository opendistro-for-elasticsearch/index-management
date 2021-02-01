/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.FailedIndex
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.isFailed
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.updateEnableManagedIndexRequest
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.MultiGetRequest
import org.elasticsearch.action.get.MultiGetResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.index.Index
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

private val log = LogManager.getLogger(TransportRetryFailedManagedIndexAction::class.java)

class TransportRetryFailedManagedIndexAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters
) : HandledTransportAction<RetryFailedManagedIndexRequest, ISMStatusResponse>(
        RetryFailedManagedIndexAction.NAME, transportService, actionFilters, ::RetryFailedManagedIndexRequest
) {
    override fun doExecute(task: Task, request: RetryFailedManagedIndexRequest, listener: ActionListener<ISMStatusResponse>) {
        RetryFailedManagedIndexHandler(client, listener, request).start()
    }

    inner class RetryFailedManagedIndexHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<ISMStatusResponse>,
        private val request: RetryFailedManagedIndexRequest
    ) {
        private val failedIndices: MutableList<FailedIndex> = mutableListOf()
        private val listOfIndexMetaDataBulk: MutableList<ManagedIndexMetaData> = mutableListOf()
        private val listOfIndexMetaData: MutableList<Pair<Index, ManagedIndexMetaData>> = mutableListOf()
        private var updated: Int = 0
        private val indicesManagedState: MutableMap<String, Boolean> = mutableMapOf()
        private var indicesToRetry = mutableMapOf<String, String>() // uuid: name

        @Suppress("SpreadOperator")
        fun start() {
            val strictExpandIndicesOptions = IndicesOptions.strictExpand()

            val clusterStateRequest = ClusterStateRequest()
            clusterStateRequest.clear()
                .indices(*request.indices.toTypedArray())
                .metadata(true)
                .local(false)
                .masterNodeTimeout(request.masterTimeout)
                .indicesOptions(strictExpandIndicesOptions)

            client.admin()
                .cluster()
                .state(clusterStateRequest, object : ActionListener<ClusterStateResponse> {
                    override fun onResponse(response: ClusterStateResponse) {
                        val indexMetadatas = response.state.metadata.indices
                        indexMetadatas.forEach {
                            indicesToRetry.putIfAbsent(it.value.indexUUID, it.key)
                        }
                        processResponse(response)
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                    }
                })
        }

        fun processResponse(clusterStateResponse: ClusterStateResponse) {
            val mReq = MultiGetRequest()
            clusterStateResponse.state.metadata.indices.map { it.value.indexUUID }
                .forEach { mReq.add(INDEX_MANAGEMENT_INDEX, it) }

            client.multiGet(mReq, object : ActionListener<MultiGetResponse> {
                override fun onResponse(response: MultiGetResponse) {
                    // config index may not be initialized
                    val f = response.responses.first()
                    if (f.isFailed && f.failure.failure is IndexNotFoundException) {
                        indicesToRetry.forEach { (uuid, name) ->
                            failedIndices.add(FailedIndex(name, uuid, "This index is not being managed."))
                        }
                        actionListener.onResponse(ISMStatusResponse(0, failedIndices))
                        return
                    }

                    response.forEach {
                        log.info("index ${it.id} is exist? ${it.response.isExists}")
                        indicesManagedState[it.id] = it.response.isExists
                    }

                    populateList(clusterStateResponse.state)

                    if (listOfIndexMetaDataBulk.isNotEmpty()) {
                        updateBulkRequest(listOfIndexMetaDataBulk.map { it.indexUuid })
                    } else {
                        updated = 0
                        actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
                        return
                    }
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                }
            })
        }

        private fun populateList(state: ClusterState) {
            for (indexMetaDataEntry in state.metadata.indices) {
                val indexMetaData = indexMetaDataEntry.value
                val managedIndexMetaData = indexMetaData.getManagedIndexMetaData()
                when {
                    indicesManagedState[indexMetaData.indexUUID] == false ->
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
                actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
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
            actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
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
                actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
                return
            } catch (inner: Exception) {
                inner.addSuppressed(e)
                log.error("failed to send failure response", inner)
            }
        }
    }
}
