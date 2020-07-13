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

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.buildMgetMetadataRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getPolicyID
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.mgetResponseToList
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.FailedIndex
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.isFailed
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.updateEnableManagedIndexRequest
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.MultiGetResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.index.Index
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
        private val listOfMetadata: MutableList<ManagedIndexMetaData> = mutableListOf()
        private val listOfIndexToMetadata: MutableList<Pair<Index, ManagedIndexMetaData>> = mutableListOf()
        private val mapOfItemIdToIndex: MutableMap<Int, Index> = mutableMapOf()
        private lateinit var clusterState: ClusterState
        private var updated: Int = 0

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
                        processResponse(response)
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(t)
                    }
                })
        }

        fun processResponse(clusterStateResponse: ClusterStateResponse) {
            clusterState = clusterStateResponse.state

            // get back metadata first to populate list
            client.multiGet(buildMgetMetadataRequest(clusterState), ActionListener.wrap(::onMgetMetadataResponse, ::onFailure))
        }

        private fun onMgetMetadataResponse(mgetResponse: MultiGetResponse) {
            val metadataList = mgetResponseToList(mgetResponse)
            clusterState.metadata.indices.forEachIndexed { ind, it ->
                val indexMetaData = it.value
                val managedIndexMetaData = metadataList[ind]
                log.info("metadata: $managedIndexMetaData for index ${it.key}")
                when {
                    indexMetaData.getPolicyID() == null ->
                        failedIndices.add(FailedIndex(indexMetaData.index.name, indexMetaData.index.uuid,
                            "This index is not being managed."))
                    managedIndexMetaData == null ->
                        failedIndices.add(FailedIndex(indexMetaData.index.name, indexMetaData.index.uuid,
                            "This index has no metadata information"))
                    !managedIndexMetaData.isFailed ->
                        failedIndices.add(FailedIndex(indexMetaData.index.name, indexMetaData.index.uuid,
                            "This index is not in failed state."))
                    else ->
                        listOfMetadata.add(managedIndexMetaData)
                }
            }

            if (listOfMetadata.isNotEmpty()) {
                bulkEnableJob(listOfMetadata.map { it.indexUuid })
            } else {
                updated = 0
                actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
            }
        }

        private fun bulkEnableJob(jobDocIds: List<String>) {
            val requestsToRetry = jobDocIds.map { updateEnableManagedIndexRequest(it) }
            val bulkRequest = BulkRequest().add(requestsToRetry)

            client.bulk(bulkRequest, ActionListener.wrap(::onEnableJobBulkResponse, ::onFailure))
        }

        private fun onEnableJobBulkResponse(bulkResponse: BulkResponse) {
            for (bulkItemResponse in bulkResponse) {
                log.info("enable job bulk response item id: ${bulkItemResponse.id}")
                val managedIndexMetaData = listOfMetadata.first { it.indexUuid == bulkItemResponse.id }
                if (bulkItemResponse.isFailed) {
                    failedIndices.add(FailedIndex(managedIndexMetaData.index, managedIndexMetaData.indexUuid, bulkItemResponse.failureMessage))
                } else {
                    listOfIndexToMetadata.add(
                        Pair(
                            Index(managedIndexMetaData.index, managedIndexMetaData.indexUuid),
                            managedIndexMetaData.copy(
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
                            )
                        )
                    )
                }
            }

            if (listOfIndexToMetadata.isNotEmpty()) {
                listOfIndexToMetadata.forEachIndexed { ind, (index, _) ->
                    mapOfItemIdToIndex[ind] = index
                }

                val updateMetadataRequests = listOfIndexToMetadata.map { (index, metadata) ->
                    log.info("try to save metadata [$metadata] in retry")
                    val builder = metadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS, true)
                    UpdateRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, index.uuid + "metadata").doc(builder)
                }
                val bulkUpdateMetadataRequest = BulkRequest().add(updateMetadataRequests)

                client.bulk(bulkUpdateMetadataRequest, ActionListener.wrap(::onBulkUpdateMetadataResponse, ::onFailure))
            } else {
                updated = 0
                actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
            }
        }

        private fun onBulkUpdateMetadataResponse(bulkResponse: BulkResponse) {
            val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }
            failedResponses.forEach {
                val index = mapOfItemIdToIndex[it.itemId]
                if (index != null) {
                    failedIndices.add(FailedIndex(index.name, index.uuid, "failed to update metadata for index ${index.name}"))
                }
            }

            updated = (bulkResponse.items ?: arrayOf()).size - failedResponses.size
            actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
        }

        fun onFailure(e: Exception) {
            try {
                if (e is ClusterBlockException) {
                    failedIndices.addAll(listOfIndexToMetadata.map {
                        FailedIndex(it.first.name, it.first.uuid, "failed to update with ClusterBlockException. ${e.message}")
                    })
                }

                updated = 0
                actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
            } catch (inner: Exception) {
                inner.addSuppressed(e)
                log.error("failed to send failure response", inner)
            }
        }
    }
}
