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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.getManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.getPolicyID
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataRequest
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.FailedIndex
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.UPDATED_INDICES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.buildInvalidIndexResponse
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.isFailed
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.updateEnableManagedIndexRequest
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.common.Strings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.index.Index
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BytesRestResponse
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestController
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestStatus

class RestRetryFailedManagedIndexAction(
    settings: Settings,
    controller: RestController
) : BaseRestHandler(settings) {

    private val log = LogManager.getLogger(javaClass)

    init {
        controller.registerHandler(RestRequest.Method.POST, RETRY_BASE_URI, this)
        controller.registerHandler(RestRequest.Method.POST, "$RETRY_BASE_URI/{index}", this)
    }

    override fun getName(): String {
        return "retry_failed_managed_index"
    }

    @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))
        if (indices == null || indices.isEmpty()) {
            throw IllegalArgumentException("Missing indices")
        }
        val body = if (request.hasContent()) {
            XContentHelper.convertToMap(request.requiredContent(), false, request.xContentType).v2()
        } else {
            mapOf()
        }

        val strictExpandIndicesOptions = IndicesOptions.strictExpand()

        val clusterStateRequest = ClusterStateRequest()
        clusterStateRequest.clear()
            .indices(*indices)
            .metaData(true)
            .masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()))
            .indicesOptions(strictExpandIndicesOptions)

        return RestChannelConsumer {
            client.admin()
                .cluster()
                .state(clusterStateRequest, IndexDestinationHandler(client, it, body["state"] as String?))
        }
    }

    inner class IndexDestinationHandler(
        private val client: NodeClient,
        private val channel: RestChannel,
        private val startState: String?
    ) : ActionListener<ClusterStateResponse> {

        private val failedIndices: MutableList<FailedIndex> = mutableListOf()
        private val listOfIndexMetaDataBulk: MutableList<ManagedIndexMetaData> = mutableListOf()

        private val listOfIndexMetaData: MutableList<Pair<Index, ManagedIndexMetaData>> = mutableListOf()

        override fun onResponse(clusterStateResponse: ClusterStateResponse) {
            val state = clusterStateResponse.state
            populateList(state)

            if (listOfIndexMetaDataBulk.isNotEmpty()) {
                updateBulkRequest(listOfIndexMetaDataBulk.map { it.indexUuid })
            } else {
                val builder = channel.newBuilder().startObject()
                buildInvalidIndexResponse(builder, failedIndices)
                return channel.sendResponse(BytesRestResponse(RestStatus.OK, builder.endObject()))
            }
        }

        private fun populateList(state: ClusterState) {
            for (indexMetaDataEntry in state.metaData.indices) {
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

            // TODO add ES retryPolicy on RestStatus.TOO_MANY_REQUESTS
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
                            actionMetaData = managedIndexMetaData.actionMetaData?.copy(failed = false, consumedRetries = 0, lastRetryTime = null),
                            transitionTo = startState,
                            errorNotificationFailure = null,
                            info = mapOf("message" to "Attempting to retry")
                        ))
                    )
                }
            }

            if (listOfIndexMetaData.isNotEmpty()) {
                val updateManagedIndexMetaDataRequest = UpdateManagedIndexMetaDataRequest(listOfIndexMetaData)
                client.execute(
                    UpdateManagedIndexMetaDataAction,
                    updateManagedIndexMetaDataRequest,
                    ActionListener.wrap(::onUpdateManagedIndexMetaDataActionResponse, ::onFailure)
                )
            } else {
                val builder = channel.newBuilder().startObject()
                buildInvalidIndexResponse(builder, failedIndices)
                return channel.sendResponse(BytesRestResponse(RestStatus.OK, builder.endObject()))
            }
        }

        private fun createEnableBulkRequest(documentIds: List<String>): List<DocWriteRequest<*>> {
            return documentIds.map { updateEnableManagedIndexRequest(it) }
        }

        private fun onUpdateManagedIndexMetaDataActionResponse(response: AcknowledgedResponse) {
            val builder = channel.newBuilder().startObject()
            if (response.isAcknowledged) {
                builder.field(UPDATED_INDICES, listOfIndexMetaData.size)
            } else {
                failedIndices.addAll(listOfIndexMetaData.map {
                    FailedIndex(it.first.name, it.first.uuid, "failed to update IndexMetaData")
                })
            }
            buildInvalidIndexResponse(builder, failedIndices)
            return channel.sendResponse(BytesRestResponse(RestStatus.OK, builder.endObject()))
        }

        override fun onFailure(e: Exception) {
            try {
                val builder = channel.newBuilder().startObject()

                if (e is ClusterBlockException) {
                    failedIndices.addAll(listOfIndexMetaData.map {
                        FailedIndex(it.first.name, it.first.uuid, "failed to update with ClusterBlockException. ${e.message}")
                    })
                }

                buildInvalidIndexResponse(builder, failedIndices)
                return channel.sendResponse(BytesRestResponse(RestStatus.OK, builder.endObject()))
            } catch (inner: Exception) {
                inner.addSuppressed(e)
                log.error("failed to send failure response", inner)
            }
        }
    }

    companion object {
        const val RETRY_BASE_URI = "${IndexStateManagementPlugin.ISM_BASE_URI}/retry"
    }
}
