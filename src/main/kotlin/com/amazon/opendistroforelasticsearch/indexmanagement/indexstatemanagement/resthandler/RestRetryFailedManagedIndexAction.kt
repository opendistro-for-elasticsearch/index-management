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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.buildMgetMetadataRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getPolicyID
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.mgetResponseToList
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.FailedIndex
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.UPDATED_INDICES
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.buildInvalidIndexResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.isFailed
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.updateEnableManagedIndexRequest
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.MultiGetResponse
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.common.Strings
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.index.Index
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.BytesRestResponse
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestResponseListener

class RestRetryFailedManagedIndexAction : BaseRestHandler() {

    private val log = LogManager.getLogger(javaClass)

    override fun routes(): List<Route> {
        return listOf(
            Route(POST, RETRY_BASE_URI),
            Route(POST, "$RETRY_BASE_URI/{index}")
        )
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
            .metadata(true)
            .local(false)
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
        private val listOfMetadata: MutableList<ManagedIndexMetaData> = mutableListOf()
        private val listOfIndexToMetadata: MutableList<Pair<Index, ManagedIndexMetaData>> = mutableListOf()
        private lateinit var clusterState: ClusterState

        override fun onResponse(clusterStateResponse: ClusterStateResponse) {
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
                enableJobBulkRequest(listOfMetadata.map { it.indexUuid })
            } else {
                val builder = channel.newBuilder().startObject()
                builder.field(UPDATED_INDICES, 0)
                buildInvalidIndexResponse(builder, failedIndices)
                return channel.sendResponse(BytesRestResponse(RestStatus.OK, builder.endObject()))
            }
        }

        private fun enableJobBulkRequest(jobDocIds: List<String>) {
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
                                transitionTo = startState,
                                info = mapOf("message" to "Attempting to retry")
                            )
                        )
                    )
                }
            }

            if (listOfIndexToMetadata.isNotEmpty()) {
                val mapOfItemIdToIndex = mutableMapOf<Int, Index>()
                listOfIndexToMetadata.forEachIndexed { ind, (index, _) ->
                    mapOfItemIdToIndex[ind] = index
                }

                val updateMetadataRequests = listOfIndexToMetadata.map { (index, metadata) ->
                    log.info("try to save metadata [$metadata] in retry")
                    val builder = metadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS, true)
                    UpdateRequest(INDEX_MANAGEMENT_INDEX, index.uuid + "metadata").doc(builder)
                }
                val bulkUpdateMetadataRequest = BulkRequest().add(updateMetadataRequests)

                client.bulk(bulkUpdateMetadataRequest, onBulkUpdateMetadataResponse(mapOfItemIdToIndex))
            } else {
                val builder = channel.newBuilder().startObject()
                builder.field(UPDATED_INDICES, 0)
                buildInvalidIndexResponse(builder, failedIndices)
                return channel.sendResponse(BytesRestResponse(RestStatus.OK, builder.endObject()))
            }
        }

        private fun onBulkUpdateMetadataResponse(mapOfItemIdToIndex: Map<Int, Index>): RestResponseListener<BulkResponse> {
            return object : RestResponseListener<BulkResponse>(channel) {
                override fun buildResponse(bulkResponse: BulkResponse): RestResponse {
                    val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }
                    failedResponses.forEach {
                        val index = mapOfItemIdToIndex[it.itemId]
                        if (index != null) {
                            failedIndices.add(FailedIndex(index.name, index.uuid, "failed to update metadata for index ${index.name}"))
                        }
                    }

                    val builder = channel.newBuilder().startObject()
                    builder.field(UPDATED_INDICES, (bulkResponse.items ?: arrayOf()).size - failedResponses.size)
                    buildInvalidIndexResponse(builder, failedIndices)
                    return BytesRestResponse(RestStatus.OK, builder.endObject())
                }
            }
        }

        override fun onFailure(e: Exception) {
            try {
                val builder = channel.newBuilder().startObject()

                if (e is ClusterBlockException) {
                    failedIndices.addAll(listOfIndexToMetadata.map {
                        FailedIndex(it.first.name, it.first.uuid, "failed to update with ClusterBlockException. ${e.message}")
                    })
                }

                builder.field(UPDATED_INDICES, 0)
                buildInvalidIndexResponse(builder, failedIndices)
                return channel.sendResponse(BytesRestResponse(RestStatus.OK, builder.endObject()))
            } catch (inner: Exception) {
                inner.addSuppressed(e)
                log.error("failed to send failure response", inner)
            }
        }
    }

    companion object {
        const val RETRY_BASE_URI = "$ISM_BASE_URI/retry"
    }
}
