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
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
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
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestActionListener
import org.elasticsearch.rest.action.RestResponseListener

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

    @Suppress("SpreadOperator")
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
        channel: RestChannel,
        private val startState: String?
    ) : RestActionListener<ClusterStateResponse>(channel) {

        private val failedIndices: MutableList<FailedIndex> = mutableListOf()
        private val listOfIndexMetaData: MutableList<Pair<Index, ManagedIndexMetaData>> = mutableListOf()

        override fun processResponse(clusterStateResponse: ClusterStateResponse) {
            val state = clusterStateResponse.state
            populateList(state)

            val builder = channel.newBuilder().startObject()
            if (listOfIndexMetaData.isNotEmpty()) {
                val updateManagedIndexMetaDataRequest = UpdateManagedIndexMetaDataRequest(listOfIndexMetaData)

                try {
                    client.execute(UpdateManagedIndexMetaDataAction, updateManagedIndexMetaDataRequest,
                        object : RestResponseListener<AcknowledgedResponse>(channel) {
                            override fun buildResponse(acknowledgedResponse: AcknowledgedResponse): RestResponse {
                                if (acknowledgedResponse.isAcknowledged) {
                                    builder.field(UPDATED_INDICES, listOfIndexMetaData.size)
                                } else {
                                    failedIndices.addAll(listOfIndexMetaData.map {
                                        FailedIndex(it.first.name, it.first.uuid, "failed to update IndexMetaData")
                                    })
                                }
                                buildInvalidIndexResponse(builder, failedIndices)
                                return BytesRestResponse(RestStatus.OK, builder.endObject())
                            }
                        }
                    )
                } catch (e: ClusterBlockException) {
                    failedIndices.addAll(listOfIndexMetaData.map {
                        FailedIndex(it.first.name, it.first.uuid, "failed to update with ClusterBlockException. ${e.message}")
                    })
                    buildInvalidIndexResponse(builder, failedIndices)
                    channel.sendResponse(BytesRestResponse(RestStatus.OK, builder.endObject()))
                }
            } else {
                buildInvalidIndexResponse(builder, failedIndices)
                channel.sendResponse(BytesRestResponse(RestStatus.OK, builder.endObject()))
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
                    managedIndexMetaData.policyRetryInfo == null || !managedIndexMetaData.policyRetryInfo.failed ->
                        failedIndices.add(FailedIndex(indexMetaData.index.name, indexMetaData.index.uuid, "This index is not in failed state."))
                    else ->
                        listOfIndexMetaData.add(
                            Pair(
                                indexMetaData.index,
                                managedIndexMetaData.copy(
                                    stepMetaData = null,
                                    policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
                                    transitionTo = startState,
                                    info = mapOf("message" to "Attempting to retry")
                                )
                            )
                        )
                }
            }
        }
    }

    companion object {
        const val RETRY_BASE_URI = "${IndexStateManagementPlugin.ISM_BASE_URI}/retry"
    }
}
