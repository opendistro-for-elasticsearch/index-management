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

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.ISM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.getPolicyID
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.FailedIndex
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.UPDATED_INDICES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.buildInvalidIndexResponse
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.common.Strings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.Index
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BytesRestResponse
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestController
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestActionListener
import org.elasticsearch.rest.action.RestResponseListener
import java.io.IOException

class RestRemovePolicyAction(settings: Settings, controller: RestController) : BaseRestHandler(settings) {

    init {
        controller.registerHandler(POST, REMOVE_POLICY_BASE_URI, this)
        controller.registerHandler(POST, "$REMOVE_POLICY_BASE_URI/{index}", this)
    }

    override fun getName(): String = "remove_policy_action"

    @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))

        if (indices.isNullOrEmpty()) {
            throw IllegalArgumentException("Missing indices")
        }

        val strictExpandOptions = IndicesOptions.strictExpand()

        val clusterStateRequest = ClusterStateRequest()
            .clear()
            .indices(*indices)
            .metaData(true)
            .local(false)
            .indicesOptions(strictExpandOptions)

        return RestChannelConsumer {
            client.admin()
                .cluster()
                .state(clusterStateRequest, RemovePolicyHandler(client, it))
        }
    }

    inner class RemovePolicyHandler(
        private val client: NodeClient,
        channel: RestChannel
    ) : RestActionListener<ClusterStateResponse>(channel) {

        private val failedIndices: MutableList<FailedIndex> = mutableListOf()
        private val indicesToRemovePolicyFrom: MutableList<Index> = mutableListOf()

        @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
        override fun processResponse(clusterStateResponse: ClusterStateResponse) {
            val state = clusterStateResponse.state
            populateLists(state)

            val builder = channel.newBuilder().startObject()
            if (indicesToRemovePolicyFrom.isNotEmpty()) {
                val updateSettingsRequest = UpdateSettingsRequest()
                    .indices(*indicesToRemovePolicyFrom.map { it.name }.toTypedArray())
                    .settings(
                        Settings.builder().putNull(ManagedIndexSettings.POLICY_ID.key)
                    )

                try {
                    client.execute(UpdateSettingsAction.INSTANCE, updateSettingsRequest,
                        object : RestResponseListener<AcknowledgedResponse>(channel) {
                            override fun buildResponse(response: AcknowledgedResponse): RestResponse {
                                if (response.isAcknowledged) {
                                    builder.field(UPDATED_INDICES, indicesToRemovePolicyFrom.size)
                                } else {
                                    builder.field(UPDATED_INDICES, 0)
                                    failedIndices.addAll(indicesToRemovePolicyFrom.map {
                                        FailedIndex(it.name, it.uuid, "Failed to remove policy")
                                    })
                                }

                                buildInvalidIndexResponse(builder, failedIndices)
                                return BytesRestResponse(RestStatus.OK, builder.endObject())
                            }
                        }
                    )
                } catch (e: ClusterBlockException) {
                    failedIndices.addAll(indicesToRemovePolicyFrom.map {
                        FailedIndex(it.name, it.uuid, "Failed to remove policy due to ClusterBlockException: ${e.message}")
                    })

                    builder.field(UPDATED_INDICES, 0)
                    buildInvalidIndexResponse(builder, failedIndices)
                    channel.sendResponse(BytesRestResponse(RestStatus.OK, builder.endObject()))
                }
            } else {
                builder.field(UPDATED_INDICES, 0)
                buildInvalidIndexResponse(builder, failedIndices)
                channel.sendResponse(BytesRestResponse(RestStatus.OK, builder.endObject()))
            }
        }

        private fun populateLists(state: ClusterState) {
            for (indexMetaDataEntry in state.metaData.indices) {
                val indexMetaData = indexMetaDataEntry.value
                when {
                    indexMetaData.getPolicyID() == null ->
                        failedIndices.add(
                            FailedIndex(indexMetaData.index.name, indexMetaData.index.uuid, "This index does not have a policy to remove")
                        )
                    indexMetaData.state == IndexMetaData.State.CLOSE ->
                        failedIndices.add(FailedIndex(indexMetaData.index.name, indexMetaData.index.uuid, "This index is closed"))
                    else -> indicesToRemovePolicyFrom.add(indexMetaData.index)
                }
            }
        }
    }

    companion object {
        const val REMOVE_POLICY_BASE_URI = "$ISM_BASE_URI/remove"
    }
}
