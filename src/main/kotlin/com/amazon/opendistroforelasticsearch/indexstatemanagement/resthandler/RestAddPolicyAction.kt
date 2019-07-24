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
import org.elasticsearch.ElasticsearchTimeoutException
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
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentHelper
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
import java.time.Duration
import java.time.Instant

class RestAddPolicyAction(settings: Settings, controller: RestController) : BaseRestHandler(settings) {

    init {
        controller.registerHandler(POST, ADD_POLICY_BASE_URI, this)
        controller.registerHandler(POST, "$ADD_POLICY_BASE_URI/{index}", this)
    }

    override fun getName(): String = "add_policy_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))

        if (indices.isNullOrEmpty()) {
            throw IllegalArgumentException("Missing indices")
        }

        val body = if (request.hasContent()) {
            XContentHelper.convertToMap(request.requiredContent(), false, request.xContentType).v2()
        } else {
            mapOf()
        }

        val policyID = requireNotNull(body.getOrDefault("policy_id", null)) { "Missing policy_id" }

        val strictExpandOptions = IndicesOptions.strictExpand()

        val clusterStateRequest = ClusterStateRequest()
            .clear()
            .indices(*indices)
            .metaData(true)
            .waitForTimeout(TimeValue.timeValueMillis(ADD_POLICY_TIMEOUT_IN_MILLIS))
            .indicesOptions(strictExpandOptions)

        val startTime = Instant.now()
        return RestChannelConsumer {
            client.admin()
                .cluster()
                .state(clusterStateRequest, AddPolicyHandler(client, it, policyID as String, startTime))
        }
    }

    inner class AddPolicyHandler(
        private val client: NodeClient,
        channel: RestChannel,
        private val policyID: String,
        private val startTime: Instant
    ) : RestActionListener<ClusterStateResponse>(channel) {

        private val failedIndices: MutableList<FailedIndex> = mutableListOf()
        private val indicesToAddPolicyTo: MutableList<Index> = mutableListOf()

        override fun processResponse(clusterStateResponse: ClusterStateResponse) {
            val state = clusterStateResponse.state
            populateLists(state)

            val builder = channel.newBuilder().startObject()
            if (indicesToAddPolicyTo.isNotEmpty()) {
                val timeSinceClusterStateRequest: Duration = Duration.between(startTime, Instant.now())

                // Timeout for UpdateSettingsRequest in milliseconds
                val updateSettingsTimeout = ADD_POLICY_TIMEOUT_IN_MILLIS - timeSinceClusterStateRequest.toMillis()

                // If after the ClusterStateResponse we go over the timeout for Add Policy (30 seconds), throw an
                // exception since UpdateSettingsRequest cannot have a negative timeout
                if (updateSettingsTimeout < 0) {
                    throw ElasticsearchTimeoutException("Add policy API timed out after ClusterStateResponse")
                }

                val updateSettingsRequest = UpdateSettingsRequest()
                    .indices(*indicesToAddPolicyTo.map { it.name }.toTypedArray())
                    .settings(Settings.builder().put(ManagedIndexSettings.POLICY_ID.key, policyID))
                    .timeout(TimeValue.timeValueMillis(updateSettingsTimeout))

                try {
                    client.execute(UpdateSettingsAction.INSTANCE, updateSettingsRequest,
                        object : RestResponseListener<AcknowledgedResponse>(channel) {
                            override fun buildResponse(response: AcknowledgedResponse): RestResponse {
                                if (response.isAcknowledged) {
                                    builder.field(UPDATED_INDICES, indicesToAddPolicyTo.size)
                                } else {
                                    failedIndices.addAll(indicesToAddPolicyTo.map {
                                        FailedIndex(it.name, it.uuid, "Failed to add policy")
                                    })
                                }

                                buildInvalidIndexResponse(builder, failedIndices)
                                return BytesRestResponse(RestStatus.OK, builder.endObject())
                            }
                        }
                    )
                } catch (e: ClusterBlockException) {
                    failedIndices.addAll(indicesToAddPolicyTo.map {
                        FailedIndex(it.name, it.uuid, "Failed to add policy due to ClusterBlockingException: ${e.message}"
                        )
                    })

                    buildInvalidIndexResponse(builder, failedIndices)
                    channel.sendResponse(BytesRestResponse(RestStatus.OK, builder.endObject()))
                }
            } else {
                buildInvalidIndexResponse(builder, failedIndices)
                channel.sendResponse(BytesRestResponse(RestStatus.OK, builder.endObject()))
            }
        }

        private fun populateLists(state: ClusterState) {
            for (indexMetaDataEntry in state.metaData.indices) {
                val indexMetaData = indexMetaDataEntry.value
                when {
                    indexMetaData.getPolicyID() != null ->
                        failedIndices.add(
                            FailedIndex(
                                indexMetaData.index.name,
                                indexMetaData.index.uuid,
                                "This index already has a policy, use the update policy API to update index policies"
                            )
                        )
                    indexMetaData.state == IndexMetaData.State.CLOSE ->
                        failedIndices.add(FailedIndex(indexMetaData.index.name, indexMetaData.index.uuid, "This index is closed"))
                    else -> indicesToAddPolicyTo.add(indexMetaData.index)
                }
            }
        }
    }

    companion object {
        const val ADD_POLICY_BASE_URI = "$ISM_BASE_URI/add"

        const val ADD_POLICY_TIMEOUT_IN_MILLIS = 30000L
    }
}
