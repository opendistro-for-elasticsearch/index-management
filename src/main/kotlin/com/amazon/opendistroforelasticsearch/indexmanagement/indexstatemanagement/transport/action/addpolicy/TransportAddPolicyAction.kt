package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.addpolicy

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

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getPolicyID
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.FailedIndex
import org.elasticsearch.ElasticsearchTimeoutException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.Index
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService
import java.lang.Exception
import java.time.Duration
import java.time.Instant

class TransportAddPolicyAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters
) : HandledTransportAction<AddPolicyRequest, ISMStatusResponse>(
        AddPolicyAction.NAME, transportService, actionFilters, ::AddPolicyRequest
) {
    override fun doExecute(task: Task, request: AddPolicyRequest, listener: ActionListener<ISMStatusResponse>) {
        AddPolicyHandler(client, listener, request).start()
    }

    inner class AddPolicyHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<ISMStatusResponse>,
        private val request: AddPolicyRequest
    ) {
        private lateinit var startTime: Instant
        private val failedIndices: MutableList<FailedIndex> = mutableListOf()
        private val indicesToAddPolicyTo: MutableList<Index> = mutableListOf()
        private var updated: Int = 0

        @Suppress("SpreadOperator")
        fun start() {
            val strictExpandOptions = IndicesOptions.strictExpand()

            val clusterStateRequest = ClusterStateRequest()
                .clear()
                .indices(*request.indices.toTypedArray())
                .metadata(true)
                .local(false)
                .waitForTimeout(TimeValue.timeValueMillis(ADD_POLICY_TIMEOUT_IN_MILLIS))
                .indicesOptions(strictExpandOptions)

            startTime = Instant.now()

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

        @Suppress("SpreadOperator")
        fun processResponse(clusterStateResponse: ClusterStateResponse) {
            val state = clusterStateResponse.state
            populateLists(state)

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
                        .settings(Settings.builder().put(ManagedIndexSettings.POLICY_ID.key, request.policyID))
                        .timeout(TimeValue.timeValueMillis(updateSettingsTimeout))

                try {
                    client.execute(UpdateSettingsAction.INSTANCE, updateSettingsRequest,
                        object : ActionListener<AcknowledgedResponse> {
                            override fun onResponse(response: AcknowledgedResponse) {
                                if (response.isAcknowledged) {
                                    updated = indicesToAddPolicyTo.size
                                } else {
                                    updated = 0
                                    failedIndices.addAll(indicesToAddPolicyTo.map {
                                        FailedIndex(it.name, it.uuid, "Failed to add policy")
                                    })
                                }

                                actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
                            }

                            override fun onFailure(t: Exception) {
                                actionListener.onFailure(t)
                            }
                        }
                    )
                } catch (e: ClusterBlockException) {
                    failedIndices.addAll(indicesToAddPolicyTo.map {
                        FailedIndex(it.name, it.uuid, "Failed to add policy due to ClusterBlockingException: ${e.message}"
                        )
                    })
                    updated = 0
                    actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
                }
            } else {
                updated = 0
                actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
            }
        }

        private fun populateLists(state: ClusterState) {
            for (indexMetaDataEntry in state.metadata.indices) {
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
                    indexMetaData.state == IndexMetadata.State.CLOSE ->
                        failedIndices.add(FailedIndex(indexMetaData.index.name, indexMetaData.index.uuid, "This index is closed"))
                    else -> indicesToAddPolicyTo.add(indexMetaData.index)
                }
            }
        }
    }

    companion object {
        const val ADD_POLICY_TIMEOUT_IN_MILLIS = 30000L
    }
}
