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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.removepolicy

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getPolicyID
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.FailedIndex
import org.apache.logging.log4j.LogManager
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
import org.elasticsearch.index.Index
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

private val log = LogManager.getLogger(TransportRemovePolicyAction::class.java)

class TransportRemovePolicyAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters
) : HandledTransportAction<RemovePolicyRequest, ISMStatusResponse>(
        RemovePolicyAction.NAME, transportService, actionFilters, ::RemovePolicyRequest
) {
    override fun doExecute(task: Task, request: RemovePolicyRequest, listener: ActionListener<ISMStatusResponse>) {
        RemovePolicyHandler(client, listener, request).start()
    }

    inner class RemovePolicyHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<ISMStatusResponse>,
        private val request: RemovePolicyRequest
    ) {

        private val failedIndices: MutableList<FailedIndex> = mutableListOf()
        private val indicesToRemovePolicyFrom: MutableList<Index> = mutableListOf()
        private var updated: Int = 0

        @Suppress("SpreadOperator")
        fun start() {
            val strictExpandOptions = IndicesOptions.strictExpand()

            val clusterStateRequest = ClusterStateRequest()
                .clear()
                .indices(*request.indices.toTypedArray())
                .metadata(true)
                .local(false)
                .indicesOptions(strictExpandOptions)

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

        @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
        fun processResponse(clusterStateResponse: ClusterStateResponse) {
            val state = clusterStateResponse.state
            populateLists(state)

            if (indicesToRemovePolicyFrom.isNotEmpty()) {
                val updateSettingsRequest = UpdateSettingsRequest()
                    .indices(*indicesToRemovePolicyFrom.map { it.name }.toTypedArray())
                    .settings(Settings.builder().putNull(ManagedIndexSettings.POLICY_ID.key))

                try {
                    client.execute(UpdateSettingsAction.INSTANCE, updateSettingsRequest,
                        object : ActionListener<AcknowledgedResponse> {
                            override fun onResponse(response: AcknowledgedResponse) {
                                if (response.isAcknowledged) {
                                    updated = indicesToRemovePolicyFrom.size
                                } else {
                                    updated = 0
                                    failedIndices.addAll(indicesToRemovePolicyFrom.map {
                                        FailedIndex(it.name, it.uuid, "Failed to remove policy")
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
                    failedIndices.addAll(indicesToRemovePolicyFrom.map {
                        FailedIndex(it.name, it.uuid, "Failed to remove policy due to ClusterBlockException: ${e.message}")
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
                    indexMetaData.getPolicyID() == null ->
                        failedIndices.add(
                            FailedIndex(indexMetaData.index.name, indexMetaData.index.uuid, "This index does not have a policy to remove")
                        )
                    indexMetaData.state == IndexMetadata.State.CLOSE ->
                        failedIndices.add(FailedIndex(indexMetaData.index.name, indexMetaData.index.uuid, "This index is closed"))
                    else -> indicesToRemovePolicyFrom.add(indexMetaData.index)
                }
            }
        }
    }
}
