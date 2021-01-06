/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.explain

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getPolicyID
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.get.MultiGetRequest
import org.elasticsearch.action.get.MultiGetResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

private val log = LogManager.getLogger(TransportExplainAction::class.java)

class TransportExplainAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<ExplainRequest, ExplainResponse>(
        ExplainAction.NAME, transportService, actionFilters, ::ExplainRequest
) {
    override fun doExecute(task: Task, request: ExplainRequest, listener: ActionListener<ExplainResponse>) {
        ExplainHandler(client, listener, request).start()
    }

    inner class ExplainHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<ExplainResponse>,
        private val request: ExplainRequest
    ) {
        lateinit var response: GetResponse

        private val indexNames = mutableListOf<String>()
        private val indexMetadataUuids = mutableListOf<String>()
        private val indexPolicyIds = mutableListOf<String?>()

        @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
        fun start() {
            val clusterStateRequest = ClusterStateRequest()
            val strictExpandIndicesOptions = IndicesOptions.strictExpand()

            clusterStateRequest.clear()
                .indices(*request.indices.toTypedArray())
                .metadata(true)
                .indicesOptions(strictExpandIndicesOptions)

            client.admin().cluster().state(clusterStateRequest, object : ActionListener<ClusterStateResponse> {
                override fun onResponse(response: ClusterStateResponse) {
                    onClusterStateResponse(response)
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(t)
                }
            })
        }

        // retrieve index uuid from cluster state
        private fun onClusterStateResponse(response: ClusterStateResponse) {
            for (indexMetadataEntry in response.state.metadata.indices) {
                indexNames.add(indexMetadataEntry.key)
                indexMetadataUuids.add(indexMetadataEntry.value.indexUUID + "metadata")
                indexPolicyIds.add(indexMetadataEntry.value.getPolicyID())
            }

            val mgetRequest = MultiGetRequest()
            indexMetadataUuids.forEach { mgetRequest.add(MultiGetRequest.Item(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, it)) }
            client.multiGet(mgetRequest, object : ActionListener<MultiGetResponse> {
                override fun onResponse(response: MultiGetResponse) {
                    processResponse(response)
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(t)
                }
            })
        }

        fun processResponse(response: MultiGetResponse) {
            val indexMetadatas = mutableListOf<ManagedIndexMetaData?>()

            response.responses.forEach {
                log.info("explain response: ${it.id}")
                if (it.response != null) {
                    indexMetadatas.add(getMetadata(it.response))
                } else {
                    indexMetadatas.add(null)
                }
            }

            actionListener.onResponse(ExplainResponse(indexNames, indexPolicyIds, indexMetadatas))
        }

        private fun getMetadata(response: GetResponse): ManagedIndexMetaData? {
            log.info("transfer multiget response toXContent for ${response.sourceAsMap}")
            if (response.sourceAsBytesRef == null) return null

            val xcp = XContentHelper.createParser(
                xContentRegistry,
                LoggingDeprecationHandler.INSTANCE,
                response.sourceAsBytesRef,
                XContentType.JSON)
            return ManagedIndexMetaData.parseWithType(xcp,
                response.id, response.seqNo, response.primaryTerm)
        }
    }
}
