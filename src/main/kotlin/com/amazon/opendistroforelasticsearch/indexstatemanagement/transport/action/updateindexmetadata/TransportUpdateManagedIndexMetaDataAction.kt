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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.transport.action.updateindexmetadata

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementHistory
import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.TransportMasterNodeAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.AckedClusterStateUpdateTask
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.metadata.MetaData
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.util.function.Supplier

class TransportUpdateManagedIndexMetaDataAction : TransportMasterNodeAction<UpdateManagedIndexMetaDataRequest, AcknowledgedResponse> {

    @Inject
    constructor(
        client: Client,
        threadPool: ThreadPool,
        clusterService: ClusterService,
        transportService: TransportService,
        actionFilters: ActionFilters,
        indexNameExpressionResolver: IndexNameExpressionResolver,
        indexStateManagementHistory: IndexStateManagementHistory
    ) : super(
        UpdateManagedIndexMetaDataAction.name(),
        transportService,
        clusterService,
        threadPool,
        actionFilters,
        indexNameExpressionResolver,
        Supplier { UpdateManagedIndexMetaDataRequest() }
    ) {
        this.client = client
        this.indexStateManagementHistory = indexStateManagementHistory
    }

    private val log = LogManager.getLogger(javaClass)
    private val client: Client
    private val indexStateManagementHistory: IndexStateManagementHistory

    override fun checkBlock(request: UpdateManagedIndexMetaDataRequest, state: ClusterState): ClusterBlockException? {
        // https://github.com/elastic/elasticsearch/commit/ae14b4e6f96b554ca8f4aaf4039b468f52df0123
        // This commit will help us to give each individual index name and the error that is cause it. For now it will be a generic error message.
        val indicesToAddTo = request.indicesToAddManagedIndexMetaDataTo.map { it.first.name }.toTypedArray()
        val indicesToRemoveFrom = request.indicesToRemoveManagedIndexMetaDataFrom.map { it.name }.toTypedArray()
        val indices = indicesToAddTo + indicesToRemoveFrom

        return state.blocks.indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indices)
    }

    override fun masterOperation(
        request: UpdateManagedIndexMetaDataRequest,
        state: ClusterState,
        listener: ActionListener<AcknowledgedResponse>
    ) {
        clusterService.submitStateUpdateTask(
            IndexStateManagementPlugin.PLUGIN_NAME,
            object : AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {
                override fun execute(currentState: ClusterState): ClusterState {
                    // If there are no indices to make changes to, return early.
                    // Also doing this because when creating a metaDataBuilder and making no changes to it, for some
                    // reason the task does not complete, leading to indefinite suspension.
                    if (request.indicesToAddManagedIndexMetaDataTo.isEmpty() &&
                        request.indicesToRemoveManagedIndexMetaDataFrom.isEmpty()
                    ) {
                        return currentState
                    }

                    val metaDataBuilder = MetaData.builder(currentState.metaData)

                    for (pair in request.indicesToAddManagedIndexMetaDataTo) {
                        metaDataBuilder.put(IndexMetaData.builder(currentState.metaData.index(pair.first))
                            .putCustom(ManagedIndexMetaData.MANAGED_INDEX_METADATA, pair.second.toMap()))
                    }

                    for (index in request.indicesToRemoveManagedIndexMetaDataFrom) {
                        val indexMetaDataBuilder = IndexMetaData.builder(currentState.metaData.index(index))
                        indexMetaDataBuilder.removeCustom(ManagedIndexMetaData.MANAGED_INDEX_METADATA)

                        metaDataBuilder.put(indexMetaDataBuilder)
                    }

                    return ClusterState.builder(currentState).metaData(metaDataBuilder).build()
                }

                override fun newResponse(acknowledged: Boolean): AcknowledgedResponse {
                    return AcknowledgedResponse(acknowledged)
                }
            }
        )

        // Adding history is a best effort task.
        GlobalScope.launch(Dispatchers.IO + CoroutineName("ManagedIndexMetaData-AddHistory")) {
            val managedIndexMetaData = request.indicesToAddManagedIndexMetaDataTo.map { it.second }
            indexStateManagementHistory.addHistory(managedIndexMetaData)
        }
    }

    override fun newResponse(): AcknowledgedResponse {
        return AcknowledgedResponse()
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }
}
