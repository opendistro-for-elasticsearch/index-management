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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.action.updateindexmetadata

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.TransportMasterNodeAction
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
        threadPool: ThreadPool,
        clusterService: ClusterService,
        transportService: TransportService,
        actionFilters: ActionFilters,
        indexNameExpressionResolver: IndexNameExpressionResolver
    ) : super(
        UPDATE_MANAGED_INDEX_METADATA_ACTION_NAME,
        transportService,
        clusterService,
        threadPool,
        actionFilters,
        indexNameExpressionResolver,
        Supplier { UpdateManagedIndexMetaDataRequest() }
    )

    override fun checkBlock(request: UpdateManagedIndexMetaDataRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks.globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    override fun masterOperation(
        request: UpdateManagedIndexMetaDataRequest,
        state: ClusterState,
        listener: ActionListener<AcknowledgedResponse>
    ) {
        clusterService.submitStateUpdateTask(
            "${IndexStateManagementPlugin.PLUGIN_NAME}-${request.index.name}-${request.index.uuid}",
            object : AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {
                override fun execute(currentState: ClusterState): ClusterState {
                    return ClusterState.builder(currentState).metaData(
                        MetaData.builder(currentState.metaData).put(
                            IndexMetaData.builder(currentState.metaData.index(request.index))
                                .putCustom(ManagedIndexMetaData.MANAGED_INDEX_METADATA, request.managedIndexMetaData.toMap())
                        )
                    )
                        .build()
                }

                override fun newResponse(acknowledged: Boolean): AcknowledgedResponse {
                    return AcknowledgedResponse(acknowledged)
                }
            }
        )
    }

    override fun newResponse(): AcknowledgedResponse {
        return AcknowledgedResponse()
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }
}
