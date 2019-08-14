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

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementIndices
import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin
import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.INDEX_STATE_MANAGEMENT_HISTORY_TYPE
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
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
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.time.Instant
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
        indexStateManagementIndices: IndexStateManagementIndices
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
        this.indexStateManagementIndices = indexStateManagementIndices
    }

    private val log = LogManager.getLogger(javaClass)
    private val client: Client
    private val indexStateManagementIndices: IndexStateManagementIndices

    override fun checkBlock(request: UpdateManagedIndexMetaDataRequest, state: ClusterState): ClusterBlockException? {
        // https://github.com/elastic/elasticsearch/commit/ae14b4e6f96b554ca8f4aaf4039b468f52df0123
        // This commit will help us to give each individual index name and the error that is cause it. For now it will be a generic error message.
        val indices = request.listOfIndexMetadata.map { it.first.name }.toTypedArray()
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
                    val metaDataBuilder = MetaData.builder(currentState.metaData)

                    for (pair in request.listOfIndexMetadata) {
                        metaDataBuilder.put(IndexMetaData.builder(currentState.metaData.index(pair.first))
                            .putCustom(ManagedIndexMetaData.MANAGED_INDEX_METADATA, pair.second.toMap()))
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
            addHistory(request)
        }
    }

    private suspend fun addHistory(request: UpdateManagedIndexMetaDataRequest) {
        indexStateManagementIndices.initHistoryIndex()
        val docWriteRequest: List<DocWriteRequest<*>> = request.listOfIndexMetadata.map { indexHistory(it.second) }

        val bulkRequest = BulkRequest().add(docWriteRequest)
        client.bulk(bulkRequest, ActionListener.wrap(::onBulkResponse, ::onFailure))
    }

    private fun indexHistory(managedIndexMetaData: ManagedIndexMetaData): IndexRequest {
        val builder = XContentFactory.jsonBuilder()
            .startObject()
                .startObject(INDEX_STATE_MANAGEMENT_HISTORY_TYPE)
        managedIndexMetaData.toXContent(builder, ToXContent.EMPTY_PARAMS)
        builder
            .field("history_timestamp", Instant.now().toEpochMilli())
                .endObject()
            .endObject()
        return IndexRequest(IndexStateManagementIndices.HISTORY_WRITE_INDEX)
            .source(builder)
    }

    private fun onBulkResponse(bulkResponse: BulkResponse) {
        for (bulkItemResponse in bulkResponse) {
            if (bulkItemResponse.isFailed) {
                log.error("Failed to add history. Id: ${bulkItemResponse.id}, failureMessage: ${bulkItemResponse.failureMessage}")
            }
        }
    }

    private fun onFailure(e: Exception) {
        log.error("failed to index indexMetaData History.", e)
    }

    override fun newResponse(): AcknowledgedResponse {
        return AcknowledgedResponse()
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }
}
