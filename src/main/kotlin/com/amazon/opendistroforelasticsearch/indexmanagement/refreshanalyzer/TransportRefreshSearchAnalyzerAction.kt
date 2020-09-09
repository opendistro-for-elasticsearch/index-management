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

package com.amazon.opendistroforelasticsearch.indexmanagement.refreshanalyzer

import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.DefaultShardOperationFailedException
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.routing.ShardRouting
import org.elasticsearch.cluster.routing.ShardsIterator
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.index.analysis.AnalysisRegistry
import org.elasticsearch.index.shard.IndexShard
import org.elasticsearch.indices.IndicesService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.io.IOException

class TransportRefreshSearchAnalyzerAction :
        TransportBroadcastByNodeAction<
                RefreshSearchAnalyzerRequest,
                RefreshSearchAnalyzerResponse,
                ShardRefreshSearchAnalyzerResponse> {

    @Inject
    constructor(
        clusterService: ClusterService,
        transportService: TransportService,
        indicesService: IndicesService,
        actionFilters: ActionFilters,
        analysisRegistry: AnalysisRegistry,
        indexNameExpressionResolver: IndexNameExpressionResolver?
    ) : super(
            RefreshSearchAnalyzerAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            Writeable.Reader { RefreshSearchAnalyzerRequest() },
            ThreadPool.Names.MANAGEMENT
    ) {
        this.analysisRegistry = analysisRegistry
        this.indicesService = indicesService
    }

    private val indicesService: IndicesService
    private val analysisRegistry: AnalysisRegistry

    @Throws(IOException::class)
    override fun readShardResult(`in`: StreamInput): ShardRefreshSearchAnalyzerResponse? {
        return ShardRefreshSearchAnalyzerResponse(`in`)
    }

    override fun newResponse(
            request: RefreshSearchAnalyzerRequest?,
            totalShards: Int,
            successfulShards: Int,
            failedShards: Int,
            results: List<ShardRefreshSearchAnalyzerResponse>,
            shardFailures: List<DefaultShardOperationFailedException>,
            clusterState: ClusterState?
    ): RefreshSearchAnalyzerResponse {
        val shardResponses: MutableMap<String, List<String>> = HashMap()
        for (response in results) {
            shardResponses.put(response.indexName, response.reloadedAnalyzers)
        }
        return RefreshSearchAnalyzerResponse(totalShards, successfulShards, failedShards, shardFailures, shardResponses)
    }

    @Throws(IOException::class)
    override fun readRequestFrom(`in`: StreamInput): RefreshSearchAnalyzerRequest? {
        return RefreshSearchAnalyzerRequest(`in`)
    }

    @Throws(IOException::class)
    override fun shardOperation(request: RefreshSearchAnalyzerRequest?, shardRouting: ShardRouting): ShardRefreshSearchAnalyzerResponse {
        val indexShard: IndexShard = indicesService.indexServiceSafe(shardRouting.shardId().index).getShard(shardRouting.shardId().id())
        logger.info("Reloading search analyzers for ${shardRouting.shardId().index.name}")
        val reloadedAnalyzers: List<String> = indexShard.mapperService().reloadSearchAnalyzers(analysisRegistry)
        logger.info("Reload successful for ${shardRouting.shardId().index.name}")
        return ShardRefreshSearchAnalyzerResponse(shardRouting.shardId(), shardRouting.indexName, reloadedAnalyzers)
    }

    /**
     * The refresh request works against *all* shards.
     */
    override fun shards(clusterState: ClusterState, request: RefreshSearchAnalyzerRequest?, concreteIndices: Array<String?>?): ShardsIterator? {
        return clusterState.routingTable().allShards(concreteIndices)
    }

    override fun checkGlobalBlock(state: ClusterState, request: RefreshSearchAnalyzerRequest?): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    override fun checkRequestBlock(state: ClusterState, request: RefreshSearchAnalyzerRequest?, concreteIndices: Array<String?>?):
            ClusterBlockException? {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices)
    }
}
