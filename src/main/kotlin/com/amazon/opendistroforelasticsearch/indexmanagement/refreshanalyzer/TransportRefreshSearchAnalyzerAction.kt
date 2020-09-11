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

import org.apache.logging.log4j.LogManager
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
                RefreshSearchAnalyzerShardResponse> {

    private val log = LogManager.getLogger(javaClass)

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
    override fun readShardResult(si: StreamInput): RefreshSearchAnalyzerShardResponse? {
        return RefreshSearchAnalyzerShardResponse(si)
    }

    override fun newResponse(
        request: RefreshSearchAnalyzerRequest,
        totalShards: Int,
        successfulShards: Int,
        failedShards: Int,
        shardResponses: List<RefreshSearchAnalyzerShardResponse>,
        shardFailures: List<DefaultShardOperationFailedException>,
        clusterState: ClusterState
    ): RefreshSearchAnalyzerResponse {
        return RefreshSearchAnalyzerResponse(totalShards, successfulShards, failedShards, shardFailures, shardResponses)
    }

    @Throws(IOException::class)
    override fun readRequestFrom(si: StreamInput): RefreshSearchAnalyzerRequest {
        return RefreshSearchAnalyzerRequest(si)
    }

    @Throws(IOException::class)
    override fun shardOperation(request: RefreshSearchAnalyzerRequest, shardRouting: ShardRouting): RefreshSearchAnalyzerShardResponse {
        val indexShard: IndexShard = indicesService.indexServiceSafe(shardRouting.shardId().index).getShard(shardRouting.shardId().id())
        val reloadedAnalyzers: List<String> = indexShard.mapperService().reloadSearchAnalyzers(analysisRegistry)
        log.info("Reload successful, index: ${shardRouting.shardId().index.name}, shard: ${shardRouting.shardId().id}, " +
                "is_primary: ${shardRouting.primary()}")
        return RefreshSearchAnalyzerShardResponse(shardRouting.shardId(), reloadedAnalyzers)
    }

    /**
     * The refresh request works against *all* shards.
     */
    override fun shards(clusterState: ClusterState, request: RefreshSearchAnalyzerRequest?, concreteIndices: Array<String?>?): ShardsIterator? {
        return clusterState.routingTable().allShards(concreteIndices)
    }

    override fun checkGlobalBlock(state: ClusterState, request: RefreshSearchAnalyzerRequest?): ClusterBlockException {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    override fun checkRequestBlock(state: ClusterState, request: RefreshSearchAnalyzerRequest?, concreteIndices: Array<String?>?):
            ClusterBlockException {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices)
    }
}
