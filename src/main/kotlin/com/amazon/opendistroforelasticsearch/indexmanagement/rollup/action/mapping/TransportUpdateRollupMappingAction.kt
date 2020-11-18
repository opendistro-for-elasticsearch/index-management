/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.mapping

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils.Companion._META
import com.amazon.opendistroforelasticsearch.indexmanagement.util._DOC
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.TransportMasterNodeAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.lang.Exception

class TransportUpdateRollupMappingAction @Inject constructor(
    threadPool: ThreadPool,
    clusterService: ClusterService,
    transportService: TransportService,
    actionFilters: ActionFilters,
    indexNameExpressionResolver: IndexNameExpressionResolver,
    val client: Client
) : TransportMasterNodeAction<UpdateRollupMappingRequest, AcknowledgedResponse>(
    UpdateRollupMappingAction.INSTANCE.name(),
    transportService,
    clusterService,
    threadPool,
    actionFilters,
    Writeable.Reader { UpdateRollupMappingRequest(it) },
    indexNameExpressionResolver
) {

    private val log = LogManager.getLogger(javaClass)

    override fun checkBlock(request: UpdateRollupMappingRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks.indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, arrayOf(request.rollup.targetIndex))
    }

    @Suppress("ReturnCount")
    override fun masterOperation(
        request: UpdateRollupMappingRequest,
        state: ClusterState,
        listener: ActionListener<AcknowledgedResponse>
    ) {
        val index = state.metadata.index(request.rollup.targetIndex)
        if (index == null) {
            log.debug("Could not find index [$index]")
            return listener.onFailure(IllegalStateException("Could not find index [$index]"))
        }
        val mappings = index.mapping()
        if (mappings == null) {
            log.debug("Could not find mapping for index [$index]")
            return listener.onFailure(IllegalStateException("Could not find mapping for index [$index]"))
        }
        val source = mappings.sourceAsMap
        if (source == null) {
            log.debug("Could not find source for index mapping [$index]")
            return listener.onFailure(IllegalStateException("Could not find source for index mapping [$index]"))
        }

        val rollup = XContentHelper.convertToMap(
            BytesReference.bytes(request.rollup.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITHOUT_TYPE)),
            false,
            XContentType.JSON
        ).v2()
        val metaMappings = mutableMapOf<String, Any>()
        // TODO: Clean this up
        val meta = source[_META]
        if (meta == null) {
            // TODO: Is schema_version always present?
            log.debug("Could not find meta mappings for index [$index], creating meta mappings")
            val rollupJobEntries = mapOf<String, Any>(request.rollup.id to rollup)
            val rollups = mapOf<String, Any>("rollups" to rollupJobEntries)
            metaMappings[_META] = rollups
        } else {
            val rollups = (meta as Map<*, *>)["rollups"]
            if (rollups == null) {
                log.debug("Could not find meta rollup mappings for index [$index], creating meta rollup mappings")
                val rollupJobEntries = mapOf<String, Any>(request.rollup.id to rollup)
                val updatedRollups = mapOf<String, Any>("rollups" to rollupJobEntries)
                metaMappings[_META] = updatedRollups
            } else {
                if ((rollups as Map<*, *>).containsKey(request.rollup.id)) {
                    log.debug("Meta rollup mappings already contain rollup ${request.rollup.id} for index [$index]")
                    return listener.onFailure(
                        IllegalStateException("Meta rollup mappings already contain rollup ${request.rollup.id} for index [$index]")
                    )
                }

                // In this case rollup mappings exists and there is no entry for request.rollup.id
                val rollupJobEntries = rollups.toMutableMap()
                rollupJobEntries[request.rollup.id] = rollup
                val updatedRollups = mapOf<String, Any>("rollups" to rollupJobEntries)
                metaMappings[_META] = updatedRollups
            }
        }

        // TODO: Does schema_version get overwritten?
        val putMappingRequest = PutMappingRequest(request.rollup.targetIndex).type(_DOC).source(metaMappings)
        client.admin().indices().putMapping(putMappingRequest, object : ActionListener<AcknowledgedResponse> {
            override fun onResponse(response: AcknowledgedResponse) {
                listener.onResponse(response)
            }

            override fun onFailure(e: Exception) {
                listener.onFailure(e)
            }
        })
    }

    override fun read(sin: StreamInput): AcknowledgedResponse = AcknowledgedResponse(sin)

    override fun executor(): String = ThreadPool.Names.SAME
}
