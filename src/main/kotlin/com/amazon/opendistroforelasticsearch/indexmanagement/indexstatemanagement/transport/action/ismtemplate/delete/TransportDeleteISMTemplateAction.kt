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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ismtemplate.delete

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.ISMTemplateService
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.TransportMasterNodeAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService

private val log = LogManager.getLogger(TransportDeleteISMTemplateAction::class.java)

class TransportDeleteISMTemplateAction @Inject constructor(
        transportService: TransportService,
        clusterService: ClusterService,
        threadPool: ThreadPool,
        actionFilters: ActionFilters,
        indexNameExpressionResolver: IndexNameExpressionResolver,
        val client: Client,
        val ismTemplateService: ISMTemplateService
) : TransportMasterNodeAction<DeleteISMTemplateRequest, AcknowledgedResponse>(
        DeleteISMTemplateAction.NAME,
        transportService,
        clusterService,
        threadPool,
        actionFilters,
        Writeable.Reader { DeleteISMTemplateRequest(it) },
        indexNameExpressionResolver
) {
    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    override fun read(sin: StreamInput): AcknowledgedResponse {
        return AcknowledgedResponse(sin)
    }

    override fun masterOperation(request: DeleteISMTemplateRequest, state: ClusterState, listener: ActionListener<AcknowledgedResponse>) {
        ismTemplateService.deleteISMTemplate(request.templateName, request.masterNodeTimeout(), listener)
    }

    override fun checkBlock(request: DeleteISMTemplateRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

}
