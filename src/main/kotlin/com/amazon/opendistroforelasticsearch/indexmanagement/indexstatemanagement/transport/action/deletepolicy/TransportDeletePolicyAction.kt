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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.util.use
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

class TransportDeletePolicyAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters
) : HandledTransportAction<DeletePolicyRequest, DeleteResponse>(
        DeletePolicyAction.NAME, transportService, actionFilters, ::DeletePolicyRequest
) {
    override fun doExecute(task: Task, request: DeletePolicyRequest, listener: ActionListener<DeleteResponse>) {
        val deleteRequest = DeleteRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, request.policyID)
                .setRefreshPolicy(request.refreshPolicy)

        client.threadPool().threadContext.stashContext().use {
            client.delete(deleteRequest, listener)
        }
    }
}
