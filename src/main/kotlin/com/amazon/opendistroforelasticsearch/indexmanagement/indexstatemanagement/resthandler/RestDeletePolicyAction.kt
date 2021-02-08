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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.POLICY_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy.DeletePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy.DeletePolicyRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.util.REFRESH
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.DELETE
import org.elasticsearch.rest.action.RestStatusToXContentListener
import java.io.IOException

class RestDeletePolicyAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return listOf(
            Route(DELETE, "$POLICY_BASE_URI/{policyID}")
        )
    }

    override fun getName(): String = "delete_policy_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val policyId = request.param("policyID")
        if (policyId == null || policyId.isEmpty()) {
            throw IllegalArgumentException("Missing policy ID")
        }

        val refreshPolicy = RefreshPolicy.parse(request.param(REFRESH, RefreshPolicy.IMMEDIATE.value))
        val deletePolicyRequest = DeletePolicyRequest(policyId, refreshPolicy)

        return RestChannelConsumer { channel ->
            client.execute(DeletePolicyAction.INSTANCE, deletePolicyRequest, RestStatusToXContentListener(channel))
        }
    }
}
