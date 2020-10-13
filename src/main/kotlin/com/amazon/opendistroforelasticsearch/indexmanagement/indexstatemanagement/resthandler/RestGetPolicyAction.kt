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
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPolicyRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.GET
import org.elasticsearch.rest.RestRequest.Method.HEAD
import org.elasticsearch.rest.action.RestActions
import org.elasticsearch.rest.action.RestToXContentListener
import org.elasticsearch.search.fetch.subphase.FetchSourceContext

class RestGetPolicyAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return listOf(
            Route(GET, "$POLICY_BASE_URI/{policyID}"),
            Route(HEAD, "$POLICY_BASE_URI/{policyID}")
        )
    }

    override fun getName(): String {
        return "get_policy_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val policyId = request.param("policyID")

        if (policyId == null || policyId.isEmpty()) {
            throw IllegalArgumentException("Missing policy ID")
        }

        var fetchSrcContext: FetchSourceContext = FetchSourceContext.FETCH_SOURCE
        if (request.method() == HEAD) {
            fetchSrcContext = FetchSourceContext.DO_NOT_FETCH_SOURCE
        }
        val getPolicyRequest = GetPolicyRequest(policyId, RestActions.parseVersion(request), fetchSrcContext)

        return RestChannelConsumer { channel ->
            client.execute(GetPolicyAction.INSTANCE, getPolicyRequest, RestToXContentListener(channel))
        }
    }
}
