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

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.POLICY_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.SearchParams
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPoliciesAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPoliciesRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPolicyRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.DEFAULT_PAGINATION_FROM
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.DEFAULT_PAGINATION_SIZE
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.DEFAULT_POLICY_SORT_FIELD
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.DEFAULT_QUERY_STRING
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.DEFAULT_SORT_ORDER
import org.apache.logging.log4j.LogManager
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

private val log = LogManager.getLogger(RestGetPolicyAction::class.java)

class RestGetPolicyAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return listOf(
            Route(GET, POLICY_BASE_URI),
            Route(GET, "$POLICY_BASE_URI/{policyID}"),
            Route(HEAD, "$POLICY_BASE_URI/{policyID}")
        )
    }

    override fun getName(): String {
        return "get_policy_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${request.path()}")

        val policyId = request.param("policyID")

        var fetchSrcContext: FetchSourceContext = FetchSourceContext.FETCH_SOURCE
        if (request.method() == HEAD) {
            fetchSrcContext = FetchSourceContext.DO_NOT_FETCH_SOURCE
        }

        val size = request.paramAsInt("size", DEFAULT_PAGINATION_SIZE)
        val from = request.paramAsInt("from", DEFAULT_PAGINATION_FROM)
        val sortField = request.param("sortField", DEFAULT_POLICY_SORT_FIELD)
        val sortOrder = request.param("sortOrder", DEFAULT_SORT_ORDER)
        val queryString = request.param("queryString", DEFAULT_QUERY_STRING)
        val index = request.param("index", INDEX_MANAGEMENT_INDEX)

        return RestChannelConsumer { channel ->
            if (policyId == null || policyId.isEmpty()) {
                log.info("get all policies")
                val getPoliciesRequest = GetPoliciesRequest(SearchParams(size, from, sortField, sortOrder, queryString), index)
                client.execute(GetPoliciesAction.INSTANCE, getPoliciesRequest, RestToXContentListener(channel))
            } else {
                val getPolicyRequest = GetPolicyRequest(policyId, RestActions.parseVersion(request), fetchSrcContext)
                client.execute(GetPolicyAction.INSTANCE, getPolicyRequest, RestToXContentListener(channel))

            }
        }
    }
}
