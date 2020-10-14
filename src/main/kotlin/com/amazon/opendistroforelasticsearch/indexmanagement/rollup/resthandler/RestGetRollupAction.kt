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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.GET
import org.elasticsearch.rest.RestRequest.Method.HEAD
import org.elasticsearch.rest.action.RestToXContentListener
import org.elasticsearch.search.fetch.subphase.FetchSourceContext

class RestGetRollupAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return listOf(
            Route(GET, "$ROLLUP_JOBS_BASE_URI/{rollupID}"),
            Route(HEAD, "$ROLLUP_JOBS_BASE_URI/{rollupID}")
        )
    }

    override fun getName(): String {
        return "opendistro_get_rollup_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val rollupID = request.param("rollupID")
        if (rollupID == null || rollupID.isEmpty()) {
            throw IllegalArgumentException("Missing rollup ID")
        }

        val srcContext: FetchSourceContext? = if (request.method() == HEAD) FetchSourceContext.DO_NOT_FETCH_SOURCE else null
        val getRollupRequest = GetRollupRequest(rollupID, request.method(), srcContext)
        return RestChannelConsumer { channel ->
            client.execute(GetRollupAction.INSTANCE, getRollupRequest, RestToXContentListener(channel))
        }
    }
}
