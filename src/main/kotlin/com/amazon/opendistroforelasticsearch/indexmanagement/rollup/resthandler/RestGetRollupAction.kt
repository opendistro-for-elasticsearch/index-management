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
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupsAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupsRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_FROM
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_SEARCH_STRING
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_SIZE
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_SORT_DIRECTION
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_SORT_FIELD
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
            Route(GET, ROLLUP_JOBS_BASE_URI),
            Route(GET, "$ROLLUP_JOBS_BASE_URI/{rollupID}"),
            Route(HEAD, "$ROLLUP_JOBS_BASE_URI/{rollupID}")
        )
    }

    override fun getName(): String {
        return "opendistro_get_rollup_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val rollupID = request.param("rollupID")
        val searchString = request.param("search", DEFAULT_SEARCH_STRING)
        val from = request.paramAsInt("from", DEFAULT_FROM)
        val size = request.paramAsInt("size", DEFAULT_SIZE)
        val sortField = request.param("sortField", DEFAULT_SORT_FIELD)
        val sortDirection = request.param("sortDirection", DEFAULT_SORT_DIRECTION)
        return RestChannelConsumer { channel ->
            if (rollupID == null || rollupID.isEmpty()) {
                val req = GetRollupsRequest(
                    searchString,
                    from,
                    size,
                    sortField,
                    sortDirection
                )
                client.execute(GetRollupsAction.INSTANCE, req, RestToXContentListener(channel))
            } else {
                val req = GetRollupRequest(rollupID, if (request.method() == HEAD) FetchSourceContext.DO_NOT_FETCH_SOURCE else null)
                client.execute(GetRollupAction.INSTANCE, req, RestToXContentListener(channel))
            }
        }
    }
}
