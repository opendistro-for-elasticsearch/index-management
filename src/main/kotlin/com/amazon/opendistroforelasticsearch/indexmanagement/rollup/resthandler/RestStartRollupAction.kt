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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.start.StartRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.start.StartRollupRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class RestStartRollupAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return listOf(
            Route(POST, "$ROLLUP_JOBS_BASE_URI/{rollupID}/_start")
        )
    }

    override fun getName(): String {
        return "opendistro_start_rollup_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("rollupID", Rollup.NO_ID)
        val startRequest = StartRollupRequest(id)
        return RestChannelConsumer { channel ->
            client.execute(StartRollupAction.INSTANCE, startRequest, RestToXContentListener(channel))
        }
    }
}
