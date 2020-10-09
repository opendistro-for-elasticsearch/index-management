/*
 *
 *  * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License").
 *  * You may not use this file except in compliance with the License.
 *  * A copy of the License is located at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * or in the "license" file accompanying this file. This file is distributed
 *  * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  * express or implied. See the License for the specific language governing
 *  * permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.explain.ExplainRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.explain.ExplainRollupRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.GET
import org.elasticsearch.rest.action.RestToXContentListener

class RestExplainRollupAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return listOf(Route(GET, "$ROLLUP_JOBS_BASE_URI/{rollupID}/_explain"))
    }

    override fun getName(): String = "opendistro_explain_rollup_action"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val rollupIDs: List<String> = Strings.splitStringByCommaToArray(request.param("rollupID")).toList()
        if (rollupIDs.isEmpty()) {
            throw IllegalArgumentException("Missing rollupID")
        }
        val explainRequest = ExplainRollupRequest(rollupIDs)
        return RestChannelConsumer { channel ->
            client.execute(ExplainRollupAction.INSTANCE, explainRequest, RestToXContentListener(channel))
        }
    }

}
