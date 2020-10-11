/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Params
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.explain.ExplainAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.explain.ExplainRequest
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.support.master.MasterNodeRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.GET
import org.elasticsearch.rest.action.RestToXContentListener

private val log = LogManager.getLogger(RestExplainAction::class.java)

class RestExplainAction : BaseRestHandler() {

    companion object {
        const val EXPLAIN_BASE_URI = "$ISM_BASE_URI/explain"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(GET, EXPLAIN_BASE_URI),
            Route(GET, "$EXPLAIN_BASE_URI/{index}")
        )
    }

    override fun getName(): String {
        return "ism_explain_action"
    }

    @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.info("${request.method()} ${request.path()}")

        val indices: Array<String> = Strings.splitStringByCommaToArray(request.param("index"))

        // if (indices == null || indices.isEmpty()) {
        //     throw IllegalArgumentException("Missing indices")
        // }

        val size = request.paramAsInt("size", 20)
        val from = request.paramAsInt("from", 0)
        val sortField = request.param("sortField", "managed_index.index")
        val sortOrder = request.param("sortOrder", "asc")
        val queryString = request.param("queryString", "*")

        log.info("request params: $size, $from, $sortField, $sortOrder, $queryString")

        val params = Params(size, from, sortField, sortOrder, queryString)
        val explainRequest = ExplainRequest(indices.toList(), request.paramAsBoolean("local", false),
                request.paramAsTime("master_timeout", MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT), params)

        return RestChannelConsumer { channel ->
            client.execute(ExplainAction.INSTANCE, explainRequest, RestToXContentListener(channel))
        }
    }
}
