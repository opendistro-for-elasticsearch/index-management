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

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.removepolicy.RemovePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.removepolicy.RemovePolicyRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class RestRemovePolicyAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return listOf(
            Route(POST, REMOVE_POLICY_BASE_URI),
            Route(POST, "$REMOVE_POLICY_BASE_URI/{index}")
        )
    }

    override fun getName(): String = "remove_policy_action"

    @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String> = Strings.splitStringByCommaToArray(request.param("index"))

        val removePolicyRequest = RemovePolicyRequest(indices.toList())

        return RestChannelConsumer { channel ->
            client.execute(RemovePolicyAction.INSTANCE, removePolicyRequest, RestToXContentListener(channel))
        }
    }

    companion object {
        const val REMOVE_POLICY_BASE_URI = "$ISM_BASE_URI/remove"
    }
}
