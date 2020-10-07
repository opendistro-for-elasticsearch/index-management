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
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.addpolicy.AddPolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.addpolicy.AddPolicyRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class RestAddPolicyAction : BaseRestHandler() {

    override fun getName(): String = "add_policy_action"

    override fun routes(): List<Route> {
        return listOf(
            Route(POST, ADD_POLICY_BASE_URI),
            Route(POST, "$ADD_POLICY_BASE_URI/{index}")
        )
    }

    @Throws(IOException::class)
    @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))

        if (indices.isNullOrEmpty()) {
            throw IllegalArgumentException("Missing indices")
        }

        val body = if (request.hasContent()) {
            XContentHelper.convertToMap(request.requiredContent(), false, request.xContentType).v2()
        } else {
            mapOf()
        }

        val policyID = requireNotNull(body.getOrDefault("policy_id", null)) { "Missing policy_id" }

        val addPolicyRequest = AddPolicyRequest(indices.toList(), policyID as String)

        return RestChannelConsumer { channel ->
            client.execute(AddPolicyAction.INSTANCE, addPolicyRequest, RestToXContentListener(channel))
        }
    }

    companion object {
        const val ADD_POLICY_BASE_URI = "$ISM_BASE_URI/add"
    }
}
