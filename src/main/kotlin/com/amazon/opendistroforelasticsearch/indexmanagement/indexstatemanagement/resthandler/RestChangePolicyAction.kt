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
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.changepolicy.ChangePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.changepolicy.ChangePolicyRequest
import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.Strings
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class RestChangePolicyAction : BaseRestHandler() {

    private val log = LogManager.getLogger(javaClass)

    override fun routes(): List<Route> {
        return listOf(
            Route(POST, CHANGE_POLICY_BASE_URI),
            Route(POST, "$CHANGE_POLICY_BASE_URI/{index}")
        )
    }

    override fun getName(): String = "change_policy_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))
        if (indices == null || indices.isEmpty()) {
            throw IllegalArgumentException("Missing index")
        }

        val xcp = request.contentParser()
        ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
        val changePolicy = ChangePolicy.parse(xcp)

        val changePolicyRequest = ChangePolicyRequest(indices.toList(), changePolicy)

        return RestChannelConsumer { channel ->
            client.execute(ChangePolicyAction.INSTANCE, changePolicyRequest, RestToXContentListener(channel))
        }
    }

    companion object {
        const val CHANGE_POLICY_BASE_URI = "$ISM_BASE_URI/change_policy"
        const val INDEX_NOT_MANAGED = "This index is not being managed"
        const val INDEX_IN_TRANSITION = "Cannot change policy while transitioning to new state"
        const val INDEX_NOT_INITIALIZED = "This managed index has not been initialized yet"
    }
}
