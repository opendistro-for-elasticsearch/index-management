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
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.ALLOW_LIST
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.getDisallowedActions
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IF_PRIMARY_TERM
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IF_SEQ_NO
import com.amazon.opendistroforelasticsearch.indexmanagement.util.REFRESH
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.index.seqno.SequenceNumbers
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.BytesRestResponse
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.PUT
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestResponseListener
import java.io.IOException
import java.time.Instant

private val log = LogManager.getLogger(RestIndexPolicyAction::class.java)

class RestIndexPolicyAction(
    settings: Settings,
    val clusterService: ClusterService
) : BaseRestHandler() {

    @Volatile private var allowList = ALLOW_LIST.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALLOW_LIST) { allowList = it }
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(PUT, POLICY_BASE_URI),
            Route(PUT, "$POLICY_BASE_URI/{policyID}")
        )
    }

    override fun getName(): String {
        return "index_policy_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("policyID", Policy.NO_ID)
        if (Policy.NO_ID == id) {
            throw IllegalArgumentException("Missing policy ID")
        }

        val xcp = request.contentParser()
        val policy = Policy.parseWithType(xcp = xcp, id = id).copy(lastUpdatedTime = Instant.now())
        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)

        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }

        val disallowedActions = policy.getDisallowedActions(allowList)
        if (disallowedActions.isNotEmpty()) {
            return RestChannelConsumer { channel ->
                channel.sendResponse(
                    BytesRestResponse(
                        RestStatus.FORBIDDEN,
                        "You have actions that are not allowed in your policy $disallowedActions"
                    )
                )
            }
        }

        val indexPolicyRequest = IndexPolicyRequest(id, policy, seqNo, primaryTerm, refreshPolicy)

        return RestChannelConsumer { channel ->
            client.execute(IndexPolicyAction.INSTANCE, indexPolicyRequest, object : RestResponseListener<IndexPolicyResponse>(channel) {
                override fun buildResponse(response: IndexPolicyResponse): RestResponse {
                    val restResponse = BytesRestResponse(response.status, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                    if (response.status == RestStatus.CREATED) {
                        val location = "$POLICY_BASE_URI/${response.id}"
                        restResponse.addHeader("Location", location)
                    }
                    return restResponse
                }
            })
        }
    }
}
