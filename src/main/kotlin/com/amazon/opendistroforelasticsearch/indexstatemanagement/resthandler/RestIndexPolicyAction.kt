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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementIndices
import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.INDEX_STATE_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.INDEX_STATE_MANAGEMENT_TYPE
import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.POLICY_BASE_URI
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.Policy.Companion.POLICY_TYPE
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.REFRESH
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util._ID
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util._VERSION
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.lucene.uid.Versions
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.BytesRestResponse
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestController
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.PUT
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestActions
import org.elasticsearch.rest.action.RestResponseListener
import java.io.IOException
import java.time.Instant

class RestIndexPolicyAction(
    settings: Settings,
    controller: RestController,
    indexStateManagementIndices: IndexStateManagementIndices
) : BaseRestHandler(settings) {
    private var ismIndices = indexStateManagementIndices

    init {
        controller.registerHandler(PUT, POLICY_BASE_URI, this)
        controller.registerHandler(PUT, "$POLICY_BASE_URI/{policyID}", this)
        ismIndices = indexStateManagementIndices
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
        val policy = Policy.parseWithType(xcp, id).copy(lastUpdatedTime = Instant.now())
        val policyVersion = RestActions.parseVersion(request)
        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }
        return RestChannelConsumer { channel ->
            IndexPolicyHandler(client, channel, id, policyVersion, refreshPolicy, policy).start()
        }
    }

    inner class IndexPolicyHandler(
        client: NodeClient,
        channel: RestChannel,
        private val policyId: String,
        private val policyVersion: Long,
        private val refreshPolicy: WriteRequest.RefreshPolicy,
        private var newPolicy: Policy
    ) : AsyncActionHandler(client, channel) {
        fun start() {
            if (!ismIndices.indexStateManagementIndexExists()) {
                ismIndices.initIndexStateManagementIndex(ActionListener.wrap(::onCreateMappingsResponse, ::onFailure))
            } else {
                putPolicy()
            }
        }

        private fun onCreateMappingsResponse(response: CreateIndexResponse) {
            if (response.isAcknowledged) {
                logger.info("Created $INDEX_STATE_MANAGEMENT_INDEX with mappings.")
                putPolicy()
            } else {
                logger.error("Create $INDEX_STATE_MANAGEMENT_INDEX mappings call not acknowledged.")
                channel.sendResponse(
                        BytesRestResponse(
                                RestStatus.INTERNAL_SERVER_ERROR,
                                response.toXContent(channel.newErrorBuilder(), ToXContent.EMPTY_PARAMS))
                )
            }
        }

        private fun putPolicy() {
            val indexRequest = IndexRequest(INDEX_STATE_MANAGEMENT_INDEX, INDEX_STATE_MANAGEMENT_TYPE)
                    .setRefreshPolicy(refreshPolicy)
                    .source(newPolicy.toXContent(channel.newBuilder()))
                    .id(policyId)
                    .timeout(IndexRequest.DEFAULT_TIMEOUT)
            if (policyVersion == Versions.MATCH_ANY) {
                indexRequest.opType(DocWriteRequest.OpType.CREATE)
            } else {
                indexRequest.version(policyVersion)
            }
            client.index(indexRequest, indexPolicyResponse())
        }

        private fun indexPolicyResponse(): RestResponseListener<IndexResponse> {
            return object : RestResponseListener<IndexResponse>(channel) {
                @Throws(Exception::class)
                override fun buildResponse(response: IndexResponse): RestResponse {
                    if (response.shardInfo.successful < 1) {
                        return BytesRestResponse(response.status(), response.toXContent(channel.newErrorBuilder(),
                                ToXContent.EMPTY_PARAMS))
                    }

                    val builder = channel.newBuilder()
                            .startObject()
                            .field(_ID, response.id)
                            .field(_VERSION, response.version)
                            .field(POLICY_TYPE, newPolicy)
                            .endObject()

                    val restResponse = BytesRestResponse(response.status(), builder)
                    if (response.status() == RestStatus.CREATED) {
                        val location = "$POLICY_BASE_URI/${response.id}"
                        restResponse.addHeader("Location", location)
                    }
                    return restResponse
                }
            }
        }
    }
}
