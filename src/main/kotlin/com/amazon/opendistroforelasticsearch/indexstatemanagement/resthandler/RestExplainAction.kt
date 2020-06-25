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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.getPolicyID
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.BytesRestResponse
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.GET
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestBuilderListener

class RestExplainAction : BaseRestHandler() {

    companion object {
        const val EXPLAIN_BASE_URI = "${IndexStateManagementPlugin.ISM_BASE_URI}/explain"
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
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))
        if (indices == null || indices.isEmpty()) {
            throw IllegalArgumentException("Missing indices")
        }

        val clusterStateRequest = ClusterStateRequest()
        val strictExpandIndicesOptions = IndicesOptions.strictExpand()

        clusterStateRequest.clear()
            .indices(*indices)
            .metadata(true)
            .local(false)
            .local(request.paramAsBoolean("local", clusterStateRequest.local()))
            .masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()))
            .indicesOptions(strictExpandIndicesOptions)

        return RestChannelConsumer { channel ->
            client.admin().cluster().state(clusterStateRequest, explainListener(channel))
        }
    }

    private fun explainListener(channel: RestChannel): RestBuilderListener<ClusterStateResponse> {
        return object : RestBuilderListener<ClusterStateResponse>(channel) {
            override fun buildResponse(clusterStateResponse: ClusterStateResponse, builder: XContentBuilder): RestResponse {
                val state = clusterStateResponse.state

                builder.startObject()
                for (indexMetadataEntry in state.metadata.indices) {
                    builder.startObject(indexMetadataEntry.key)
                    val indexMetadata = indexMetadataEntry.value
                    val managedIndexMetaDataMap = indexMetadata.getCustomData(ManagedIndexMetaData.MANAGED_INDEX_METADATA)

                    builder.field(ManagedIndexSettings.POLICY_ID.key, indexMetadata.getPolicyID())
                    if (managedIndexMetaDataMap != null) {
                        val managedIndexMetaData = ManagedIndexMetaData.fromMap(managedIndexMetaDataMap)
                        managedIndexMetaData.toXContent(builder, ToXContent.EMPTY_PARAMS)
                    }
                    builder.endObject()
                }
                builder.endObject()
                return BytesRestResponse(RestStatus.OK, builder)
            }
        }
    }
}
