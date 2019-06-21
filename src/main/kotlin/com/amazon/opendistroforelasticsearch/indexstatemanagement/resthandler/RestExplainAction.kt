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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.getPolicyName
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BytesRestResponse
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestController
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestBuilderListener

class RestExplainAction(
    settings: Settings,
    controller: RestController
) : BaseRestHandler(settings) {

    companion object {
        const val EXPLAIN_BASE_URI = "${IndexStateManagementPlugin.ISM_BASE_URI}/explain"
    }

    init {
        controller.registerHandler(RestRequest.Method.GET, EXPLAIN_BASE_URI, this)
        controller.registerHandler(RestRequest.Method.GET, "$EXPLAIN_BASE_URI/{index}", this)
    }

    override fun getName(): String {
        return "ism_explain_action"
    }

    @Suppress("SpreadOperator")
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))
        if (indices == null || indices.isEmpty()) {
            throw IllegalArgumentException("Missing indices")
        }

        val clusterStateRequest = ClusterStateRequest()
        val strictExpandIndicesOptions = IndicesOptions.strictExpand()

        clusterStateRequest.clear()
            .indices(*indices)
            .metaData(true)
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
                for (indexMetadataEntry in state.metaData.indices) {
                    builder.startObject(indexMetadataEntry.key)
                    val indexMetadata = indexMetadataEntry.value
                    val managedIndexMetaDataMap = indexMetadata.getCustomData(ManagedIndexMetaData.MANAGED_INDEX_METADATA)

                    // TODO Use param to enable and disable showing null/blank policy names.
                    builder.field(ManagedIndexSettings.POLICY_NAME.key, indexMetadata.getPolicyName())

                    if (managedIndexMetaDataMap != null) {
                        val managedIndexMetaData = ManagedIndexMetaData.fromMap(
                            indexMetadata.index.name,
                            indexMetadata.index.uuid,
                            managedIndexMetaDataMap
                        )

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
