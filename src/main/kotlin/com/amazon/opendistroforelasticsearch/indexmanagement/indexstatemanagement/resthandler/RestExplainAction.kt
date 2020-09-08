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

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getPolicyID
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.get.MultiGetRequest
import org.elasticsearch.action.get.MultiGetResponse
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.BytesRestResponse
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.GET
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestBuilderListener

class RestExplainAction : BaseRestHandler() {

    private val log = LogManager.getLogger(javaClass)

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

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))
        if (indices == null || indices.isEmpty()) {
            throw IllegalArgumentException("Missing indices")
        }

        return RestChannelConsumer { channel ->
            ExplainHandler(client, channel, indices).start()
        }
    }

    inner class ExplainHandler(
        client: NodeClient,
        channel: RestChannel,
        private val indices: Array<String>
    ) : AsyncActionHandler(client, channel) {

        lateinit var response: GetResponse

        private val indexNames = mutableListOf<String>()
        private val indexMetadataUuids = mutableListOf<String>()
        private val indexPolicyIds = mutableListOf<String?>()

        @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
        fun start() {
            val clusterStateRequest = ClusterStateRequest()
            val strictExpandIndicesOptions = IndicesOptions.strictExpand()

            clusterStateRequest.clear()
                    .indices(*indices)
                    .metadata(true)
                    .indicesOptions(strictExpandIndicesOptions)

            client.admin().cluster().state(clusterStateRequest, ActionListener.wrap(::onClusterStateResponse, ::onFailure))
        }

        // retrieve index uuid from cluster state
        private fun onClusterStateResponse(response: ClusterStateResponse) {
            for (indexMetadataEntry in response.state.metadata.indices) {
                indexNames.add(indexMetadataEntry.key)
                indexMetadataUuids.add(indexMetadataEntry.value.indexUUID + "metadata")
                indexPolicyIds.add(indexMetadataEntry.value.getPolicyID())
            }

            val mgetRequest = MultiGetRequest()
            indexMetadataUuids.forEach { mgetRequest.add(MultiGetRequest.Item(INDEX_MANAGEMENT_INDEX, it)) }
            client.multiGet(mgetRequest, mgetMetadataListener(channel))
        }

        private fun mgetMetadataListener(channel: RestChannel): RestBuilderListener<MultiGetResponse> {
            return object : RestBuilderListener<MultiGetResponse>(channel) {
                override fun buildResponse(response: MultiGetResponse, builder: XContentBuilder): RestResponse {
                    builder.startObject()
                    response.responses.forEachIndexed { ind, it ->
                        builder.startObject(indexNames[ind])
                        builder.field(ManagedIndexSettings.POLICY_ID.key, indexPolicyIds[ind])
                        if (it.response != null) {
                            log.info("Explain ${indexNames[ind]}: ${it.response.sourceAsString}")
                            processMetadata(it.response, builder)
                        } else {
                            log.info("Explain response is null for ${indexNames[ind]}")
                        }
                        builder.endObject()
                    }
                    builder.endObject()
                    return BytesRestResponse(RestStatus.OK, builder)
                }
            }
        }

        private fun processMetadata(response: GetResponse, builder: XContentBuilder): XContentBuilder {
            if (response.sourceAsBytesRef == null) return builder

            val xcp = XContentHelper.createParser(
                    channel.request().xContentRegistry,
                    LoggingDeprecationHandler.INSTANCE,
                    response.sourceAsBytesRef,
                    XContentType.JSON)
            val managedIndexMetaData = ManagedIndexMetaData.parseWithType(xcp,
                    response.id, response.seqNo, response.primaryTerm)

            managedIndexMetaData.toXContent(builder, ToXContent.EMPTY_PARAMS)
            return builder
        }
    }
}
