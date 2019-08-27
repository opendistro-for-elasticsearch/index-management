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

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.INDEX_STATE_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.ISM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.getManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.getPolicyID
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ChangePolicy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.coordinator.SweptManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.FAILED_INDICES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.FAILURES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.FailedIndex
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.UPDATED_INDICES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.isSafeToChange
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.updateManagedIndexRequest
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.Strings
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.IdsQueryBuilder
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BytesRestResponse
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestController
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestResponseListener
import org.elasticsearch.search.builder.SearchSourceBuilder
import java.io.IOException

class RestChangePolicyAction(
    settings: Settings,
    controller: RestController,
    val clusterService: ClusterService
) : BaseRestHandler(settings) {

    private val log = LogManager.getLogger(javaClass)

    init {
        controller.registerHandler(POST, CHANGE_POLICY_BASE_URI, this)
        controller.registerHandler(POST, "$CHANGE_POLICY_BASE_URI/{index}", this)
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

        return RestChannelConsumer { channel ->
            ChangePolicyHandler(client, channel, indices, changePolicy).start()
        }
    }

    inner class ChangePolicyHandler(
        client: NodeClient,
        channel: RestChannel,
        private val indices: Array<String>,
        private val changePolicy: ChangePolicy
    ) : AsyncActionHandler(client, channel) {

        private val failedIndices = mutableListOf<FailedIndex>()
        private val managedIndexUuids = mutableListOf<Pair<String, String>>()
        private val indexUuidToCurrentState = mutableMapOf<String, String>()
        lateinit var policy: Policy

        fun start() {
            val getRequest = GetRequest(INDEX_STATE_MANAGEMENT_INDEX, changePolicy.policyID)

            client.get(getRequest, ActionListener.wrap(::onGetPolicyResponse, ::onFailure))
        }

        private fun onGetPolicyResponse(response: GetResponse) {
            if (!response.isExists || response.isSourceEmpty) {
                return channel.sendResponse(BytesRestResponse(RestStatus.NOT_FOUND, "Could not find policy=${changePolicy.policyID}"))
            }

            policy = XContentHelper.createParser(
                channel.request().xContentRegistry,
                LoggingDeprecationHandler.INSTANCE,
                response.sourceAsBytesRef,
                XContentType.JSON
            ).use { Policy.parseWithType(it, response.id, response.seqNo, response.primaryTerm) }

            getClusterState()
        }

        @Suppress("SpreadOperator")
        private fun getClusterState() {
            val clusterStateRequest = ClusterStateRequest()
                .clear()
                .indices(*indices)
                .metaData(true)
                .local(false)
                .indicesOptions(IndicesOptions.strictExpand())

            client.admin().cluster().state(clusterStateRequest, ActionListener.wrap(::processResponse, ::onFailure))
        }

        private fun processResponse(response: ClusterStateResponse) {
            val includedStates = changePolicy.include.map { it.state }.toSet()
            response.state.metaData.indices.forEach {
                val indexMetaData = it.value
                val currentState = indexMetaData.getManagedIndexMetaData()?.stateMetaData?.name
                if (currentState != null) {
                    indexUuidToCurrentState[indexMetaData.indexUUID] = currentState
                }
                when {
                    // If there is no policyID on the index then it's not currently being managed
                    indexMetaData.getPolicyID() == null ->
                        failedIndices.add(FailedIndex(indexMetaData.index.name, indexMetaData.index.uuid, INDEX_NOT_MANAGED))
                    // else if there exists a transitionTo on the ManagedIndexMetaData then we will
                    // fail as they might not of meant to add a ChangePolicy when its on the next state
                    indexMetaData.getManagedIndexMetaData()?.transitionTo != null ->
                        failedIndices.add(FailedIndex(indexMetaData.index.name, indexMetaData.index.uuid, INDEX_IN_TRANSITION))
                    // else if there is no ManagedIndexMetaData yet then the managed index has not initialized and we can change the policy safely
                    indexMetaData.getManagedIndexMetaData() == null ->
                        managedIndexUuids.add(indexMetaData.index.name to indexMetaData.index.uuid)
                    // else if the includedStates is empty (i.e. not being used) then we will always try to update the managed index
                    includedStates.isEmpty() -> managedIndexUuids.add(indexMetaData.index.name to indexMetaData.index.uuid)
                    // else only update the managed index if its currently in one of the included states
                    includedStates.contains(indexMetaData.getManagedIndexMetaData()?.stateMetaData?.name) ->
                        managedIndexUuids.add(indexMetaData.index.name to indexMetaData.index.uuid)
                    // else the managed index did not match any of the included state filters and we will not update it
                    else -> log.debug("Skipping ${indexMetaData.index.name} as it does not match any of the include state filters")
                }
            }

            client.search(
                getManagedIndexConfigSearchQuery(managedIndexUuids.map { (_, indexUuid) -> indexUuid }.toTypedArray()),
                ActionListener.wrap(::onSearchResponse, ::onFailure)
            )
        }

        private fun onSearchResponse(response: SearchResponse) {
            val foundManagedIndices = mutableSetOf<String>()
            val sweptConfigs = response.hits
                .map {
                    // The id is the index uuid
                    foundManagedIndices.add(it.id)
                    SweptManagedIndexConfig.parseWithType(contentParser(it.sourceRef), it.seqNo, it.primaryTerm)
                }

            // If we do not find a matching ManagedIndexConfig for one of the provided managedIndexUuids
            // it means that we have not yet created a job for that index (which can happen during the small
            // gap of adding a policy_id to an index and a job being created by the coordinator, or the coordinator
            // failing to create a job and waiting for the sweep to create the job). We will add these as failed indices
            // that can not be updated from the ChangePolicy yet.
            managedIndexUuids.forEach {
                val (index, indexUuid) = it
                if (!foundManagedIndices.contains(indexUuid)) {
                    failedIndices.add(FailedIndex(index, indexUuid, INDEX_NOT_INITIALIZED))
                }
            }

            if (sweptConfigs.isEmpty()) {
                channel.sendResponse(getRestResponse(0, failedIndices))
            } else {
                updateManagedIndexConfig(sweptConfigs)
            }
        }

        private fun updateManagedIndexConfig(sweptConfigs: List<SweptManagedIndexConfig>) {
            val mapOfItemIdToIndex = mutableMapOf<Int, Pair<String, String>>()
            val bulkRequest = BulkRequest()
            sweptConfigs.forEachIndexed { index, sweptConfig ->
                // compare the sweptconfig policy to the get policy here and update changePolicy
                val currentStateName = indexUuidToCurrentState[sweptConfig.uuid]
                val updatedChangePolicy = changePolicy
                    .copy(safe = sweptConfig.policy?.isSafeToChange(currentStateName, policy, changePolicy) == true)
                bulkRequest.add(updateManagedIndexRequest(sweptConfig.copy(changePolicy = updatedChangePolicy)))
                mapOfItemIdToIndex[index] = sweptConfig.index to sweptConfig.uuid
            }
            client.bulk(bulkRequest, onBulkResponse(mapOfItemIdToIndex))
        }

        private fun onBulkResponse(mapOfItemIdToIndex: Map<Int, Pair<String, String>>): RestResponseListener<BulkResponse> {
            return object : RestResponseListener<BulkResponse>(channel) {
                override fun buildResponse(bulkResponse: BulkResponse): RestResponse {
                    val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }
                    failedResponses.forEach {
                        val indexPair = mapOfItemIdToIndex[it.itemId]
                        if (indexPair != null) {
                            failedIndices.add(FailedIndex(indexPair.first, indexPair.second, it.failureMessage))
                        }
                    }

                    return getRestResponse((bulkResponse.items ?: arrayOf()).size - failedResponses.size, failedIndices)
                }
            }
        }

        private fun getRestResponse(updated: Int, failedIndices: List<FailedIndex>): BytesRestResponse {
            val builder = channel.newBuilder()
                .startObject()
                .field(UPDATED_INDICES, updated)
                .field(FAILURES, failedIndices.isNotEmpty())
                .field(FAILED_INDICES, failedIndices)
                .endObject()
            return BytesRestResponse(RestStatus.OK, builder)
        }

        private fun contentParser(bytesReference: BytesReference): XContentParser {
            return XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON)
        }
    }

    @Suppress("SpreadOperator")
    private fun getManagedIndexConfigSearchQuery(managedIndexUuids: Array<String>): SearchRequest {
        val idsQuery = IdsQueryBuilder().addIds(*managedIndexUuids)
        return SearchRequest()
            .indices(INDEX_STATE_MANAGEMENT_INDEX)
            .source(SearchSourceBuilder.searchSource()
                .seqNoAndPrimaryTerm(true)
                .size(MAX_HITS)
                .fetchSource(
                    arrayOf(
                        "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.INDEX_FIELD}",
                        "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.INDEX_UUID_FIELD}",
                        "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.POLICY_ID_FIELD}",
                        "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.POLICY_FIELD}",
                        "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.CHANGE_POLICY_FIELD}"
                    ),
                    emptyArray()
                )
                .query(idsQuery))
    }

    companion object {
        const val CHANGE_POLICY_BASE_URI = "$ISM_BASE_URI/change_policy"
        const val INDEX_NOT_MANAGED = "This index is not being managed"
        const val INDEX_IN_TRANSITION = "Cannot change policy while transitioning to new state"
        const val INDEX_NOT_INITIALIZED = "This managed index has not been initialized yet"
        const val MAX_HITS = 10_000
    }
}
