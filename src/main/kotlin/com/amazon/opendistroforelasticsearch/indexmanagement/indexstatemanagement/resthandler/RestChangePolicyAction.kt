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

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getPolicyID
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.coordinator.SweptManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.FAILED_INDICES
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.FAILURES
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.FailedIndex
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.UPDATED_INDICES
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.isSafeToChange
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.updateManagedIndexRequest
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.get.MultiGetRequest
import org.elasticsearch.action.get.MultiGetResponse
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.Strings
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.Index
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.BytesRestResponse
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestResponseListener
import org.elasticsearch.search.fetch.subphase.FetchSourceContext
import java.io.IOException

class RestChangePolicyAction(val clusterService: ClusterService) : BaseRestHandler() {

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
        private val managedIndicesToUpdate = mutableListOf<Pair<String, String>>()
        private val indexUuidToCurrentState = mutableMapOf<String, String>()
        private lateinit var policy: Policy
        private lateinit var getPolicyResponse: GetResponse
        private lateinit var clusterState: ClusterState

        /**
         *  get back the policy to change, update config index mapping
         *  get cluster state of indices to change policy
         *  get metadata for these indices, determine which managed indices need to be changed
         */
        fun start() {
            val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, changePolicy.policyID)

            client.get(getRequest, ActionListener.wrap(::onGetPolicyResponse, ::onFailure))
        }

        private fun onGetPolicyResponse(response: GetResponse) {
            if (!response.isExists || response.isSourceEmpty) {
                return channel.sendResponse(BytesRestResponse(RestStatus.NOT_FOUND, "Could not find policy=${changePolicy.policyID}"))
            }
            this.getPolicyResponse = response

            IndexUtils.checkAndUpdateConfigIndexMapping(
                clusterService.state(),
                client.admin().indices(),
                ActionListener.wrap(::onUpdateMapping, ::onFailure)
            )
        }

        private fun onUpdateMapping(acknowledgedResponse: AcknowledgedResponse) {
            if (!acknowledgedResponse.isAcknowledged) {
                return channel.sendResponse(BytesRestResponse(RestStatus.FAILED_DEPENDENCY,
                    "Could not update $INDEX_MANAGEMENT_INDEX with new mapping."))
            }

            policy = XContentHelper.createParser(
                channel.request().xContentRegistry,
                LoggingDeprecationHandler.INSTANCE,
                getPolicyResponse.sourceAsBytesRef,
                XContentType.JSON
            ).use { Policy.parseWithType(it, getPolicyResponse.id, getPolicyResponse.seqNo, getPolicyResponse.primaryTerm) }

            getClusterState()
        }

        @Suppress("SpreadOperator")
        private fun getClusterState() {
            val clusterStateRequest = ClusterStateRequest()
                .clear()
                .indices(*indices)
                .metadata(true)
                .local(false)
                .indicesOptions(IndicesOptions.strictExpand())

            client.admin().cluster().state(clusterStateRequest, ActionListener.wrap(::onClusterStateResponse, ::onFailure))
        }

        @Suppress("ComplexMethod")
        private fun onClusterStateResponse(response: ClusterStateResponse) {
            clusterState = response.state

            // get back managed index metadata
            client.multiGet(buildMgetMetadataRequest(clusterState), ActionListener.wrap(::onMgetMetadataResponse, ::onFailure))
        }

        /** check which managed index to get and update, based comparing its metadata */
        @Suppress("ComplexMethod")
        private fun onMgetMetadataResponse(mgetResponse: MultiGetResponse) {
            val metadataList = mgetResponseToList(mgetResponse)
            val includedStates = changePolicy.include.map { it.state }.toSet()

            clusterState.metadata.indices.forEachIndexed { ind, it ->
                val indexMetaData = it.value
                val managedIndexMetadata: ManagedIndexMetaData? = metadataList[ind]

                val currentState = managedIndexMetadata?.stateMetaData?.name
                if (currentState != null) {
                    indexUuidToCurrentState[indexMetaData.indexUUID] = currentState
                }

                when {
                    // If there is no policyID on the index then it's not currently being managed
                    indexMetaData.getPolicyID() == null ->
                        failedIndices.add(FailedIndex(indexMetaData.index.name, indexMetaData.index.uuid, INDEX_NOT_MANAGED))
                    // else if there exists a transitionTo on the ManagedIndexMetaData then we will
                    // fail as they might not of meant to add a ChangePolicy when its on the next state
                    managedIndexMetadata?.transitionTo != null ->
                        failedIndices.add(FailedIndex(indexMetaData.index.name, indexMetaData.index.uuid, INDEX_IN_TRANSITION))
                    // else if there is no ManagedIndexMetaData yet then the managed index has not initialized and we can change the policy safely
                    managedIndexMetadata == null ->
                        managedIndicesToUpdate.add(indexMetaData.index.name to indexMetaData.index.uuid)
                    // else if the includedStates is empty (i.e. not being used) then we will always try to update the managed index
                    includedStates.isEmpty() -> managedIndicesToUpdate.add(indexMetaData.index.name to indexMetaData.index.uuid)
                    // else only update the managed index if its currently in one of the included states
                    includedStates.contains(managedIndexMetadata.stateMetaData?.name) ->
                        managedIndicesToUpdate.add(indexMetaData.index.name to indexMetaData.index.uuid)
                    // else the managed index did not match any of the included state filters and we will not update it
                    else -> log.debug("Skipping ${indexMetaData.index.name} as it does not match any of the include state filters")
                }
            }

            if (managedIndicesToUpdate.isEmpty()) {
                channel.sendResponse(getRestResponse(0, failedIndices))
            } else {
                client.multiGet(
                    mgetManagedIndexConfigRequest(managedIndicesToUpdate.map { (_, indexUuid) -> indexUuid }.toTypedArray()),
                    ActionListener.wrap(::onMultiGetResponse, ::onFailure)
                )
            }
        }

        /** get back a list of managed indices to update */
        private fun onMultiGetResponse(response: MultiGetResponse) {
            val foundManagedIndices = mutableSetOf<String>()
            val sweptConfigs = response.responses.mapNotNull {
                if (!it.isFailed && it.response != null) {
                    foundManagedIndices.add(it.response.id) // This id is the index uuid
                    SweptManagedIndexConfig.parseWithType(contentParser(it.response.sourceAsBytesRef), it.response.seqNo, it.response.primaryTerm)
                } else {
                    null
                }
            }

            // If we do not find a matching ManagedIndexConfig for one of the provided managedIndexUuids
            // it means that we have not yet created a job for that index (which can happen during the small
            // gap of adding a policy_id to an index and a job being created by the coordinator, or the coordinator
            // failing to create a job and waiting for the sweep to create the job). We will add these as failed indices
            // that can not be updated from the ChangePolicy yet.
            managedIndicesToUpdate.forEach {
                val (indexName, indexUuid) = it
                if (!foundManagedIndices.contains(indexUuid)) {
                    failedIndices.add(FailedIndex(indexName, indexUuid, INDEX_NOT_INITIALIZED))
                }
            }

            if (sweptConfigs.isEmpty()) {
                channel.sendResponse(getRestResponse(0, failedIndices))
            } else {
                updateManagedIndexConfig(sweptConfigs)
            }
        }

        private fun updateManagedIndexConfig(sweptConfigs: List<SweptManagedIndexConfig>) {
            val mapOfItemIdToIndex = mutableMapOf<Int, Index>()
            val bulkUpdateManagedIndexRequest = BulkRequest()
            sweptConfigs.forEachIndexed { id, sweptConfig ->
                // compare the sweptconfig policy to the get policy here and update changePolicy
                val currentStateName = indexUuidToCurrentState[sweptConfig.uuid]
                val updatedChangePolicy = changePolicy
                    .copy(isSafe = sweptConfig.policy?.isSafeToChange(currentStateName, policy, changePolicy) == true)
                bulkUpdateManagedIndexRequest.add(updateManagedIndexRequest(sweptConfig.copy(changePolicy = updatedChangePolicy)))
                mapOfItemIdToIndex[id] = Index(sweptConfig.index, sweptConfig.uuid)
            }
            client.bulk(bulkUpdateManagedIndexRequest, onBulkResponse(mapOfItemIdToIndex))
        }

        private fun onBulkResponse(mapOfItemIdToIndex: Map<Int, Index>): RestResponseListener<BulkResponse> {
            return object : RestResponseListener<BulkResponse>(channel) {
                override fun buildResponse(bulkResponse: BulkResponse): RestResponse {
                    val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }
                    failedResponses.forEach {
                        val index = mapOfItemIdToIndex[it.itemId]
                        if (index != null) {
                            failedIndices.add(FailedIndex(index.name, index.uuid, it.failureMessage))
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
    }

    @Suppress("SpreadOperator")
    private fun mgetManagedIndexConfigRequest(managedIndexUuids: Array<String>): MultiGetRequest {
        val request = MultiGetRequest()
        val includes = arrayOf(
            "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.INDEX_FIELD}",
            "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.INDEX_UUID_FIELD}",
            "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.POLICY_ID_FIELD}",
            "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.POLICY_FIELD}",
            "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.CHANGE_POLICY_FIELD}"
        )
        val excludes = emptyArray<String>()
        val fetchSourceContext = FetchSourceContext(true, includes, excludes)
        managedIndexUuids.forEach { request.add(MultiGetRequest.Item(INDEX_MANAGEMENT_INDEX, it).fetchSourceContext(fetchSourceContext)) }
        return request
    }

    companion object {
        const val CHANGE_POLICY_BASE_URI = "$ISM_BASE_URI/change_policy"
        const val INDEX_NOT_MANAGED = "This index is not being managed"
        const val INDEX_IN_TRANSITION = "Cannot change policy while transitioning to new state"
        const val INDEX_NOT_INITIALIZED = "This managed index has not been initialized yet"
    }
}
