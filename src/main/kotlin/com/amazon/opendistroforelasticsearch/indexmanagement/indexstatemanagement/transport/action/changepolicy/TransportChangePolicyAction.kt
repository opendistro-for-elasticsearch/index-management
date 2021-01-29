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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.changepolicy

import com.amazon.opendistroforelasticsearch.commons.ConfigConstants
import com.amazon.opendistroforelasticsearch.commons.authuser.User
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.coordinator.SweptManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler.RestChangePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.FailedIndex
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.isSafeToChange
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.updateManagedIndexRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils
import com.amazon.opendistroforelasticsearch.indexmanagement.util.NO_ID
import com.amazon.opendistroforelasticsearch.indexmanagement.util.resolveUser
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.get.MultiGetRequest
import org.elasticsearch.action.get.MultiGetResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.fetch.subphase.FetchSourceContext
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

private val log = LogManager.getLogger(TransportChangePolicyAction::class.java)

class TransportChangePolicyAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<ChangePolicyRequest, ISMStatusResponse>(
    ChangePolicyAction.NAME, transportService, actionFilters, ::ChangePolicyRequest
) {

    @Volatile lateinit var user: User

    override fun doExecute(task: Task, request: ChangePolicyRequest, listener: ActionListener<ISMStatusResponse>) {
        val userStr = client.threadPool().threadContext.getTransient<String>(ConfigConstants.OPENDISTRO_SECURITY_USER_INFO_THREAD_CONTEXT)
        user = resolveUser(User.parse(userStr))

        client.threadPool().threadContext.stashContext().use {
            ChangePolicyHandler(client, listener, request).start()
        }
    }

    @Suppress("TooManyFunctions")
    inner class ChangePolicyHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<ISMStatusResponse>,
        private val request: ChangePolicyRequest
    ) {
        private val failedIndices = mutableListOf<FailedIndex>()
        private val managedIndexUuids = mutableListOf<Pair<String, String>>()
        private val indexUuidToCurrentState = mutableMapOf<String, String>()
        lateinit var policy: Policy
        lateinit var response: GetResponse
        private var updated: Int = 0

        fun start() {
            val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, request.changePolicy.policyID)

            client.get(getRequest, ActionListener.wrap(::onGetPolicyResponse, ::onFailure))
        }

        private fun onFailure(t: Exception) {
            log.info("change policy failure $t")
            actionListener.onFailure(t)
        }

        private fun onGetPolicyResponse(response: GetResponse) {
            if (!response.isExists || response.isSourceEmpty) {
                actionListener.onFailure(ElasticsearchStatusException("Could not find policy=${request.changePolicy.policyID}", RestStatus.NOT_FOUND))
                return
            }
            this.response = response
            IndexUtils.checkAndUpdateConfigIndexMapping(
                clusterService.state(),
                client.admin().indices(),
                ActionListener.wrap(::onUpdateMapping, ::onFailure))
        }

        private fun onUpdateMapping(acknowledgedResponse: AcknowledgedResponse) {
            if (!acknowledgedResponse.isAcknowledged) {
                actionListener.onFailure(ElasticsearchStatusException(
                    "Could not update ${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX} with new mapping.",
                    RestStatus.FAILED_DEPENDENCY))
                return
            }
            policy = XContentHelper.createParser(
                xContentRegistry,
                LoggingDeprecationHandler.INSTANCE,
                response.sourceAsBytesRef,
                XContentType.JSON
            ).use { it.parseWithType(response.id, response.seqNo, response.primaryTerm, Policy.Companion::parse) }

            getClusterState()
        }

        @Suppress("SpreadOperator")
        private fun getClusterState() {
            val clusterStateRequest = ClusterStateRequest()
                .clear()
                .indices(*request.indices.toTypedArray())
                .metadata(true)
                .local(false)
                .indicesOptions(IndicesOptions.strictExpand())

            client.admin().cluster().state(clusterStateRequest, ActionListener.wrap(::processResponse, ::onFailure))
        }

        @Suppress("ComplexMethod")
        private fun processResponse(response: ClusterStateResponse) {
            val includedStates = request.changePolicy.include.map { it.state }.toSet()

            response.state.metadata.indices.forEach {
                val indexMetaData = it.value
                val currentState = indexMetaData.getManagedIndexMetaData()?.stateMetaData?.name
                if (currentState != null) {
                    indexUuidToCurrentState[indexMetaData.indexUUID] = currentState
                }
                when {
                    // if there exists a transitionTo on the ManagedIndexMetaData then we will
                    // fail as they might not of meant to add a ChangePolicy when its on the next state
                    indexMetaData.getManagedIndexMetaData()?.transitionTo != null ->
                        failedIndices.add(FailedIndex(indexMetaData.index.name, indexMetaData.index.uuid, RestChangePolicyAction.INDEX_IN_TRANSITION))
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

            if (managedIndexUuids.isEmpty()) {
                updated = 0
                actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
                return
            } else {
                client.multiGet(
                    getManagedIndexConfigMultiGetRequest(managedIndexUuids.map { (_, indexUuid) -> indexUuid }.toTypedArray()),
                        ActionListener.wrap(::onMultiGetResponse, ::onFailure))
            }
        }

        private fun onMultiGetResponse(response: MultiGetResponse) {
            val foundManagedIndices = mutableSetOf<String>()
            val sweptConfigs = response.responses.mapNotNull {
                // The id is the index uuid
                if (!it.response.isExists) { // meaning this index is not managed
                    val indexUuid = it.response.id
                    val indexName = managedIndexUuids.find { (_, second) -> second == indexUuid }?.first
                    if (indexName != null) {
                        failedIndices.add(FailedIndex(indexName, indexUuid, RestChangePolicyAction.INDEX_NOT_MANAGED))
                    }
                }
                if (!it.isFailed && !it.response.isSourceEmpty) {
                    foundManagedIndices.add(it.response.id)
                    contentParser(it.response.sourceAsBytesRef).parseWithType(NO_ID, it.response.seqNo,
                            it.response.primaryTerm, SweptManagedIndexConfig.Companion::parse)
                } else {
                    null
                }
            }

            if (sweptConfigs.isEmpty()) {
                updated = 0
                actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
                return
            } else {
                updateManagedIndexConfig(sweptConfigs)
            }
        }

        private fun updateManagedIndexConfig(sweptConfigs: List<SweptManagedIndexConfig>) {
            val mapOfItemIdToIndex = mutableMapOf<Int, Pair<String, String>>()
            val bulkRequest = BulkRequest()
            sweptConfigs.forEachIndexed { index, sweptConfig ->
                // compare the sweptConfig policy to the get policy here and update changePolicy
                val currentStateName = indexUuidToCurrentState[sweptConfig.uuid]
                val updatedChangePolicy = request.changePolicy
                        .copy(isSafe = sweptConfig.policy?.isSafeToChange(currentStateName, policy, request.changePolicy) == true)
                bulkRequest.add(updateManagedIndexRequest(sweptConfig.copy(changePolicy = updatedChangePolicy), user))
                mapOfItemIdToIndex[index] = sweptConfig.index to sweptConfig.uuid
            }

            client.bulk(bulkRequest, object : ActionListener<BulkResponse> {
                override fun onResponse(response: BulkResponse) {
                    onBulkResponse(response, mapOfItemIdToIndex)
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(t)
                }
            })
        }

        private fun onBulkResponse(bulkResponse: BulkResponse, mapOfItemIdToIndex: Map<Int, Pair<String, String>>) {
            val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }
            failedResponses.forEach {
                val indexPair = mapOfItemIdToIndex[it.itemId]
                if (indexPair != null) {
                    failedIndices.add(FailedIndex(indexPair.first, indexPair.second, it.failureMessage))
                }
            }

            updated = (bulkResponse.items ?: arrayOf()).size - failedResponses.size
            actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
        }

        private fun contentParser(bytesReference: BytesReference): XContentParser {
            return XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON)
        }

        @Suppress("SpreadOperator")
        private fun getManagedIndexConfigMultiGetRequest(managedIndexUuids: Array<String>): MultiGetRequest {
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
            managedIndexUuids.forEach {
                request.add(MultiGetRequest.Item(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, it).fetchSourceContext(fetchSourceContext)) }
            return request
        }
    }
}
