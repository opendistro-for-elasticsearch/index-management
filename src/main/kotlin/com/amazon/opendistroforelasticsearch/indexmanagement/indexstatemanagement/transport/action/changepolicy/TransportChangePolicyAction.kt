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
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.contentParser
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.buildMgetMetadataRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.mgetResponseToList
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
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
import com.amazon.opendistroforelasticsearch.indexmanagement.util.use
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.ExceptionsHelper
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
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.Index
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

    override fun doExecute(task: Task, request: ChangePolicyRequest, listener: ActionListener<ISMStatusResponse>) {
        val userStr = client.threadPool().threadContext.getTransient<String>(ConfigConstants.OPENDISTRO_SECURITY_USER_INFO_THREAD_CONTEXT)
        val user = resolveUser(User.parse(userStr))

        client.threadPool().threadContext.stashContext().use {
            ChangePolicyHandler(client, listener, request, user).start()
        }
    }

    @Suppress("TooManyFunctions")
    inner class ChangePolicyHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<ISMStatusResponse>,
        private val request: ChangePolicyRequest,
        private val user: User
    ) {

        private val failedIndices = mutableListOf<FailedIndex>()
        private val managedIndicesToUpdate = mutableListOf<Pair<String, String>>()
        private val indexUuidToCurrentState = mutableMapOf<String, String>()
        private val changePolicy = request.changePolicy
        private lateinit var policy: Policy
        private lateinit var getPolicyResponse: GetResponse
        private lateinit var clusterState: ClusterState
        private var updated: Int = 0

        fun start() {
            val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, changePolicy.policyID)

            client.get(getRequest, ActionListener.wrap(::onGetPolicyResponse, ::onFailure))
        }

        private fun onGetPolicyResponse(response: GetResponse) {
            if (!response.isExists || response.isSourceEmpty) {
                actionListener.onFailure(ElasticsearchStatusException("Could not find policy=${request.changePolicy.policyID}", RestStatus.NOT_FOUND))
                return
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
                actionListener.onFailure(ElasticsearchStatusException(
                    "Could not update ${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX} with new mapping.",
                    RestStatus.FAILED_DEPENDENCY))
                return
            }

            policy = XContentHelper.createParser(
                xContentRegistry,
                LoggingDeprecationHandler.INSTANCE,
                getPolicyResponse.sourceAsBytesRef,
                XContentType.JSON
            ).use { it.parseWithType(getPolicyResponse.id, getPolicyResponse.seqNo, getPolicyResponse.primaryTerm, Policy.Companion::parse) }

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

            client.admin().cluster().state(clusterStateRequest, ActionListener.wrap(::onClusterStateResponse, ::onFailure))
        }

        @Suppress("ComplexMethod")
        private fun onClusterStateResponse(response: ClusterStateResponse) {
            clusterState = response.state

            // get back managed index metadata
            client.multiGet(buildMgetMetadataRequest(clusterState), ActionListener.wrap(::onMgetMetadataResponse, ::onFailure))
        }

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
                    // if there exists a transitionTo on the ManagedIndexMetaData then we will
                    // fail as they might not of meant to add a ChangePolicy when its on the next state
                    managedIndexMetadata?.transitionTo != null ->
                        failedIndices.add(FailedIndex(indexMetaData.index.name, indexMetaData.index.uuid,
                            RestChangePolicyAction.INDEX_IN_TRANSITION
                        ))
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
                updated = 0
                actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
                return
            } else {
                client.multiGet(
                    mgetManagedIndexConfigRequest(managedIndicesToUpdate.map { (_, indexUuid) -> indexUuid }.toTypedArray()),
                    ActionListener.wrap(::onMultiGetResponse, ::onFailure)
                )
            }
        }

        private fun onMultiGetResponse(response: MultiGetResponse) {
            val foundManagedIndices = mutableSetOf<String>()
            val sweptConfigs = response.responses.mapNotNull {
                // The id is the index uuid
                if (!it.response.isExists) { // meaning this index is not managed
                    val indexUuid = it.response.id
                    val indexName = managedIndicesToUpdate.find { (_, second) -> second == indexUuid }?.first
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
            val mapOfItemIdToIndex = mutableMapOf<Int, Index>()
            val bulkUpdateManagedIndexRequest = BulkRequest()
            sweptConfigs.forEachIndexed { id, sweptConfig ->
                // compare the sweptconfig policy to the get policy here and update changePolicy
                val currentStateName = indexUuidToCurrentState[sweptConfig.uuid]
                val updatedChangePolicy = changePolicy
                    .copy(isSafe = sweptConfig.policy?.isSafeToChange(currentStateName, policy, changePolicy) == true)
                bulkUpdateManagedIndexRequest.add(updateManagedIndexRequest(sweptConfig.copy(changePolicy = updatedChangePolicy), user))
                mapOfItemIdToIndex[id] = Index(sweptConfig.index, sweptConfig.uuid)
            }
            client.bulk(bulkUpdateManagedIndexRequest, object : ActionListener<BulkResponse> {
                override fun onResponse(response: BulkResponse) {
                    onBulkResponse(response, mapOfItemIdToIndex)
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                }
            })
        }

        private fun onBulkResponse(bulkResponse: BulkResponse, mapOfItemIdToIndex: Map<Int, Index>) {
            val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }
            failedResponses.forEach {
                val indexPair = mapOfItemIdToIndex[it.itemId]
                if (indexPair != null) {
                    failedIndices.add(FailedIndex(indexPair.name, indexPair.uuid, it.failureMessage))
                }
            }

            updated = (bulkResponse.items ?: arrayOf()).size - failedResponses.size
            actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
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
            managedIndexUuids.forEach { request.add(MultiGetRequest.Item(
                IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, it).fetchSourceContext(fetchSourceContext).routing(it)) }
            return request
        }

        private fun onFailure(t: Exception) {
            actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
        }
    }
}
