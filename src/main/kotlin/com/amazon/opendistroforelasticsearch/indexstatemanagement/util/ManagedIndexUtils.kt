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
@file:Suppress("TooManyFunctions")
@file:JvmName("ManagedIndexUtils")
package com.amazon.opendistroforelasticsearch.indexstatemanagement.util

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.INDEX_STATE_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexstatemanagement.ManagedIndexCoordinator
import com.amazon.opendistroforelasticsearch.indexstatemanagement.action.Action
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Transition
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.RolloverActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.TransitionsActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.coordinator.ClusterStateManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.coordinator.SweptManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StateMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.delete.AttemptDeleteStep
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder
import java.time.Instant
import java.time.temporal.ChronoUnit

fun createManagedIndexRequest(index: String, uuid: String, policyID: String): IndexRequest {
    val managedIndexConfig = ManagedIndexConfig(
        jobName = index,
        index = index,
        indexUuid = uuid,
        enabled = true,
        jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
        jobLastUpdatedTime = Instant.now(),
        jobEnabledTime = Instant.now(),
        policyID = policyID,
        policy = null,
        policySeqNo = null,
        policyPrimaryTerm = null,
        changePolicy = null
    )

    return IndexRequest(INDEX_STATE_MANAGEMENT_INDEX)
            .id(uuid)
            .create(true)
            .source(managedIndexConfig.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
}

fun createManagedIndexRequest(managedIndexConfig: ManagedIndexConfig): IndexRequest {
    return IndexRequest(INDEX_STATE_MANAGEMENT_INDEX)
            .id(managedIndexConfig.indexUuid)
            .setIfPrimaryTerm(managedIndexConfig.primaryTerm)
            .setIfSeqNo(managedIndexConfig.seqNo)
            .source(managedIndexConfig.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
}

fun deleteManagedIndexRequest(uuid: String): DeleteRequest {
    return DeleteRequest(INDEX_STATE_MANAGEMENT_INDEX, uuid)
}

fun updateManagedIndexRequest(clusterStateManagedIndexConfig: ClusterStateManagedIndexConfig): UpdateRequest {
    return UpdateRequest(INDEX_STATE_MANAGEMENT_INDEX,
            clusterStateManagedIndexConfig.uuid)
            .setIfPrimaryTerm(clusterStateManagedIndexConfig.primaryTerm)
            .setIfSeqNo(clusterStateManagedIndexConfig.seqNo)
            .doc(clusterStateManagedIndexConfig.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
}

/**
 * Creates IndexRequests for [ManagedIndexConfig].
 *
 * Finds ManagedIndices that exist in the cluster state that do not yet exist in [INDEX_STATE_MANAGEMENT_INDEX]
 * which means we need to create the [ManagedIndexConfig].
 *
 * @param clusterStateManagedIndexConfigs map of IndexUuid to [ClusterStateManagedIndexConfig].
 * @param currentManagedIndexConfigs map of IndexUuid to [SweptManagedIndexConfig].
 * @return list of [DocWriteRequest].
 */
@OpenForTesting
fun getCreateManagedIndexRequests(
    clusterStateManagedIndexConfigs: Map<String, ClusterStateManagedIndexConfig>,
    currentManagedIndexConfigs: Map<String, SweptManagedIndexConfig>
): List<DocWriteRequest<*>> {
    return clusterStateManagedIndexConfigs.filter { (uuid) ->
        !currentManagedIndexConfigs.containsKey(uuid)
    }.map { createManagedIndexRequest(it.value.index, it.value.uuid, it.value.policyID) }
}

/**
 * Creates DeleteRequests for [ManagedIndexConfig].
 *
 * Finds ManagedIndices that exist in [INDEX_STATE_MANAGEMENT_INDEX] that do not exist in the cluster state
 * anymore which means we need to delete the [ManagedIndexConfig].
 *
 * @param clusterStateManagedIndexConfigs map of IndexUuid to [ClusterStateManagedIndexConfig].
 * @param currentManagedIndexConfigs map of IndexUuid to [SweptManagedIndexConfig].
 * @return list of [DocWriteRequest].
 */
@OpenForTesting
fun getDeleteManagedIndexRequests(
    clusterStateManagedIndexConfigs: Map<String, ClusterStateManagedIndexConfig>,
    currentManagedIndexConfigs: Map<String, SweptManagedIndexConfig>
): List<DocWriteRequest<*>> {
    return currentManagedIndexConfigs.filter { (uuid) ->
        !clusterStateManagedIndexConfigs.containsKey(uuid)
    }.map { deleteManagedIndexRequest(it.value.uuid) }
}

/**
 * Creates UpdateRequests for [ManagedIndexConfig].
 *
 * Finds ManagedIndices that exist both in cluster state and in [INDEX_STATE_MANAGEMENT_INDEX] that
 * need to be updated. We know a [ManagedIndexConfig] needs to be updated when the policyID differs between
 * the [ClusterStateManagedIndexConfig] and the [SweptManagedIndexConfig]. And we know
 * a [ManagedIndexConfig] has not yet been updated if it's ChangePolicy does not match the new policyID.
 *
 * @param clusterStateManagedIndexConfigs map of IndexUuid to [ClusterStateManagedIndexConfig].
 * @param currentManagedIndexConfigs map of IndexUuid to [SweptManagedIndexConfig].
 * @return list of [DocWriteRequest].
 */
@OpenForTesting
fun getUpdateManagedIndexRequests(
    clusterStateManagedIndexConfigs: Map<String, ClusterStateManagedIndexConfig>,
    currentManagedIndexConfigs: Map<String, SweptManagedIndexConfig>
): List<DocWriteRequest<*>> {
    return clusterStateManagedIndexConfigs.asSequence()
            .filter { (uuid, clusterConfig) ->
                val sweptConfig = currentManagedIndexConfigs[uuid]
                sweptConfig != null &&
                        // Verify they have different policy names which means we should update it
                        sweptConfig.policyID != clusterConfig.policyID &&
                        // Verify it is not already being updated
                        sweptConfig.changePolicy?.policyID != clusterConfig.policyID
            }
            .map {
                val sweptConfig = currentManagedIndexConfigs[it.key]
                if (sweptConfig == null) {
                    updateManagedIndexRequest(it.value)
                } else {
                    updateManagedIndexRequest(
                            it.value.copy(seqNo = sweptConfig.seqNo,
                                    primaryTerm = sweptConfig.primaryTerm)
                    )
                }
            }.toList()
}

fun getSweptManagedIndexSearchRequest(): SearchRequest {
    val boolQueryBuilder = BoolQueryBuilder().filter(QueryBuilders.existsQuery(ManagedIndexConfig.MANAGED_INDEX_TYPE))
    return SearchRequest()
            .indices(INDEX_STATE_MANAGEMENT_INDEX)
            .source(SearchSourceBuilder.searchSource()
                    // TODO: Get all ManagedIndices at once or split into searchAfter queries?
                    .size(ManagedIndexCoordinator.MAX_HITS)
                    .seqNoAndPrimaryTerm(true)
                    .fetchSource(
                            arrayOf(
                                    "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.INDEX_FIELD}",
                                    "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.INDEX_UUID_FIELD}",
                                    "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.POLICY_ID_FIELD}",
                                    "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.CHANGE_POLICY_FIELD}"
                            ),
                            emptyArray()
                    )
                    .query(boolQueryBuilder))
}

@Suppress("ReturnCount")
fun Transition.evaluateConditions(
    indexCreationDate: Instant,
    numDocs: Long,
    indexSize: ByteSizeValue,
    transitionStartTime: Instant
): Boolean {
    // If there are no conditions, treat as always true
    if (this.conditions == null) return true

    if (this.conditions.docCount != null) {
        return this.conditions.docCount <= numDocs
    }

    if (this.conditions.indexAge != null) {
        val elapsedTime = Instant.now().toEpochMilli() - indexCreationDate.toEpochMilli()
        return this.conditions.indexAge.millis <= elapsedTime
    }

    if (this.conditions.size != null) {
        return this.conditions.size <= indexSize
    }

    if (this.conditions.cron != null) {
        // If a cron pattern matches the time between the start of "attempt_transition" to now then we consider it meeting the condition
        return this.conditions.cron.getNextExecutionTime(transitionStartTime) <= Instant.now()
    }

    // We should never reach this
    return false
}

@Suppress("ReturnCount")
fun RolloverActionConfig.evaluateConditions(
    indexCreationDate: Instant,
    numDocs: Long,
    indexSize: ByteSizeValue
): Boolean {

    if (this.minDocs != null) {
        return this.minDocs <= numDocs
    }

    if (this.minAge != null) {
        val elapsedTime = Instant.now().toEpochMilli() - indexCreationDate.toEpochMilli()
        return this.minAge.millis <= elapsedTime
    }

    if (this.minSize != null) {
        return this.minSize <= indexSize
    }

    // If no conditions specified we default to true
    return true
}

fun Policy.getStateToExecute(managedIndexMetaData: ManagedIndexMetaData): State? {
    if (managedIndexMetaData.transitionTo != null) {
        return this.states.find { it.name == managedIndexMetaData.transitionTo }
    }
    return this.states.find { managedIndexMetaData.stateMetaData != null && it.name == managedIndexMetaData.stateMetaData.name }
}

// TODO: This doesn't look great because it thinks actionIndex can be null while action is not (should never happen)
//  Should further break down ManagedIndexMetaData into StateMetaData, ActionMetaData, StepMetaData
fun State.getActionToExecute(
    clusterService: ClusterService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData
): Action? {
    var actionConfig: ActionConfig?

    // If we are transitioning to this state get the first action in the state
    // If the action/actionIndex are null it means we just initialized and should get the first action from the state
    if (managedIndexMetaData.transitionTo != null || managedIndexMetaData.actionMetaData == null) {
        actionConfig = this.actions.firstOrNull() ?: TransitionsActionConfig(this.transitions)
    } else if (managedIndexMetaData.actionMetaData.name == ActionConfig.ActionType.TRANSITION.type) {
        // If the current action is transition and we do not have a transitionTo set then we should be in Transition
        actionConfig = TransitionsActionConfig(this.transitions)
    } else {
        // Get the current actionConfig that is in the ManagedIndexMetaData
        actionConfig = this.actions.filterIndexed { index, config ->
            index == managedIndexMetaData.actionMetaData.index && config.type.type == managedIndexMetaData.actionMetaData.name
        }.firstOrNull()

        if (actionConfig == null) return null

        // TODO: Refactor so we can get isLastStep from somewhere besides an instantiated Action class so we can simplify this to a when block
        // If stepCompleted is true and this is the last step of the action then we should get the next action
        if (managedIndexMetaData.stepMetaData != null && managedIndexMetaData.stepMetaData.completed) {
            val action = actionConfig.toAction(clusterService, client, managedIndexMetaData)
            if (action.isLastStep(managedIndexMetaData.stepMetaData.name)) {
                actionConfig = this.actions.getOrNull(managedIndexMetaData.actionMetaData.index + 1) ?: TransitionsActionConfig(this.transitions)
            }
        }
    }

    return actionConfig.toAction(clusterService, client, managedIndexMetaData)
}

fun State.getUpdatedStateMetaData(managedIndexMetaData: ManagedIndexMetaData): StateMetaData {
    // If the current ManagedIndexMetaData state does not match this state, it means we transitioned and need to update the startStartTime
    val stateMetaData = managedIndexMetaData.stateMetaData
    return when {
        stateMetaData == null -> StateMetaData(this.name, Instant.now().toEpochMilli())
        stateMetaData.name != this.name -> StateMetaData(this.name, Instant.now().toEpochMilli())
        else -> stateMetaData
    }
}

fun Action.getUpdatedActionMetaData(managedIndexMetaData: ManagedIndexMetaData, state: State): ActionMetaData {
    val stateMetaData = managedIndexMetaData.stateMetaData
    val actionMetaData = managedIndexMetaData.actionMetaData

    return when {
        stateMetaData?.name != state.name -> ActionMetaData(this.type.type, Instant.now().toEpochMilli(), this.config.actionIndex, false, 0)
        actionMetaData?.index != this.config.actionIndex -> ActionMetaData(this.type.type, Instant.now().toEpochMilli(), this.config.actionIndex, false, 0)
        else -> actionMetaData
    }
}

@Suppress("ReturnCount")
fun ManagedIndexMetaData.getUpdatedManagedIndexMetaData(
    state: State?,
    action: Action?,
    step: Step?
): ManagedIndexMetaData {
    // State can be null if the transition_to state or the current metadata state is not found in the policy
    if (state == null) {
        return this.copy(
            policyRetryInfo = PolicyRetryInfoMetaData(true, 0),
            info = mapOf("message" to "Failed to find state=${this.transitionTo ?: this.stateMetaData} in policy=${this.policyID}")
        )
    }

    // Action can only be null if the metadata action type/actionIndex do not match in state.actions
    // Step can only be null if Action is null
    if (action == null || step == null) {
        return this.copy(
            policyRetryInfo = PolicyRetryInfoMetaData(true, 0),
            info = mapOf("message" to "Failed to find action=${this.actionMetaData} in state=${this.stateMetaData}")
        )
    }

    val updatedStateMetaData = state.getUpdatedStateMetaData(this)
    val updatedActionMetaData = action.getUpdatedActionMetaData(this, state)
    val updatedStepMetaData = step.getUpdatedManagedIndexMetaData(this)

    // TODO Update Action retry logic.

    return this.copy(
        policyCompleted = action.type == ActionConfig.ActionType.TRANSITION && state.transitions.isEmpty(),
        rolledOver = updatedStepMetaData.rolledOver,
        stateMetaData = updatedStateMetaData,
        actionMetaData = updatedActionMetaData,
        stepMetaData = updatedStepMetaData.stepMetaData,
        transitionTo = updatedStepMetaData.transitionTo,
        // TODO: ConsumedRetries
        policyRetryInfo = updatedStepMetaData.policyRetryInfo,
        info = updatedStepMetaData.info
    )
}

val ManagedIndexMetaData.isSuccessfulDelete: Boolean
    get() = (this.actionMetaData != null && this.actionMetaData.name == ActionConfig.ActionType.DELETE.type && !this.actionMetaData.failed) &&
            (this.stepMetaData != null && this.stepMetaData.name == AttemptDeleteStep.name && this.stepMetaData.completed) &&
            (this.policyRetryInfo != null && !this.policyRetryInfo.failed)

val ManagedIndexMetaData.isFailed: Boolean
    get() {
        if (this.policyRetryInfo != null && this.policyRetryInfo.failed) {
            // If PolicyRetryInfo is failed then the ManagedIndex has failed.
            return true
        } else if (this.policyRetryInfo != null && !this.policyRetryInfo.failed && this.actionMetaData == null) {
            // If PolicyRetryInfo is NOT failed and the actionMetaData is null it means we haven't started any action. Then the ManagedIndex has NOT failed.
            return false
        } else if (this.actionMetaData != null && this.actionMetaData.failed) {
            // If ActionMetaData is not null and some action is failed. Then the ManagedIndex has failed.
            return true
        }
        return false
    }
