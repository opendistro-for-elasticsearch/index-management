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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.optionalTimeField
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ChangePolicy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Transition
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ActionRetry
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
import org.elasticsearch.script.ScriptService
import org.elasticsearch.search.builder.SearchSourceBuilder
import java.time.Instant
import java.time.temporal.ChronoUnit

fun managedIndexConfigIndexRequest(index: String, uuid: String, policyID: String, jobInterval: Int): IndexRequest {
    val managedIndexConfig = ManagedIndexConfig(
        jobName = index,
        index = index,
        indexUuid = uuid,
        enabled = true,
        jobSchedule = IntervalSchedule(Instant.now(), jobInterval, ChronoUnit.MINUTES),
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

fun managedIndexConfigIndexRequest(managedIndexConfig: ManagedIndexConfig): IndexRequest {
    return IndexRequest(INDEX_STATE_MANAGEMENT_INDEX)
            .id(managedIndexConfig.indexUuid)
            .setIfPrimaryTerm(managedIndexConfig.primaryTerm)
            .setIfSeqNo(managedIndexConfig.seqNo)
            .source(managedIndexConfig.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
}

private fun updateEnabledField(uuid: String, enabled: Boolean, enabledTime: Long?): UpdateRequest {
    val builder = XContentFactory.jsonBuilder()
        .startObject()
            .startObject(ManagedIndexConfig.MANAGED_INDEX_TYPE)
                .optionalTimeField(ManagedIndexConfig.LAST_UPDATED_TIME_FIELD, Instant.now())
                .field(ManagedIndexConfig.ENABLED_FIELD, enabled)
                .field(ManagedIndexConfig.ENABLED_TIME_FIELD, enabledTime)
            .endObject()
        .endObject()
    return UpdateRequest(INDEX_STATE_MANAGEMENT_INDEX, uuid).doc(builder)
}

fun updateDisableManagedIndexRequest(uuid: String): UpdateRequest {
    return updateEnabledField(uuid, false, null)
}

fun updateEnableManagedIndexRequest(uuid: String): UpdateRequest {
    return updateEnabledField(uuid, true, Instant.now().toEpochMilli())
}

fun deleteManagedIndexRequest(uuid: String): DeleteRequest {
    return DeleteRequest(INDEX_STATE_MANAGEMENT_INDEX, uuid)
}

fun updateManagedIndexRequest(sweptManagedIndexConfig: SweptManagedIndexConfig): UpdateRequest {
    return UpdateRequest(INDEX_STATE_MANAGEMENT_INDEX, sweptManagedIndexConfig.uuid)
        .setIfPrimaryTerm(sweptManagedIndexConfig.primaryTerm)
        .setIfSeqNo(sweptManagedIndexConfig.seqNo)
        .doc(getPartialChangePolicyBuilder(sweptManagedIndexConfig.changePolicy))
}

/**
 * Creates IndexRequests for [ManagedIndexConfig].
 *
 * Finds ManagedIndices that exist in the cluster state that do not yet exist in [INDEX_STATE_MANAGEMENT_INDEX]
 * which means we need to create the [ManagedIndexConfig].
 *
 * @param clusterStateManagedIndexConfigs map of IndexUuid to [ClusterStateManagedIndexConfig].
 * @param currentManagedIndexConfigs map of IndexUuid to [SweptManagedIndexConfig].
 * @param jobInterval dynamic int setting from cluster settings
 * @return list of [DocWriteRequest].
 */
@OpenForTesting
fun getCreateManagedIndexRequests(
    clusterStateManagedIndexConfigs: Map<String, ClusterStateManagedIndexConfig>,
    currentManagedIndexConfigs: Map<String, SweptManagedIndexConfig>,
    jobInterval: Int
): List<DocWriteRequest<*>> {
    return clusterStateManagedIndexConfigs.filter { (uuid) ->
        !currentManagedIndexConfigs.containsKey(uuid)
    }.map { managedIndexConfigIndexRequest(it.value.index, it.value.uuid, it.value.policyID, jobInterval) }
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
    numDocs: Long?,
    indexSize: ByteSizeValue?,
    transitionStartTime: Instant
): Boolean {
    // If there are no conditions, treat as always true
    if (this.conditions == null) return true

    if (this.conditions.docCount != null && numDocs != null) {
        return this.conditions.docCount <= numDocs
    }

    if (this.conditions.indexAge != null) {
        val elapsedTime = Instant.now().toEpochMilli() - indexCreationDate.toEpochMilli()
        return this.conditions.indexAge.millis <= elapsedTime
    }

    if (this.conditions.size != null && indexSize != null) {
        return this.conditions.size <= indexSize
    }

    if (this.conditions.cron != null) {
        // If a cron pattern matches the time between the start of "attempt_transition" to now then we consider it meeting the condition
        return this.conditions.cron.getNextExecutionTime(transitionStartTime) <= Instant.now()
    }

    // We should never reach this
    return false
}

fun Transition.hasStatsConditions(): Boolean = this.conditions?.docCount != null || this.conditions?.size != null

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

fun State.getActionToExecute(
    clusterService: ClusterService,
    scriptService: ScriptService,
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
        if (managedIndexMetaData.stepMetaData != null && managedIndexMetaData.stepMetaData.stepStatus == Step.StepStatus.COMPLETED) {
            val action = actionConfig.toAction(clusterService, scriptService, client, managedIndexMetaData)
            if (action.isLastStep(managedIndexMetaData.stepMetaData.name)) {
                actionConfig = this.actions.getOrNull(managedIndexMetaData.actionMetaData.index + 1) ?: TransitionsActionConfig(this.transitions)
            }
        }
    }

    return actionConfig.toAction(clusterService, scriptService, client, managedIndexMetaData)
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
        stateMetaData?.name != state.name ->
            ActionMetaData(this.type.type, Instant.now().toEpochMilli(), this.config.actionIndex, false, 0, 0, null)
        actionMetaData?.index != this.config.actionIndex ->
            ActionMetaData(this.type.type, Instant.now().toEpochMilli(), this.config.actionIndex, false, 0, 0, null)
        // RetryAPI will reset startTime to null for actionMetaData and we'll reset it to "now" here
        else -> actionMetaData.copy(startTime = actionMetaData.startTime ?: Instant.now().toEpochMilli())
    }
}

fun Action.shouldBackoff(actionMetaData: ActionMetaData?, actionRetry: ActionRetry?): Pair<Boolean, Long?>? {
    return this.config.configRetry?.backoff?.shouldBackoff(actionMetaData, actionRetry)
}

@Suppress("ReturnCount")
fun Action.hasTimedOut(actionMetaData: ActionMetaData?): Boolean {
    if (actionMetaData?.startTime == null) return false
    val configTimeout = this.config.configTimeout
    if (configTimeout == null) return false
    return (Instant.now().toEpochMilli() - actionMetaData.startTime) > configTimeout.timeout.millis
}

@Suppress("ReturnCount")
fun ManagedIndexMetaData.getStartingManagedIndexMetaData(
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
    val updatedStepMetaData = step.getStartingStepMetaData()

    return this.copy(
        stateMetaData = updatedStateMetaData,
        actionMetaData = updatedActionMetaData,
        stepMetaData = updatedStepMetaData,
        info = mapOf("message" to "Starting action ${action.type} and working on ${step.name}")
    )
}

@Suppress("ReturnCount")
fun ManagedIndexMetaData.getCompletedManagedIndexMetaData(
    action: Action,
    step: Step
): ManagedIndexMetaData {
    val updatedStepMetaData = step.getUpdatedManagedIndexMetaData(this)
    val actionMetaData = updatedStepMetaData.actionMetaData ?: return this.copy(
        policyRetryInfo = PolicyRetryInfoMetaData(true, 0),
        info = mapOf("message" to "Failed due to ActionMetaData being null")
    )

    val updatedActionMetaData = if (updatedStepMetaData.stepMetaData?.stepStatus == Step.StepStatus.FAILED) {
        when {
            action.config.configRetry == null -> actionMetaData.copy(failed = true)
            actionMetaData.consumedRetries >= action.config.configRetry!!.count -> actionMetaData.copy(failed = true)
            else -> actionMetaData.copy(
                failed = false,
                consumedRetries = actionMetaData.consumedRetries + 1,
                lastRetryTime = Instant.now().toEpochMilli())
        }
    } else {
        actionMetaData
    }

    return this.copy(
        policyCompleted = updatedStepMetaData.policyCompleted,
        rolledOver = updatedStepMetaData.rolledOver,
        actionMetaData = updatedActionMetaData,
        stepMetaData = updatedStepMetaData.stepMetaData,
        transitionTo = updatedStepMetaData.transitionTo,
        policyRetryInfo = updatedStepMetaData.policyRetryInfo,
        info = updatedStepMetaData.info
    )
}

val ManagedIndexMetaData.isSuccessfulDelete: Boolean
    get() = (this.actionMetaData?.name == ActionConfig.ActionType.DELETE.type && !this.actionMetaData.failed) &&
            (this.stepMetaData?.name == AttemptDeleteStep.name && this.stepMetaData.stepStatus == Step.StepStatus.COMPLETED) &&
            (this.policyRetryInfo?.failed != true)

val ManagedIndexMetaData.isFailed: Boolean
    get() {
        // If PolicyRetryInfo is failed then the ManagedIndex has failed.
        if (this.policyRetryInfo?.failed == true) return true
        // If ActionMetaData is not null and some action is failed. Then the ManagedIndex has failed.
        if (this.actionMetaData?.failed == true) return true
        return false
    }

/**
 * We will change the policy if a change policy exists and if we are currently in
 * a Transitions action (which means we're safely at the end of a state). If a
 * transitionTo exists on the [ManagedIndexMetaData] it should still be fine to
 * change policy as we have not actually transitioned yet. If the next action is a transition.
 * Or if the rest API determined it was "safe". Meaning the new policy has the same structure
 * of the current state so it should be safe to immediately change even in the middle of the state.
 *
 * @param managedIndexMetaData current [ManagedIndexMetaData]
 * @return {@code true} if we should change policy, {@code false} if not
 */
@Suppress("ReturnCount")
fun ManagedIndexConfig.shouldChangePolicy(managedIndexMetaData: ManagedIndexMetaData, actionToExecute: Action?): Boolean {
    if (this.changePolicy == null) {
        return false
    }

    if (this.changePolicy.safe) {
        return true
    }

    // we need this in so that we can change policy before the first transition happens so policy doesnt get completed
    // before we have a chance to change policy
    if (actionToExecute?.type == ActionConfig.ActionType.TRANSITION) {
        return true
    }

    if (managedIndexMetaData.actionMetaData?.name != ActionConfig.ActionType.TRANSITION.type) {
        return false
    }

    return true
}

val ManagedIndexMetaData.wasReadOnly: Boolean
    get() {
        // Retrieve wasReadOnly property from ActionProperties found within ActionMetaData
        return this.actionMetaData?.actionProperties?.wasReadOnly == true
    }

fun ManagedIndexMetaData.hasVersionConflict(managedIndexConfig: ManagedIndexConfig): Boolean =
    this.policySeqNo != managedIndexConfig.policySeqNo || this.policyPrimaryTerm != managedIndexConfig.policyPrimaryTerm

fun ManagedIndexConfig.hasDifferentJobInterval(jobInterval: Int): Boolean {
    val schedule = this.schedule
    when (schedule) {
        is IntervalSchedule -> {
            return schedule.interval != jobInterval
        }
    }
    return false
}

/**
 * A policy is safe to change to a new policy when each policy has the current state
 * the [ManagedIndexConfig] is in and that state has the same actions in the same order.
 * This allows simple things like configuration updates to happen which won't break the execution/contract
 * between [ManagedIndexMetaData] and [ManagedIndexConfig] as the metadata only knows about the current state.
 * We never consider a policy safe to immediately change if the ChangePolicy contains a state to transition to.
 *
 * @param stateName the name of the state the [ManagedIndexConfig] is currently in
 * @param nextPolicy the new policy we will eventually try to change to
 * @return if its safe to change
 */
@Suppress("ReturnCount")
fun Policy.isSafeToChange(stateName: String?, newPolicy: Policy, changePolicy: ChangePolicy): Boolean {
    // if stateName is null it means we either have not initialized the job (no metadata to pull stateName from)
    // or we failed to load the initial policy, both cases its safe to change the policy
    if (stateName == null) return true
    if (changePolicy.state != null) return false
    val currentState = this.states.find { it.name == stateName }
    val nextState = newPolicy.states.find { it.name == stateName }
    if (currentState == null || nextState == null) {
        return false
    }

    if (currentState.actions.size != nextState.actions.size) {
        return false
    }

    currentState.actions.forEachIndexed { index, action ->
        val nextStateAction = nextState.actions[index]
        if (action.type != nextStateAction.type) return@isSafeToChange false
    }

    return true
}
