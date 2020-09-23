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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.action.Action
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.convertToMap
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getPolicyID
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.retry
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.string
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StateMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.ALLOW_LIST
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.ALLOW_LIST_NONE
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.DEFAULT_ISM_ENABLED
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.DEFAULT_JOB_INTERVAL
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.INDEX_STATE_MANAGEMENT_ENABLED
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.JOB_INTERVAL
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.managedIndexConfigIndexRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.getActionToExecute
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.getStartingManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.getStateToExecute
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.getCompletedManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.getUpdatedActionMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.hasDifferentJobInterval
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.hasTimedOut
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.hasVersionConflict
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.isAllowed
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.isFailed
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.isSafeToChange
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.isSuccessfulDelete
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.shouldBackoff
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.shouldChangePolicy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.updateDisableManagedIndexRequest
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.LockModel
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.bulk.BackoffPolicy
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.health.ClusterHealthStatus
import org.elasticsearch.cluster.health.ClusterStateHealth
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.Index
import org.elasticsearch.index.engine.VersionConflictEngineException
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptService
import org.elasticsearch.script.TemplateScript
import java.time.Instant
import java.time.temporal.ChronoUnit

@Suppress("TooManyFunctions")
object ManagedIndexRunner : ScheduledJobRunner,
        CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("ManagedIndexRunner")) {

    private val logger = LogManager.getLogger(javaClass)

    private lateinit var clusterService: ClusterService
    private lateinit var client: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var scriptService: ScriptService
    private lateinit var settings: Settings
    private var indexStateManagementEnabled: Boolean = DEFAULT_ISM_ENABLED
    @Suppress("MagicNumber")
    private val savePolicyRetryPolicy = BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(250), 3)
    @Suppress("MagicNumber")
    private val updateMetaDataRetryPolicy = BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(250), 3)
    @Suppress("MagicNumber")
    private val errorNotificationRetryPolicy = BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(250), 3)
    private var jobInterval: Int = DEFAULT_JOB_INTERVAL
    private var allowList: List<String> = ALLOW_LIST_NONE

    fun registerClusterService(clusterService: ClusterService): ManagedIndexRunner {
        this.clusterService = clusterService
        return this
    }

    fun registerClient(client: Client): ManagedIndexRunner {
        this.client = client
        return this
    }

    fun registerNamedXContentRegistry(xContentRegistry: NamedXContentRegistry): ManagedIndexRunner {
        this.xContentRegistry = xContentRegistry
        return this
    }

    fun registerScriptService(scriptService: ScriptService): ManagedIndexRunner {
        this.scriptService = scriptService
        return this
    }

    fun registerSettings(settings: Settings): ManagedIndexRunner {
        this.settings = settings
        return this
    }

    // must be called after registerSettings and registerClusterService in IndexManagementPlugin
    fun registerConsumers(): ManagedIndexRunner {
        jobInterval = JOB_INTERVAL.get(settings)
        clusterService.clusterSettings.addSettingsUpdateConsumer(JOB_INTERVAL) {
            jobInterval = it
        }

        indexStateManagementEnabled = INDEX_STATE_MANAGEMENT_ENABLED.get(settings)
        clusterService.clusterSettings.addSettingsUpdateConsumer(INDEX_STATE_MANAGEMENT_ENABLED) {
            indexStateManagementEnabled = it
        }

        allowList = ALLOW_LIST.get(settings)
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALLOW_LIST) {
            allowList = it
        }
        return this
    }

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        if (job !is ManagedIndexConfig) {
            throw IllegalArgumentException("Invalid job type, found ${job.javaClass.simpleName} with id: ${context.jobId}")
        }

        launch {
            // Attempt to acquire lock
            val lock: LockModel? = context.lockService.suspendUntil { acquireLock(job, context, it) }
            if (lock == null) {
                logger.debug("Could not acquire lock for ${job.index}")
            } else {
                runManagedIndexConfig(job)
                // Release lock
                val released: Boolean = context.lockService.suspendUntil { release(lock, it) }
                if (!released) {
                    logger.debug("Could not release lock for ${job.index}")
                }
            }
        }
    }

    @Suppress("ReturnCount", "ComplexMethod", "LongMethod")
    private suspend fun runManagedIndexConfig(managedIndexConfig: ManagedIndexConfig) {
        // doing a check of local cluster health as we do not want to overload master node with potentially a lot of calls
        if (clusterIsRed()) {
            logger.debug("Skipping current execution of ${managedIndexConfig.index} because of red cluster health")
            return
        }

        // Get current IndexMetaData and ManagedIndexMetaData
        val indexMetaData = getIndexMetaData(managedIndexConfig.index)
        if (indexMetaData == null) {
            return
        }
        val managedIndexMetaData = indexMetaData.getManagedIndexMetaData()

        // If policy or managedIndexMetaData is null then initialize
        val policy = managedIndexConfig.policy
        if (policy == null || managedIndexMetaData == null) {
            initManagedIndex(managedIndexConfig, managedIndexMetaData)
            return
        }

        // If there is a mismatch between _settings policy_id and job policy_id it means the user
        // tried to change it using the _settings API or we failed to update during ChangePolicy
        // so we will attempt to self heal and disallow the use of _settings API to modify policy_id
        if (indexMetaData.getPolicyID() != managedIndexConfig.policyID) {
            updateIndexPolicyIDSetting(managedIndexConfig.index, managedIndexConfig.policyID)
        }

        // If the policy was completed or failed then return early and disable job so it stops scheduling work
        if (managedIndexMetaData.policyCompleted == true || managedIndexMetaData.isFailed) {
            disableManagedIndexConfig(managedIndexConfig)
            return
        }

        if (managedIndexMetaData.hasVersionConflict(managedIndexConfig)) {
            val info = mapOf("message" to "There is a version conflict between your previous execution and your managed index")
            val updated = updateManagedIndexMetaData(managedIndexMetaData.copy(policyRetryInfo = PolicyRetryInfoMetaData(true, 0), info = info))
            if (updated) disableManagedIndexConfig(managedIndexConfig)
            return
        }

        val state = policy.getStateToExecute(managedIndexMetaData)
        val action: Action? = state?.getActionToExecute(clusterService, scriptService, client, managedIndexMetaData)
        val step: Step? = action?.getStepToExecute()
        val currentActionMetaData = action?.getUpdatedActionMetaData(managedIndexMetaData, state)

        // If Index State Management is disabled and the current step is not null and safe to disable on
        // then disable the job and return early
        if (!indexStateManagementEnabled && step != null && step.isSafeToDisableOn) {
            disableManagedIndexConfig(managedIndexConfig)
            return
        }

        if (action?.hasTimedOut(currentActionMetaData) == true) {
            val info = mapOf("message" to "Action timed out")
            logger.error("Action=${action.type.type} has timed out")
            val updated = updateManagedIndexMetaData(managedIndexMetaData
                .copy(actionMetaData = currentActionMetaData?.copy(failed = true), info = info))
            if (updated) disableManagedIndexConfig(managedIndexConfig)
            return
        }

        if (managedIndexConfig.shouldChangePolicy(managedIndexMetaData, action)) {
            initChangePolicy(managedIndexConfig, managedIndexMetaData, action)
            return
        }

        val shouldBackOff = action?.shouldBackoff(currentActionMetaData, action.config.configRetry)
        if (shouldBackOff?.first == true) {
            // If we should back off then exit early.
            logger.info("Backoff for retrying. Remaining time ${shouldBackOff.second}")
            return
        }

        if (managedIndexMetaData.stepMetaData?.stepStatus == Step.StepStatus.STARTING) {
            val isIdempotent = step?.isIdempotent()
            logger.info("Previous execution failed to update step status, isIdempotent=$isIdempotent")
            if (isIdempotent != true) {
                val info = mapOf("message" to "Previous action was not able to update IndexMetaData.")
                val updated = updateManagedIndexMetaData(managedIndexMetaData.copy(policyRetryInfo = PolicyRetryInfoMetaData(true, 0), info = info))
                if (updated) disableManagedIndexConfig(managedIndexConfig)
                return
            }
        }

        // If this action is not allowed and the step to be executed is the first step in the action then we will fail
        // as this action has been removed from the AllowList, but if its not the first step we will let it finish as it's already inflight
        if (action?.isAllowed(allowList) == false && action.isFirstStep(step?.name)) {
            val info = mapOf("message" to "Attempted to execute action=${action.type.type} which is not allowed.")
            val updated = updateManagedIndexMetaData(managedIndexMetaData.copy(policyRetryInfo = PolicyRetryInfoMetaData(true, 0), info = info))
            if (updated) disableManagedIndexConfig(managedIndexConfig)
            return
        }

        // If any of State, Action, Step components come back as null then we are moving to error in ManagedIndexMetaData
        val startingManagedIndexMetaData = managedIndexMetaData.getStartingManagedIndexMetaData(state, action, step)
        val updateResult = updateManagedIndexMetaData(startingManagedIndexMetaData)

        if (updateResult && state != null && action != null && step != null && currentActionMetaData != null) {
            // Step null check is done in getStartingManagedIndexMetaData
            step.preExecute(logger).execute().postExecute(logger)
            var executedManagedIndexMetaData = startingManagedIndexMetaData.getCompletedManagedIndexMetaData(action, step)

            if (executedManagedIndexMetaData.isFailed) {
                try {
                    // if the policy has no error_notification this will do nothing otherwise it will try to send the configured error message
                    publishErrorNotification(policy, executedManagedIndexMetaData)
                } catch (e: Exception) {
                    logger.error("Failed to publish error notification", e)
                    val errorMessage = e.message ?: "Failed to publish error notification"
                    val mutableInfo = executedManagedIndexMetaData.info?.toMutableMap() ?: mutableMapOf()
                    mutableInfo["errorNotificationFailure"] = errorMessage
                    executedManagedIndexMetaData = executedManagedIndexMetaData.copy(info = mutableInfo.toMap())
                }
            }

            if (executedManagedIndexMetaData.isSuccessfulDelete) {
                return
            }

            if (!updateManagedIndexMetaData(executedManagedIndexMetaData)) {
                logger.error("Failed to update ManagedIndexMetaData after executing the Step : ${step.name}")
            }

            if (managedIndexConfig.hasDifferentJobInterval(jobInterval)) {
                updateJobInterval(managedIndexConfig, jobInterval)
            }
        }
    }

    private suspend fun initManagedIndex(managedIndexConfig: ManagedIndexConfig, managedIndexMetaData: ManagedIndexMetaData?) {
        var policy: Policy? = managedIndexConfig.policy
        val policyID = managedIndexConfig.changePolicy?.policyID ?: managedIndexConfig.policyID
        // If policy does not currently exist, we need to save the policy on the ManagedIndexConfig for the first time
        // or if a change policy exists then we will also execute the change as we are still in initialization phase
        if (policy == null || managedIndexConfig.changePolicy != null) {
            // Get the policy by the name unless a ChangePolicy exists then allow the change to happen during initialization
            policy = getPolicy(policyID)
            // Attempt to save the policy
            if (policy != null) {
                val saved = savePolicyToManagedIndexConfig(managedIndexConfig, policy)
                // If we failed to save the policy, don't initialize ManagedIndexMetaData
                if (!saved) return
            }
            // If we failed to get the policy then we will update the ManagedIndexMetaData with error info
        }

        // at this point we either successfully saved the policy or we failed to get the policy
        val updatedManagedIndexMetaData = if (policy == null) {
            getFailedInitializedManagedIndexMetaData(managedIndexMetaData, managedIndexConfig, policyID)
        } else {
            // Initializing ManagedIndexMetaData for the first time
            getInitializedManagedIndexMetaData(managedIndexMetaData, managedIndexConfig, policy)
        }

        updateManagedIndexMetaData(updatedManagedIndexMetaData)
    }

    @Suppress("ReturnCount")
    private suspend fun getPolicy(policyID: String): Policy? {
        try {
            val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, policyID)
            val getResponse: GetResponse = client.suspendUntil { get(getRequest, it) }
            if (!getResponse.isExists || getResponse.isSourceEmpty) {
                return null
            }

            val policySource = getResponse.sourceAsBytesRef
            // Intellij complains about createParser/parseWithType blocking because it sees they throw IOExceptions
            return withContext(Dispatchers.IO) {
                val xcp = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                        policySource, XContentType.JSON)
                Policy.parseWithType(xcp, getResponse.id, getResponse.seqNo, getResponse.primaryTerm)
            }
        } catch (e: Exception) {
            logger.error("Failed to get policy: $policyID", e)
            return null
        }
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun disableManagedIndexConfig(managedIndexConfig: ManagedIndexConfig) {
        val updatedManagedIndexConfig = managedIndexConfig.copy(enabled = false, jobEnabledTime = null)
        val indexRequest = updateDisableManagedIndexRequest(updatedManagedIndexConfig.indexUuid)
        try {
            val indexResponse: UpdateResponse = client.suspendUntil { update(indexRequest, it) }
            if (indexResponse.status() != RestStatus.OK) {
                logger.error("Failed to disable ManagedIndexConfig(${managedIndexConfig.index}) Error : indexResponse.status()")
            }
        } catch (e: Exception) {
            logger.error("Failed to disable ManagedIndexConfig(${managedIndexConfig.index})", e)
        }
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun savePolicyToManagedIndexConfig(managedIndexConfig: ManagedIndexConfig, policy: Policy): Boolean {
        val updatedManagedIndexConfig = managedIndexConfig.copy(policyID = policy.id, policy = policy,
                policySeqNo = policy.seqNo, policyPrimaryTerm = policy.primaryTerm, changePolicy = null)
        val indexRequest = managedIndexConfigIndexRequest(updatedManagedIndexConfig)
        var savedPolicy = false
        try {
            savePolicyRetryPolicy.retry(logger) {
                val indexResponse: IndexResponse = client.suspendUntil { index(indexRequest, it) }
                savedPolicy = indexResponse.status() == RestStatus.OK
            }
        } catch (e: VersionConflictEngineException) {
            logger.error("Failed to save policy(${policy.id}) to ManagedIndexConfig(${managedIndexConfig.index}). ${e.message}")
        } catch (e: Exception) {
            logger.error("Failed to save policy(${policy.id}) to ManagedIndexConfig(${managedIndexConfig.index})", e)
        }
        return savedPolicy
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun updateJobInterval(managedIndexConfig: ManagedIndexConfig, jobInterval: Int) {
        try {
            val updatedManagedIndexConfig = managedIndexConfig
                .copy(jobSchedule = IntervalSchedule(getIntervalStartTime(managedIndexConfig), jobInterval, ChronoUnit.MINUTES))
            val indexRequest = managedIndexConfigIndexRequest(updatedManagedIndexConfig)
            val indexResponse: IndexResponse = client.suspendUntil { index(indexRequest, it) }
            if (indexResponse.status() != RestStatus.OK) {
                logger.error("Failed to update ManagedIndexConfig(${managedIndexConfig.index}) job interval")
            }
        } catch (e: VersionConflictEngineException) {
            logger.error("Failed to update ManagedIndexConfig(${managedIndexConfig.index}) job interval. ${e.message}")
        } catch (e: Exception) {
            logger.error("Failed to update ManagedIndexConfig(${managedIndexConfig.index}) job interval", e)
        }
    }

    private fun getFailedInitializedManagedIndexMetaData(
        managedIndexMetaData: ManagedIndexMetaData?,
        managedIndexConfig: ManagedIndexConfig,
        policyID: String
    ): ManagedIndexMetaData {
        // we either haven't initialized any metadata yet or we have already initialized metadata but still have no policy
        return managedIndexMetaData?.copy(
            policyRetryInfo = PolicyRetryInfoMetaData(failed = true, consumedRetries = 0),
            info = mapOf("message" to "Fail to load policy: $policyID")
        ) ?: ManagedIndexMetaData(
            index = managedIndexConfig.index,
            indexUuid = managedIndexConfig.indexUuid,
            policyID = policyID,
            policySeqNo = null,
            policyPrimaryTerm = null,
            policyCompleted = false,
            rolledOver = false,
            transitionTo = null,
            stateMetaData = null,
            actionMetaData = null,
            stepMetaData = null,
            policyRetryInfo = PolicyRetryInfoMetaData(failed = true, consumedRetries = 0),
            info = mapOf("message" to "Fail to load policy: $policyID")
        )
    }

    @Suppress("ComplexMethod")
    private suspend fun getInitializedManagedIndexMetaData(
        managedIndexMetaData: ManagedIndexMetaData?,
        managedIndexConfig: ManagedIndexConfig,
        policy: Policy
    ): ManagedIndexMetaData {
        val state = managedIndexConfig.changePolicy?.state ?: policy.defaultState
        val stateMetaData = StateMetaData(state, Instant.now().toEpochMilli())

        return when {
            managedIndexMetaData == null -> ManagedIndexMetaData(
                index = managedIndexConfig.index,
                indexUuid = managedIndexConfig.indexUuid,
                policyID = policy.id,
                policySeqNo = policy.seqNo,
                policyPrimaryTerm = policy.primaryTerm,
                policyCompleted = false,
                rolledOver = false,
                transitionTo = null,
                stateMetaData = stateMetaData,
                actionMetaData = null,
                stepMetaData = null,
                policyRetryInfo = PolicyRetryInfoMetaData(failed = false, consumedRetries = 0),
                info = mapOf("message" to "Successfully initialized policy: ${policy.id}")
            )
            managedIndexMetaData.policySeqNo == null || managedIndexMetaData.policyPrimaryTerm == null ->
                // If there is seqNo and PrimaryTerm it is first time populating Policy.
                managedIndexMetaData.copy(
                    policyID = policy.id,
                    policySeqNo = policy.seqNo,
                    policyPrimaryTerm = policy.primaryTerm,
                    stateMetaData = stateMetaData,
                    policyRetryInfo = PolicyRetryInfoMetaData(failed = false, consumedRetries = 0),
                    info = mapOf("message" to "Successfully initialized policy: ${policy.id}")
                )
            // this is an edge case where a user deletes the job config or index and we already have a policySeqNo/primaryTerm
            // in the metadata, in this case we just want to say we successfully initialized the policy again but we will not
            // modify the state, action, etc. so it can resume where it left off
            managedIndexMetaData.policySeqNo == policy.seqNo && managedIndexMetaData.policyPrimaryTerm == policy.primaryTerm
                && managedIndexMetaData.policyID == policy.id ->
                // If existing PolicySeqNo and PolicyPrimaryTerm is equal to cached Policy then no issue.
                managedIndexMetaData.copy(
                    policyRetryInfo = PolicyRetryInfoMetaData(failed = false, consumedRetries = 0),
                    info = mapOf("message" to "Successfully initialized policy: ${policy.id}")
                )
            else ->
                // else this means we either tried to load a policy with a different id, seqno, or primaryterm from what is
                // in the metadata and we cannot gaurantee it will work with the current state in managedIndexMetaData
                managedIndexMetaData.copy(
                    policyRetryInfo = PolicyRetryInfoMetaData(failed = true, consumedRetries = 0),
                    info = mapOf("message" to "Fail to load policy: ${policy.id} with " +
                        "seqNo ${policy.seqNo} and primaryTerm ${policy.primaryTerm} as it" +
                        " does not match what's in the metadata [policyID=${managedIndexMetaData.policyID}," +
                        " policySeqNo=${managedIndexMetaData.policySeqNo}, policyPrimaryTerm=${managedIndexMetaData.policyPrimaryTerm}]")
                )
        }
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun updateManagedIndexMetaData(managedIndexMetaData: ManagedIndexMetaData): Boolean {
        var result = false
        try {
            val request = UpdateManagedIndexMetaDataRequest(
                indicesToAddManagedIndexMetaDataTo = listOf(
                    Pair(Index(managedIndexMetaData.index, managedIndexMetaData.indexUuid), managedIndexMetaData)
                )
            )
            updateMetaDataRetryPolicy.retry(logger) {
                val response: AcknowledgedResponse = client.suspendUntil { execute(UpdateManagedIndexMetaDataAction.INSTANCE, request, it) }
                if (response.isAcknowledged) {
                    result = true
                } else {
                    logger.error("Failed to save ManagedIndexMetaData for [index=${managedIndexMetaData.index}]")
                }
            }
        } catch (e: ClusterBlockException) {
            logger.error("There was ClusterBlockException trying to update the metadata for ${managedIndexMetaData.index}. Message: ${e.message}", e)
        } catch (e: Exception) {
            logger.error("Failed to save ManagedIndexMetaData for [index=${managedIndexMetaData.index}]", e)
        }
        return result
    }

    /**
     * Initializes the change policy process where we will get the policy using the change policy's policyID, update the [ManagedIndexMetaData]
     * to reflect the new policy, and save the new policy to the [ManagedIndexConfig] while resetting the change policy to null
     */
    @Suppress("ReturnCount")
    private suspend fun initChangePolicy(
        managedIndexConfig: ManagedIndexConfig,
        managedIndexMetaData: ManagedIndexMetaData,
        actionToExecute: Action?
    ) {

        // should never happen since we only call this if there is a changePolicy, but we'll do it to make changePolicy non null
        val changePolicy = managedIndexConfig.changePolicy
        if (changePolicy == null) {
            logger.debug("initChangePolicy was called with a null ChangePolicy, ManagedIndexConfig: $managedIndexConfig")
            return
        }

        // get the policy we'll attempt to change to
        val policy = getPolicy(changePolicy.policyID)

        // update the ManagedIndexMetaData with new information
        val updatedManagedIndexMetaData = if (policy == null) {
            managedIndexMetaData.copy(
                info = mapOf("message" to "Failed to load change policy: ${changePolicy.policyID}"),
                policyRetryInfo = PolicyRetryInfoMetaData(failed = true, consumedRetries = 0)
            )
        } else {
            // if the action to execute is transition then set the actionMetaData to a new transition metadata to reflect we are
            // in transition (in case we triggered change policy from entering transition) or to reflect this is a new policy transition phase
            val newTransitionMetaData = ActionMetaData(ActionConfig.ActionType.TRANSITION.type, Instant.now().toEpochMilli(), -1,
                false, 0, 0, null)
            val actionMetaData = if (actionToExecute?.type == ActionConfig.ActionType.TRANSITION) {
                newTransitionMetaData
            } else {
                managedIndexMetaData.actionMetaData
            }
            managedIndexMetaData.copy(
                info = mapOf("message" to "Attempting to change policy to ${policy.id}"),
                transitionTo = changePolicy.state,
                actionMetaData = actionMetaData,
                stepMetaData = null,
                policyCompleted = false,
                policySeqNo = policy.seqNo,
                policyPrimaryTerm = policy.primaryTerm,
                policyID = policy.id
            )
        }

        // check if the safe flag was set by the Change Policy REST API, if it was then do a second validation
        // before allowing a change to happen
        if (changePolicy.isSafe) {
            // if policy is null then we are only updating error information in metadata so its fine to continue
            if (policy != null) {
                // current policy being null should never happen as we have a check at the top of runner
                // if it is unsafe to change then we set safe back to false so we don't keep doing this check every execution
                if (managedIndexConfig.policy?.isSafeToChange(managedIndexMetaData.stateMetaData?.name, policy, changePolicy) != true) {
                    updateManagedIndexConfig(managedIndexConfig.copy(changePolicy = managedIndexConfig.changePolicy.copy(isSafe = false)))
                    return
                }
            }
        }

        /*
        * Try to update the ManagedIndexMetaData in cluster state, we need to do this first before updating the
        * ManagedIndexConfig because if this fails we can fail early and still retry this whole process on the next
        * execution whereas if we do the update to ManagedIndexConfig first we lose the ChangePolicy on the job and
        * could fail to update the ManagedIndexMetaData which would put us in a bad state
        * */
        val updated = updateManagedIndexMetaData(updatedManagedIndexMetaData)

        if (!updated || policy == null) return

        // this will save the new policy on the job and reset the change policy back to null
        val saved = savePolicyToManagedIndexConfig(managedIndexConfig, policy)

        if (saved) {
            /*
            * If we successfully saved the the new policy then the last thing we need to do is update the
            * opendistro.indexstatemanagement.policy_id setting to the new policy id we don't care that much if this fails, because we'll
            * have a check in the beginning of the runner to read in the setting and compare it with the policy_id on the job and update
            * the setting if they ever differ, as we do not allow someone to change an existing policy using _settings API
            * */
            updateIndexPolicyIDSetting(managedIndexConfig.index, changePolicy.policyID)
        }
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun updateManagedIndexConfig(updatedManagedIndexConfig: ManagedIndexConfig) {
        try {
            val indexRequest = managedIndexConfigIndexRequest(updatedManagedIndexConfig)
            val indexResponse: IndexResponse = client.suspendUntil { index(indexRequest, it) }
            if (indexResponse.status() != RestStatus.OK) {
                logger.error("Failed to update ManagedIndexConfig(${updatedManagedIndexConfig.index})")
            }
        } catch (e: VersionConflictEngineException) {
            logger.error("Failed to update ManagedIndexConfig(${updatedManagedIndexConfig.index}). ${e.message}")
        } catch (e: Exception) {
            logger.error("Failed to update ManagedIndexConfig(${updatedManagedIndexConfig.index})", e)
        }
    }

    /**
     * Once we successfully swap over a ChangePolicy then we need to update the [ManagedIndexSettings.POLICY_ID] setting.
     *
     * We will constantly check the [ManagedIndexSettings.POLICY_ID] against the [ManagedIndexConfig] policyID and if
     * there is ever a mismatch we will overwrite the [ManagedIndexSettings.POLICY_ID] with the [ManagedIndexConfig] policyID.
     *
     * We do this because if this fails we want to ensure we try again on the next execution of the job. At the same time, this
     * will disallow the user from directly using the _settings API to change the policy_id. We do not want to allow this,
     * they must use the ChangePolicy API as the [ManagedIndexSettings.POLICY_ID] is referring to the currently running policy.
     */
    private suspend fun updateIndexPolicyIDSetting(index: String, policyID: String) {
        try {
            val settings = Settings.builder().put(ManagedIndexSettings.POLICY_ID.key, policyID).build()
            val updateSettingsRequest = UpdateSettingsRequest(index).settings(settings)
            val response: AcknowledgedResponse = client.admin().indices().suspendUntil { updateSettings(updateSettingsRequest, it) }
            if (!response.isAcknowledged) {
                logger.warn("Updating policy_id ($policyID) for $index was not acknowledged")
            }
        } catch (e: Exception) {
            logger.error("There was an error while trying to update the policy_id ($policyID) setting for $index", e)
        }
    }

    private suspend fun publishErrorNotification(policy: Policy, managedIndexMetaData: ManagedIndexMetaData) {
        policy.errorNotification?.run {
            errorNotificationRetryPolicy.retry(logger) {
                withContext(Dispatchers.IO) {
                    destination.publish(null, compileTemplate(messageTemplate, managedIndexMetaData))
                }
            }
        }
    }

    private fun compileTemplate(template: Script, managedIndexMetaData: ManagedIndexMetaData): String {
        return try {
            scriptService.compile(template, TemplateScript.CONTEXT)
                    .newInstance(template.params + mapOf("ctx" to managedIndexMetaData.convertToMap()))
                    .execute()
        } catch (e: Exception) {
            val message = "There was an error compiling mustache template"
            logger.error(message, e)
            e.message ?: message
        }
    }

    private fun clusterIsRed(): Boolean = ClusterStateHealth(clusterService.state()).status == ClusterHealthStatus.RED

    private suspend fun getIndexMetaData(index: String): IndexMetadata? {
        var indexMetaData: IndexMetadata? = null
        try {
            val clusterStateRequest = ClusterStateRequest()
                .clear()
                .indices(index)
                .metadata(true)
                .local(false)
                .indicesOptions(IndicesOptions.strictExpand())

            val response: ClusterStateResponse = client.admin().cluster().suspendUntil { state(clusterStateRequest, it) }

            indexMetaData = response.state.metadata.indices.firstOrNull()?.value
            if (indexMetaData == null) {
                logger.error("Could not find IndexMetaData in master cluster state for $index")
            }
        } catch (e: Exception) {
            logger.error("Failed to get IndexMetaData from master cluster state for index=$index", e)
        }

        return indexMetaData
    }

    // TODO: This is a hacky solution to get the current start time off the job interval as job-scheduler currently does not
    //  make this public, long term solution is to make the changes in job-scheduler, cherry-pick back into ISM supported versions and
    //  republish job-scheduler spi to maven, in the interim we will parse the current interval start time
    private suspend fun getIntervalStartTime(managedIndexConfig: ManagedIndexConfig): Instant {
        return withContext(Dispatchers.IO) {
            val intervalJsonString = managedIndexConfig.schedule.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()
            val xcp = XContentType.JSON.xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, intervalJsonString)
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation) // start of schedule block
            ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp::getTokenLocation) // "interval"
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation) // start of interval block
            var startTime: Long? = null
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    "start_time" -> startTime = xcp.longValue()
                }
            }
            Instant.ofEpochMilli(requireNotNull(startTime))
        }
    }
}
