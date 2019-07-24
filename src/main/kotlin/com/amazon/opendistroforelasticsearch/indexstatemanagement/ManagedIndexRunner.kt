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

package com.amazon.opendistroforelasticsearch.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.INDEX_STATE_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexstatemanagement.action.Action
import com.amazon.opendistroforelasticsearch.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataRequest
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.getManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.retry
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.RetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StateMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.createManagedIndexRequest
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.getActionToExecute
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.getStateToExecute
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.getUpdatedManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.isSuccessfulDelete
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.LockModel
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.bulk.BackoffPolicy
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.Index
import org.elasticsearch.rest.RestStatus
import java.time.Instant

object ManagedIndexRunner : ScheduledJobRunner,
        CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("ManagedIndexRunner")) {

    private val logger = LogManager.getLogger(javaClass)

    private lateinit var clusterService: ClusterService
    private lateinit var client: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    @Suppress("MagicNumber")
    private val savePolicyRetryPolicy = BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(250), 3)
    @Suppress("MagicNumber")
    private val updateMetaDataRetryPolicy = BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(250), 3)

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

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        if (job !is ManagedIndexConfig) {
            throw IllegalArgumentException("Invalid job type, found ${job.javaClass.simpleName} with id: ${context.jobId}")
        }

        launch {
            // Attempt to acquire lock
            val lock: LockModel? = withContext(Dispatchers.IO) { context.lockService.acquireLock(job, context) }
            if (lock == null) {
                logger.debug("Could not acquire lock for ${job.index}")
            } else {
                runManagedIndexConfig(job)
                // Release lock
                val released = withContext(Dispatchers.IO) { context.lockService.release(lock) }
                if (!released) {
                    logger.debug("Could not release lock for ${job.index}")
                }
            }
        }
    }

    // TODO: Implement logic for when ISM is moved to STOPPING/STOPPED state when we add those APIs
    @Suppress("ReturnCount")
    private suspend fun runManagedIndexConfig(managedIndexConfig: ManagedIndexConfig) {
        // Get current IndexMetaData and ManagedIndexMetaData
        val indexMetaData = clusterService.state().metaData().index(managedIndexConfig.index)
        if (indexMetaData == null) {
            logger.error("Could not find IndexMetaData in cluster state for ${managedIndexConfig.index}")
            return
        }
        val managedIndexMetaData = indexMetaData.getManagedIndexMetaData()

        // If policy or managedIndexMetaData is null then initialize
        val policy = managedIndexConfig.policy
        if (policy == null || managedIndexMetaData == null) {
            initManagedIndex(managedIndexConfig, managedIndexMetaData)
            return
        }

        // If the policy was completed then return early and disable job so it stops scheduling work
        if (managedIndexMetaData.policyCompleted == true) {
            // TODO: Update ManagedIndexConfig to disabled
            return
        }

        // TODO: If we're failed and consumed all retries then return early
        // Should also disable the job as retry requires an API call where we can reenable it

        // TODO: Compare policy version of ManagedIndexMetaData with policy version of job
        // If mismatch, update ManagedIndexMetaData with Version Conflict error

        // TODO: Check if there is a ChangePolicy attached to job and try to switch when possible

        val state = policy.getStateToExecute(managedIndexMetaData)
        val action: Action? = state?.getActionToExecute(clusterService, client, managedIndexMetaData)
        val step: Step? = action?.getStepToExecute()

        // If any of the above components come back as null then we are moving to error in ManagedIndexMetaData
        if (state != null && action != null && step != null) {
            step.execute()
        }

        val updatedManagedIndexMetaData = managedIndexMetaData.getUpdatedManagedIndexMetaData(state, action, step)

        // TODO: Check if we can move this into the TransportUpdateManagedIndexMetaDataAction to cover all cases where
        //  IndexMetaData does not exist anymore since if this current execution was a delete step and it was
        //  successful then the IndexMetaData will be wiped and will throw a NPE if we attempt to update it
        if (updatedManagedIndexMetaData.isSuccessfulDelete) {
            return
        }

        updateManagedIndexMetaData(updatedManagedIndexMetaData)
    }

    private suspend fun initManagedIndex(managedIndexConfig: ManagedIndexConfig, managedIndexMetaData: ManagedIndexMetaData?) {
        var policy: Policy? = managedIndexConfig.policy
        // If policy does not currently exist, we need to save the policy on the ManagedIndexConfig for the first time
        if (policy == null) {
            // Get the policy by the name unless a ChangePolicy exists then allow the change to happen during initialization
            policy = getPolicy(managedIndexConfig.changePolicy?.policyID ?: managedIndexConfig.policyID)
            // Attempt to save the policy
            if (policy != null) {
                val saved = savePolicyToManagedIndexConfig(managedIndexConfig, policy)
                // If we failed to save the policy, don't initialize ManagedIndexMetaData
                if (!saved) return
            }
            // If policy is still null we handle it in the managedIndexMetaData by moving to ERROR
        }

        // Initializing ManagedIndexMetaData for the first time
        if (managedIndexMetaData == null) {
            initializeManagedIndexMetaData(managedIndexConfig, policy)
        } else {
            // TODO: This could happen when deleting a ManagedIndexConfig document
            // We should compare the cached policy with the ManagedIndexMetaData and make sure we have the same policy, seqNo, primaryTerm
            // otherwise update to ERROR with policy/version conflict
        }
    }

    private suspend fun getPolicy(policyID: String): Policy? {
        val getRequest = GetRequest(INDEX_STATE_MANAGEMENT_INDEX, policyID)
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
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun savePolicyToManagedIndexConfig(managedIndexConfig: ManagedIndexConfig, policy: Policy): Boolean {
        val updatedManagedIndexConfig = managedIndexConfig.copy(policyID = policy.id, policy = policy,
                policySeqNo = policy.seqNo, policyPrimaryTerm = policy.primaryTerm, changePolicy = null)
        val indexRequest = createManagedIndexRequest(updatedManagedIndexConfig)
        var savedPolicy = false
        try {
            savePolicyRetryPolicy.retry(logger) {
                val indexResponse: IndexResponse = client.suspendUntil { index(indexRequest, it) }
                savedPolicy = indexResponse.status() == RestStatus.OK
            }
        } catch (e: Exception) {
            logger.error("Failed to save policy(${policy.id}) to ManagedIndexConfig(${managedIndexConfig.index})", e)
        }
        return savedPolicy
    }

    private suspend fun initializeManagedIndexMetaData(managedIndexConfig: ManagedIndexConfig, policy: Policy?) {
        val stateMetaData = if (policy?.defaultState != null) {
            StateMetaData(policy.defaultState, Instant.now().toEpochMilli())
        } else {
            null
        }

        val managedIndexMetaData = ManagedIndexMetaData(
            index = managedIndexConfig.index,
            indexUuid = managedIndexConfig.indexUuid,
            policyID = managedIndexConfig.policyID,
            policySeqNo = policy?.seqNo,
            policyPrimaryTerm = policy?.primaryTerm,
            policyCompleted = false,
            rolledOver = false,
            transitionTo = null,
            stateMetaData = stateMetaData,
            actionMetaData = null,
            stepMetaData = null,
            // TODO fix retryInfo when we implement retry logic.
            retryInfo = RetryInfoMetaData(failed = policy == null, consumedRetries = 0),
            info = mapOf("message" to "${if (policy == null) "Fail to load" else "Successfully initialized"} policy: ${managedIndexConfig.policyID}")
        )

        updateManagedIndexMetaData(managedIndexMetaData)
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun updateManagedIndexMetaData(managedIndexMetaData: ManagedIndexMetaData) {
        try {
            val request = UpdateManagedIndexMetaDataRequest(
                    listOf(Pair(Index(managedIndexMetaData.index, managedIndexMetaData.indexUuid), managedIndexMetaData))
            )
            updateMetaDataRetryPolicy.retry(logger) {
                val response: AcknowledgedResponse = client.suspendUntil { execute(UpdateManagedIndexMetaDataAction, request, it) }
                if (!response.isAcknowledged) {
                    logger.error("Failed to save ManagedIndexMetaData")
                }
            }
        } catch (e: ClusterBlockException) {
            logger.error("There was ClusterBlockException trying to update the metadata for ${managedIndexMetaData.index}. Message: ${e.message}")
        } catch (e: Exception) {
            logger.error("Failed to save ManagedIndexMetaData")
        }
    }
}
