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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataRequest
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.getManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.createManagedIndexRequest
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
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.service.ClusterService
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
                // TODO: Temporary until the initial ScheduledJobParser passes seqNo/primaryTerm
                runManagedIndexConfig(job.copy(seqNo = context.jobVersion.seqNo, primaryTerm = context.jobVersion.primaryTerm))
                // Release lock
                val released = withContext(Dispatchers.IO) { context.lockService.release(lock) }
                if (!released) {
                    logger.debug("Could not release lock for ${job.index}")
                }
            }
        }
    }

    // TODO: Implement logic for when ISM is moved to STOPPING/STOPPED state
    private suspend fun runManagedIndexConfig(managedIndexConfig: ManagedIndexConfig) {

        logger.info("runJob for index: ${managedIndexConfig.index}")

        // Get current IndexMetaData and ManagedIndexMetaData
        val indexMetaData = clusterService.state().metaData().index(managedIndexConfig.index)
        if (indexMetaData == null) {
            logger.error("Could not find IndexMetaData in cluster state for ${managedIndexConfig.index}")
            return
        }
        val managedIndexMetaData = indexMetaData.getManagedIndexMetaData()

        // If policy or managedIndexMetaData is null then initialize
        if (managedIndexConfig.policy == null || managedIndexMetaData == null) {
            initManagedIndex(managedIndexConfig, managedIndexMetaData)
            return
        }

        // TODO: Compare policy version of ManagedIndexMetaData with policy version of job
        // If mismatch, move into ERROR step with Version Conflict error

        // TODO: Check if there is a ChangePolicy attached to job
        // If there is and we're currently in transition step then make the switch
        // If we're not done with the state yet then do nothing

        // TODO: Evaluate which state, action, step we're on from the policy and ManagedIndexMetaData

        // TODO: Execute the step

        // TODO: Update the cluster state with the updated ManagedIndexMetaData
    }

    private suspend fun initManagedIndex(
        managedIndexConfig: ManagedIndexConfig,
        managedIndexMetaData: ManagedIndexMetaData?
    ) {
        // If policy does not currently exist, get policy by name and save it to ManagedIndexConfig
        var policy: Policy? = managedIndexConfig.policy
        if (policy == null) {
            policy = getPolicy(managedIndexConfig.policyName)

            if (policy != null) {
                val saved = savePolicyToManagedIndexConfig(managedIndexConfig, policy)
                if (!saved) return // If we failed to save the policy, don't initialize ManagedIndexMetaData
            }
            // If policy is null we handle it in the managedIndexMetaData by moving to ERROR
        }

        // Initialize ManagedIndexMetaData
        if (managedIndexMetaData == null) {
            initializeManagedIndexMetaData(managedIndexConfig, policy)
        } else {
            // TODO: This could happen when deleting a ManagedIndexConfig document
            // We should compare the cached policy with the ManagedIndexMetaData and make sure we have the same policy, seqNo, primaryTerm
            // otherwise update to ERROR with policy/version conflict
        }
    }

    private suspend fun getPolicy(policyName: String): Policy? {
        val getRequest = GetRequest(INDEX_STATE_MANAGEMENT_INDEX, policyName).preference("_local")
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

    private suspend fun savePolicyToManagedIndexConfig(managedIndexConfig: ManagedIndexConfig, policy: Policy): Boolean {
        val updatedManagedIndexConfig = managedIndexConfig.copy(policy = policy,
                policySeqNo = policy.seqNo, policyPrimaryTerm = policy.primaryTerm)
        val indexRequest = createManagedIndexRequest(updatedManagedIndexConfig)
        val indexResponse: IndexResponse = client.suspendUntil { index(indexRequest, it) }
        if (indexResponse.status() != RestStatus.OK) {
            logger.error("Failed to save policy(${policy.id}) to ManagedIndexConfig(${managedIndexConfig.index})")
            return false
        }
        return true
    }

    private suspend fun initializeManagedIndexMetaData(
        managedIndexConfig: ManagedIndexConfig,
        policy: Policy?
    ) {
        // TODO: Implement with real information, retries, error handling
        // If policy is null it means it does not exist or has empty source and we should move to ERROR
        val managedIndexMetaData = ManagedIndexMetaData(
            index = managedIndexConfig.index,
            indexUuid = managedIndexConfig.indexUuid,
            policyName = managedIndexConfig.policyName,
            policySeqNo = policy?.seqNo,
            policyPrimaryTerm = policy?.primaryTerm,
            state = policy?.defaultState,
            stateStartTime = if (policy != null) Instant.now().toEpochMilli() else null,
            transitionTo = null,
            actionIndex = null,
            action = null,
            actionStartTime = null,
            step = null,
            stepStartTime = null,
            stepCompleted = null,
            failed = policy == null,
            info = if (policy == null) mapOf("message" to "Could not load policy: ${managedIndexConfig.policyName}") else null,
            consumedRetries = null
        )
        val request = UpdateManagedIndexMetaDataRequest(
            listOf(Pair(Index(managedIndexConfig.index, managedIndexConfig.indexUuid), managedIndexMetaData))
        )
        try {
            val response: AcknowledgedResponse = client.suspendUntil { execute(UpdateManagedIndexMetaDataAction, request, it) }
        } catch (e: ClusterBlockException) {
            logger.error("There was ClusterBlockException trying to update the metadata for ${managedIndexConfig.index}. Message: ${e.message}")
        }
    }
}
