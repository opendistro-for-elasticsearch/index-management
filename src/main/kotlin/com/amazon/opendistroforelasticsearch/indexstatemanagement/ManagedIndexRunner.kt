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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.action.updateindexmetadata.UpdateManagedIndexMetaDataRequest
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.getManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.createManagedIndexRequest
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.LockModel
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner
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
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.rest.RestStatus

object ManagedIndexRunner : ScheduledJobRunner, CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default) {

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
            throw IllegalArgumentException("Invalid job type")
        }

        launch {
            // Attempt to acquire lock
            var lock: LockModel? = null
            withContext(Dispatchers.IO) { lock = context.lockService.acquireLock(job, context) }
            if (lock == null) {
                logger.info("Could not acquire lock for ${job.index}")
            } else {
                // TODO: Temporary until the initial ScheduledJobParser passes seqNo/primaryTerm
                runManagedIndexConfig(job.copy(seqNo = context.jobVersion.seqNo, primaryTerm = context.jobVersion.primaryTerm))
                // Release lock
                withContext(Dispatchers.IO) { context.lockService.release(lock) }
            }
        }
    }

    // TODO: Implement logic for when ISM is moved to STOPPING/STOPPED state
    private suspend fun runManagedIndexConfig(managedIndexConfig: ManagedIndexConfig) {

        logger.info("runJob for index: ${managedIndexConfig.index}")

        // Get current IndexMetaData and ManagedIndexMetaData
        val indexMetaData = clusterService.state().metaData().index(managedIndexConfig.index)
        val managedIndexMetaData = indexMetaData.getManagedIndexMetaData()

        // If policy or managedIndexMetaData is null then initialize
        if (managedIndexConfig.policy == null || managedIndexMetaData == null) {
            initManagedIndex(managedIndexConfig, indexMetaData, managedIndexMetaData)
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
        indexMetaData: IndexMetaData,
        managedIndexMetaData: ManagedIndexMetaData?
    ) {
        // If policy does not currently exist, get policy by name and save it to ManagedIndexConfig
        if (managedIndexConfig.policy == null) {
            val policy = getPolicy(managedIndexConfig.policyName)

            if (policy != null) {
                val saved = savePolicyToManagedIndexConfig(managedIndexConfig, policy)
                if (!saved) return // If we failed to save the policy, don't initialize ManagedIndexMetaData
            }
        }

        // Initialize ManagedIndexMetaData
        if (managedIndexMetaData == null) {
            initializeManagedIndexMetaData(indexMetaData)
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
        return withContext(Dispatchers.IO) {
            val xcp = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                    policySource, XContentType.JSON)
            Policy.parseWithType(xcp)
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

    private suspend fun initializeManagedIndexMetaData(indexMetaData: IndexMetaData) {
        // TODO: Implement with real information, retries, error handling
        val managedIndexMetaData = ManagedIndexMetaData(
                indexMetaData.index.name,
                indexMetaData.index.uuid,
                "${indexMetaData.index.name}_POLICY_NAME",
                "${indexMetaData.index.name}_POLICY_VERSION",
                "${indexMetaData.index.name}_STATE",
                "${indexMetaData.index.name}_STATE_START_TIME",
                "${indexMetaData.index.name}_ACTION_INDEX",
                "${indexMetaData.index.name}_ACTION",
                "${indexMetaData.index.name}_ACTION_START_TIME",
                "${indexMetaData.index.name}_STEP",
                "${indexMetaData.index.name}_STEP_START_TIME",
                "${indexMetaData.index.name}_FAILED_STEP"
        )
        val request = UpdateManagedIndexMetaDataRequest(indexMetaData.index, managedIndexMetaData)
        val response: AcknowledgedResponse = client.suspendUntil { execute(UpdateManagedIndexMetaDataAction, request, it) }
    }
}
