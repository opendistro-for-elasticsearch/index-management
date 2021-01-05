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
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.contentParser
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getClusterStateManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getPolicyID
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.mgetManagedIndexMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.retry
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.shouldCreateManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.shouldDeleteManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.coordinator.ClusterStateManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.coordinator.SweptManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.COORDINATOR_BACKOFF_COUNT
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.COORDINATOR_BACKOFF_MILLIS
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.INDEX_STATE_MANAGEMENT_ENABLED
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.JOB_INTERVAL
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.POLICY_ID
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.SWEEP_PERIOD
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.deleteManagedIndexMetadataRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.util.OpenForTesting
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.managedIndexConfigIndexRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.deleteManagedIndexRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.getCreateManagedIndexRequests
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.getDeleteManagedIndexRequests
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.getDeleteManagedIndices
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.getSweptManagedIndexSearchRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.isFailed
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.isPolicyCompleted
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.updateEnableManagedIndexRequest
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
import org.elasticsearch.action.bulk.BackoffPolicy
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterChangedEvent
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.ClusterStateListener
import org.elasticsearch.cluster.LocalNodeMasterListener
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.component.LifecycleListener
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.Index
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.threadpool.Scheduler
import org.elasticsearch.threadpool.ThreadPool

/**
 * Listens for cluster changes to pick up new indices to manage.
 * Sweeps the cluster state and [INDEX_MANAGEMENT_INDEX] for [ManagedIndexConfig].
 *
 * This class listens for [ClusterChangedEvent] to pick up on [ManagedIndexConfig] to create or delete.
 * Also sets up a background process that sweeps the cluster state for [ClusterStateManagedIndexConfig]
 * and the [INDEX_MANAGEMENT_INDEX] for [SweptManagedIndexConfig]. It will then compare these
 * ManagedIndices to appropriately create or delete each [ManagedIndexConfig]. Each node that has
 * the [IndexManagementPlugin] installed will have an instance of this class, but only the elected
 * master node will set up the background sweep process and listen for [ClusterChangedEvent].
 *
 * We do not allow updating to a new policy through Coordinator as this can have bad side effects. If
 * a user wants to update an existing [ManagedIndexConfig] to a new policy (or updated version of policy)
 * then they must use the ChangePolicy API.
 */
@Suppress("TooManyFunctions")
@OpenForTesting
class ManagedIndexCoordinator(
    settings: Settings,
    private val client: Client,
    private val clusterService: ClusterService,
    private val threadPool: ThreadPool,
    indexManagementIndices: IndexManagementIndices
) : LocalNodeMasterListener, ClusterStateListener,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("ManagedIndexCoordinator")),
    LifecycleListener() {

    private val logger = LogManager.getLogger(javaClass)
    private val ismIndices = indexManagementIndices

    private var scheduledFullSweep: Scheduler.Cancellable? = null

    @Volatile private var lastFullSweepTimeNano = System.nanoTime()
    @Volatile private var indexStateManagementEnabled = INDEX_STATE_MANAGEMENT_ENABLED.get(settings)
    @Volatile private var sweepPeriod = SWEEP_PERIOD.get(settings)
    @Volatile private var retryPolicy =
            BackoffPolicy.constantBackoff(COORDINATOR_BACKOFF_MILLIS.get(settings), COORDINATOR_BACKOFF_COUNT.get(settings))
    @Volatile private var jobInterval = JOB_INTERVAL.get(settings)

    init {
        clusterService.addListener(this)
        clusterService.addLifecycleListener(this)
        clusterService.addLocalNodeMasterListener(this)
        clusterService.clusterSettings.addSettingsUpdateConsumer(SWEEP_PERIOD) {
            sweepPeriod = it
            initBackgroundSweep()
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(JOB_INTERVAL) {
            jobInterval = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(INDEX_STATE_MANAGEMENT_ENABLED) {
            indexStateManagementEnabled = it
            if (!indexStateManagementEnabled) disable() else enable()
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(COORDINATOR_BACKOFF_MILLIS, COORDINATOR_BACKOFF_COUNT) {
            millis, count -> retryPolicy = BackoffPolicy.constantBackoff(millis, count)
        }
    }

    override fun onMaster() {
        // Init background sweep when promoted to being master
        initBackgroundSweep()
    }

    override fun offMaster() {
        // Cancel background sweep when demoted from being master
        scheduledFullSweep?.cancel()
    }

    override fun executorName(): String {
        return ThreadPool.Names.MANAGEMENT
    }

    @Suppress("ReturnCount")
    override fun clusterChanged(event: ClusterChangedEvent) {
        if (!isIndexStateManagementEnabled()) return

        if (event.isNewCluster) return

        if (!event.localNodeMaster()) return

        if (!event.metadataChanged()) return

        launch { sweepClusterChangedEvent(event) }
    }

    override fun afterStart() {
        initBackgroundSweep()
    }

    override fun beforeStop() {
        scheduledFullSweep?.cancel()
    }

    private fun enable() {
        initBackgroundSweep()
        indexStateManagementEnabled = true

        // Calling initBackgroundSweep() beforehand runs a sweep ensuring that policies removed from indices
        // and indices being deleted are accounted for prior to re-enabling jobs
        launch {
            try {
                logger.debug("Re-enabling jobs for managed indices")
                reenableJobs()
            } catch (e: Exception) {
                logger.error("Failed to re-enable jobs for managed indices", e)
            }
        }
    }

    private fun disable() {
        scheduledFullSweep?.cancel()
        indexStateManagementEnabled = false
    }

    private suspend fun reenableJobs() {
        /*
         * Iterate through all indices and create update requests to update the ManagedIndexConfig for indices that
         * meet the following conditions:
         *   1. Is being managed? ( has policyID in index settings )
         *   2. If being managed, then try to retrieve metadata from config index to check
         *      - Does not have a completed Policy
         *      - Does not have a failed Policy
         */

        val clusterStateRequest = ClusterStateRequest()
            .clear()
            .metadata(true)
            .local(false)
            .indices("*")
            .indicesOptions(IndicesOptions.strictExpand())
        val response: ClusterStateResponse = client.admin().cluster().suspendUntil { state(clusterStateRequest, it) }

        val indicesWithPolicyID = mutableListOf<Index>()
        response.state.metadata.indices.forEach {
            if (it.value.getPolicyID() != null) {
                indicesWithPolicyID.add(it.value.index)
            }
        }

        val enableManagedIndicesRequests = mutableListOf<UpdateRequest>()
        val metadataList = client.mgetManagedIndexMetadata(indicesWithPolicyID)
        metadataList.forEach {
            if (it != null && !(it.isPolicyCompleted || it.isFailed)) {
                enableManagedIndicesRequests.add(updateEnableManagedIndexRequest(it.indexUuid))
            }
        }

        updateManagedIndices(enableManagedIndicesRequests, false)
    }

    private fun isIndexStateManagementEnabled(): Boolean = indexStateManagementEnabled == true

    /** create, delete job document based on index's policyID setting changes */
    @OpenForTesting
    fun sweepClusterChangedEvent(event: ClusterChangedEvent) {
        val managedIndicesDeleted = event.indicesDeleted()
            .filter { event.previousState().metadata().index(it)?.getPolicyID() != null }

        launch {
            val indicesDeletedRequests = managedIndicesDeleted.map { deleteManagedIndexRequest(it.uuid) }

            /**
             * Update existing indices that have added or removed policy_ids
             * There doesn't seem to be a fast way of finding indices that meet the above conditions without
             * iterating over every index in cluster state and comparing current policy_id with previous policy_id
             * If this turns out to be a performance bottle neck we can remove this and enforce
             * addPolicy/removePolicy API usage for existing indices and let the background sweep pick up
             * any changes from users that ignore and use the ES settings API
             */
            var hasCreateRequests = false
            val updateManagedIndicesRequests = mutableListOf<DocWriteRequest<*>>()
            event.state().metadata().indices().forEach {
                val previousIndexMetaData = event.previousState().metadata().index(it.value.index)
                val policyID = it.value.getPolicyID()
                val request: DocWriteRequest<*>? = when {
                    it.value.shouldCreateManagedIndexConfig(previousIndexMetaData) && policyID != null -> {
                        hasCreateRequests = true
                        managedIndexConfigIndexRequest(it.value.index.name, it.value.indexUUID, policyID, jobInterval)
                    }
                    it.value.shouldDeleteManagedIndexConfig(previousIndexMetaData) ->
                        deleteManagedIndexRequest(it.value.indexUUID)
                    else -> null
                }
                if (request != null) updateManagedIndicesRequests.add(request)
            }

            updateManagedIndices(updateManagedIndicesRequests + indicesDeletedRequests, hasCreateRequests)
        }

        launch(CoroutineName("remove-metadata")) {
            val indicesToRemoveMetadata = getIndicesToDeleteMetadataFrom(event.state()) + managedIndicesDeleted
            val deleteRequests = indicesToRemoveMetadata.map { deleteManagedIndexMetadataRequest(it.uuid) }
            clearManagedIndexMetaData(deleteRequests)
        }
    }

    /**
     * Background sweep process that periodically sweeps for updates to ManagedIndices
     *
     * This background sweep will only be initialized if the local node is the elected master node.
     * Creates a runnable that is executed as a coroutine in the shared pool of threads on JVM.
     */
    @OpenForTesting
    fun initBackgroundSweep() {
        // If ISM is disabled return early
        if (!isIndexStateManagementEnabled()) return

        // Do not setup background sweep if we're not the elected master node
        if (!clusterService.state().nodes().isLocalNodeElectedMaster) return

        // Cancel existing background sweep
        scheduledFullSweep?.cancel()

        // Setup an anti-entropy/self-healing background sweep, in case we fail to create a ManagedIndexConfig job
        val scheduledSweep = Runnable {
            val elapsedTime = getFullSweepElapsedTime()

            // Rate limit to at most one full sweep per sweep period
            // The schedule runs may wake up a few milliseconds early
            // Delta will be giving some buffer on the schedule to allow waking up slightly earlier
            val delta = sweepPeriod.millis - elapsedTime.millis
            if (delta < BUFFER) { // give 20ms buffer.
                launch {
                    try {
                        logger.debug("Performing background sweep of managed indices")
                        sweep()
                    } catch (e: Exception) {
                        logger.error("Failed to sweep managed indices", e)
                    }
                }
            }
        }

        scheduledFullSweep = threadPool.scheduleWithFixedDelay(scheduledSweep, sweepPeriod, ThreadPool.Names.SAME)
    }

    private fun getFullSweepElapsedTime(): TimeValue =
        TimeValue.timeValueNanos(System.nanoTime() - lastFullSweepTimeNano)

    /**
     * Sweeps the [INDEX_MANAGEMENT_INDEX] and cluster state.
     *
     * Sweeps the [INDEX_MANAGEMENT_INDEX] and cluster state for any [DocWriteRequest] that need to happen
     * and executes them in batch as a bulk request.
     */
    @OpenForTesting
    suspend fun sweep() {
        val currentManagedIndices = sweepManagedIndexJobs(client, ismIndices.indexManagementIndexExists())
        val clusterStateManagedIndices = sweepClusterState(clusterService.state())

        val createManagedIndexRequests =
                getCreateManagedIndexRequests(clusterStateManagedIndices, currentManagedIndices, jobInterval)
        val deleteManagedIndexRequests =
                getDeleteManagedIndexRequests(clusterStateManagedIndices, currentManagedIndices)
        val requests = createManagedIndexRequests + deleteManagedIndexRequests

        updateManagedIndices(requests, createManagedIndexRequests.isNotEmpty())

        // clear metadata
        val indicesToDeleteMetadata =
            getIndicesToDeleteMetadataFrom(clusterService.state()) + getDeleteManagedIndices(clusterStateManagedIndices, currentManagedIndices)
        val deleteRequests = indicesToDeleteMetadata.map { deleteManagedIndexMetadataRequest(it.uuid) }
        clearManagedIndexMetaData(deleteRequests)

        lastFullSweepTimeNano = System.nanoTime()
    }

    /**
     * Sweeps the [INDEX_MANAGEMENT_INDEX] for ManagedIndices.
     *
     * Sweeps the [INDEX_MANAGEMENT_INDEX] for ManagedIndices and only fetches the index, index_uuid,
     * policy_id, and change_policy fields to convert into a [SweptManagedIndexConfig].
     *
     * @return map of IndexUuid to [SweptManagedIndexConfig].
     */
    @OpenForTesting
    suspend fun sweepManagedIndexJobs(
        client: Client,
        indexManagementIndexExists: Boolean
    ): Map<String, SweptManagedIndexConfig> {
        if (!indexManagementIndexExists) return mapOf()

        val managedIndexSearchRequest = getSweptManagedIndexSearchRequest()
        val response: SearchResponse = client.suspendUntil { search(managedIndexSearchRequest, it) }
        return response.hits.map {
            it.id to SweptManagedIndexConfig.parseWithType(contentParser(it.sourceRef),
                    it.seqNo, it.primaryTerm)
        }.toMap()
    }

    /**
     * Sweeps the cluster state for ManagedIndices.
     *
     * Sweeps the cluster state for ManagedIndices by checking for the [POLICY_ID] in the index settings.
     * If the [POLICY_ID] is null or blank it's treated as not existing.
     *
     * @return map of IndexUuid to [ClusterStateManagedIndexConfig].
     */
    @OpenForTesting
    fun sweepClusterState(clusterState: ClusterState): Map<String, ClusterStateManagedIndexConfig> {
        return clusterState.metadata().indices().values()
                .mapNotNull {
                    val clusterConfig = it.value.getClusterStateManagedIndexConfig()
                    clusterConfig?.run {
                        uuid to ClusterStateManagedIndexConfig(index = index, uuid = uuid, policyID = policyID)
                    }
        }.toMap()
    }

    @OpenForTesting
    suspend fun updateManagedIndices(requests: List<DocWriteRequest<*>>, hasCreateRequests: Boolean = false) {
        var requestsToRetry = requests
        if (requestsToRetry.isEmpty()) return

        if (hasCreateRequests) {
            val created = ismIndices.attemptInitStateManagementIndex(client)
            if (!created) {
                logger.error("Failed to create $INDEX_MANAGEMENT_INDEX")
                return
            }
        }

        retryPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
            val bulkRequest = BulkRequest().add(requestsToRetry)
            val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
            val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }
            requestsToRetry = failedResponses.filter { it.status() == RestStatus.TOO_MANY_REQUESTS }
                    .map { bulkRequest.requests()[it.itemId] }

            if (requestsToRetry.isNotEmpty()) {
                val retryCause = failedResponses.first { it.status() == RestStatus.TOO_MANY_REQUESTS }.failure.cause
                throw ExceptionsHelper.convertToElastic(retryCause)
            }
        }
    }

    /**
     * Returns a list of [Index]es that need to have their [ManagedIndexMetaData] removed
     * This method only consider policyID equals to null, not index being deleted
     */
    suspend fun getIndicesToDeleteMetadataFrom(clusterState: ClusterState): List<Index> {
        val indicesWithNullPolicyID = mutableListOf<Index>()
        clusterState.metadata().indices().values().forEach {
            if (it.value.getPolicyID() == null) {
                indicesWithNullPolicyID.add(it.value.index)
            }
        }

        return getIndicesToRemoveMetadataFrom(indicesWithNullPolicyID)
    }

    /** If this index has no policyID but [ManagedIndexMetaData] is not null then metadata should be removed. */
    suspend fun getIndicesToRemoveMetadataFrom(indicesWithNullPolicyID: List<Index>): List<Index> {
        val indicesToRemoveManagedIndexMetaDataFrom = mutableListOf<Index>()
        val metadataList = client.mgetManagedIndexMetadata(indicesWithNullPolicyID)
        metadataList.forEach {
            if (it != null) indicesToRemoveManagedIndexMetaDataFrom.add(Index(it.index, it.indexUuid))
        }
        return indicesToRemoveManagedIndexMetaDataFrom
    }

    /** Removes the [ManagedIndexMetaData] from the given list of [Index]es. */
    @OpenForTesting
    @Suppress("TooGenericExceptionCaught")
    suspend fun clearManagedIndexMetaData(deleteRequests: List<DocWriteRequest<*>>) {
        try {
            // If list of indices is empty, no request necessary
            if (deleteRequests.isEmpty()) return

            retryPolicy.retry(logger) {
                val bulkRequest = BulkRequest().add(deleteRequests)
                val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
                bulkResponse.forEach {
                    if (it.isFailed) logger.error("Failed to clear ManagedIndexMetadata for [index=${it.index}]. " +
                            "Id: ${it.id}, failureMessage: ${it.failureMessage}")
                }
            }
        } catch (e: Exception) {
            logger.error("Failed to clear ManagedIndexMetadata ", e)
        }
    }

    companion object {
        const val MAX_HITS = 10_000
        const val BUFFER = 20L
    }
}
