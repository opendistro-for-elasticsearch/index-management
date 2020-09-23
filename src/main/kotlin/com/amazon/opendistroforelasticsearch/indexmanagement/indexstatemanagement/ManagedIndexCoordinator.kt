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
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getClusterStateManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getPolicyID
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.retry
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.shouldCreateManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.shouldDeleteManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.shouldDeleteManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
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
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.util.OpenForTesting
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.managedIndexConfigIndexRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.deleteManagedIndexRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.getCreateManagedIndexRequests
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.getDeleteManagedIndexRequests
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
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterChangedEvent
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.ClusterStateListener
import org.elasticsearch.cluster.LocalNodeMasterListener
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.component.LifecycleListener
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentType
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

        if (!event.localNodeMaster()) return

        // TODO: Look into event.isNewCluster, can we return early if true?
        // if (event.isNewCluster) { }

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
        val clusterStateRequest = ClusterStateRequest()
            .clear()
            .metadata(true)
            .local(false)
            .indices("*")
            .indicesOptions(IndicesOptions.strictExpand())

        val response: ClusterStateResponse = client.admin().cluster().suspendUntil { state(clusterStateRequest, it) }

        /*
         * Iterate through all indices and create update requests to update the ManagedIndexConfig for indices that
         * meet the following conditions:
         *   1. Is being managed (has ManagedIndexMetaData)
         *   2. Does not have a completed Policy
         *   3. Does not have a failed Policy
         */
        val updateManagedIndicesRequests: List<DocWriteRequest<*>> = response.state.metadata.indices.mapNotNull {
            val managedIndexMetaData = it.value.getManagedIndexMetaData()
            if (!(managedIndexMetaData == null || managedIndexMetaData.isPolicyCompleted || managedIndexMetaData.isFailed)) {
                updateEnableManagedIndexRequest(it.value.indexUUID)
            } else {
                null
            }
        }

        updateManagedIndices(updateManagedIndicesRequests, false)
    }

    private fun isIndexStateManagementEnabled(): Boolean = indexStateManagementEnabled == true

    @OpenForTesting
    suspend fun sweepClusterChangedEvent(event: ClusterChangedEvent) {
        val indicesDeletedRequests = event.indicesDeleted()
                    .filter { event.previousState().metadata().index(it)?.getPolicyID() != null }
                    .map { deleteManagedIndexRequest(it.uuid) }

        /*
        * Update existing indices that have added or removed policy_ids
        * There doesn't seem to be a fast way of finding indices that meet the above conditions without
        * iterating over every index in cluster state and comparing current policy_id with previous policy_id
        * If this turns out to be a performance bottle neck we can remove this and enforce
        * addPolicy/removePolicy API usage for existing indices and let the background sweep pick up
        * any changes from users that ignore and use the ES settings API
        * */
        var hasCreateRequests = false
        val updateManagedIndicesRequests = mutableListOf<DocWriteRequest<*>>()
        val indicesToRemoveManagedIndexMetaDataFrom = mutableListOf<Index>()
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

            if (it.value.shouldDeleteManagedIndexMetaData()) indicesToRemoveManagedIndexMetaDataFrom.add(it.value.index)
        }

        updateManagedIndices(updateManagedIndicesRequests + indicesDeletedRequests, hasCreateRequests)
        clearManagedIndexMetaData(indicesToRemoveManagedIndexMetaDataFrom)
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

        val indicesToDeleteManagedIndexMetaDataFrom = getIndicesToDeleteManagedIndexMetaDataFrom(clusterService.state())

        val requests = createManagedIndexRequests + deleteManagedIndexRequests
        updateManagedIndices(requests, createManagedIndexRequests.isNotEmpty())
        clearManagedIndexMetaData(indicesToDeleteManagedIndexMetaDataFrom)
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

    private fun contentParser(bytesReference: BytesReference): XContentParser {
        return XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON)
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
            val bulkResponse: BulkResponse = client.suspendUntil { client.bulk(bulkRequest, it) }
            val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }
            requestsToRetry = failedResponses.filter { it.status() == RestStatus.TOO_MANY_REQUESTS }
                    .map { bulkRequest.requests()[it.itemId] }

            if (requestsToRetry.isNotEmpty()) {
                val retryCause = failedResponses.first { it.status() == RestStatus.TOO_MANY_REQUESTS }.failure.cause
                throw ExceptionsHelper.convertToElastic(retryCause)
            }
        }
    }

    /** Returns a list of [Index]es that need to have their [ManagedIndexMetaData] removed. */
    @OpenForTesting
    fun getIndicesToDeleteManagedIndexMetaDataFrom(clusterState: ClusterState): List<Index> {
        return clusterState.metadata().indices().values().mapNotNull {
            if (it.value.shouldDeleteManagedIndexMetaData()) it.value.index else null
        }
    }

    /** Removes the [ManagedIndexMetaData] from the given list of [Index]es. */
    @OpenForTesting
    @Suppress("TooGenericExceptionCaught")
    suspend fun clearManagedIndexMetaData(indices: List<Index>) {
        try {
            // If list of indices is empty, no request necessary
            if (indices.isEmpty()) return

            val request = UpdateManagedIndexMetaDataRequest(indicesToRemoveManagedIndexMetaDataFrom = indices)

            retryPolicy.retry(logger) {
                val response: AcknowledgedResponse = client.suspendUntil { execute(UpdateManagedIndexMetaDataAction.INSTANCE, request, it) }

                if (!response.isAcknowledged) logger.error("Failed to remove ManagedIndexMetaData for [indices=$indices]")
            }
        } catch (e: Exception) {
            logger.error("Failed to remove ManagedIndexMetaData for [indices=$indices]", e)
        }
    }

    companion object {
        const val MAX_HITS = 10_000
        const val BUFFER = 20L
    }
}
