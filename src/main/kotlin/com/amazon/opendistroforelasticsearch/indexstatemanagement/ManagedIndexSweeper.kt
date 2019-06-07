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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.sweeper.ClusterStateManagedIndex
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ManagedIndex
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.sweeper.SweptManagedIndex
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings.Companion.INDEX_STATE_MANAGEMENT_ENABLED
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings.Companion.POLICY_NAME
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings.Companion.SWEEP_PERIOD
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.createManagedIndexRequest
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.deleteManagedIndexRequest
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.updateManagedIndexRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ResourceAlreadyExistsException
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.LocalNodeMasterListener
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.component.LifecycleListener
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.threadpool.Scheduler
import org.elasticsearch.threadpool.ThreadPool
import kotlin.coroutines.CoroutineContext

/**
 * Sweeps the cluster state and [INDEX_STATE_MANAGEMENT_INDEX] for [ManagedIndex].
 *
 * This class sets up a background process that sweeps the cluster state for [ClusterStateManagedIndex]
 * and the [INDEX_STATE_MANAGEMENT_INDEX] for [SweptManagedIndex]. It will then compare these
 * ManagedIndices to appropriately create, delete, or update each [ManagedIndex]. Each node that has
 * the [IndexStateManagementPlugin] installed will have an instance of this class, but only the elected
 * master node will actually set up the background sweep process.
 */
class ManagedIndexSweeper(
    settings: Settings,
    private val client: Client,
    private val clusterService: ClusterService,
    private val threadPool: ThreadPool,
    indexStateManagementIndices: IndexStateManagementIndices
) : LocalNodeMasterListener, CoroutineScope, LifecycleListener() {

    private val logger = LogManager.getLogger(javaClass)
    private val ismIndices = indexStateManagementIndices

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default

    private var scheduledFullSweep: Scheduler.Cancellable? = null

    @Volatile private var lastFullSweepTimeNano = System.nanoTime()
    @Volatile private var indexStateManagementEnabled = INDEX_STATE_MANAGEMENT_ENABLED.get(settings)
    @Volatile private var sweepPeriod = SWEEP_PERIOD.get(settings)

    init {
        clusterService.addLifecycleListener(this)
        clusterService.addLocalNodeMasterListener(this)
        clusterService.clusterSettings.addSettingsUpdateConsumer(SWEEP_PERIOD) {
            sweepPeriod = it
            initBackgroundSweep()
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(INDEX_STATE_MANAGEMENT_ENABLED) {
            indexStateManagementEnabled = it
            if (!indexStateManagementEnabled) disable() else enable()
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

    override fun afterStart() {
        initBackgroundSweep()
    }

    override fun beforeStop() {
        scheduledFullSweep?.cancel()
    }

    private fun enable() {
        initBackgroundSweep()
        indexStateManagementEnabled = true
    }

    private fun disable() {
        scheduledFullSweep?.cancel()
        indexStateManagementEnabled = false
    }

    private fun isIndexStateManagementEnabled(): Boolean = indexStateManagementEnabled == true

    /**
     * Background sweep process that periodically sweeps for updates to ManagedIndices
     *
     * This background sweep will only be initialized if the local node is the elected master node.
     * Creates a runnable that is executed as a coroutine in the shared pool of threads on JVM.
     */
    private fun initBackgroundSweep() {
        // If ISM is disabled return early
        // TODO: Should we have separate ISM-enabled and Sweeper-enabled flags?
        if (!isIndexStateManagementEnabled()) return

        // Do not setup background sweep if we're not the elected master node
        if (!clusterService.state().nodes().isLocalNodeElectedMaster) return

        // Cancel existing background sweep
        scheduledFullSweep?.cancel()

        // Setup an anti-entropy/self-healing background sweep, in case we fail to create a ManagedIndex job
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
     * Sweeps the [INDEX_STATE_MANAGEMENT_INDEX] and cluster state.
     *
     * Sweeps the [INDEX_STATE_MANAGEMENT_INDEX] and cluster state for any [DocWriteRequest] that need to happen
     * and executes them in batch as a bulk request.
     */
    private suspend fun sweep() {
        // TODO: Cleanup ManagedIndexMetaData for non-existing indices once ManagedIndexMetaData is implemented

        val currentManagedIndices = sweepManagedIndexJobs()
        val clusterStateManagedIndices = sweepClusterState()

        val createManagedIndexRequests =
                getCreateManagedIndexRequests(clusterStateManagedIndices, currentManagedIndices)

        val deleteManagedIndexRequests =
                getDeleteManagedIndexRequests(clusterStateManagedIndices, currentManagedIndices)

        val updateManagedIndexRequests =
                getUpdateManagedIndexRequests(clusterStateManagedIndices, currentManagedIndices)

        if (createManagedIndexRequests.isNotEmpty()) {
            val created = attemptInitStateManagementIndex()
            if (!created) {
                logger.debug("Failed to create $INDEX_STATE_MANAGEMENT_INDEX")
                lastFullSweepTimeNano = System.nanoTime()
                return
            }
        }

        doBulkRequests(createManagedIndexRequests + deleteManagedIndexRequests + updateManagedIndexRequests)
        lastFullSweepTimeNano = System.nanoTime()
    }

    /**
     * Sweeps the [INDEX_STATE_MANAGEMENT_INDEX] for ManagedIndices.
     *
     * Sweeps the [INDEX_STATE_MANAGEMENT_INDEX] for ManagedIndices and only fetches the index, index_uuid,
     * policy_name, and change_policy fields to convert into a [SweptManagedIndex].
     *
     * @return map of IndexUuid to [SweptManagedIndex].
     */
    private suspend fun sweepManagedIndexJobs(): Map<String, SweptManagedIndex> {
        if (!ismIndices.indexStateManagementIndexExists()) return mapOf()

        val boolQueryBuilder = BoolQueryBuilder().filter(QueryBuilders.existsQuery(ManagedIndex.MANAGED_INDEX_TYPE))
        val managedIndexSearchRequest = SearchRequest()
                .indices(INDEX_STATE_MANAGEMENT_INDEX)
                .source(SearchSourceBuilder.searchSource()
                        // TODO: Get all ManagedIndices at once or split into searchAfter queries?
                        .size(MAX_HITS)
                        .fetchSource(
                                arrayOf(
                                        "${ManagedIndex.MANAGED_INDEX_TYPE}.${ManagedIndex.INDEX_FIELD}",
                                        "${ManagedIndex.MANAGED_INDEX_TYPE}.${ManagedIndex.INDEX_UUID_FIELD}",
                                        "${ManagedIndex.MANAGED_INDEX_TYPE}.${ManagedIndex.POLICY_NAME_FIELD}",
                                        "${ManagedIndex.MANAGED_INDEX_TYPE}.${ManagedIndex.CHANGE_POLICY_FIELD}"
                                ),
                                emptyArray()
                        )
                        .query(boolQueryBuilder))

        val response: SearchResponse = client.suspendUntil { search(managedIndexSearchRequest, it) }
        val sweptManagedIndices = mutableMapOf<String, SweptManagedIndex>()

        response.hits.forEach {
            val sweptManagedIndex =
                    SweptManagedIndex.parseWithType(SweptManagedIndex.parser(it.sourceRef), it.seqNo, it.primaryTerm)
            sweptManagedIndices[sweptManagedIndex.uuid] = sweptManagedIndex
        }

        return sweptManagedIndices
    }

    /**
     * Sweeps the cluster state for ManagedIndices.
     *
     * Sweeps the cluster state for ManagedIndices by checking for the [POLICY_NAME] in the index settings.
     * If the [POLICY_NAME] is null or blank it's treated as not existing.
     *
     * @return map of IndexUuid to [ClusterStateManagedIndex].
     */
    private fun sweepClusterState(): Map<String, ClusterStateManagedIndex> {
        val clusterStateManagedIndices = mutableMapOf<String, ClusterStateManagedIndex>()
        clusterService.state().metaData().indices().values().forEach {
            val policyName = it.value.settings.get(POLICY_NAME.key)
            if (!policyName.isNullOrBlank()) {
                val index = it.value.index.name
                val uuid = it.value.index.uuid
                clusterStateManagedIndices[uuid] =
                        ClusterStateManagedIndex(index = index, uuid = uuid, policyName = policyName)
            }
        }
        return clusterStateManagedIndices.toMap()
    }

    /**
     * Creates IndexRequests for ManagedIndices.
     *
     * Finds ManagedIndices that exist in the cluster state that do not yet exist in [INDEX_STATE_MANAGEMENT_INDEX]
     * which means we need to create the [ManagedIndex].
     *
     * @param clusterStateManagedIndices map of IndexUuid to [ClusterStateManagedIndex].
     * @param currentManagedIndices map of IndexUuid to [SweptManagedIndex].
     * @return list of [DocWriteRequest].
     */
    private fun getCreateManagedIndexRequests(
        clusterStateManagedIndices: Map<String, ClusterStateManagedIndex>,
        currentManagedIndices: Map<String, SweptManagedIndex>
    ): List<DocWriteRequest<*>> {
        return clusterStateManagedIndices.filter { (uuid) ->
            !currentManagedIndices.containsKey(uuid)
        }.map { createManagedIndexRequest(it.value.index, it.value.uuid, it.value.policyName) }
    }

    /**
     * Creates DeleteRequests for ManagedIndices.
     *
     * Finds ManagedIndices that exist in [INDEX_STATE_MANAGEMENT_INDEX] that do not exist in the cluster state
     * anymore which means we need to delete the [ManagedIndex].
     *
     * @param clusterStateManagedIndices map of IndexUuid to [ClusterStateManagedIndex].
     * @param currentManagedIndices map of IndexUuid to [SweptManagedIndex].
     * @return list of [DocWriteRequest].
     */
    private fun getDeleteManagedIndexRequests(
        clusterStateManagedIndices: Map<String, ClusterStateManagedIndex>,
        currentManagedIndices: Map<String, SweptManagedIndex>
    ): List<DocWriteRequest<*>> {
        return currentManagedIndices.filter { (uuid) ->
            !clusterStateManagedIndices.containsKey(uuid)
        }.map { deleteManagedIndexRequest(it.value.uuid) }
    }

    /**
     * Creates UpdateRequests for ManagedIndices.
     *
     * Finds ManagedIndices that exist both in cluster state and in [INDEX_STATE_MANAGEMENT_INDEX] that
     * need to be updated. We know a [ManagedIndex] needs to be updated when the policyName differs between
     * the [ClusterStateManagedIndex] and the [SweptManagedIndex]. And we know a [ManagedIndex] has not yet
     * been updated if it's ChangePolicy does not match the new policyName.
     *
     * @param clusterStateManagedIndices map of IndexUuid to [ClusterStateManagedIndex].
     * @param currentManagedIndices map of IndexUuid to [SweptManagedIndex].
     * @return list of [DocWriteRequest].
     */
    private fun getUpdateManagedIndexRequests(
        clusterStateManagedIndices: Map<String, ClusterStateManagedIndex>,
        currentManagedIndices: Map<String, SweptManagedIndex>
    ): List<DocWriteRequest<*>> {
        return clusterStateManagedIndices.asSequence()
                .filter { (uuid, clusterStateManagedIndex) ->
                    currentManagedIndices[uuid] != null &&
                            // Verify they have different policy names which means we should update it
                            currentManagedIndices[uuid]?.policyName != clusterStateManagedIndex.policyName &&
                            // Verify it is not already being updated
                            currentManagedIndices[uuid]?.changePolicy?.policyName != clusterStateManagedIndex.policyName
                }
                .map {
                    val currentManagedIndex = currentManagedIndices[it.key]
                    if (currentManagedIndex == null) {
                        updateManagedIndexRequest(it.value)
                    } else {
                        updateManagedIndexRequest(
                            it.value.copy(seqNo = currentManagedIndex.seqNo, primaryTerm = currentManagedIndex.primaryTerm)
                        )
                    }
                }.toList()
    }

    /**
     * Attempt to create [INDEX_STATE_MANAGEMENT_INDEX] and return whether it exists
     */
    private suspend fun attemptInitStateManagementIndex(): Boolean {
        if (ismIndices.indexStateManagementIndexExists()) return true

        return try {
            val response: CreateIndexResponse = client.suspendUntil { ismIndices.initIndexStateManagementIndex(it) }
            return response.isAcknowledged
        } catch (e: ResourceAlreadyExistsException) {
            true
        } catch (e: Exception) {
            false
        }
    }

    /**
     * Executes [requests] in single bulk request
     */
    private suspend fun doBulkRequests(requests: List<DocWriteRequest<*>>) {
        if (requests.isEmpty()) return
        val bulkRequest = BulkRequest().add(requests)
        val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
        // TODO: handle failures
    }

    companion object {
        const val MAX_HITS = 10_000
        const val BUFFER = 20L
    }
}
