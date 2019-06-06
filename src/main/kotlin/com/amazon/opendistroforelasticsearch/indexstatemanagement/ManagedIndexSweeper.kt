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

    private suspend fun sweep() {
        // TODO: Cleanup ManagedIndexMetaData for non-existing indices once ManagedIndexMetaData is implemented
        // TODO: Can do create/delete/update actions in parallel

        val currentManagedIndices = sweepManagedIndexJobs()
        val managedIndices = sweepClusterState()

        // Creating new ManagedIndex jobs
        // Find indices that have no ManagedIndex jobs and create new ManagedIndex jobs
        val managedIndicesToCreate = managedIndices.filter { (uuid) ->
            !currentManagedIndices.containsKey(uuid)
        }.values.toList()

        if (managedIndicesToCreate.isNotEmpty()) {
            attemptCreateManagedIndices(managedIndicesToCreate)
        }

        // Deleting existing ManagedIndex jobs
        // Any current ManagedIndex job that does not exist in the managedIndices from cluster state should be deleted
        val currentManagedIndicesToDelete = currentManagedIndices.filter { (uuid) ->
            !managedIndices.containsKey(uuid)
        }.values.toList()

        if (currentManagedIndicesToDelete.isNotEmpty()) {
            deleteCurrentManagedIndices(currentManagedIndicesToDelete)
        }

        // Updating existing ManagedIndex jobs
        // Both sets have index but policy names are different, ensure that the existing managed
        // index job does not have the new policy in change_policy already
        val currentManagedIndicesToUpdate = managedIndices.asSequence()
                .filter {(uuid, clusterStateManagedIndex) ->
                    currentManagedIndices[uuid] != null &&
                            // Verify they have different policy names which means we should update it
                            currentManagedIndices[uuid]?.policyName != clusterStateManagedIndex.policyName &&
                            // Verify it is not already being updated
                            currentManagedIndices[uuid]?.changePolicy?.policyName != clusterStateManagedIndex.policyName
                }
                .map {
                    val currentManagedIndex = currentManagedIndices[it.key]
                    if (currentManagedIndex == null) {
                        it.value
                    } else {
                        it.value.copy(seqNo = currentManagedIndex.seqNo, primaryTerm = currentManagedIndex.primaryTerm)
                    }
                }.toList()

        if (currentManagedIndicesToUpdate.isNotEmpty()) {
            updateCurrentManagedIndices(currentManagedIndicesToUpdate)
        }
    }

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
            val sweptManagedIndex = SweptManagedIndex.parseWithType(SweptManagedIndex.parser(it.sourceRef), it.seqNo, it.primaryTerm)
            sweptManagedIndices[sweptManagedIndex.uuid] = sweptManagedIndex
        }

        return sweptManagedIndices
    }

    private fun sweepClusterState(): Map<String, ClusterStateManagedIndex> {
        val clusterStateManagedIndices = mutableMapOf<String, ClusterStateManagedIndex>()
        clusterService.state().metaData().indices().values().forEach {
            val policyName = it.value.settings.get(POLICY_NAME.key)
            if (!policyName.isNullOrBlank()) {
                val index = it.value.index.name
                val uuid = it.value.index.uuid
                clusterStateManagedIndices[uuid] = ClusterStateManagedIndex(index = index, uuid = uuid, policyName = policyName)
            }
        }
        return clusterStateManagedIndices.toMap()
    }

    private fun getFullSweepElapsedTime(): TimeValue {
        return TimeValue.timeValueNanos(System.nanoTime() - lastFullSweepTimeNano)
    }

    private suspend fun attemptCreateManagedIndices(managedIndices: List<ClusterStateManagedIndex>) {
        if (!ismIndices.indexStateManagementIndexExists()) {
            val response: CreateIndexResponse = client.suspendUntil { ismIndices.initIndexStateManagementIndex(it) }
            if (!response.isAcknowledged) {
                logger.error("Create $INDEX_STATE_MANAGEMENT_INDEX mappings call not acknowledged.")
                return
            }
            logger.info("Created $INDEX_STATE_MANAGEMENT_INDEX with mappings.")
        }
        createManagedIndices(managedIndices)
    }

    private suspend fun createManagedIndices(managedIndices: List<ClusterStateManagedIndex>) {
        if (managedIndices.isEmpty()) return
        val indexRequests = managedIndices.map { createManagedIndexRequest(it.index, it.uuid, it.policyName) }
        val bulkRequest = BulkRequest().add(indexRequests)
        val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
        // TODO: handle failures
    }

    private suspend fun deleteCurrentManagedIndices(currentManagedIndices: List<SweptManagedIndex>) {
        if (currentManagedIndices.isEmpty()) return
        val deleteRequests = currentManagedIndices.map { deleteManagedIndexRequest(it.uuid) }
        val bulkRequest = BulkRequest().add(deleteRequests)
        val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
        // TODO: handle failures
    }

    private suspend fun updateCurrentManagedIndices(managedIndices: List<ClusterStateManagedIndex>) {
        if (managedIndices.isEmpty()) return
        val updateRequests = managedIndices.map { updateManagedIndexRequest(it) }
        val bulkRequest = BulkRequest().add(updateRequests)
        val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
        // TODO: handle failures
    }

    companion object {
        const val MAX_HITS = 10_000
        const val BUFFER = 20L
    }
}
