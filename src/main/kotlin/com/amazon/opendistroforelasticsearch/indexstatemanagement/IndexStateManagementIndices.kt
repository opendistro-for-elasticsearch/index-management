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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings.Companion.ISM_HISTORY_INDEX_MAX_AGE
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings.Companion.ISM_HISTORY_MAX_DOCS
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings.Companion.ISM_HISTORY_ROLLOVER_CHECK_PERIOD
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.OpenForTesting
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util._DOC
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ResourceAlreadyExistsException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.alias.Alias
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest
import org.elasticsearch.client.Client
import org.elasticsearch.client.IndicesAdminClient
import org.elasticsearch.cluster.LocalNodeMasterListener
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.threadpool.Scheduler.Cancellable
import org.elasticsearch.threadpool.ThreadPool

// TODO: Handle updating mappings on newer versions
@OpenForTesting
class IndexStateManagementIndices(
    settings: Settings,
    private val client: IndicesAdminClient,
    private val threadPool: ThreadPool,
    private val clusterService: ClusterService
) : LocalNodeMasterListener {

    private var scheduledRollover: Cancellable? = null
    private val logger = LogManager.getLogger(javaClass)

    @Volatile private var historyMaxDocs = ISM_HISTORY_MAX_DOCS.get(settings)

    @Volatile private var historyMaxAge = ISM_HISTORY_INDEX_MAX_AGE.get(settings)

    @Volatile private var historyRolloverCheckPeriod = ISM_HISTORY_ROLLOVER_CHECK_PERIOD.get(settings)

    private val indexStateManagementMappings = javaClass.classLoader.getResource("mappings/opendistro-ism-config.json").readText()

    private val indexStateManagementHistoryMappings = javaClass.classLoader.getResource("mappings/opendistro-ism-history.json").readText()

    init {
        clusterService.addLocalNodeMasterListener(this)
        clusterService.clusterSettings.addSettingsUpdateConsumer(ISM_HISTORY_MAX_DOCS) { historyMaxDocs = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ISM_HISTORY_INDEX_MAX_AGE) { historyMaxAge = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ISM_HISTORY_ROLLOVER_CHECK_PERIOD) {
            historyRolloverCheckPeriod = it
            rescheduleRollover()
        }
    }

    override fun onMaster() {
        try {
            // try to rollover immediately as we might be restarting the cluster
            rolloverHistoryIndex()
            // schedule the next rollover for approx MAX_AGE later
            scheduledRollover = threadPool.scheduleWithFixedDelay({ rolloverHistoryIndex() }, historyRolloverCheckPeriod, executorName())
        } catch (e: Exception) {
            // This should be run on cluster startup
            logger.error("Error creating ISM history index. History can't be recorded until master node is restarted.", e)
        }
    }

    override fun offMaster() {
        scheduledRollover?.cancel()
    }

    override fun executorName(): String {
        return ThreadPool.Names.MANAGEMENT
    }

    private fun rescheduleRollover() {
        if (clusterService.state().nodes.isLocalNodeElectedMaster) {
            scheduledRollover?.cancel()
            scheduledRollover = threadPool.scheduleWithFixedDelay({ rolloverHistoryIndex() }, historyRolloverCheckPeriod, executorName())
        }
    }

    fun initIndexStateManagementIndex(actionListener: ActionListener<CreateIndexResponse>) {
        if (!indexStateManagementIndexExists()) {
            val indexRequest = CreateIndexRequest(INDEX_STATE_MANAGEMENT_INDEX)
                    .mapping(_DOC, indexStateManagementMappings, XContentType.JSON)
            client.create(indexRequest, actionListener)
        }
    }

    fun indexStateManagementIndexExists(): Boolean = clusterService.state().routingTable.hasIndex(INDEX_STATE_MANAGEMENT_INDEX)

    /**
     * Attempt to create [INDEX_STATE_MANAGEMENT_INDEX] and return whether it exists
     */
    @Suppress("ReturnCount")
    suspend fun attemptInitStateManagementIndex(client: Client): Boolean {
        if (indexStateManagementIndexExists()) return true

        return try {
            val response: CreateIndexResponse = client.suspendUntil { initIndexStateManagementIndex(it) }
            if (response.isAcknowledged) {
                return true
            }
            logger.error("Creating $INDEX_STATE_MANAGEMENT_INDEX with mappings NOT acknowledged")
            return false
        } catch (e: ResourceAlreadyExistsException) {
            true
        } catch (e: Exception) {
            logger.error("Error trying to create $INDEX_STATE_MANAGEMENT_INDEX", e)
            false
        }
    }

    /**
     * ============== History =============
     */
    fun indexStateManagementIndexHistoryExists(): Boolean = clusterService.state().routingTable.hasIndex(INDEX_STATE_MANAGEMENT_INDEX)

    suspend fun initHistoryIndex() {
        if (!indexStateManagementIndexHistoryExists())
            createHistoryIndex(HISTORY_INDEX_PATTERN, HISTORY_WRITE_INDEX)
    }

    private suspend fun createHistoryIndex(index: String, alias: String? = null): Boolean {
        // This should be a fast check of local cluster state. Should be exceedingly rare that the local cluster
        // state does not contain the index and multiple nodes concurrently try to create the index.
        // If it does happen that error is handled we catch the ResourceAlreadyExistsException
        val existsResponse: IndicesExistsResponse = client.suspendUntil {
            client.exists(IndicesExistsRequest(index).local(true), it)
        }
        if (existsResponse.isExists) return true

        val request = CreateIndexRequest(index).mapping(_DOC, indexStateManagementHistoryMappings, XContentType.JSON)
        if (alias != null) request.alias(Alias(alias))
        return try {
            val createIndexResponse: CreateIndexResponse = client.suspendUntil { client.create(request, it) }
            if (createIndexResponse.isAcknowledged) {
                true
            } else {
                logger.error("Creating $index with mappings NOT acknowledged")
                false
            }
        } catch (e: ResourceAlreadyExistsException) {
            true
        } catch (e: Exception) {
            logger.error("Error trying to create $index", e)
            false
        }
    }

    fun rolloverHistoryIndex(): Boolean {
        if (!indexStateManagementIndexHistoryExists()) {
            return false
        }

        // We have to pass null for newIndexName in order to get Elastic to increment the index count.
        val request = RolloverRequest(HISTORY_WRITE_INDEX, null)
        request.createIndexRequest.index(HISTORY_INDEX_PATTERN)
            .mapping(_DOC, indexStateManagementHistoryMappings, XContentType.JSON)
        request.addMaxIndexDocsCondition(historyMaxDocs)
        request.addMaxIndexAgeCondition(historyMaxAge)
        val response = client.rolloversIndex(request).actionGet()
        if (!response.isRolledOver) {
            logger.info("$HISTORY_WRITE_INDEX not rolled over. Conditions were: ${response.conditionStatus}")
        }
        return response.isRolledOver
    }

    companion object {
        const val HISTORY_WRITE_INDEX = ".opendistro-ism-managed-index-history-write"
        const val HISTORY_INDEX_PATTERN = "<.opendistro-ism-managed-index-history-{now/d}-1>"
        const val HISTORY_ALL = ".opendistro-ism-managed-index-history*"
    }
}
