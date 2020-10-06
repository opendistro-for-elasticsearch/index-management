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

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexmanagement.util._DOC
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.LocalNodeMasterListener
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.threadpool.Scheduler
import org.elasticsearch.threadpool.ThreadPool
import java.time.Instant

class IndexStateManagementHistory(
    settings: Settings,
    private val client: Client,
    private val threadPool: ThreadPool,
    private val clusterService: ClusterService,
    private val indexManagementIndices: IndexManagementIndices
) : LocalNodeMasterListener {

    private val logger = LogManager.getLogger(javaClass)
    private var scheduledRollover: Scheduler.Cancellable? = null

    @Volatile private var historyEnabled = ManagedIndexSettings.HISTORY_ENABLED.get(settings)

    @Volatile private var historyMaxDocs = ManagedIndexSettings.HISTORY_MAX_DOCS.get(settings)

    @Volatile private var historyMaxAge = ManagedIndexSettings.HISTORY_INDEX_MAX_AGE.get(settings)

    @Volatile private var historyRolloverCheckPeriod = ManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD.get(settings)

    @Volatile private var historyRetentionPeriod = ManagedIndexSettings.HISTORY_RETENTION_PERIOD.get(settings)

    init {
        clusterService.addLocalNodeMasterListener(this)
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.HISTORY_ENABLED) {
            historyEnabled = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.HISTORY_MAX_DOCS) { historyMaxDocs = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.HISTORY_INDEX_MAX_AGE) { historyMaxAge = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD) {
            historyRolloverCheckPeriod = it
            rescheduleRollover()
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.HISTORY_RETENTION_PERIOD) {
            historyRetentionPeriod = it
        }
    }

    override fun onMaster() {
        try {
            // try to rollover immediately as we might be restarting the cluster
            rolloverHistoryIndex()
            // schedule the next rollover for approx MAX_AGE later
            scheduledRollover = threadPool.scheduleWithFixedDelay({ rolloverAndDeleteHistoryIndex() }, historyRolloverCheckPeriod, executorName())
        } catch (e: Exception) {
            // This should be run on cluster startup
            logger.error("Error creating ISM history index.", e)
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
            scheduledRollover = threadPool.scheduleWithFixedDelay({ rolloverAndDeleteHistoryIndex() }, historyRolloverCheckPeriod, executorName())
        }
    }

    private fun rolloverAndDeleteHistoryIndex() {
        if (historyEnabled) rolloverHistoryIndex()
        deleteOldHistoryIndex()
    }

    private fun rolloverHistoryIndex(): Boolean {
        if (!indexManagementIndices.indexStateManagementIndexHistoryExists()) {
            return false
        }

        // We have to pass null for newIndexName in order to get Elastic to increment the index count.
        val request = RolloverRequest(IndexManagementIndices.HISTORY_WRITE_INDEX_ALIAS, null)
        request.createIndexRequest.index(IndexManagementIndices.HISTORY_INDEX_PATTERN)
            .mapping(_DOC,
                IndexManagementIndices.indexStateManagementHistoryMappings, XContentType.JSON)
        request.addMaxIndexDocsCondition(historyMaxDocs)
        request.addMaxIndexAgeCondition(historyMaxAge)
        val response = client.admin().indices().rolloverIndex(request).actionGet()
        if (!response.isRolledOver) {
            logger.info("${IndexManagementIndices.HISTORY_WRITE_INDEX_ALIAS} not rolled over. Conditions were: ${response.conditionStatus}")
        }
        return response.isRolledOver
    }

    @Suppress("SpreadOperator", "NestedBlockDepth", "ComplexMethod")
    private fun deleteOldHistoryIndex() {
        val indexToDelete = mutableListOf<String>()

        val clusterStateRequest = ClusterStateRequest()
            .clear()
            .indices(IndexManagementIndices.HISTORY_ALL)
            .metadata(true)
            .local(true)
            .indicesOptions(IndicesOptions.strictExpand())

        val clusterStateResponse = client.admin().cluster().state(clusterStateRequest).actionGet()

        for (entry in clusterStateResponse.state.metadata.indices()) {
            val indexMetaData = entry.value
            val creationTime = indexMetaData.creationDate

            if ((Instant.now().toEpochMilli() - creationTime) > historyRetentionPeriod.millis) {
                val alias = indexMetaData.aliases.firstOrNull { IndexManagementIndices.HISTORY_WRITE_INDEX_ALIAS == it.value.alias }
                if (alias != null && historyEnabled) {
                    // If index has write alias and history is enable, don't delete the index.
                    continue
                }

                indexToDelete.add(indexMetaData.index.name)
            }
        }

        if (indexToDelete.isNotEmpty()) {
            val deleteRequest = DeleteIndexRequest(*indexToDelete.toTypedArray())
            val deleteResponse = client.admin().indices().delete(deleteRequest).actionGet()
            if (!deleteResponse.isAcknowledged) {
                logger.error("could not delete one or more ISM history index. $indexToDelete. Retrying one by one.")
                for (index in indexToDelete) {
                    try {
                        val singleDeleteRequest = DeleteIndexRequest(*indexToDelete.toTypedArray())
                        val singleDeleteResponse = client.admin().indices().delete(singleDeleteRequest).actionGet()
                        if (!singleDeleteResponse.isAcknowledged) {
                            logger.error("could not delete one or more ISM history index. $index.")
                        }
                    } catch (e: IndexNotFoundException) {
                        logger.debug("$index was already deleted. ${e.message}")
                    }
                }
            }
        }
    }

    @Suppress("NestedBlockDepth")
    suspend fun addManagedIndexMetaDataHistory(managedIndexMetaData: List<ManagedIndexMetaData>) {
        if (!historyEnabled) {
            logger.debug("Index State Management history is not enabled")
            return
        }

        if (!indexManagementIndices.checkAndUpdateHistoryIndex()) {
            logger.error("Failed to create or update the ism history index:")
            return // we can't continue to add the history documents below as it would potentially create dynamic mappings
        }

        val docWriteRequest: List<DocWriteRequest<*>> = managedIndexMetaData
            .filter { shouldAddManagedIndexMetaDataToHistory(it) }
            .map { createManagedIndexMetaDataHistoryIndexRequest(it) }

        if (docWriteRequest.isNotEmpty()) {
            val bulkRequest = BulkRequest().add(docWriteRequest)

            try {
                val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }

                for (bulkItemResponse in bulkResponse) {
                    if (bulkItemResponse.isFailed) {
                        logger.error("Failed to add history. Id: ${bulkItemResponse.id}, failureMessage: ${bulkItemResponse.failureMessage}")
                    }
                }
            } catch (e: Exception) {
                logger.error("failed to index indexMetaData History.", e)
            }
        }
    }

    private fun shouldAddManagedIndexMetaDataToHistory(managedIndexMetaData: ManagedIndexMetaData): Boolean {
        return when (managedIndexMetaData.stepMetaData?.stepStatus) {
            Step.StepStatus.STARTING -> false
            Step.StepStatus.CONDITION_NOT_MET -> false
            else -> true
        }
    }

    private fun createManagedIndexMetaDataHistoryIndexRequest(managedIndexMetaData: ManagedIndexMetaData): IndexRequest {
        val builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(IndexManagementPlugin.INDEX_STATE_MANAGEMENT_HISTORY_TYPE)
        managedIndexMetaData.toXContent(builder, ToXContent.EMPTY_PARAMS)
        builder
            .field("history_timestamp", Instant.now().toEpochMilli())
            .endObject()
            .endObject()
        return IndexRequest(IndexManagementIndices.HISTORY_WRITE_INDEX_ALIAS)
            .source(builder)
    }
}
