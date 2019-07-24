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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.step.rollover

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.getRolloverAlias
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.RolloverActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.RetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.evaluateConditions
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.rest.RestStatus
import java.time.Instant

@Suppress("ReturnCount", "TooGenericExceptionCaught")
class AttemptRolloverStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: RolloverActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step("attempt_rollover", managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var failed: Boolean = false
    private var stepCompleted: Boolean = false
    private var info: Map<String, Any>? = null

    // TODO: Incorporate retries from config and consumed retries from metadata
    override suspend fun execute() {
        // If we have already rolled over this index then fail as we only allow an index to be rolled over once
        if (managedIndexMetaData.rolledOver == true) {
            failed = true
            info = mapOf("message" to "This index has already been rolled over")
            return
        }

        val alias = getAliasOrUpdateInfo()
        // If alias is null we already updated failed info from getAliasOrUpdateInfo and can return early
        alias ?: return

        val statsResponse = getIndexStatsOrUpdateInfo()
        // If statsResponse is null we already updated failed info from getIndexStatsOrUpdateInfo and can return early
        statsResponse ?: return

        val indexCreationDate = Instant.ofEpochMilli(clusterService.state().metaData().index(managedIndexMetaData.index).creationDate)
        val numDocs = statsResponse.primaries.docs.count
        val indexSize = ByteSizeValue(statsResponse.primaries.docs.totalSizeInBytes)

        if (config.evaluateConditions(indexCreationDate, numDocs, indexSize)) {
            executeRollover(alias)
        } else {
            info = mapOf("message" to "Attempting to rollover")
        }
    }

    private suspend fun executeRollover(alias: String) {
        try {
            val request = RolloverRequest(alias, null)
            val response: RolloverResponse = client.admin().indices().suspendUntil { rolloverIndex(request, it) }

            // Do not need to check for isRolledOver as we are not passing any conditions or dryrun=true
            // which are the only two ways it comes back as false

            // If response isAcknowledged it means the index was created and alias was added to new index
            if (response.isAcknowledged) {
                stepCompleted = true
                info = mapOf("message" to "Rolled over index")
            } else {
                // If the alias update response is NOT acknowledged we will get back isAcknowledged=false
                // This means the new index was created but we failed to swap the alias
                failed = true
                info = mapOf("message" to "New index created (${response.newIndex}), but failed to update alias")
            }
        } catch (e: Exception) {
            failed = true
            val mutableInfo = mutableMapOf("message" to "Failed to rollover index")
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo.put("cause", errorMessage)
            info = mutableInfo.toMap()
        }
    }

    private fun getAliasOrUpdateInfo(): String? {
        val alias = clusterService.state().metaData().index(managedIndexMetaData.index).getRolloverAlias()

        if (alias == null) {
            failed = true
            info = mapOf("message" to "There is no valid rollover_alias=$alias set on ${managedIndexMetaData.index}")
        }

        return alias
    }

    private suspend fun getIndexStatsOrUpdateInfo(): IndicesStatsResponse? {
        try {
            val statsRequest = IndicesStatsRequest()
                    .indices(managedIndexMetaData.index).clear().docs(true)
            val statsResponse: IndicesStatsResponse = client.admin().indices().suspendUntil { stats(statsRequest, it) }

            if (statsResponse.status == RestStatus.OK) {
                return statsResponse
            }

            failed = true
            info = mapOf(
                "message" to "Failed to get index stats",
                "status" to statsResponse.status,
                "shard_failures" to statsResponse.shardFailures.map { it.toString() }
            )
            return null
        } catch (e: Exception) {
            failed = true
            val mutableInfo = mutableMapOf("message" to "Failed to get index stats")
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo.put("cause", errorMessage)
            info = mutableInfo.toMap()
            return null
        }
    }

    // TODO: retries, stepStartTime not resetting when same step
    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            // TODO only update stepStartTime when first try of step and not retries
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), !failed),
            // TODO we should refactor such that transitionTo is not reset in the step.
            transitionTo = null,
            // TODO properly attempt retry and update RetryInfo.
            retryInfo = if (currentMetaData.retryInfo != null) currentMetaData.retryInfo.copy(failed = failed) else RetryInfoMetaData(failed, 0),
            info = info
        )
    }
}
