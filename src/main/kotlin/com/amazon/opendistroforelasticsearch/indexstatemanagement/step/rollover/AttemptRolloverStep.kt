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
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    @Suppress("TooGenericExceptionCaught")
    override suspend fun execute() {
        // If we have already rolled over this index then fail as we only allow an index to be rolled over once
        if (managedIndexMetaData.rolledOver == true) {
            stepStatus = StepStatus.FAILED
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
            stepStatus = StepStatus.CONDITION_NOT_MET
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
                stepStatus = StepStatus.COMPLETED
                info = mapOf("message" to "Rolled over index")
            } else {
                // If the alias update response is NOT acknowledged we will get back isAcknowledged=false
                // This means the new index was created but we failed to swap the alias
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to "New index created (${response.newIndex}), but failed to update alias")
            }
        } catch (e: Exception) {
            stepStatus = StepStatus.FAILED
            val mutableInfo = mutableMapOf("message" to "Failed to rollover index")
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo.put("cause", errorMessage)
            info = mutableInfo.toMap()
        }
    }

    private fun getAliasOrUpdateInfo(): String? {
        val alias = clusterService.state().metaData().index(managedIndexMetaData.index).getRolloverAlias()

        if (alias == null) {
            stepStatus = StepStatus.FAILED
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

            logger.debug(
                "Failed to get index stats for index: [${managedIndexMetaData.index}], status response: [${statsResponse.status}]"
            )

            stepStatus = StepStatus.FAILED
            info = mapOf(
                "message" to "Failed to evaluate conditions for rollover",
                "shard_failures" to statsResponse.shardFailures.map { it.toString() }
            )
        } catch (e: Exception) {
            stepStatus = StepStatus.FAILED
            val mutableInfo = mutableMapOf("message" to "Failed to evaluate conditions for rollover")
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo["cause"] = errorMessage
            info = mutableInfo.toMap()
        }

        return null
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }
}
