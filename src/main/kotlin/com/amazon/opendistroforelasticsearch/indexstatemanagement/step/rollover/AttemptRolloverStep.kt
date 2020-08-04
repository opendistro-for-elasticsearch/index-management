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
import org.elasticsearch.common.unit.TimeValue
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

    override fun isIdempotent() = false

    @Suppress("TooGenericExceptionCaught")
    override suspend fun execute() {
        val index = managedIndexMetaData.index
        // If we have already rolled over this index then fail as we only allow an index to be rolled over once
        if (managedIndexMetaData.rolledOver == true) {
            logger.warn("$index was already rolled over, cannot execute rollover step")
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

        val indexCreationDate = clusterService.state().metaData().index(index).creationDate
        val indexAgeTimeValue = if (indexCreationDate == -1L) {
            logger.warn("$index had an indexCreationDate=-1L, cannot use for comparison")
            // since we cannot use for comparison, we can set it to 0 as minAge will never be <= 0
            TimeValue.timeValueMillis(0)
        } else {
            TimeValue.timeValueMillis(Instant.now().toEpochMilli() - indexCreationDate)
        }
        val numDocs = statsResponse.primaries.docs?.count ?: 0
        val indexSize = ByteSizeValue(statsResponse.primaries.docs?.totalSizeInBytes ?: 0)
        val conditions = listOfNotNull(
                config.minAge?.let {
                    RolloverActionConfig.MIN_INDEX_AGE_FIELD to mapOf(
                            "condition" to it.toString(),
                            "current" to indexAgeTimeValue.toString(),
                            "creationDate" to indexCreationDate
                    )
                },
                config.minDocs?.let {
                    RolloverActionConfig.MIN_DOC_COUNT_FIELD to mapOf(
                            "condition" to it,
                            "current" to numDocs
                    )
                },
                config.minSize?.let {
                    RolloverActionConfig.MIN_SIZE_FIELD to mapOf(
                            "condition" to it.toString(),
                            "current" to indexSize.toString()
                    )
                }
        ).toMap()

        if (config.evaluateConditions(indexAgeTimeValue, numDocs, indexSize)) {
            logger.info("$index rollover conditions evaluated to true [indexCreationDate=$indexCreationDate," +
                    " numDocs=$numDocs, indexSize=${indexSize.bytes}]")
            executeRollover(alias, conditions)
        } else {
            stepStatus = StepStatus.CONDITION_NOT_MET
            info = mapOf("message" to "Attempting to rollover", "conditions" to conditions)
        }
    }

    @Suppress("ComplexMethod")
    private suspend fun executeRollover(alias: String, conditions: Map<String, Map<String, Any?>>) {
        try {
            val request = RolloverRequest(alias, null)
            val response: RolloverResponse = client.admin().indices().suspendUntil { rolloverIndex(request, it) }

            // Do not need to check for isRolledOver as we are not passing any conditions or dryrun=true
            // which are the only two ways it comes back as false

            // If response isAcknowledged it means the index was created and alias was added to new index
            if (response.isAcknowledged) {
                stepStatus = StepStatus.COMPLETED
                info = listOfNotNull(
                    "message" to "Rolled over index",
                    if (conditions.isEmpty()) null else "conditions" to conditions // don't show empty conditions object if no conditions specified
                ).toMap()
            } else {
                // If the alias update response is NOT acknowledged we will get back isAcknowledged=false
                // This means the new index was created but we failed to swap the alias
                stepStatus = StepStatus.FAILED
                info = listOfNotNull(
                    "message" to "New index created (${response.newIndex}), but failed to update alias",
                    if (conditions.isEmpty()) null else "conditions" to conditions // don't show empty conditions object if no conditions specified
                ).toMap()
            }
        } catch (e: Exception) {
            logger.error("Failed to rollover index [index=${managedIndexMetaData.index}]", e)
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
            logger.error("Failed to evaluate conditions for rollover [index=${managedIndexMetaData.index}]", e)
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
            rolledOver = if (currentMetaData.rolledOver == true) true else stepStatus == StepStatus.COMPLETED,
            transitionTo = null,
            info = info
        )
    }
}
