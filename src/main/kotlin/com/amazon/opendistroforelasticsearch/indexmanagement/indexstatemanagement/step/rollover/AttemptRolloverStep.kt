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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.rollover

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getRolloverAlias
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.getUsefulCauseString
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.RolloverActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.evaluateConditions
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
    override suspend fun execute(): AttemptRolloverStep {
        // If we have already rolled over this index then fail as we only allow an index to be rolled over once
        if (managedIndexMetaData.rolledOver == true) {
            logger.warn("$indexName was already rolled over, cannot execute rollover step")
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to getFailedDuplicateRolloverMessage(indexName))
            return this
        }

        val alias = getAliasOrUpdateInfo()
        // If alias is null we already updated failed info from getAliasOrUpdateInfo and can return early
        alias ?: return this

        val statsResponse = getIndexStatsOrUpdateInfo()
        // If statsResponse is null we already updated failed info from getIndexStatsOrUpdateInfo and can return early
        statsResponse ?: return this

        val indexCreationDate = clusterService.state().metadata().index(indexName).creationDate
        val indexAgeTimeValue = if (indexCreationDate == -1L) {
            logger.warn("$indexName had an indexCreationDate=-1L, cannot use for comparison")
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
            logger.info("$indexName rollover conditions evaluated to true [indexCreationDate=$indexCreationDate," +
                    " numDocs=$numDocs, indexSize=${indexSize.bytes}]")
            executeRollover(alias, conditions)
        } else {
            stepStatus = StepStatus.CONDITION_NOT_MET
            info = mapOf("message" to getAttemptingMessage(indexName), "conditions" to conditions)
        }

        return this
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
                    "message" to getSuccessMessage(indexName),
                    if (conditions.isEmpty()) null else "conditions" to conditions // don't show empty conditions object if no conditions specified
                ).toMap()
            } else {
                // If the alias update response is NOT acknowledged we will get back isAcknowledged=false
                // This means the new index was created but we failed to swap the alias
                val message = getFailedAliasUpdateMessage(indexName, response.newIndex)
                logger.warn(message)
                stepStatus = StepStatus.FAILED
                info = listOfNotNull(
                    "message" to message,
                    if (conditions.isEmpty()) null else "conditions" to conditions // don't show empty conditions object if no conditions specified
                ).toMap()
            }
        } catch (e: Exception) {
            handleException(e)
        }
    }

    private fun getAliasOrUpdateInfo(): String? {
        val alias = clusterService.state().metadata().index(indexName).getRolloverAlias()

        if (alias == null) {
            val message = getFailedNoValidAliasMessage(indexName)
            logger.warn(message)
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to message)
        }

        return alias
    }

    private suspend fun getIndexStatsOrUpdateInfo(): IndicesStatsResponse? {
        try {
            val statsRequest = IndicesStatsRequest()
                    .indices(indexName).clear().docs(true)
            val statsResponse: IndicesStatsResponse = client.admin().indices().suspendUntil { stats(statsRequest, it) }

            if (statsResponse.status == RestStatus.OK) {
                return statsResponse
            }

            val message = getFailedEvaluateMessage(indexName)
            logger.warn("$message - ${statsResponse.status}")
            stepStatus = StepStatus.FAILED
            info = mapOf(
                "message" to message,
                "shard_failures" to statsResponse.shardFailures.map { it.getUsefulCauseString() }
            )
        } catch (e: Exception) {
            val message = getFailedEvaluateMessage(indexName)
            logger.error(message, e)
            stepStatus = StepStatus.FAILED
            val mutableInfo = mutableMapOf("message" to message)
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo["cause"] = errorMessage
            info = mutableInfo.toMap()
        }

        return null
    }

    private fun handleException(e: Exception) {
        val message = getFailedMessage(indexName)
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        val mutableInfo = mutableMapOf("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            rolledOver = if (currentMetaData.rolledOver == true) true else stepStatus == StepStatus.COMPLETED,
            transitionTo = null,
            info = info
        )
    }

    companion object {
        fun getFailedMessage(index: String) = "Failed to rollover index [index=$index]"
        fun getFailedAliasUpdateMessage(index: String, newIndex: String) =
            "New index created, but failed to update alias [index=$index, newIndex=$newIndex]"
        fun getFailedNoValidAliasMessage(index: String) = "Missing rollover_alias index setting [index=$index]"
        fun getFailedDuplicateRolloverMessage(index: String) = "Index has already been rolled over [index=$index]"
        fun getFailedEvaluateMessage(index: String) = "Failed to evaluate conditions for rollover [index=$index]"
        fun getAttemptingMessage(index: String) = "Attempting to rollover index [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully rolled over index [index=$index]"
    }
}
