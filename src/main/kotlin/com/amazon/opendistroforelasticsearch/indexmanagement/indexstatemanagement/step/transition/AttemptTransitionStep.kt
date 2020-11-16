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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.transition

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.getUsefulCauseString
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.TransitionsActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.evaluateConditions
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.hasStatsConditions
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.transport.RemoteTransportException
import java.time.Instant
import kotlin.Exception

/**
 * Attempt to transition to the next state
 *
 * This step compares the transition conditions configuration with the real time index stats data
 * to check if the [ManagedIndexConfig] should move to the next state defined in its policy.
 */
class AttemptTransitionStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: TransitionsActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step("attempt_transition", managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stateName: String? = null
    private var stepStatus = StepStatus.STARTING
    private var policyCompleted: Boolean = false
    private var info: Map<String, Any>? = null

    override fun isIdempotent() = true

    @Suppress("TooGenericExceptionCaught", "ReturnCount", "ComplexMethod")
    override suspend fun execute(): AttemptTransitionStep {
        try {
            if (config.transitions.isEmpty()) {
                logger.info("$indexName transitions are empty, completing policy")
                policyCompleted = true
                stepStatus = StepStatus.COMPLETED
                return this
            }

            val indexCreationDate = clusterService.state().metadata().index(indexName).creationDate
            val indexCreationDateInstant = Instant.ofEpochMilli(indexCreationDate)
            if (indexCreationDate == -1L) {
                logger.warn("$indexName had an indexCreationDate=-1L, cannot use for comparison")
            }
            val stepStartTime = getStepStartTime()
            var numDocs: Long? = null
            var indexSize: ByteSizeValue? = null

            if (config.transitions.any { it.hasStatsConditions() }) {
                val statsRequest = IndicesStatsRequest()
                    .indices(indexName).clear().docs(true)
                val statsResponse: IndicesStatsResponse = client.admin().indices().suspendUntil { stats(statsRequest, it) }

                if (statsResponse.status != RestStatus.OK) {
                    val message = getFailedStatsMessage(indexName)
                    logger.warn("$message - ${statsResponse.status}")
                    stepStatus = StepStatus.FAILED
                    info = mapOf(
                        "message" to message,
                        "shard_failures" to statsResponse.shardFailures.map { it.getUsefulCauseString() }
                    )
                    return this
                }
                numDocs = statsResponse.primaries.getDocs()?.count ?: 0
                indexSize = ByteSizeValue(statsResponse.primaries.getDocs()?.totalSizeInBytes ?: 0)
            }

            // Find the first transition that evaluates to true and get the state to transition to, otherwise return null if none are true
            stateName = config.transitions.find { it.evaluateConditions(indexCreationDateInstant, numDocs, indexSize, stepStartTime) }?.stateName
            val message: String
            val stateName = stateName // shadowed on purpose to prevent var from changing
            if (stateName != null) {
                logger.info("$indexName transition conditions evaluated to true [indexCreationDate=$indexCreationDate," +
                        " numDocs=$numDocs, indexSize=${indexSize?.bytes},stepStartTime=${stepStartTime.toEpochMilli()}]")
                stepStatus = StepStatus.COMPLETED
                message = getSuccessMessage(indexName, stateName)
            } else {
                stepStatus = StepStatus.CONDITION_NOT_MET
                message = getEvaluatingMessage(indexName)
            }
            info = mapOf("message" to message)
        } catch (e: RemoteTransportException) {
            handleException(ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: Exception) {
            handleException(e)
        }

        return this
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
            policyCompleted = policyCompleted,
            transitionTo = stateName,
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            info = info
        )
    }

    companion object {
        fun getFailedMessage(index: String) = "Failed to transition index [index=$index]"
        fun getFailedStatsMessage(index: String) = "Failed to get stats information for the index [index=$index]"
        fun getEvaluatingMessage(index: String) = "Evaluating transition conditions [index=$index]"
        fun getSuccessMessage(index: String, state: String) = "Transitioning to $state [index=$index]"
    }
}
