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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.step.transition

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.TransitionsActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.evaluateConditions
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.hasStatsConditions
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.rest.RestStatus
import java.lang.Exception
import java.time.Instant

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

    @Suppress("TooGenericExceptionCaught")
    override suspend fun execute() {
        try {
            if (config.transitions.isEmpty()) {
                policyCompleted = true
                stepStatus = StepStatus.COMPLETED
                return
            }

            var numDocs: Long? = null
            var indexSize: ByteSizeValue? = null

            if (config.transitions.any { it.hasStatsConditions() }) {
                val statsRequest = IndicesStatsRequest()
                    .indices(managedIndexMetaData.index).clear().docs(true)
                val statsResponse: IndicesStatsResponse = client.admin().indices().suspendUntil { stats(statsRequest, it) }

                if (statsResponse.status != RestStatus.OK) {
                    logger.debug(
                        "Failed to get index stats for index: [${managedIndexMetaData.index}], status response: [${statsResponse.status}]"
                    )

                    stepStatus = StepStatus.FAILED
                    info = mapOf(
                        "message" to "Failed to evaluate conditions for transition",
                        "shard_failures" to statsResponse.shardFailures.map { it.toString() }
                    )
                    return
                }

                numDocs = statsResponse.primaries.docs!!.count
                indexSize = ByteSizeValue(statsResponse.primaries.docs!!.totalSizeInBytes)
                // Find the first transition that evaluates to true and get the state to transition to, otherwise return null if none are true
            }

            // Find the first transition that evaluates to true and get the state to transition to, otherwise return null if none are true
            stateName = config.transitions.find { it.evaluateConditions(getIndexCreationDate(), numDocs, indexSize, getStepStartTime()) }?.stateName
            val message = if (stateName == null) {
                stepStatus = StepStatus.CONDITION_NOT_MET
                "Attempting to transition"
            } else {
                stepStatus = StepStatus.COMPLETED
                "Transitioning to $stateName"
            }
            info = mapOf("message" to message)
        } catch (e: Exception) {
            logger.error("Failed to transition index [index=${managedIndexMetaData.index}]", e)
            stepStatus = StepStatus.FAILED
            val mutableInfo = mutableMapOf("message" to "Failed to transition index")
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo["cause"] = errorMessage
            info = mutableInfo.toMap()
        }
    }

    private fun getIndexCreationDate(): Instant =
        Instant.ofEpochMilli(clusterService.state().metaData().index(managedIndexMetaData.index).creationDate)

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            policyCompleted = policyCompleted,
            transitionTo = stateName,
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            info = info
        )
    }
}
