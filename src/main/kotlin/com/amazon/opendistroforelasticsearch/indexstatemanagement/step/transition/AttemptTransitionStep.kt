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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.RetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.evaluateConditions
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.rest.RestStatus
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
    // TODO: Update ManagedIndexMetaData start times to be Long, move to extension function on MetaData
    private var stateName: String? = null
    private var failed: Boolean = false
    private var info: Map<String, Any>? = null

    override suspend fun execute() {
        val statsRequest = IndicesStatsRequest()
                .indices(managedIndexMetaData.index).clear().docs(true)
        val statsResponse: IndicesStatsResponse = client.admin().indices().suspendUntil { stats(statsRequest, it) }

        if (statsResponse.status != RestStatus.OK) {
            failed = true
            info = mapOf(
                "message" to "Failed to get index stats",
                "status" to statsResponse.status,
                "shard_failures" to statsResponse.shardFailures.map { it.toString() }
            )
            return
        }

        val indexCreationDate = Instant.ofEpochMilli(clusterService.state().metaData().index(managedIndexMetaData.index).creationDate)
        val numDocs = statsResponse.primaries.docs.count
        val indexSize = ByteSizeValue(statsResponse.primaries.docs.totalSizeInBytes)

        // Find the first transition that evaluates to true and get the state to transition to, otherwise return null if none are true
        stateName = config.transitions.find { it.evaluateConditions(indexCreationDate, numDocs, indexSize, getStepStartTime()) }?.stateName
        // TODO: Style of message, past tense, present tense, etc.
        val message = if (stateName == null) "Attempting to transition" else "Transitioning to $stateName"
        info = mapOf("message" to message)
    }

    // TODO: MetaData needs to be updated with correct step information
    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            transitionTo = stateName,
            // TODO only update stepStartTime when first try of step and not retries
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stateName != null),
            // TODO properly attempt retry and update RetryInfo.
            retryInfo = if (currentMetaData.retryInfo != null) currentMetaData.retryInfo.copy(failed = failed) else RetryInfoMetaData(failed, 0),
            info = info
        )
    }
}
