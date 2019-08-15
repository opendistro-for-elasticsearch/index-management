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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.step.forcemerge

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ForceMergeActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.ActionProperties
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse
import org.elasticsearch.rest.RestStatus
import java.time.Duration
import java.time.Instant

class WaitForForceMergeStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: ForceMergeActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    @Suppress("TooGenericExceptionCaught") // TODO see if we can refactor to catch GenericException in Runner.
    override suspend fun execute() {
        val indexName = managedIndexMetaData.index

        logger.info("Waiting for force merge to complete on [$indexName]")

        // Retrieve maxNumSegments value from ActionProperties. If ActionProperties is null, update failed info and return early.
        val maxNumSegments = getMaxNumSegments() ?: return

        // Get the number of shards with a segment count greater than maxNumSegments, meaning they are still merging
        val shardsStillMergingSegments = getShardsStillMergingSegments(indexName, maxNumSegments)
        // If shardsStillMergingSegments is null, failed info has already been updated and can return early
        shardsStillMergingSegments ?: return

        // If there are no longer shardsStillMergingSegments, then the force merge has completed
        if (shardsStillMergingSegments == 0) {
            logger.info("Force merge completed on [$indexName]")

            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to "Force merge completed")
        } else {
            /*
             * If there are still shards with segments merging then no action is taken and the step will be reevaluated
             * on the next run of the ManagedIndexRunner.
             *
             * However, if an internal timeout of 12 hours is reached, the force_merge action will be marked as failed.
             * This is for cases where the force merge stops silently, such as when nodes relocate during a force
             * merge operation. Without a timeout, the segment count would stop going down as merging would no longer
             * occur and the managed index would become stuck in this action.
             */
            val timeWaitingForForceMerge: Duration = Duration.between(getStepStartTime(), Instant.now())
            if (timeWaitingForForceMerge.toSeconds() > FORCE_MERGE_TIMEOUT_IN_SECONDS) {
                logger.error(
                    "Force merge on [$indexName] timed out with [$shardsStillMergingSegments] shards containing unmerged segments"
                )

                stepStatus = StepStatus.FAILED
                info = mapOf("message" to "Force merge timed out")
            } else {
                logger.debug(
                    "Force merge still running on [$indexName] with [$shardsStillMergingSegments] shards containing unmerged segments"
                )

                stepStatus = StepStatus.CONDITION_NOT_MET
                info = mapOf("message" to "Waiting for force merge to complete")
            }
        }
    }

    private fun getMaxNumSegments(): Int? {
        val actionProperties = managedIndexMetaData.actionMetaData?.actionProperties

        return if (actionProperties != null) {
            actionProperties.getInt(ActionProperties.MAX_NUM_SEGMENTS)
        } else {
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to "Unable to retrieve [${ActionProperties.MAX_NUM_SEGMENTS}] from ActionProperties=$actionProperties")
            null
        }
    }

    private suspend fun getShardsStillMergingSegments(indexName: String, maxNumSegments: Int): Int? {
        try {
            val statsRequest = IndicesStatsRequest().indices(indexName)
            val statsResponse: IndicesStatsResponse = client.admin().indices().suspendUntil { stats(statsRequest, it) }

            if (statsResponse.status == RestStatus.OK) {
                return statsResponse.shards.count { it.stats.segments.count > maxNumSegments }
            }

            stepStatus = StepStatus.FAILED
            info = mapOf(
                "message" to "Failed to get index stats",
                "status" to statsResponse.status,
                "shard_failures" to statsResponse.shardFailures.map { it.toString() }
            )
        } catch (e: Exception) {
            stepStatus = StepStatus.FAILED
            val mutableInfo = mutableMapOf("message" to "Failed to get index stats")
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo["cause"] = errorMessage
            info = mutableInfo.toMap()
        }

        return null
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            // TODO we should refactor such that transitionTo is not reset in the step.
            transitionTo = null,
            info = info
        )
    }

    companion object {
        const val name = "wait_for_force_merge"

        const val FORCE_MERGE_TIMEOUT_IN_SECONDS = 43200L // 12 hours
    }
}
