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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.forcemerge

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.getUsefulCauseString
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ForceMergeActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionProperties
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
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
) : Step(name, managedIndexMetaData, false) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override fun isIdempotent() = true

    @Suppress("TooGenericExceptionCaught", "ReturnCount")
    override suspend fun execute(): WaitForForceMergeStep {
        // Retrieve maxNumSegments value from ActionProperties. If ActionProperties is null, update failed info and return early.
        val maxNumSegments = getMaxNumSegments() ?: return this

        // Get the number of shards with a segment count greater than maxNumSegments, meaning they are still merging
        val shardsStillMergingSegments = getShardsStillMergingSegments(indexName, maxNumSegments)
        // If shardsStillMergingSegments is null, failed info has already been updated and can return early
        shardsStillMergingSegments ?: return this

        // If there are no longer shardsStillMergingSegments, then the force merge has completed
        if (shardsStillMergingSegments == 0) {
            val message = getSuccessMessage(indexName)
            logger.info(message)
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to message)
        } else {
            /*
             * If there are still shards with segments merging then no action is taken and the step will be reevaluated
             * on the next run of the ManagedIndexRunner.
             *
             * However, if a given ActionTimeout or an internal timeout of 12 hours is reached, the force_merge action
             * will be marked as failed. The internal timeout is for cases where the force merge stops silently,
             * such as when shards relocate to different nodes during a force merge operation. Since ActionTimeout
             * is optional, if no timeout is given, the segment count would stop going down as merging would no longer
             * occur and the managed index would become stuck in this action.
             */
            val timeWaitingForForceMerge: Duration = Duration.between(getActionStartTime(), Instant.now())
            // Get ActionTimeout if given, otherwise use default timeout of 12 hours
            val timeoutInSeconds: Long = config.configTimeout?.timeout?.seconds ?: FORCE_MERGE_TIMEOUT_IN_SECONDS

            if (timeWaitingForForceMerge.toSeconds() > timeoutInSeconds) {
                logger.error("Force merge on [$indexName] timed out with" +
                    " [$shardsStillMergingSegments] shards containing unmerged segments")

                stepStatus = StepStatus.FAILED
                info = mapOf("message" to getFailedTimedOutMessage(indexName))
            } else {
                logger.debug("Force merge still running on [$indexName] with" +
                    " [$shardsStillMergingSegments] shards containing unmerged segments")

                stepStatus = StepStatus.CONDITION_NOT_MET
                info = mapOf("message" to getWaitingMessage(indexName))
            }
        }

        return this
    }

    private fun getMaxNumSegments(): Int? {
        val actionProperties = managedIndexMetaData.actionMetaData?.actionProperties

        if (actionProperties?.maxNumSegments == null) {
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to "Unable to retrieve [${ActionProperties.Properties.MAX_NUM_SEGMENTS.key}]" +
                    " from ActionProperties=$actionProperties")
            return null
        }

        return actionProperties.maxNumSegments
    }

    private suspend fun getShardsStillMergingSegments(indexName: String, maxNumSegments: Int): Int? {
        try {
            val statsRequest = IndicesStatsRequest().indices(indexName)
            val statsResponse: IndicesStatsResponse = client.admin().indices().suspendUntil { stats(statsRequest, it) }

            if (statsResponse.status == RestStatus.OK) {
                return statsResponse.shards.count {
                    val count = it.stats.segments?.count
                    if (count == null) {
                        logger.warn("$indexName wait for force merge had null segments")
                        false
                    } else {
                        count > maxNumSegments
                    }
                }
            }

            val message = getFailedSegmentCheckMessage(indexName)
            logger.warn("$message - ${statsResponse.status}")
            stepStatus = StepStatus.FAILED
            info = mapOf(
                "message" to message,
                "shard_failures" to statsResponse.shardFailures.map { it.getUsefulCauseString() }
            )
        } catch (e: Exception) {
            val message = getFailedSegmentCheckMessage(indexName)
            logger.error(message, e)
            stepStatus = StepStatus.FAILED
            val mutableInfo = mutableMapOf("message" to message)
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo["cause"] = errorMessage
            info = mutableInfo.toMap()
        }

        return null
    }

    private fun getActionStartTime(): Instant {
        if (managedIndexMetaData.actionMetaData?.startTime == null) {
            return Instant.now()
        }

        return Instant.ofEpochMilli(managedIndexMetaData.actionMetaData.startTime)
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        // if the step is completed set actionProperties back to null
        val currentActionMetaData = currentMetaData.actionMetaData
        val updatedActionMetaData = currentActionMetaData?.let {
            if (stepStatus != StepStatus.COMPLETED) it
            else currentActionMetaData.copy(actionProperties = null)
        }
        return currentMetaData.copy(
            actionMetaData = updatedActionMetaData,
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    companion object {
        const val name = "wait_for_force_merge"
        const val FORCE_MERGE_TIMEOUT_IN_SECONDS = 43200L // 12 hours
        fun getFailedTimedOutMessage(index: String) = "Force merge timed out [index=$index]"
        fun getFailedSegmentCheckMessage(index: String) = "Failed to check segments when waiting for force merge to complete [index=$index]"
        fun getWaitingMessage(index: String) = "Waiting for force merge to complete [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully confirmed segments force merged [index=$index]"
    }
}
