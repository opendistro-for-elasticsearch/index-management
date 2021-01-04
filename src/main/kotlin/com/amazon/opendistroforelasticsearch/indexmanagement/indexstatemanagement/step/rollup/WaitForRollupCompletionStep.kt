/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.rollup

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.explain.ExplainRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.explain.ExplainRollupRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import java.lang.Exception

class WaitForRollupCompletionStep(
    val clusterService: ClusterService,
    val client: Client,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private val logger = LogManager.getLogger(javaClass)
    private var rollupJobId: String? = null

    override fun isIdempotent() = true

    override suspend fun execute(): WaitForRollupCompletionStep {
        rollupJobId = managedIndexMetaData.actionMetaData?.actionProperties?.rollupId

        if (rollupJobId == null) {
            logger.error("There is no rollupJobId")
            processMightNotHaveBeenProcessed()
        } else {
            val explainRollupRequest = ExplainRollupRequest(listOf(rollupJobId!!))
            try {
                withContext(Dispatchers.IO) {
                    val response = client.execute(ExplainRollupAction.INSTANCE, explainRollupRequest).actionGet()
                    logger.info("Received the response ${response.getIdsToExplain().keys}")

                    if (response.getIdsToExplain()[rollupJobId!!]?.metadata?.status == null) {
                        logger.warn("Job $rollupJobId has not started yet")
                        stepStatus = StepStatus.CONDITION_NOT_MET
                        info = mapOf("message" to RollupMetadata.Status.INIT.type)
                    } else {
                        logger.info("in else block")
                        processRollupMetadataStatus(response.getIdsToExplain().getValue(rollupJobId!!)!!.metadata!!)
                    }
                }
            } catch (e: Exception) {
                processFailure(e)
            }
        }

        return this
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        val currentActionMetaData = currentMetaData.actionMetaData
        val currentActionProperties = currentActionMetaData?.actionProperties
        return currentMetaData.copy(
                actionMetaData = currentActionMetaData?.copy(actionProperties = currentActionProperties?.copy(
                        hasRollupFailed = stepStatus == StepStatus.FAILED)),
                stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
                transitionTo = null,
                info = info
        )
    }

    private fun processRollupMetadataStatus(rollupMetadata: RollupMetadata) {
        when (rollupMetadata.status) {
            RollupMetadata.Status.INIT -> {
                stepStatus = StepStatus.CONDITION_NOT_MET
                info = mapOf("message" to getJobProcessingMessage(rollupJobId!!))
            }
            RollupMetadata.Status.STARTED -> {
                stepStatus = StepStatus.CONDITION_NOT_MET
                info = mapOf("message" to getJobProcessingMessage(rollupJobId!!))
            }
            RollupMetadata.Status.FAILED -> {
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to getJobFailedMessage(rollupJobId!!))
            }
            RollupMetadata.Status.FINISHED -> {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("message" to getJobCompletionMessage(rollupJobId!!))
            }
            RollupMetadata.Status.RETRY -> {
                stepStatus = StepStatus.CONDITION_NOT_MET
                info = mapOf("message" to getJobProcessingMessage(rollupJobId!!))
            }
            RollupMetadata.Status.STOPPED -> {
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to getJobFailedMessage(rollupJobId!!))
            }
        }
    }

    private fun processFailure(e: Exception) {
        stepStatus = StepStatus.FAILED
        val message = getFailedMessage(rollupJobId!!, indexName)
        logger.error(message, e)
        val mutableInfo = mutableMapOf("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    fun processMightNotHaveBeenProcessed() {
        stepStatus = StepStatus.STARTING
    }

    companion object {
        const val name = "wait_for_rollup_completion"
        fun getFailedMessage(rollupJob: String, index: String) = "Failed to get the status of rollup job [$rollupJob] on index [$index]"
        fun getJobProcessingMessage(rollupJob: String) = "Rollup job [$rollupJob] is still processing"
        fun getJobCompletionMessage(rollupJob: String) = "Rollup job [$rollupJob] completed"
        fun getJobFailedMessage(rollupJob: String) = "Rollup job [$rollupJob] stopped"
    }
}
