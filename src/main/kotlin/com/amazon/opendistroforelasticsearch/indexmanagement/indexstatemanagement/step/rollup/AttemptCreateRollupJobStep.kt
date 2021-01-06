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
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionProperties
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index.IndexRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index.IndexRollupRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.start.StartRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.start.StartRollupRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.ISMRollup
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.index.engine.VersionConflictEngineException
import java.lang.Exception

class AttemptCreateRollupJobStep(
    val clusterService: ClusterService,
    val client: Client,
    val ismRollup: ISMRollup,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private var rollupId: String? = null
    private var previousRunRollupId: String? = null
    private var hasPreviousRollupAttemptFailed: Boolean? = null

    override fun isIdempotent() = true

    override suspend fun execute(): Step {
        previousRunRollupId = managedIndexMetaData.actionMetaData?.actionProperties?.rollupId
        hasPreviousRollupAttemptFailed = managedIndexMetaData.actionMetaData?.actionProperties?.hasRollupFailed

        // Creating a rollup job
        val rollup = ismRollup.toRollup(indexName)
        rollupId = rollup.id
        logger.info("Attempting to create a rollup job $rollupId for index $indexName")

        val indexRollupRequest = IndexRollupRequest(rollup, WriteRequest.RefreshPolicy.IMMEDIATE)

        try {
            withContext(Dispatchers.IO) {
                val response = client.execute(IndexRollupAction.INSTANCE, indexRollupRequest).actionGet()
                logger.info("Received status ${response.status.status} on trying to create rollup job $rollupId")
            }

            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to getSuccessMessage(rollupId!!, indexName))
        } catch (e: VersionConflictEngineException) {
            val message = getFailedJobExistsMessage(rollupId!!, indexName)
            logger.info(message)
            if (rollupId == previousRunRollupId && hasPreviousRollupAttemptFailed != null && hasPreviousRollupAttemptFailed!!) {
                withContext(Dispatchers.IO) {
                    startRollupJob()
                }
            } else {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("info" to message)
            }
        } catch (e: Exception) {
            val message = getFailedMessage(rollupId!!, indexName)
            logger.error(message, e)
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to message, "cause" to "${e.message}")
        }

        return this
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        val currentActionMetaData = currentMetaData.actionMetaData
        return currentMetaData.copy(
                actionMetaData = currentActionMetaData?.copy(actionProperties = ActionProperties(rollupId = rollupId)),
                stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
                transitionTo = null,
                info = info
        )
    }

    private fun startRollupJob() {
        logger.info("Attempting to re-start the job $rollupId")
        try {
            val startRollupRequest = StartRollupRequest(rollupId!!)
            client.execute(StartRollupAction.INSTANCE, startRollupRequest).actionGet()
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to getSuccessRestartMessage(rollupId!!))
        } catch (e: Exception) {
            val message = getFailedToStartMessage(rollupId!!)
            logger.error(message, e)
            stepStatus = StepStatus.CONDITION_NOT_MET
            info = mapOf("message" to message)
        }
    }

    companion object {
        const val name = "attempt_create_rollup"
        fun getFailedMessage(rollupId: String, index: String) = "Failed to create the rollup job [$rollupId] for index [$index]"
        fun getFailedJobExistsMessage(rollupId: String, index: String) = "Rollup job [$rollupId] already exists for index [$index]"
        fun getFailedToStartMessage(rollupId: String) = "Failed to start the rollup job [$rollupId]"
        fun getSuccessMessage(rollupId: String, index: String) = "Successfully created the rollup job [$rollupId] for index [$index]"
        fun getSuccessRestartMessage(rollupId: String) = "Successfully restarted the rollup job [$rollupId]"
    }
}
