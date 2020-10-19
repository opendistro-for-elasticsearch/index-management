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
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.explain.ExplainRollupResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import java.lang.Exception

class WaitForRollupCompletionStep(
    val clusterService: ClusterService,
    val client: Client,
    val rollupJobId: String,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private val logger = LogManager.getLogger(javaClass)

    override fun isIdempotent() = true

    override suspend fun execute(): WaitForRollupCompletionStep {
        val explainRollupRequest = ExplainRollupRequest(listOf(rollupJobId))
        client.execute(ExplainRollupAction.INSTANCE, explainRollupRequest, object : ActionListener<ExplainRollupResponse> {
            override fun onFailure(e: Exception?) {
                processFailure(e)
            }

            override fun onResponse(response: ExplainRollupResponse?) {
                if (response?.getIdsToExplain()?.get(rollupJobId)?.metadata?.status == null) {
                    processMightNotHaveBeenProcessed()
                }
                processRollupMetadataStatus(response!!.getIdsToExplain().getValue(rollupJobId)!!.metadata!!)
            }
        })

        return this
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    // TODO: Need to finalize the display status we want to show for each case
    fun processRollupMetadataStatus(rollupMetadata: RollupMetadata) {
        when (rollupMetadata.status) {
            RollupMetadata.Status.INIT -> {
                stepStatus = StepStatus.STARTING
                info = mapOf("status" to RollupMetadata.Status.INIT.type)
            }
            RollupMetadata.Status.STARTED -> {
                stepStatus = StepStatus.STARTING
                info = mapOf("status" to RollupMetadata.Status.STARTED.type)
            }
            RollupMetadata.Status.FAILED -> {
                stepStatus = StepStatus.FAILED
                info = mapOf("status" to RollupMetadata.Status.FAILED.type)
            }
            RollupMetadata.Status.FINISHED -> {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("status" to RollupMetadata.Status.FINISHED.type)
            }
            RollupMetadata.Status.RETRY -> {
                stepStatus = StepStatus.STARTING
                info = mapOf("status" to RollupMetadata.Status.RETRY.type)
            }
            RollupMetadata.Status.STOPPED -> {
                stepStatus = StepStatus.CONDITION_NOT_MET
                info = mapOf("status" to RollupMetadata.Status.STOPPED.type)
            }
        }
    }

    fun processFailure(e: Exception?) {
        stepStatus = StepStatus.FAILED
        val message = getFailedMessage(rollupJobId, indexName)
        logger.error(message, e)
        val mutableInfo = mutableMapOf("message" to message)
        val errorMessage = e?.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    fun processMightNotHaveBeenProcessed() {
        stepStatus = StepStatus.CONDITION_NOT_MET
    }

    companion object {
        const val name = "wait_for_rollup_completion"
        fun getFailedMessage(rollupJob: String, index: String) = "Failed to get the status of rollup job [$rollupJob] on index [$index]"
    }
}
