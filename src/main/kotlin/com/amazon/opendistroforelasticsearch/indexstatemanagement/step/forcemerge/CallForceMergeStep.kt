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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_WRITE
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.rest.RestStatus

class CallForceMergeStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: ForceMergeActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var wasReadOnly: Boolean = false
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    @Suppress("TooGenericExceptionCaught") // TODO see if we can refactor to catch GenericException in Runner.
    override suspend fun execute() {
        val indexName = managedIndexMetaData.index

        logger.info("Attempting to force merge on [$indexName]")

        // If index is already read_only, it is not necessary to set it here
        // The ManagedIndexMetaData will be updated to denote this so that the index is not set to read_write after a force merge completes
        if (isReadOnly(indexName)) {
            logger.info("Index [$indexName] is already read_only, moving on to force merge")
            wasReadOnly = true
        } else {
            logger.info("Attempting to set [$indexName] to read_only")
            val indexSetToReadOnly = setIndexToReadOnly(indexName)

            // If setIndexToReadOnly() returns false, updating settings failed and failed info was already updated. Can return early.
            if (!indexSetToReadOnly) return
        }

        executeForceMerge(indexName)
    }

    private suspend fun executeForceMerge(indexName: String) {
        try {
            val request = ForceMergeRequest(indexName).maxNumSegments(config.maxNumSegments)
            val response: ForceMergeResponse = client.admin().indices().suspendUntil { forceMerge(request, it) }

            // If response isAcknowledged then the force merge operation has started
            if (response.status == RestStatus.OK) {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("message" to "Started force merge")
            } else {
                // Otherwise the request to force merge encountered some problem
                stepStatus = StepStatus.FAILED
                info = mapOf(
                    "message" to "Failed to start force merge",
                    "status" to response.status,
                    "shard_failures" to response.shardFailures.map { it.toString() }
                )
            }
        } catch (e: Exception) {
            stepStatus = StepStatus.FAILED
            val mutableInfo = mutableMapOf("message" to "Failed to start force merge")
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo["cause"] = errorMessage
            info = mutableInfo.toMap()
        }
    }

    private suspend fun isReadOnly(indexName: String): Boolean {
        val getSettingsResponse: GetSettingsResponse = client.admin().indices()
            .suspendUntil { getSettings(GetSettingsRequest().indices(indexName), it) }
        val blocksWrite: String? = getSettingsResponse.getSetting(indexName, SETTING_BLOCKS_WRITE)

        // If the "index.blocks.write" setting is set to "true", the index is read_only
        if (blocksWrite != null && blocksWrite == "true") {
            return true
        }

        // Otherwise if "index.blocks.write" is null or "false", the index is not read_only
        return false
    }

    private suspend fun setIndexToReadOnly(indexName: String): Boolean {
        try {
            val updateSettingsRequest = UpdateSettingsRequest()
                .indices(indexName)
                .settings(
                    Settings.builder().put(SETTING_BLOCKS_WRITE, true)
                )
            val response: AcknowledgedResponse = client.admin().indices()
                .suspendUntil { updateSettings(updateSettingsRequest, it) }

            if (response.isAcknowledged) {
                logger.info("Successfully set [$indexName] to read_only for force_merge action")
                return true
            }

            // If response is not acknowledged, then add failed info
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to "Failed to set index to read_only")
            return false
        } catch (e: Exception) {
            stepStatus = StepStatus.FAILED
            val mutableInfo = mutableMapOf("message" to "Failed to set index to read_only")
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo["cause"] = errorMessage
            info = mutableInfo.toMap()
            return false
        }
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            wasReadOnly = wasReadOnly,
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            // TODO we should refactor such that transitionTo is not reset in the step.
            transitionTo = null,
            info = info
        )
    }

    companion object {
        const val name = "call_force_merge"
    }
}
