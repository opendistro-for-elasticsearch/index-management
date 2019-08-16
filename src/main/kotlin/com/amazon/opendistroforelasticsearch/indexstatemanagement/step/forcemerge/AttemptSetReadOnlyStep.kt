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
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings

class AttemptSetReadOnlyStep(
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

        // If isReadOnly returns null, the GetSettings request failed and failed info was already updated, can return early
        val indexIsReadOnly = isReadOnly(indexName) ?: return

        // If index is already read-only, it is not necessary to set it here
        // The ManagedIndexMetaData will be updated to denote this so that the index is not set to read-write after a force merge completes
        if (indexIsReadOnly) {
            logger.info("Index [$indexName] is already read-only, moving on to force merge")
            wasReadOnly = true
        } else {
            logger.info("Attempting to set [$indexName] to read-only for force_merge action")
            val indexSetToReadOnly = setIndexToReadOnly(indexName)

            // If setIndexToReadOnly returns false, updating settings failed and failed info was already updated, can return early
            if (!indexSetToReadOnly) return
        }

        // Complete step since index is read-only
        stepStatus = StepStatus.COMPLETED
        info = mapOf("message" to "Set index to read-only")
    }

    private suspend fun isReadOnly(indexName: String): Boolean? {
        try {
            val getSettingsResponse: GetSettingsResponse = client.admin().indices()
                .suspendUntil { getSettings(GetSettingsRequest().indices(indexName), it) }
            val blocksWrite: String? = getSettingsResponse.getSetting(indexName, IndexMetaData.SETTING_BLOCKS_WRITE)

            // If the "index.blocks.write" setting is set to "true", the index is read-only
            if (blocksWrite != null && blocksWrite == "true") {
                return true
            }

            // Otherwise if "index.blocks.write" is null or "false", the index is not read-only
            return false
        } catch (e: Exception) {
            stepStatus = StepStatus.FAILED
            val mutableInfo = mutableMapOf("message" to "Failed to get index settings")
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo["cause"] = errorMessage
            info = mutableInfo.toMap()
            return null
        }
    }

    private suspend fun setIndexToReadOnly(indexName: String): Boolean {
        try {
            val updateSettingsRequest = UpdateSettingsRequest()
                .indices(indexName)
                .settings(
                    Settings.builder().put(IndexMetaData.SETTING_BLOCKS_WRITE, true)
                )
            val response: AcknowledgedResponse = client.admin().indices()
                .suspendUntil { updateSettings(updateSettingsRequest, it) }

            if (response.isAcknowledged) {
                logger.info("Successfully set [$indexName] to read-only for force_merge action")
                return true
            }

            // If response is not acknowledged, then add failed info
            logger.error("Request to set [$indexName] to read-only was NOT acknowledged")
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to "Failed to set index to read-only")
        } catch (e: Exception) {
            stepStatus = StepStatus.FAILED
            val mutableInfo = mutableMapOf("message" to "Failed to set index to read-only")
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo["cause"] = errorMessage
            info = mutableInfo.toMap()
        }

        return false
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        val currentActionMetaData = currentMetaData.actionMetaData

        return currentMetaData.copy(
            actionMetaData = currentActionMetaData?.copy(actionProperties = ActionProperties(wasReadOnly = wasReadOnly)),
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            // TODO we should refactor such that transitionTo is not reset in the step.
            transitionTo = null,
            info = info
        )
    }

    companion object {
        const val name = "attempt_set_read_only"
    }
}
