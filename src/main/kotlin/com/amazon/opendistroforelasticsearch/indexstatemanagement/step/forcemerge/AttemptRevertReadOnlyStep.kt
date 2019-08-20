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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.wasReadOnly
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings

class AttemptRevertReadOnlyStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: ForceMergeActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    @Suppress("TooGenericExceptionCaught")
    override suspend fun execute() {
        val indexName = managedIndexMetaData.index

        // If the index was read-only before the force merge, no action is necessary
        if (managedIndexMetaData.wasReadOnly) {
            logger.debug("The index [$indexName] was read-only prior to the force_merge action, no action necessary")
            info = mapOf("message" to "Index was read-only before force_merge, no changes made")
        } else {
            // Otherwise, set the index back to read-write
            val indexSetToReadWrite = setIndexToReadWrite(indexName)

            // If setIndexToReadWrite returns false, updating settings failed and failed info was already updated. Can return early.
            if (!indexSetToReadWrite) return

            info = mapOf("message" to "Set index to read-write")
        }

        stepStatus = StepStatus.COMPLETED
    }

    private suspend fun setIndexToReadWrite(indexName: String): Boolean {
        try {
            val updateSettingsRequest = UpdateSettingsRequest()
                .indices(indexName)
                .settings(
                    Settings.builder().put(IndexMetaData.SETTING_BLOCKS_WRITE, false)
                )
            val response: AcknowledgedResponse = client.admin().indices()
                .suspendUntil { updateSettings(updateSettingsRequest, it) }

            if (response.isAcknowledged) {
                logger.info("Successfully set [$indexName] back to read_write after force_merge")
                return true
            }

            // If response is not acknowledged, then add failed info
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to "Failed to set index back to read_write")
        } catch (e: Exception) {
            stepStatus = StepStatus.FAILED
            val mutableInfo = mutableMapOf("message" to "Failed to set index back to read_write")
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo["cause"] = errorMessage
            info = mutableInfo.toMap()
        }

        return false
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        val currentActionMetaData = currentMetaData.actionMetaData

        return currentMetaData.copy(
            // Resetting actionProperties since the force_merge action is over
            actionMetaData = currentActionMetaData?.copy(actionProperties = null),
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    companion object {
        const val name = "attempt_revert_read_only"
    }
}
