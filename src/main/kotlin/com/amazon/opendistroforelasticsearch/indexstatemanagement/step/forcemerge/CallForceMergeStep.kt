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
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.common.settings.Settings

class CallForceMergeStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: ForceMergeActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var wasReadOnly: Boolean = false
    private var failed: Boolean = false
    private var stepCompleted: Boolean = false
    private var info: Map<String, Any>? = null

    // TODO: Incorporate retries from config and consumed retries from metadata
    @Suppress("TooGenericExceptionCaught") // TODO see if we can refactor to catch GenericException in Runner.
    override suspend fun execute() {
        // TODO: Add wasReadOnly to ManagedIndexMetaData and check before setting to read_only
    }

    // TODO: retries, stepStartTime not resetting when same step
    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            // TODO only update stepStartTime when first try of step and not retries
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), !failed),
            // TODO add wasReadOnly in ManagedIndexMetaData
            // TODO we should refactor such that transitionTo is not reset in the step.
            transitionTo = null,
            info = info
        )
    }

    private suspend fun isReadOnly(indexName: String): Boolean {
        val getSettingsResponse: GetSettingsResponse = client.admin().indices()
            .suspendUntil { getSettings(GetSettingsRequest().indices(indexName), it) }
        val blocksWrite: String? = getSettingsResponse.getSetting(indexName, INDEX_BLOCKS_WRITE)

        // If the "index.blocks.write" setting is set to "true", the index is read_only
        if (blocksWrite != null && blocksWrite == "true") {
            return true
        }

        // Otherwise if "index.blocks.write" is null or "false", the index is not read_only
        return false
    }

    private suspend fun setIndexToReadOnly(indexName: String) {
        // TODO: Add try/catch and logger.error() if request !isAcknowledged

        val updateSettingsRequest = UpdateSettingsRequest()
            .indices(indexName)
            .settings(
                Settings.builder().put(INDEX_BLOCKS_WRITE, true)
            )
        val response: AcknowledgedResponse = client.admin().indices()
            .suspendUntil { updateSettings(updateSettingsRequest, it) }
    }

    companion object {
        const val name = "call_force_merge"

        const val INDEX_BLOCKS_WRITE = "index.blocks.write"
    }
}
