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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.allocation

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.AllocationActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings

class AttemptAllocationStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: AllocationActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step("attempt_allocation", managedIndexMetaData) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override fun isIdempotent() = true

    override suspend fun execute(): AttemptAllocationStep {
        try {
            val response: AcknowledgedResponse = client.admin()
                .indices()
                .suspendUntil { updateSettings(UpdateSettingsRequest(buildSettings(), managedIndexMetaData.index), it) }
            handleResponse(response)
        } catch (e: Exception) {
            handleException(e)
        }

        return this
    }

    private fun buildSettings(): Settings {
        val builder = Settings.builder()
        config.require.forEach { (key, value) -> builder.put(SETTINGS_PREFIX + AllocationActionConfig.REQUIRE + "." + key, value) }
        config.include.forEach { (key, value) -> builder.put(SETTINGS_PREFIX + AllocationActionConfig.INCLUDE + "." + key, value) }
        config.exclude.forEach { (key, value) -> builder.put(SETTINGS_PREFIX + AllocationActionConfig.EXCLUDE + "." + key, value) }
        return builder.build()
    }

    private fun handleException(e: Exception) {
        val message = getFailedMessage(indexName)
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        val mutableInfo = mutableMapOf("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    private fun handleResponse(response: AcknowledgedResponse) {
        if (response.isAcknowledged) {
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to getSuccessMessage(indexName))
        } else {
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to getFailedMessage(indexName))
        }
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    companion object {
        private const val SETTINGS_PREFIX = "index.routing.allocation."
        fun getFailedMessage(index: String) = "Failed to update allocation setting [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully updated allocation setting [index=$index]"
    }
}
