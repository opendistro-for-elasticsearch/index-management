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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.step.readwrite

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ReadWriteActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings

class SetReadWriteStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: ReadWriteActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step("set_read_write", managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var failed: Boolean = false
    private var info: Map<String, Any>? = null

    // TODO: Incorporate retries from config and consumed retries from metadata
    override suspend fun execute() {
        try {
            val updateSettingsRequest = UpdateSettingsRequest()
                .indices(managedIndexMetaData.index)
                .settings(
                    Settings.builder().put("index.blocks.write", false)
                )
            val response: AcknowledgedResponse = client.admin().indices()
                .suspendUntil { updateSettings(updateSettingsRequest, it) }

            if (response.isAcknowledged) {
                info = mapOf("message" to "Set index to read-write")
            } else {
                failed = true
                info = mapOf("message" to "Failed to set index to read-write")
            }
        } catch (e: Exception) {
            failed = true
            val mutableInfo = mutableMapOf("message" to "Failed to set index to read-write")
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo["cause"] = errorMessage
            info = mutableInfo.toMap()
        }
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            // TODO only update stepStartTime when first try of step and not retries
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), !failed),
            // TODO we should refactor such that transitionTo is not reset in the step.
            transitionTo = null,
            info = info
        )
    }
}
