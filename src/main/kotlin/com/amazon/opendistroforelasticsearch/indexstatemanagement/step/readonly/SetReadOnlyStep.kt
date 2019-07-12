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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.step.readonly

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ReadOnlyActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings

class SetReadOnlyStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: ReadOnlyActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step("set_read_only", managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var failed: Boolean = false
    private var info: Map<String, Any>? = null

    // TODO: Incorporate retries from config and consumed retries from metadata
    override suspend fun execute() {
        try {
            val updateSettingsRequest = UpdateSettingsRequest()
                .indices(managedIndexMetaData.index)
                .settings(
                    Settings.builder().put("index.blocks.write", true)
                )
            val response: AcknowledgedResponse = client.admin().indices()
                .suspendUntil { updateSettings(updateSettingsRequest, it) }

            if (response.isAcknowledged) {
                info = mapOf("message" to "Set index to read-only")
            } else {
                failed = true
                info = mapOf("message" to "Failed to set index to read-only")
            }
        } catch (e: Exception) {
            failed = true
            val mutableInfo = mutableMapOf("message" to "Failed to set index to read-only")
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo.put("cause", errorMessage)
            info = mutableInfo.toMap()
        }
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            step = name,
            stepStartTime = getStepStartTime().toEpochMilli(),
            transitionTo = null,
            stepCompleted = !failed,
            failed = failed,
            info = info
        )
    }
}
