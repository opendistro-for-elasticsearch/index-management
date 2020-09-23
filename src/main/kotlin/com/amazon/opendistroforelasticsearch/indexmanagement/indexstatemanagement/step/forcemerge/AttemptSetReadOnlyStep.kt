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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.forcemerge

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ForceMergeActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.transport.RemoteTransportException

class AttemptSetReadOnlyStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: ForceMergeActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override fun isIdempotent() = true

    override suspend fun execute(): AttemptSetReadOnlyStep {
        val indexSetToReadOnly = setIndexToReadOnly(indexName)

        // If setIndexToReadOnly returns false, updating settings failed and failed info was already updated, can return early
        if (!indexSetToReadOnly) return this

        // Complete step since index is read-only
        stepStatus = StepStatus.COMPLETED
        info = mapOf("message" to getSuccessMessage(indexName))

        return this
    }

    @Suppress("TooGenericExceptionCaught")
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
                return true
            }

            // If response is not acknowledged, then add failed info
            val message = getFailedMessage(indexName)
            logger.warn(message)
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to message)
        } catch (e: RemoteTransportException) {
            handleException(ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: Exception) {
            handleException(e)
        }

        return false
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

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData =
        currentMetaData.copy(stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus), transitionTo = null, info = info)

    companion object {
        const val name = "attempt_set_read_only"
        fun getFailedMessage(index: String) = "Failed to set index to read-only [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully set index to read-only [index=$index]"
    }
}
