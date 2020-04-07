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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.step.close

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.CloseActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.snapshots.SnapshotInProgressException

class AttemptCloseStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: CloseActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step("attempt_close", managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    @Suppress("TooGenericExceptionCaught")
    override suspend fun execute() {
        val index = managedIndexMetaData.index
        try {
            logger.info("Executing close on $index")
            val closeIndexRequest = CloseIndexRequest()
                .indices(index)

            val response: AcknowledgedResponse = client.admin().indices().suspendUntil { close(closeIndexRequest, it) }
            logger.info("Close index for $index was acknowledged=${response.isAcknowledged}")
            if (response.isAcknowledged) {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("message" to "Successfully closed index")
            } else {
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to "Failed to close index")
            }
        } catch (e: SnapshotInProgressException) {
            logger.warn("Failed to close index [index=$index] with snapshot in progress")
            stepStatus = StepStatus.CONDITION_NOT_MET
            info = mapOf("message" to "Index had snapshot in progress, retrying closing")
        } catch (e: Exception) {
            logger.error("Failed to set index to close [index=$index]", e)
            stepStatus = StepStatus.FAILED
            val mutableInfo = mutableMapOf("message" to "Failed to set index to close")
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo["cause"] = errorMessage
            info = mutableInfo.toMap()
        }
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }
}
