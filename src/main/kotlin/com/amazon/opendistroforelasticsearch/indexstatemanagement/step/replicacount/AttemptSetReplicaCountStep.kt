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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.step.replicacount

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ReplicaCountActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings

class AttemptSetReplicaCountStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: ReplicaCountActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step("attempt_set_replica_count", managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    @Suppress("TooGenericExceptionCaught")
    override suspend fun execute() {
        val numOfReplicas = config.numOfReplicas
        try {
            logger.info("Executing $name on ${managedIndexMetaData.index}")
            val updateSettingsRequest = UpdateSettingsRequest()
                    .indices(managedIndexMetaData.index)
                    .settings(Settings.builder().put("index.number_of_replicas", numOfReplicas))
            val response: AcknowledgedResponse = client.admin().indices()
                    .suspendUntil { updateSettings(updateSettingsRequest, it) }

            if (response.isAcknowledged) {
                logger.info("Successfully executed $name on ${managedIndexMetaData.index}")
                stepStatus = StepStatus.COMPLETED
                info = mapOf("message" to "Set number_of_replicas to $numOfReplicas")
            } else {
                logger.info("Unsuccessfully executed $name on ${managedIndexMetaData.index}")
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to "Failed to set number_of_replicas to $numOfReplicas")
            }
        } catch (e: Exception) {
            logger.error("Failed to execute $name on ${managedIndexMetaData.index}")
            stepStatus = StepStatus.STARTING
            val mutableInfo = mutableMapOf("message" to "Failed to set number_of_replicas to $numOfReplicas")
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
