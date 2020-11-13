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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.replicacount

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ReplicaCountActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.transport.RemoteTransportException

class AttemptSetReplicaCountStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: ReplicaCountActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step("attempt_set_replica_count", managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private val numOfReplicas = config.numOfReplicas

    override fun isIdempotent() = true

    @Suppress("TooGenericExceptionCaught")
    override suspend fun execute(): AttemptSetReplicaCountStep {
        try {
            val updateSettingsRequest = UpdateSettingsRequest()
                    .indices(indexName)
                    .settings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, numOfReplicas))
            val response: AcknowledgedResponse = client.admin().indices()
                    .suspendUntil { updateSettings(updateSettingsRequest, it) }

            if (response.isAcknowledged) {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("message" to getSuccessMessage(indexName, numOfReplicas))
            } else {
                val message = getFailedMessage(indexName, numOfReplicas)
                logger.warn(message)
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to message)
            }
        } catch (e: RemoteTransportException) {
            handleException(ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: Exception) {
            handleException(e)
        }

        return this
    }

    private fun handleException(e: Exception) {
        val message = getFailedMessage(indexName, numOfReplicas)
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        val mutableInfo = mutableMapOf("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    companion object {
        fun getFailedMessage(index: String, numOfReplicas: Int) = "Failed to set number_of_replicas to $numOfReplicas [index=$index]"
        fun getSuccessMessage(index: String, numOfReplicas: Int) = "Successfully set number_of_replicas to $numOfReplicas [index=$index]"
    }
}
