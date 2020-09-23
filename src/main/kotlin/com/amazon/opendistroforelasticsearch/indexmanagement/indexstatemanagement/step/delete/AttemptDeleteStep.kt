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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.delete

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.DeleteActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.snapshots.SnapshotInProgressException
import org.elasticsearch.transport.RemoteTransportException
import java.lang.Exception

class AttemptDeleteStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: DeleteActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override fun isIdempotent() = true

    @Suppress("TooGenericExceptionCaught")
    override suspend fun execute(): AttemptDeleteStep {
        try {
            val response: AcknowledgedResponse = client.admin().indices()
                .suspendUntil { delete(DeleteIndexRequest(indexName), it) }

            if (response.isAcknowledged) {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("message" to getSuccessMessage(indexName))
            } else {
                val message = getFailedMessage(indexName)
                logger.warn(message)
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to message)
            }
        } catch (e: RemoteTransportException) {
            val cause = ExceptionsHelper.unwrapCause(e)
            if (cause is SnapshotInProgressException) {
                handleSnapshotException(cause)
            } else {
                handleException(cause as Exception)
            }
        } catch (e: SnapshotInProgressException) {
            handleSnapshotException(e)
        } catch (e: Exception) {
            handleException(e)
        }

        return this
    }

    private fun handleSnapshotException(e: SnapshotInProgressException) {
        val message = getSnapshotMessage(indexName)
        logger.warn(message, e)
        stepStatus = StepStatus.CONDITION_NOT_MET
        info = mapOf("message" to message)
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

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    companion object {
        const val name = "attempt_delete"
        fun getFailedMessage(index: String) = "Failed to delete index [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully deleted index [index=$index]"
        fun getSnapshotMessage(index: String) = "Index had snapshot in progress, retrying deletion [index=$index]"
    }
}
