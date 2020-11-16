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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.snapshot

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.SnapshotActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionProperties
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.SnapshotsInProgress.State
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.transport.RemoteTransportException

class WaitForSnapshotStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: SnapshotActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override fun isIdempotent() = true

    @Suppress("ComplexMethod")
    override suspend fun execute(): WaitForSnapshotStep {
        try {
            val snapshotName = getSnapshotName() ?: return this
            val request = SnapshotsStatusRequest()
                .snapshots(arrayOf(snapshotName))
                .repository(config.repository)
            val response: SnapshotsStatusResponse = client.admin().cluster().suspendUntil { snapshotsStatus(request, it) }
            val status: SnapshotStatus? = response
                .snapshots
                .find { snapshotStatus ->
                    snapshotStatus.snapshot.snapshotId.name == snapshotName &&
                            snapshotStatus.snapshot.repository == config.repository
                }
            if (status != null) {
                when (status.state) {
                    State.INIT, State.STARTED -> {
                        stepStatus = StepStatus.CONDITION_NOT_MET
                        info = mapOf("message" to getSnapshotInProgressMessage(indexName), "state" to status.state.name)
                    }
                    State.SUCCESS -> {
                        stepStatus = StepStatus.COMPLETED
                        info = mapOf("message" to getSuccessMessage(indexName), "state" to status.state.name)
                    }
                    else -> { // State.FAILED, State.ABORTED
                        val message = getFailedExistsMessage(indexName)
                        logger.warn(message)
                        stepStatus = StepStatus.FAILED
                        info = mapOf("message" to message, "state" to status.state.name)
                    }
                }
            } else {
                val message = getFailedExistsMessage(indexName)
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
        val message = getFailedMessage(indexName)
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        val mutableInfo = mutableMapOf("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    private fun getSnapshotName(): String? {
        val actionProperties = managedIndexMetaData.actionMetaData?.actionProperties

        if (actionProperties?.snapshotName == null) {
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to getFailedActionPropertiesMessage(indexName, actionProperties))
            return null
        }

        return actionProperties.snapshotName
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    companion object {
        const val name = "wait_for_snapshot"
        fun getFailedMessage(index: String) = "Failed to get status of snapshot [index=$index]"
        fun getFailedExistsMessage(index: String) = "Snapshot doesn't exist [index=$index]"
        fun getFailedActionPropertiesMessage(index: String, actionProperties: ActionProperties?) =
            "Unable to retrieve [${ActionProperties.Properties.SNAPSHOT_NAME.key}] from ActionProperties=$actionProperties [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully created snapshot [index=$index]"
        fun getSnapshotInProgressMessage(index: String) = "Snapshot currently in progress [index=$index]"
    }
}
