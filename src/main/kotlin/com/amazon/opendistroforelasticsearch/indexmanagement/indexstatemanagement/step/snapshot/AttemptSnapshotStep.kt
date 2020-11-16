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
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.snapshots.ConcurrentSnapshotExecutionException
import org.elasticsearch.transport.RemoteTransportException
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Locale

class AttemptSnapshotStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: SnapshotActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private var snapshotName: String? = null

    override fun isIdempotent() = false

    @Suppress("TooGenericExceptionCaught", "ComplexMethod")
    override suspend fun execute(): AttemptSnapshotStep {
        try {
            snapshotName = config
                    .snapshot
                    .plus("-")
                    .plus(LocalDateTime
                            .now(ZoneId.of("UTC"))
                            .format(DateTimeFormatter.ofPattern("uuuu.MM.dd-HH:mm:ss.SSS", Locale.ROOT)))
            val mutableInfo = mutableMapOf<String, String>()

            val createSnapshotRequest = CreateSnapshotRequest()
                    .userMetadata(mapOf("snapshot_created" to "Open Distro for Elasticsearch Index Management"))
                    .indices(indexName)
                    .snapshot(snapshotName)
                    .repository(config.repository)
                    .waitForCompletion(false)

            val response: CreateSnapshotResponse = client.admin().cluster().suspendUntil { createSnapshot(createSnapshotRequest, it) }
            when (response.status()) {
                RestStatus.ACCEPTED -> {
                    stepStatus = StepStatus.COMPLETED
                    mutableInfo["message"] = getSuccessMessage(indexName)
                }
                RestStatus.OK -> {
                    stepStatus = StepStatus.COMPLETED
                    mutableInfo["message"] = getSuccessMessage(indexName)
                }
                else -> {
                    val message = getFailedMessage(indexName)
                    logger.warn("$message - $response")
                    stepStatus = StepStatus.FAILED
                    mutableInfo["message"] = getFailedMessage(indexName)
                    mutableInfo["cause"] = response.toString()
                }
            }
            info = mutableInfo.toMap()
        } catch (e: RemoteTransportException) {
            val cause = ExceptionsHelper.unwrapCause(e)
            if (cause is ConcurrentSnapshotExecutionException) {
                handleSnapshotException(cause)
            } else {
                handleException(cause as Exception)
            }
        } catch (e: ConcurrentSnapshotExecutionException) {
            handleSnapshotException(e)
        } catch (e: Exception) {
            handleException(e)
        }

        return this
    }

    private fun handleSnapshotException(e: ConcurrentSnapshotExecutionException) {
        val message = getFailedConcurrentSnapshotMessage(indexName)
        logger.debug(message, e)
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
        val currentActionMetaData = currentMetaData.actionMetaData
        return currentMetaData.copy(
                actionMetaData = currentActionMetaData?.copy(actionProperties = ActionProperties(snapshotName = snapshotName)),
                stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
                transitionTo = null,
                info = info
        )
    }

    companion object {
        const val name = "attempt_snapshot"
        fun getFailedMessage(index: String) = "Failed to create snapshot [index=$index]"
        fun getFailedConcurrentSnapshotMessage(index: String) = "Concurrent snapshot in progress, retrying next execution [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully started snapshot [index=$index]"
    }
}
