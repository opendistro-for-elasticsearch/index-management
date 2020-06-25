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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.step.snapshot

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.SnapshotActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.ActionProperties
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import org.apache.logging.log4j.LogManager
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
    override suspend fun execute() {
        try {
            logger.info("Executing snapshot on ${managedIndexMetaData.index}")
            snapshotName = config
                    .snapshot
                    .plus("-")
                    .plus(LocalDateTime
                            .now(ZoneId.of("UTC"))
                            .format(DateTimeFormatter.ofPattern("uuuu.MM.dd-HH:mm:ss.SSS", Locale.ROOT)))
            val mutableInfo = mutableMapOf<String, String>()

            val createSnapshotRequest = CreateSnapshotRequest()
                    .userMetadata(mapOf("snapshot_created" to "Open Distro for Elasticsearch Index Management"))
                    .indices(managedIndexMetaData.index)
                    .snapshot(snapshotName)
                    .repository(config.repository)
                    .waitForCompletion(false)

            val response: CreateSnapshotResponse = client.admin().cluster().suspendUntil { createSnapshot(createSnapshotRequest, it) }
            when (response.status()) {
                RestStatus.ACCEPTED -> {
                    stepStatus = StepStatus.COMPLETED
                    mutableInfo["message"] = "Snapshot creation started for index: ${managedIndexMetaData.index}"
                }
                RestStatus.OK -> {
                    stepStatus = StepStatus.COMPLETED
                    mutableInfo["message"] = "Snapshot created for index: ${managedIndexMetaData.index}"
                }
                else -> {
                    stepStatus = StepStatus.FAILED
                    mutableInfo["message"] = "There was an error during snapshot creation for index: ${managedIndexMetaData.index}"
                    mutableInfo["cause"] = response.toString()
                }
            }
            info = mutableInfo.toMap()
        } catch (e: RemoteTransportException) {
            if (e.cause is ConcurrentSnapshotExecutionException) {
                resolveSnapshotException(e.cause as ConcurrentSnapshotExecutionException)
            } else {
                resolveException(e)
            }
        } catch (e: ConcurrentSnapshotExecutionException) {
            resolveSnapshotException(e)
        } catch (e: Exception) {
            resolveException(e)
        }
    }

    private fun resolveSnapshotException(e: ConcurrentSnapshotExecutionException) {
        val message = "Snapshot creation already in progress."
        logger.debug(message, e)
        stepStatus = StepStatus.CONDITION_NOT_MET
        info = mapOf("message" to message)
    }

    private fun resolveException(e: Exception) {
        val message = "Failed to create snapshot for index: ${managedIndexMetaData.index}"
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
    }
}
