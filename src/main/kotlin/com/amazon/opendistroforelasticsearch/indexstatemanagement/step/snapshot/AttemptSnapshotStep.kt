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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.rest.RestStatus
import java.time.Clock
import java.time.ZonedDateTime

class AttemptSnapshotStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: SnapshotActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override fun isIdempotent() = true

    @Suppress("TooGenericExceptionCaught")
    override suspend fun execute() {
        try {
            logger.info("Executing snapshot on ${managedIndexMetaData.index}")
            val snapshotName = config.snapshot.plus(ZonedDateTime.now(Clock.systemUTC()))
            val mutableInfo = mutableMapOf("snapshotName" to snapshotName)

            val createSnapshotRequest = CreateSnapshotRequest()
                    .userMetadata(mapOf("snapshot_created" to "Open Distro for Elasticsearch Index Management"))
                    .indices(managedIndexMetaData.index)
                    .snapshot(config.snapshot)
                    .repository(config.repository)
                    .waitForCompletion(false)

            if (config.includeGlobalState != null) {
                createSnapshotRequest.includeGlobalState(config.includeGlobalState)
            }

            val response: CreateSnapshotResponse = client.admin().cluster().suspendUntil { createSnapshot(createSnapshotRequest, it) }
            when (response.status()) {
                RestStatus.ACCEPTED -> {
                    stepStatus = StepStatus.CONDITION_NOT_MET
                    mutableInfo["message"] = "Creating snapshot in progress for index: ${managedIndexMetaData.index}"
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
        } catch (e: Exception) {
            val message = "Failed to create snapshot for index: ${managedIndexMetaData.index}"
            logger.error(message, e)
            stepStatus = StepStatus.FAILED
            val mutableInfo = mutableMapOf("message" to message)
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

    companion object {
        const val name = "attempt_snapshot"
    }
}
