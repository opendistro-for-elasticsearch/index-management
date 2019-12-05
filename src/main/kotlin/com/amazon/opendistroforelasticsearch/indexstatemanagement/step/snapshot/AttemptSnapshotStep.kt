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

class AttemptSnapshotStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: SnapshotActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    @Suppress("TooGenericExceptionCaught")
    override suspend fun execute() {
        try {
            logger.info("Executing snapshot on ${managedIndexMetaData.index}")
            val createSnapshotRequest = CreateSnapshotRequest()
                .indices(managedIndexMetaData.index)
                .snapshot(config.snapshot)
                .repository(config.repository)
            if (config.includeGlobalState != null) {
                createSnapshotRequest.includeGlobalState(config.includeGlobalState)
            }

            val response: CreateSnapshotResponse = client.admin().cluster().suspendUntil { createSnapshot(createSnapshotRequest, it) }
            when (response.status()) {
                RestStatus.ACCEPTED -> {
                    stepStatus = StepStatus.COMPLETED
                    info = mapOf("message" to "Creating snapshot in progress for index: ${managedIndexMetaData.index}")
                }
                RestStatus.OK -> {
                    stepStatus = StepStatus.COMPLETED
                    info = mapOf("message" to "Snapshot created for index: ${managedIndexMetaData.index}")
                }
                else -> {
                    stepStatus = StepStatus.FAILED
                    info = mapOf("message" to "There was an error during snapshot creation for index: ${managedIndexMetaData.index}")
                }
            }
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
