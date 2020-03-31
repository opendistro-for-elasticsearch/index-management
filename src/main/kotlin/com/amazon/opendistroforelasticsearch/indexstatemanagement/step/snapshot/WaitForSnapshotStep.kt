package com.amazon.opendistroforelasticsearch.indexstatemanagement.step.snapshot

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.SnapshotActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService

class WaitForSnapshotStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: SnapshotActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override suspend fun execute() {
        logger.info("Waiting for snapshot to complete...")
        val request = SnapshotsStatusRequest()
            .snapshots(arrayOf(managedIndexMetaData.info?.get("snapshotName").toString()))
            .repository(config.repository)
        val response: SnapshotsStatusResponse = client.admin().cluster().suspendUntil { snapshotsStatus(request, it) }
        val status: SnapshotStatus? = response
            .snapshots
            .find { snapshotStatus ->
                snapshotStatus.snapshot.snapshotId.name == managedIndexMetaData.info?.get("snapshotName").toString()
                        && snapshotStatus.snapshot.repository == config.repository
            }
        if (status != null) {
            if (status.state.completed()) {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("message" to "Snapshot created for index: ${managedIndexMetaData.index}")
            } else {
                stepStatus = StepStatus.CONDITION_NOT_MET
                info = mapOf("message" to "Creating snapshot in progress for index: ${managedIndexMetaData.index}")
            }
        } else {
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to "Snapshot doesn't exist for index: ${managedIndexMetaData.index}")
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
        const val name = "wait_for_snapshot"
    }
}
