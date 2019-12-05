package com.amazon.opendistroforelasticsearch.indexstatemanagement.action

import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ActionConfig.ActionType
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.SnapshotActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.snapshot.AttemptSnapshotStep
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.snapshot.WaitForSnapshotStep
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService

class SnapshotAction(
    clusterService: ClusterService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData,
    config: SnapshotActionConfig
) : Action(ActionType.SNAPSHOT, config, managedIndexMetaData) {
    private val attemptSnapshotStep = AttemptSnapshotStep(clusterService, client, config, managedIndexMetaData)
    private val waitForSnapshotStep = WaitForSnapshotStep(clusterService, client, config, managedIndexMetaData)

    private val stepNameToStep: LinkedHashMap<String, Step> = linkedMapOf(
        AttemptSnapshotStep.name to attemptSnapshotStep,
        WaitForSnapshotStep.name to waitForSnapshotStep
    )

    override fun getSteps(): List<Step> = stepNameToStep.values.toList()

    @Suppress("ReturnCount")
    override fun getStepToExecute(): Step {
        // If stepMetaData is null, return the first step
        val stepMetaData = managedIndexMetaData.stepMetaData ?: return attemptSnapshotStep
        if (stepMetaData.name == AttemptSnapshotStep.name) return waitForSnapshotStep

        return attemptSnapshotStep
    }
}
