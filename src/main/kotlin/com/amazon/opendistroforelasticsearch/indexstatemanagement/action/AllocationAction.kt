package com.amazon.opendistroforelasticsearch.indexstatemanagement.action

import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.AllocationActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.allocation.AttemptAllocationStep
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService

class AllocationAction(
    clusterService: ClusterService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData,
    config: AllocationActionConfig
) : Action(ActionConfig.ActionType.ALLOCATION, config, managedIndexMetaData) {

    private val attemptAllocationStep = AttemptAllocationStep(clusterService, client, config, managedIndexMetaData)

    private val steps = listOf(attemptAllocationStep)

    override fun getSteps(): List<Step> = steps

    override fun getStepToExecute(): Step {
        return attemptAllocationStep
    }
}
