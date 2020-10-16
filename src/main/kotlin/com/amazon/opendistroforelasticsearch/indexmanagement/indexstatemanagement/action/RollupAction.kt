package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.action

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.RollupActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.rollup.AttemptCreateRollupJobStep
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.rollup.WaitForRollupCompletionStep
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService

// TODO: not sure if this the right interface
class RollupAction(
    clusterService: ClusterService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData,
    config: RollupActionConfig
) : Action(ActionConfig.ActionType.ROLLUP, config, managedIndexMetaData) {

    private val attemptCreateRollupJobStep = AttemptCreateRollupJobStep(clusterService, client, managedIndexMetaData)
    private val waitForRollupCompletionStep = WaitForRollupCompletionStep(clusterService, client, managedIndexMetaData)

    override fun getSteps(): List<Step> = listOf(attemptCreateRollupJobStep, waitForRollupCompletionStep)

    override fun getStepToExecute(): Step {
        TODO("Not yet implemented")
    }
}
