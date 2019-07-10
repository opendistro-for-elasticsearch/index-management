package com.amazon.opendistroforelasticsearch.indexstatemanagement.action

import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.CloseActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.close.AttemptCloseStep
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService

class CloseAction(
    clusterService: ClusterService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData,
    config: CloseActionConfig
) : Action(ActionConfig.ActionType.CLOSE, config, managedIndexMetaData) {

    private val attemptCloseStep = AttemptCloseStep(clusterService, client, config, managedIndexMetaData)

    private val steps = listOf(attemptCloseStep)

    override fun getSteps(): List<Step> = steps

    override fun getStepToExecute(): Step {
        return attemptCloseStep
    }
}
