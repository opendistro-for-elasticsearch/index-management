package com.amazon.opendistroforelasticsearch.indexstatemanagement.action

import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ActionConfig.ActionType
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.OpenActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.open.AttemptOpenStep
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService

class OpenAction(
    clusterService: ClusterService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData,
    config: OpenActionConfig
) : Action(ActionType.OPEN, config, managedIndexMetaData) {

    private val attemptOpenStep = AttemptOpenStep(clusterService, client, config, managedIndexMetaData)

    private val steps = listOf(attemptOpenStep)

    override fun getSteps(): List<Step> = steps

    override fun getStepToExecute(): Step {
        return attemptOpenStep
    }
}
