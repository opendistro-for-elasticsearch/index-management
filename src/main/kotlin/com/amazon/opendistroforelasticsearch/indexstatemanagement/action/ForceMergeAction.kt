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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.action

import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ActionConfig.ActionType
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ForceMergeActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.forcemerge.CallForceMergeStep
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.forcemerge.WaitForForceMergeStep
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService

class ForceMergeAction(
    clusterService: ClusterService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData,
    config: ForceMergeActionConfig
) : Action(ActionType.FORCE_MERGE, config, managedIndexMetaData) {

    private val callForceMergeStep = CallForceMergeStep(clusterService, client, config, managedIndexMetaData)
    private val waitForForceMergeStep = WaitForForceMergeStep(clusterService, client, config, managedIndexMetaData)

    private val steps = listOf(callForceMergeStep, waitForForceMergeStep)

    override fun getSteps(): List<Step> = steps

    override fun getStepToExecute(): Step {
        val stepMetaData = managedIndexMetaData.stepMetaData ?: return callForceMergeStep

        val step = stepMetaData.name
        // If callForceMergeStep has completed, get waitForForceMergeStep to execute
        if (step == CallForceMergeStep.name && stepMetaData.completed) return waitForForceMergeStep

        // If callForceMergeStep has not completed, return that step
        return callForceMergeStep
    }
}
