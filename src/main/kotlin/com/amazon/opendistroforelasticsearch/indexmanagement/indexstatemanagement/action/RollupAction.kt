/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.action

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.RollupActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.rollup.AttemptCreateRollupJobStep
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.rollup.WaitForRollupCompletionStep
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService

// TODO: not sure if this the right interface
class RollupAction(
    clusterService: ClusterService,
    client: Client,
    rollup: Rollup,
    managedIndexMetaData: ManagedIndexMetaData,
    config: RollupActionConfig
) : Action(ActionConfig.ActionType.ROLLUP, config, managedIndexMetaData) {

    private val attemptCreateRollupJobStep = AttemptCreateRollupJobStep(clusterService, client, rollup, managedIndexMetaData)
    private val waitForRollupCompletionStep = WaitForRollupCompletionStep(clusterService, client, rollup.id, managedIndexMetaData)

    override fun getSteps(): List<Step> = listOf(attemptCreateRollupJobStep, waitForRollupCompletionStep)

    override fun getStepToExecute(): Step {
        TODO("Not yet implemented")
    }
}
