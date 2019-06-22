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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.DeleteActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.delete.AttemptDeleteStep
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.ActionType
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService

class DeleteAction(
    clusterService: ClusterService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData,
    config: DeleteActionConfig
) : Action(type, managedIndexMetaData) {

    private val attemptDeleteStep =
            AttemptDeleteStep(clusterService, client, config, managedIndexMetaData)
    private val steps = listOf(attemptDeleteStep)

    override fun getSteps(): List<Step> = steps

    override fun getStepToExecute(managedIndexMetaData: ManagedIndexMetaData): Step {
        return attemptDeleteStep
    }

    companion object {
        val type = ActionType.DELETE
    }
}
