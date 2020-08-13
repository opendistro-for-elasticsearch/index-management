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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.action

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ActionConfig.ActionType
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ReplicaCountActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.replicacount.AttemptSetReplicaCountStep
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService

class ReplicaCountAction(
    clusterService: ClusterService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData,
    config: ReplicaCountActionConfig
) : Action(ActionType.REPLICA_COUNT, config, managedIndexMetaData) {

    private val attemptSetReplicaCountStep = AttemptSetReplicaCountStep(clusterService, client, config, managedIndexMetaData)
    private val steps = listOf(attemptSetReplicaCountStep)

    override fun getSteps(): List<Step> = steps

    override fun getStepToExecute(): Step = attemptSetReplicaCountStep
}
