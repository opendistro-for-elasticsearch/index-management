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
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.SnapshotActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.snapshot.AttemptSnapshotStep
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.snapshot.WaitForSnapshotStep
import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService

private val log = LogManager.getLogger(SnapshotAction::class.java)

class SnapshotAction(
    clusterService: ClusterService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData,
    val settings: Map<String, Any>,
    config: SnapshotActionConfig
) : Action(ActionType.SNAPSHOT, config, managedIndexMetaData) {
    private val attemptSnapshotStep = AttemptSnapshotStep(clusterService, client, config, managedIndexMetaData, settings)
    private val waitForSnapshotStep = WaitForSnapshotStep(clusterService, client, config, managedIndexMetaData)

    override fun getSteps(): List<Step> = listOf(attemptSnapshotStep, waitForSnapshotStep)

    @Suppress("ReturnCount")
    override fun getStepToExecute(): Step {
        // If stepMetaData is null, return the first step
        val stepMetaData = managedIndexMetaData.stepMetaData ?: return attemptSnapshotStep

        // If the current step has completed, return the next step
        if (stepMetaData.stepStatus == Step.StepStatus.COMPLETED) {
            return when (stepMetaData.name) {
                AttemptSnapshotStep.name -> waitForSnapshotStep
                else -> attemptSnapshotStep
            }
        }

        return when (stepMetaData.name) {
            AttemptSnapshotStep.name -> attemptSnapshotStep
            else -> waitForSnapshotStep
        }
    }
}
