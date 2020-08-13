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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.DeleteActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.delete.AttemptDeleteStep
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.AdminClient
import org.elasticsearch.client.Client
import org.elasticsearch.client.IndicesAdminClient
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.snapshots.SnapshotInProgressException
import org.elasticsearch.test.ESTestCase
import kotlin.IllegalArgumentException

class AttemptDeleteStepTests : ESTestCase() {

    private val clusterService: ClusterService = mock()

    fun `test delete step sets step status to completed when successful`() {
        val acknowledgedResponse = AcknowledgedResponse(true)
        val client = getClient(getAdminClient(getIndicesAdminClient(acknowledgedResponse, null)))

        runBlocking {
            val deleteActionConfig = DeleteActionConfig(0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null)
            val attemptDeleteStep = AttemptDeleteStep(clusterService, client, deleteActionConfig, managedIndexMetaData)
            attemptDeleteStep.execute()
            val updatedManagedIndexMetaData = attemptDeleteStep.getUpdatedManagedIndexMetaData(managedIndexMetaData)
            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test delete step sets step status to failed when not acknowledged`() {
        val acknowledgedResponse = AcknowledgedResponse(false)
        val client = getClient(getAdminClient(getIndicesAdminClient(acknowledgedResponse, null)))

        runBlocking {
            val deleteActionConfig = DeleteActionConfig(0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null)
            val attemptDeleteStep = AttemptDeleteStep(clusterService, client, deleteActionConfig, managedIndexMetaData)
            attemptDeleteStep.execute()
            val updatedManagedIndexMetaData = attemptDeleteStep.getUpdatedManagedIndexMetaData(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test delete step sets step status to failed when error thrown`() {
        val exception = IllegalArgumentException("example")
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val deleteActionConfig = DeleteActionConfig(0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null)
            val attemptDeleteStep = AttemptDeleteStep(clusterService, client, deleteActionConfig, managedIndexMetaData)
            attemptDeleteStep.execute()
            val updatedManagedIndexMetaData = attemptDeleteStep.getUpdatedManagedIndexMetaData(managedIndexMetaData)
            logger.info(updatedManagedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test delete step sets step status to condition not met when snapshot in progress error thrown`() {
        val exception = SnapshotInProgressException("example")
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val deleteActionConfig = DeleteActionConfig(0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null)
            val attemptDeleteStep = AttemptDeleteStep(clusterService, client, deleteActionConfig, managedIndexMetaData)
            attemptDeleteStep.execute()
            val updatedManagedIndexMetaData = attemptDeleteStep.getUpdatedManagedIndexMetaData(managedIndexMetaData)
            assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }
    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }
    private fun getIndicesAdminClient(acknowledgedResponse: AcknowledgedResponse?, exception: Exception?): IndicesAdminClient {
        assertTrue("Must provide one and only one response or exception", (acknowledgedResponse != null).xor(exception != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<AcknowledgedResponse>>(1)
                if (acknowledgedResponse != null) listener.onResponse(acknowledgedResponse)
                else listener.onFailure(exception)
            }.whenever(this.mock).delete(any(), any())
        }
    }
}