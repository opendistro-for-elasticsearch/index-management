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
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.OpenActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.open.AttemptOpenStep
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse
import org.elasticsearch.client.AdminClient
import org.elasticsearch.client.Client
import org.elasticsearch.client.IndicesAdminClient
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.test.ESTestCase
import org.elasticsearch.transport.RemoteTransportException

class AttemptOpenStepTests : ESTestCase() {

    private val clusterService: ClusterService = mock()

    fun `test open step sets step status to failed when not acknowledged`() {
        val openIndexResponse = OpenIndexResponse(false, false)
        val client = getClient(getAdminClient(getIndicesAdminClient(openIndexResponse, null)))

        runBlocking {
            val openActionConfig = OpenActionConfig(0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null)
            val attemptOpenStep = AttemptOpenStep(clusterService, client, openActionConfig, managedIndexMetaData)
            attemptOpenStep.execute()
            val updatedManagedIndexMetaData = attemptOpenStep.getUpdatedManagedIndexMetaData(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test open step sets step status to failed when error thrown`() {
        val exception = IllegalArgumentException("example")
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val openActionConfig = OpenActionConfig(0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null)
            val attemptOpenStep = AttemptOpenStep(clusterService, client, openActionConfig, managedIndexMetaData)
            attemptOpenStep.execute()
            val updatedManagedIndexMetaData = attemptOpenStep.getUpdatedManagedIndexMetaData(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test open step remote transport exception`() {
        val exception = RemoteTransportException("rte", IllegalArgumentException("nested"))
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val openActionConfig = OpenActionConfig(0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null)
            val attemptOpenStep = AttemptOpenStep(clusterService, client, openActionConfig, managedIndexMetaData)
            attemptOpenStep.execute()
            val updatedManagedIndexMetaData = attemptOpenStep.getUpdatedManagedIndexMetaData(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "nested", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }
    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }
    private fun getIndicesAdminClient(openIndexResponse: OpenIndexResponse?, exception: Exception?): IndicesAdminClient {
        assertTrue("Must provide one and only one response or exception", (openIndexResponse != null).xor(exception != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<OpenIndexResponse>>(1)
                if (openIndexResponse != null) listener.onResponse(openIndexResponse)
                else listener.onFailure(exception)
            }.whenever(this.mock).open(any(), any())
        }
    }
}