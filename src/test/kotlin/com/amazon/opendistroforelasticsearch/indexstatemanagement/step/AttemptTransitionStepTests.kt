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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.step

import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Conditions
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Transition
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.TransitionsActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.transition.AttemptTransitionStep
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.stats.CommonStats
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse
import org.elasticsearch.client.AdminClient
import org.elasticsearch.client.Client
import org.elasticsearch.client.IndicesAdminClient
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.cluster.metadata.MetaData
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.index.shard.DocsStats
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.ESTestCase
import org.elasticsearch.transport.RemoteTransportException

class AttemptTransitionStepTests : ESTestCase() {

    private val indexMetadata: IndexMetaData = mock()
    private val metadata: MetaData = mock { on { index(any<String>()) } doReturn indexMetadata }
    private val clusterState: ClusterState = mock { on { metaData() } doReturn metadata }
    private val clusterService: ClusterService = mock { on { state() } doReturn clusterState }

    private val docsStats: DocsStats = mock()
    private val primaries: CommonStats = mock { on { getDocs() } doReturn docsStats }
    private val statsResponse: IndicesStatsResponse = mock { on { primaries } doReturn primaries }

    fun `test stats response not OK`() {
        whenever(indexMetadata.creationDate).doReturn(5L)
        whenever(statsResponse.status).doReturn(RestStatus.INTERNAL_SERVER_ERROR)
        whenever(statsResponse.shardFailures).doReturn(IndicesStatsResponse.EMPTY)
        whenever(docsStats.count).doReturn(6L)
        whenever(docsStats.totalSizeInBytes).doReturn(2)
        val client = getClient(getAdminClient(getIndicesAdminClient(statsResponse, null)))

        runBlocking {
            val config = TransitionsActionConfig(listOf(Transition("some_state", Conditions(docCount = 5L))))
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null)
            val step = AttemptTransitionStep(clusterService, client, config, managedIndexMetaData)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get correct failed message", AttemptTransitionStep.getFailedStatsMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }
    }

    fun `test transitions fails on exception`() {
        whenever(indexMetadata.creationDate).doReturn(5L)
        val exception = IllegalArgumentException("example")
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val config = TransitionsActionConfig(listOf(Transition("some_state", Conditions(docCount = 5L))))
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null)
            val step = AttemptTransitionStep(clusterService, client, config, managedIndexMetaData)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "example", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    fun `test transitions remote transport exception`() {
        whenever(indexMetadata.creationDate).doReturn(5L)
        val exception = RemoteTransportException("rte", IllegalArgumentException("nested"))
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val config = TransitionsActionConfig(listOf(Transition("some_state", Conditions(docCount = 5L))))
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null)
            val step = AttemptTransitionStep(clusterService, client, config, managedIndexMetaData)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "nested", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }
    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }
    private fun getIndicesAdminClient(statsResponse: IndicesStatsResponse?, exception: Exception?): IndicesAdminClient {
        assertTrue("Must provide one and only one response or exception", (statsResponse != null).xor(exception != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<IndicesStatsResponse>>(1)
                if (statsResponse != null) listener.onResponse(statsResponse)
                else listener.onFailure(exception)
            }.whenever(this.mock).stats(any(), any())
        }
    }
}