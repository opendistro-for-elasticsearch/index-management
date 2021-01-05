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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.runner

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementHistory
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.SkipExecution
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataRequest
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.elasticsearch.Version
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.ClusterSettings
import org.elasticsearch.common.settings.Setting
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.env.Environment
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.script.ScriptService
import org.elasticsearch.test.ClusterServiceUtils
import org.elasticsearch.test.ESTestCase
import org.elasticsearch.threadpool.ThreadPool
import org.junit.Before
import org.mockito.Mockito

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class ManagedIndexRunnerTests : ESTestCase() {

    private lateinit var client: Client
    private lateinit var clusterService: ClusterService
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var scriptService: ScriptService
    private lateinit var environment: Environment
    private lateinit var indexStateManagementHistory: IndexStateManagementHistory
    private lateinit var skipFlag: SkipExecution
    private lateinit var runner: ManagedIndexRunner

    private lateinit var settings: Settings
    private lateinit var discoveryNode: DiscoveryNode
    private lateinit var threadPool: ThreadPool

    private lateinit var indexResponse: IndexResponse

    @Before
    @Throws(Exception::class)
    fun setup() {
        clusterService = Mockito.mock(ClusterService::class.java)
        xContentRegistry = Mockito.mock(NamedXContentRegistry::class.java)
        scriptService = Mockito.mock(ScriptService::class.java)
        environment = Mockito.mock(Environment::class.java)
        indexStateManagementHistory = Mockito.mock(IndexStateManagementHistory::class.java)
        skipFlag = Mockito.mock(SkipExecution::class.java)

        threadPool = Mockito.mock(ThreadPool::class.java)
        settings = Settings.builder().build()
        discoveryNode = DiscoveryNode("node", buildNewFakeTransportAddress(), Version.CURRENT)
        val settingSet = hashSetOf<Setting<*>>()
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        settingSet.add(ManagedIndexSettings.SWEEP_PERIOD)
        settingSet.add(ManagedIndexSettings.JOB_INTERVAL)
        settingSet.add(ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED)
        settingSet.add(ManagedIndexSettings.ALLOW_LIST)
        val clusterSettings = ClusterSettings(settings, settingSet)
        val originClusterService: ClusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings)
        clusterService = Mockito.spy(originClusterService)

        Mockito.`when`(environment.settings()).thenReturn(settings)

        runner = ManagedIndexRunner
                .registerClusterService(clusterService)
                .registerNamedXContentRegistry(xContentRegistry)
                .registerScriptService(scriptService)
                .registerSettings(environment.settings())
                .registerConsumers()
                .registerHistoryIndex(indexStateManagementHistory)
                .registerSkipFlag(skipFlag)
    }

    fun `test fail to delete metadata in cluster state`() {
        /**
         * if delete metadata in cluster state not successful
         * check `metadataDeleted` is false
         */

        val acknowledgedResponse = AcknowledgedResponse(false)
        indexResponse = Mockito.mock(IndexResponse::class.java)
        Mockito.`when`(indexResponse.status()).thenReturn(RestStatus.CREATED)
        client = Mockito.mock(Client::class.java)
        doAnswer { invocationOnMock ->
            val listener = invocationOnMock.getArgument<ActionListener<AcknowledgedResponse>>(2)
            listener.onResponse(acknowledgedResponse)
        }.whenever(client).execute(any(), any<UpdateManagedIndexMetaDataRequest>(), any<ActionListener<AcknowledgedResponse>>())
        doAnswer { invocationOnMock ->
            val listener = invocationOnMock.getArgument<ActionListener<IndexResponse>>(1)
            listener.onResponse(indexResponse)
        }.whenever(client).index(any(), any())

        runner.registerClient(client)

        val metadata = null
        val metadata2 = ManagedIndexMetaData(
                index = "test",
                indexUuid = "123",
                policyID = "456",
                policySeqNo = null,
                policyPrimaryTerm = null,
                policyCompleted = false,
                rolledOver = false,
                transitionTo = null,
                stateMetaData = null,
                actionMetaData = null,
                stepMetaData = null,
                policyRetryInfo = null,
                info = mapOf("message" to "hello"))

        runBlocking {
            assertEquals(true, runner.metadataDeleted)
            runner.handleClusterStateMetadata(metadata, metadata2)
            assertEquals(false, runner.metadataDeleted)
        }
    }
}
