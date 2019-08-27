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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.coordinator

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementIndices
import com.amazon.opendistroforelasticsearch.indexstatemanagement.ManagedIndexCoordinator
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings
import org.elasticsearch.Version
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterName
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.ESAllocationTestCase
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_INDEX_UUID
import org.elasticsearch.cluster.metadata.MetaData
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.ClusterSettings
import org.elasticsearch.common.settings.Setting
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.test.ClusterServiceUtils
import org.elasticsearch.test.ESTestCase
import org.elasticsearch.threadpool.Scheduler
import org.elasticsearch.threadpool.ThreadPool
import org.mockito.Mockito
import org.junit.Before

class ManagedIndexCoordinatorTests : ESAllocationTestCase() {

    private lateinit var client: Client
    private lateinit var clusterService: ClusterService
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var threadPool: ThreadPool
    private lateinit var settings: Settings

    private lateinit var indexStateManagementIndices: IndexStateManagementIndices
    private lateinit var coordinator: ManagedIndexCoordinator

    private lateinit var discoveryNode: DiscoveryNode

    @Before
    @Throws(Exception::class)
    fun setup() {
        client = Mockito.mock(Client::class.java)
        threadPool = Mockito.mock(ThreadPool::class.java)
        indexStateManagementIndices = Mockito.mock(IndexStateManagementIndices::class.java)

        val namedXContentRegistryEntries = arrayListOf<NamedXContentRegistry.Entry>()
        xContentRegistry = NamedXContentRegistry(namedXContentRegistryEntries)

        settings = Settings.builder().build()

        discoveryNode = DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap<String, String>(),
                DiscoveryNode.Role.values().toSet(), Version.CURRENT)

        val settingSet = hashSetOf<Setting<*>>()
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        settingSet.add(ManagedIndexSettings.SWEEP_PERIOD)
        settingSet.add(ManagedIndexSettings.JOB_INTERVAL)
        settingSet.add(ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED)
        settingSet.add(ManagedIndexSettings.COORDINATOR_BACKOFF_COUNT)
        settingSet.add(ManagedIndexSettings.COORDINATOR_BACKOFF_MILLIS)

        val clusterSettings = ClusterSettings(settings, settingSet)
        val originClusterService: ClusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings)
        clusterService = Mockito.spy(originClusterService)

        coordinator = ManagedIndexCoordinator(settings, client, clusterService, threadPool, indexStateManagementIndices)
    }

    fun `test after start`() {
        coordinator.afterStart()
        Mockito.verify(threadPool).scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())
    }

    fun `test before stop`() {
        val cancellable = Mockito.mock(Scheduler.Cancellable::class.java)

        coordinator.beforeStop()
        Mockito.verify(cancellable, Mockito.times(0)).cancel()

        Mockito.`when`(threadPool.scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable)
        coordinator.initBackgroundSweep()
        coordinator.beforeStop()
        Mockito.verify(cancellable).cancel()
    }

    fun `test on master`() {
        coordinator.onMaster()
        Mockito.verify(threadPool).scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())
    }

    fun `test off master`() {
        val cancellable = Mockito.mock(Scheduler.Cancellable::class.java)

        coordinator.offMaster()
        Mockito.verify(cancellable, Mockito.times(0)).cancel()

        Mockito.`when`(threadPool.scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable)
        coordinator.initBackgroundSweep()
        coordinator.offMaster()
        Mockito.verify(cancellable).cancel()
    }

    fun `test init background sweep`() {
        val cancellable = Mockito.mock(Scheduler.Cancellable::class.java)
        Mockito.`when`(threadPool.scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable)

        coordinator.initBackgroundSweep()
        Mockito.verify(threadPool).scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())

        coordinator.initBackgroundSweep()
        Mockito.verify(cancellable).cancel()
        Mockito.verify(threadPool, Mockito.times(2)).scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())
    }

    fun `test sweep cluster state`() {
        val metaData = MetaData.builder()
                .put(createIndexMetaData("index-with-policy", 1, 3, "first_policy"))
                .put(createIndexMetaData("index-with-null-policy", 1, 3, null))
                .put(createIndexMetaData("index-with-empty-policy", 1, 3, ""))
                .put(createIndexMetaData("index-with-blank-policy", 1, 3, " "))
                .put(createIndexMetaData("index-with-policy-two", 1, 3, "second_policy"))
                .build()

        val clusterState = ClusterState.builder(ClusterName("cluster_name"))
                .metaData(metaData)
                .build()

        val results = coordinator.sweepClusterState(clusterState)
        assertTrue("Missed index with policy_id: first_policy", results.values.any { it.index == "index-with-policy" })
        assertTrue("Missed index with policy_id: second_policy", results.values.any { it.index == "index-with-policy-two" })
        assertFalse("Swept index with null policy_id", results.values.any { it.index == "index-with-null-policy" })
        assertFalse("Swept index with empty policy_id", results.values.any { it.index == "index-with-empty-policy" })
        assertFalse("Swept index with blank policy_id", results.values.any { it.index == "index-with-blank-policy" })
    }

    private fun createIndexMetaData(indexName: String, replicaNumber: Int, shardNumber: Int, policyID: String?): IndexMetaData.Builder {
        val defaultSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(ManagedIndexSettings.POLICY_ID.key, policyID)
                .put(SETTING_INDEX_UUID, randomAlphaOfLength(20))
                .build()
        return IndexMetaData.Builder(indexName)
                .settings(defaultSettings)
                .numberOfReplicas(replicaNumber)
                .numberOfShards(shardNumber)
    }

    private fun <T> any(): T {
        Mockito.any<T>()
        return uninitialized()
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> uninitialized(): T = null as T
}
