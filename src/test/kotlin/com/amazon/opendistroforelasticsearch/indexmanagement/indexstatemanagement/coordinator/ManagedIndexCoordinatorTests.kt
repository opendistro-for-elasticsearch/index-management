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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.coordinator

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.MetadataService
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.elasticsearch.Version
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ESAllocationTestCase
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID
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

    private lateinit var indexManagementIndices: IndexManagementIndices
    private lateinit var metadataService: MetadataService
    private lateinit var coordinator: ManagedIndexCoordinator

    private lateinit var discoveryNode: DiscoveryNode

    @Before
    @Throws(Exception::class)
    fun setup() {
        client = Mockito.mock(Client::class.java)
        threadPool = Mockito.mock(ThreadPool::class.java)
        indexManagementIndices = Mockito.mock(IndexManagementIndices::class.java)
        metadataService = Mockito.mock(MetadataService::class.java)

        val namedXContentRegistryEntries = arrayListOf<NamedXContentRegistry.Entry>()
        xContentRegistry = NamedXContentRegistry(namedXContentRegistryEntries)

        settings = Settings.builder().build()

        discoveryNode = DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT)

        val settingSet = hashSetOf<Setting<*>>()
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        settingSet.add(ManagedIndexSettings.SWEEP_PERIOD)
        settingSet.add(ManagedIndexSettings.JOB_INTERVAL)
        settingSet.add(ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED)
        settingSet.add(ManagedIndexSettings.METADATA_SERVICE_ENABLED)
        settingSet.add(ManagedIndexSettings.COORDINATOR_BACKOFF_COUNT)
        settingSet.add(ManagedIndexSettings.COORDINATOR_BACKOFF_MILLIS)

        val clusterSettings = ClusterSettings(settings, settingSet)
        val originClusterService: ClusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings)
        clusterService = Mockito.spy(originClusterService)

        coordinator = ManagedIndexCoordinator(settings, client, clusterService, threadPool, indexManagementIndices, metadataService)
    }

    fun `test after start`() {
        coordinator.afterStart()
        Mockito.verify(threadPool, Mockito.times(2)).scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())
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
        Mockito.verify(threadPool, Mockito.times(2)).scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())
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

    private fun createIndexMetaData(indexName: String, replicaNumber: Int, shardNumber: Int, policyID: String?): IndexMetadata.Builder {
        val defaultSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(ManagedIndexSettings.POLICY_ID.key, policyID)
            .put(SETTING_INDEX_UUID, randomAlphaOfLength(20))
            .build()
        return IndexMetadata.Builder(indexName)
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
