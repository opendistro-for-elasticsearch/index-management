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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ReplicaCountActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import com.carrotsearch.randomizedtesting.RandomizedTest.sleep
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.Index
import org.junit.After
import org.junit.Assume
import org.junit.Before
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale
import kotlin.collections.HashMap

class MetadataRegressionIT : IndexStateManagementIntegTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    @Before
    fun startMetadataService() {
        // metadata service could be stopped before following tests start run
        // this will enable metadata service again
        updateClusterSetting(ManagedIndexSettings.METADATA_SERVICE_ENABLED.key, "false")
        updateClusterSetting(ManagedIndexSettings.METADATA_SERVICE_ENABLED.key, "true")
    }

    @After
    fun cleanClusterSetting() {
        // need to clean up otherwise will throw error
        updateClusterSetting(ManagedIndexSettings.METADATA_SERVICE_ENABLED.key, null, false)
        updateClusterSetting(ManagedIndexSettings.TEMPLATE_MIGRATION_ENABLED.key, null, false)
    }

    fun `test move metadata service`() {
        updateClusterSetting(ManagedIndexSettings.METADATA_SERVICE_ENABLED.key, "false")
        updateClusterSetting(ManagedIndexSettings.METADATA_SERVICE_ENABLED.key, "true")

        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val actionConfig = ReplicaCountActionConfig(10, 0)
        val states = listOf(State(name = "ReplicaCountState", actions = listOf(actionConfig), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName)

        // put some metadata into cluster state
        var indexMetadata = getIndexMetadata(indexName)
        metadataToClusterState = metadataToClusterState.copy(
            index = indexName,
            indexUuid = indexMetadata.indexUUID,
            policyID = policyID
        )
        val request = UpdateManagedIndexMetaDataRequest(
            indicesToAddManagedIndexMetaDataTo = listOf(
                Pair(Index(metadataToClusterState.index, metadataToClusterState.indexUuid), metadataToClusterState)
            )
        )
        val response: AcknowledgedResponse = client().execute(
            UpdateManagedIndexMetaDataAction.INSTANCE, request
        ).get()
        logger.info(response.isAcknowledged)
        indexMetadata = getIndexMetadata(indexName)
        logger.info("check if metadata is saved in cluster state: ${indexMetadata.getCustomData("managed_index_metadata")}")

        // create a job
        addPolicyToIndex(indexName, policyID)

        waitFor {
            assertEquals(
                "Metadata is pending migration",
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        waitFor(Instant.ofEpochSecond(120)) {
            assertEquals(
                "Happy moving",
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
            assertEquals(null, getIndexMetadata(indexName).getCustomData("managed_index_metadata"))
        }

        logger.info("metadata has moved")

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }
        waitFor {
            assertEquals(
                "Successfully initialized policy: ${policy.id}",
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                "Index did not set number_of_replicas to ${actionConfig.numOfReplicas}",
                actionConfig.numOfReplicas,
                getNumberOfReplicasSetting(indexName)
            )
        }
    }

    fun `test job can continue run from cluster state metadata`() {
        /**
         *  new version of ISM plugin can handle metadata in cluster state
         *  when job already started
         *
         *  create index, add policy to it
         *  manually add policy field to managed-index so runner won't do initialisation itself
         *  add metadata into cluster state
         *  then check if we can continue run from this added metadata
         */

        val indexName = "${testIndexName}_index_2"
        val policyID = "${testIndexName}_testPolicyName_2"
        val actionConfig = ReplicaCountActionConfig(10, 0)
        val states = listOf(State(name = "ReplicaCountState", actions = listOf(actionConfig), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName)
        addPolicyToIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // manually add policy field into managed-index
        updateManagedIndexConfigPolicy(managedIndexConfig, policy)
        logger.info("managed-index: ${getExistingManagedIndexConfig(indexName)}")

        // manually save metadata into cluster state
        var indexMetadata = getIndexMetadata(indexName)
        metadataToClusterState = metadataToClusterState.copy(
            index = indexName,
            indexUuid = indexMetadata.indexUUID,
            policyID = policyID
        )
        val request = UpdateManagedIndexMetaDataRequest(
            indicesToAddManagedIndexMetaDataTo = listOf(
                Pair(Index(metadataToClusterState.index, metadataToClusterState.indexUuid), metadataToClusterState)
            )
        )
        val response: AcknowledgedResponse = client().execute(
            UpdateManagedIndexMetaDataAction.INSTANCE, request
        ).get()

        logger.info(response.isAcknowledged)
        indexMetadata = getIndexMetadata(indexName)
        logger.info("check if metadata is saved in cluster state: ${indexMetadata.getCustomData("managed_index_metadata")}")

        waitFor {
            assertEquals(
                "Metadata is pending migration",
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        waitFor(Instant.ofEpochSecond(120)) {
            assertEquals(
                "Happy moving",
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
            assertEquals(null, getIndexMetadata(indexName).getCustomData("managed_index_metadata"))
        }

        logger.info("metadata has moved")

        // start the job run
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                "Index did not set number_of_replicas to ${actionConfig.numOfReplicas}",
                actionConfig.numOfReplicas,
                getNumberOfReplicasSetting(indexName)
            )
        }
    }

    fun `test new node skip execution when old node exist in cluster`() {
        Assume.assumeTrue(isMixedNodeRegressionTest)

        /**
         * mixedCluster-0 is new node, mixedCluster-1 is old node
         *
         * set config index to only have one shard on new node
         * so old node cannot run job because it has no shard
         * new node also cannot run job because there is an old node
         * here we check no job can be run
         *
         * then reroute shard to old node and this old node can run job
         */

        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val actionConfig = ReplicaCountActionConfig(10, 0)
        val states = listOf(State(name = "ReplicaCountState", actions = listOf(actionConfig), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName)

        val settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "0")
            .build()
        updateIndexSettings(INDEX_MANAGEMENT_INDEX, settings)

        // check config index shard position
        val shardsResponse = catIndexShard(INDEX_MANAGEMENT_INDEX)
        logger.info("check config index shard: $shardsResponse")
        val shardNode = (shardsResponse[0] as HashMap<*, *>)["node"]

        sleep(3000) // wait some time for cluster to be stable

        // move shard on node1 to node0 if exist
        if (shardNode == "mixedCluster-1") rerouteShard(INDEX_MANAGEMENT_INDEX, "mixedCluster-1", "mixedCluster-0")

        addPolicyToIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // check no job has been run
        wait { assertEquals(null, getExistingManagedIndexConfig(indexName).policy) }

        // reroute shard to node1
        rerouteShard(INDEX_MANAGEMENT_INDEX, "mixedCluster-0", "mixedCluster-1")

        val shardsResponse2 = catIndexShard(INDEX_MANAGEMENT_INDEX)
        logger.info("check config index shard: $shardsResponse2")

        // job can be ran now
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }
    }
}
