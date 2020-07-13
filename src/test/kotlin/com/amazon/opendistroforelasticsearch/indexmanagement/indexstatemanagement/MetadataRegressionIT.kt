package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ReplicaCountActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.Index
import org.junit.Assume
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class MetadataRegressionIT : IndexStateManagementITTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test still have metadata saved in cluster state`() {
        /**
         *  create index, manually save metadata into this index's cluster state;
         *  configure and start a job, check if metadata moved from cluster state to config index;
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
                UpdateManagedIndexMetaDataAction.INSTANCE, request).get()

        logger.info(response.isAcknowledged)
        indexMetadata = getIndexMetadata(indexName)
        logger.info("check if metadata is saved in cluster state: ${indexMetadata.getCustomData("managed_index_metadata")}")

        // start a job run
        addPolicyToIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // cluster state metadata is removed, explain API can get metadata from config index
        waitFor { assertEquals(null, getIndexMetadata(indexName).getCustomData("managed_index_metadata")) }
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }
        waitFor { assertEquals("Successfully initialized policy: ${policy.id}", getExplainManagedIndexMetaData(indexName).info?.get("message")) }

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals("Index did not set number_of_replicas to ${actionConfig.numOfReplicas}", actionConfig.numOfReplicas, getNumberOfReplicasSetting(indexName)) }
    }

    fun `test job can continue run from cluster state metadata`() {
        /**
         *  create index, add policy to it
         *  manually add policy field to managed-index so runner won't initialise
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
                UpdateManagedIndexMetaDataAction.INSTANCE, request).get()

        logger.info(response.isAcknowledged)
        indexMetadata = getIndexMetadata(indexName)
        logger.info("check if metadata is saved in cluster state: ${indexMetadata.getCustomData("managed_index_metadata")}")

        // start the job run
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals("Index did not set number_of_replicas to ${actionConfig.numOfReplicas}", actionConfig.numOfReplicas, getNumberOfReplicasSetting(indexName))
        }
    }

    fun `test new node skip execution when old node exist in cluster`() {
        Assume.assumeTrue(isMixedNodeRegressionTest)

        /**
         * mixedCluster-0 is new node, mixedCluster-1 is old node
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

        val configIndexName = ".opendistro-ism-config"

        val settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "0")
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "1")
                .build()
        updateIndexSettings(configIndexName, settings)

        // check config index shard position
        val shardsResponse = catIndexShard(configIndexName)
        logger.info("check config index shard: $shardsResponse")
        val shardNode = (shardsResponse[0] as HashMap<*, *>)["node"]

        // move shard on node1 to node0 if exist
        if (shardNode == "mixedCluster-1") rerouteShard(configIndexName, "mixedCluster-1", "mixedCluster-0")

        addPolicyToIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // check no job has been run
        wait { assertEquals(null, getExistingManagedIndexConfig(indexName).policy) }

        // reroute shard to node1
        rerouteShard(configIndexName, "mixedCluster-0", "mixedCluster-1")

        val shardsResponse2 = catIndexShard(configIndexName)
        logger.info("check config index shard: $shardsResponse2")

        // job can be ran now
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }
    }
}