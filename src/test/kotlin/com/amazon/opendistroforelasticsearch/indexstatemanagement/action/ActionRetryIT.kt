package com.amazon.opendistroforelasticsearch.indexstatemanagement.action

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StateMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexstatemanagement.waitFor
import java.time.Instant
import java.util.Locale

class ActionRetryIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    /**
     * We are forcing RollOver to fail in this Integ test.
     */
    fun `test failed action`() {
        val testPolicy = """
        {"policy":{"description":"Default policy","default_state":"Ingest","states":[
        {"name":"Ingest","actions":[{"retry":{"count":2,"backoff":"constant","delay":"1s"},"rollover":{"min_doc_count":100}}],"transitions":[{"state_name":"Search"}]},
        {"name":"Search","actions":[],"transitions":[{"state_name":"Delete","conditions":{"min_index_age":"30d"}}]},
        {"name":"Delete","actions":[{"delete":{}}],"transitions":[]}]}}
        """.trimIndent()

        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val policyResponse = createPolicyJson(testPolicy, policyID)
        val expectedInfoString = mapOf("message" to "There is no valid rollover_alias=null set on $indexName").toString()

        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // Change the start time so the job will trigger in 2 seconds.
        // First execution. We need to initialize the policy.
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Second execution is to fail the step once.
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())

        waitFor {
            val managedIndexMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                ActionMetaData("rollover", managedIndexMetaData.actionMetaData?.startTime, 0, false, 1,
                    managedIndexMetaData.actionMetaData?.lastRetryTime, null),
                managedIndexMetaData.actionMetaData
            )

            assertEquals(expectedInfoString, managedIndexMetaData.info.toString())
        }

        // Third execution is to fail the step second time.
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())

        waitFor {
            val managedIndexMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                ActionMetaData("rollover", managedIndexMetaData.actionMetaData?.startTime, 0, false, 2,
                    managedIndexMetaData.actionMetaData?.lastRetryTime, null),
                managedIndexMetaData.actionMetaData
            )

            assertEquals(expectedInfoString, managedIndexMetaData.info.toString())
        }

        // Fourth execution is to fail the step third time and finally fail the action.
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())

        waitFor {
            val managedIndexMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                ActionMetaData("rollover", managedIndexMetaData.actionMetaData?.startTime, 0, true, 2,
                    managedIndexMetaData.actionMetaData?.lastRetryTime, null),
                managedIndexMetaData.actionMetaData
            )

            assertEquals(expectedInfoString, managedIndexMetaData.info.toString())
        }
    }

    fun `test exponential backoff`() {
        val testPolicy = """
        {"policy":{"description":"Default policy","default_state":"Ingest","states":[
        {"name":"Ingest","actions":[{"retry":{"count":2,"backoff":"exponential","delay":"1m"},"rollover":{"min_doc_count":100}}],"transitions":[{"state_name":"Search"}]},
        {"name":"Search","actions":[],"transitions":[{"state_name":"Delete","conditions":{"min_index_age":"30d"}}]},
        {"name":"Delete","actions":[{"delete":{}}],"transitions":[]}]}}
        """.trimIndent()

        val indexName = "${testIndexName}_index_2"
        val policyID = "${testIndexName}_testPolicyName_2"

        val policyResponse = createPolicyJson(testPolicy, policyID)
        val policyResponseMap = policyResponse.asMap()
        val policySeq = policyResponseMap["_seq_no"] as Int
        val policyPrimaryTerm = policyResponseMap["_primary_term"] as Int

        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())
        // First execution. We need to initialize the policy.

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Second execution is to fail the step once.
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())
        Thread.sleep(3000)

        // Third execution should not run job since we have the retry backoff.
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())
        Thread.sleep(3000)

        // Fourth execution should not run job since we have the retry backoff.
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())

        // even if we ran couple times we should have backed off and only retried once.
        waitFor {
            val expectedInfoString = mapOf("message" to "There is no valid rollover_alias=null set on $indexName").toString()
            assertPredicatesOnMetaData(
                listOf(
                    indexName to listOf(
                        ManagedIndexSettings.POLICY_ID.key to policyID::equals,
                        ManagedIndexMetaData.INDEX to managedIndexConfig.index::equals,
                        ManagedIndexMetaData.INDEX_UUID to managedIndexConfig.indexUuid::equals,
                        ManagedIndexMetaData.POLICY_ID to managedIndexConfig.policyID::equals,
                        ManagedIndexMetaData.POLICY_SEQ_NO to policySeq::equals,
                        ManagedIndexMetaData.POLICY_PRIMARY_TERM to policyPrimaryTerm::equals,
                        ManagedIndexMetaData.ROLLED_OVER to false::equals,
                        StateMetaData.STATE to fun(stateMetaDataMap: Any?): Boolean =
                            assertStateEquals(StateMetaData("Ingest", Instant.now().toEpochMilli()), stateMetaDataMap),
                        ActionMetaData.ACTION to fun(actionMetaDataMap: Any?): Boolean =
                            assertActionEquals(
                                ActionMetaData("rollover", Instant.now().toEpochMilli(), 0, false, 1, null, null),
                                actionMetaDataMap
                            ),
                        PolicyRetryInfoMetaData.RETRY_INFO to fun(retryInfoMetaDataMap: Any?): Boolean =
                            assertRetryInfoEquals(PolicyRetryInfoMetaData(false, 0), retryInfoMetaDataMap),
                        ManagedIndexMetaData.INFO to fun(info: Any?): Boolean = expectedInfoString == info.toString()
                    )
                ), getExplainMap(indexName))
        }
    }
}
