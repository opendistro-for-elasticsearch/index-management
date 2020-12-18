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

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StateMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.rollover.AttemptRolloverStep
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
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
        createPolicyJson(testPolicy, policyID)
        val expectedInfoString = mapOf("message" to AttemptRolloverStep.getFailedNoValidAliasMessage(indexName)).toString()

        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // Change the start time so the job will trigger in 2 seconds.
        // First execution. We need to initialize the policy.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Second execution is to fail the step once.
        updateManagedIndexConfigStartTime(managedIndexConfig)

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
        updateManagedIndexConfigStartTime(managedIndexConfig)

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
        updateManagedIndexConfigStartTime(managedIndexConfig)

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
        updateManagedIndexConfigStartTime(managedIndexConfig)
        // First execution. We need to initialize the policy.

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Second execution is to fail the step once.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(1, getExplainManagedIndexMetaData(indexName).actionMetaData?.consumedRetries) }

        // Third execution should not run job since we have the retry backoff.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        Thread.sleep(5000) // currently there is nothing to compare when backing off so we have to sleep

        // Fourth execution should not run job since we have the retry backoff.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // even if we ran couple times we should have backed off and only retried once.
        waitFor {
            val expectedInfoString = mapOf("message" to AttemptRolloverStep.getFailedNoValidAliasMessage(indexName)).toString()
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
