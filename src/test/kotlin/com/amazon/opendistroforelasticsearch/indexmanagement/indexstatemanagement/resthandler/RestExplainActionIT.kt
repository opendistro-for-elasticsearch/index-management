/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StateMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import java.time.Instant
import java.util.Locale

class RestExplainActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test single index`() {
        val indexName = "${testIndexName}_movies"
        createIndex(indexName, null)
        val expected = mapOf(
            indexName to mapOf<String, String?>(
                ManagedIndexSettings.POLICY_ID.key to null
            )
        )
        assertResponseMap(expected, getExplainMap(indexName))
    }

    fun `test single index explain all`() {
        val indexName = "${testIndexName}_movies"
        createIndex(indexName, null)
        val expected = mapOf(
            "total_managed_indices" to 0
        )
        assertResponseMap(expected, getExplainMap(null))
    }

    fun `test two indices, one managed one not managed`() {
        // explicitly asks for un-managed index, will return policy_id as null
        val indexName1 = "${testIndexName}_managed"
        val indexName2 = "${testIndexName}_not_managed"
        val policy = createRandomPolicy()
        createIndex(indexName1, policy.id)
        createIndex(indexName2, null)

        val expected = mapOf(
            indexName1 to mapOf<String, Any>(
                ManagedIndexSettings.POLICY_ID.key to policy.id,
                "index" to indexName1,
                "index_uuid" to getUuid(indexName1),
                "policy_id" to policy.id
            ),
            indexName2 to mapOf<String, Any?>(
                ManagedIndexSettings.POLICY_ID.key to null
            )
        )
        waitFor {
            assertResponseMap(expected, getExplainMap("$indexName1,$indexName2"))
        }
    }

    fun `test two indices, one managed one not managed explain all`() {
        // explain all returns only managed indices
        val indexName1 = "${testIndexName}_managed"
        val indexName2 = "${testIndexName}_not_managed"
        val policy = createRandomPolicy()
        createIndex(indexName1, policy.id)
        createIndex(indexName2, null)

        val expected = mapOf(
            indexName1 to mapOf<String, Any>(
                ManagedIndexSettings.POLICY_ID.key to policy.id,
                "index" to indexName1,
                "index_uuid" to getUuid(indexName1),
                "policy_id" to policy.id,
                "enabled" to true
            ),
            "total_managed_indices" to 1
        )
        waitFor {
            assertResponseMap(expected, getExplainMap(null))
        }
    }

    fun `test index pattern`() {
        val indexName1 = "${testIndexName}_pattern"
        val indexName2 = "${indexName1}_2"
        val indexName3 = "${indexName1}_3"
        val policy = createRandomPolicy()
        createIndex(indexName1, policyID = policy.id)
        createIndex(indexName2, policyID = policy.id)
        createIndex(indexName3, null)
        val expected = mapOf(
            indexName1 to mapOf<String, Any>(
                ManagedIndexSettings.POLICY_ID.key to policy.id,
                "index" to indexName1,
                "index_uuid" to getUuid(indexName1),
                "policy_id" to policy.id
            ),
            indexName2 to mapOf<String, Any>(
                ManagedIndexSettings.POLICY_ID.key to policy.id,
                "index" to indexName2,
                "index_uuid" to getUuid(indexName2),
                "policy_id" to policy.id
            ),
            indexName3 to mapOf<String, Any?>(
                ManagedIndexSettings.POLICY_ID.key to null
            )
        )
        waitFor {
            assertResponseMap(expected, getExplainMap("$indexName1*"))
        }
    }

    fun `test attached policy`() {
        val indexName = "${testIndexName}_watermelon"
        val policy = createRandomPolicy()
        createIndex(indexName, policy.id)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val expectedInfoString = mapOf("message" to "Successfully initialized policy: ${policy.id}").toString()
            assertPredicatesOnMetaData(
                listOf(
                    indexName to listOf(
                        ManagedIndexSettings.POLICY_ID.key to policy.id::equals,
                        ManagedIndexMetaData.INDEX to managedIndexConfig.index::equals,
                        ManagedIndexMetaData.INDEX_UUID to managedIndexConfig.indexUuid::equals,
                        ManagedIndexMetaData.POLICY_ID to managedIndexConfig.policyID::equals,
                        ManagedIndexMetaData.POLICY_SEQ_NO to policy.seqNo.toInt()::equals,
                        ManagedIndexMetaData.POLICY_PRIMARY_TERM to policy.primaryTerm.toInt()::equals,
                        StateMetaData.STATE to fun(stateMetaDataMap: Any?): Boolean =
                            assertStateEquals(
                                StateMetaData(policy.defaultState, Instant.now().toEpochMilli()),
                                stateMetaDataMap
                            ),
                        PolicyRetryInfoMetaData.RETRY_INFO to fun(retryInfoMetaDataMap: Any?): Boolean =
                            assertRetryInfoEquals(PolicyRetryInfoMetaData(false, 0), retryInfoMetaDataMap),
                        ManagedIndexMetaData.INFO to fun(info: Any?): Boolean = expectedInfoString == info.toString()
                    )
                ), getExplainMap(indexName))
        }
    }

    fun `test failed policy`() {
        val indexName = "${testIndexName}_melon"
        val policyID = "${testIndexName}_does_not_exist"
        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val expectedInfoString = mapOf("message" to "Fail to load policy: $policyID").toString()
            assertPredicatesOnMetaData(
                listOf(
                    indexName to listOf(
                        ManagedIndexSettings.POLICY_ID.key to policyID::equals,
                        ManagedIndexMetaData.INDEX to managedIndexConfig.index::equals,
                        ManagedIndexMetaData.INDEX_UUID to managedIndexConfig.indexUuid::equals,
                        ManagedIndexMetaData.POLICY_ID to managedIndexConfig.policyID::equals,
                        PolicyRetryInfoMetaData.RETRY_INFO to fun(retryInfoMetaDataMap: Any?): Boolean =
                            assertRetryInfoEquals(PolicyRetryInfoMetaData(true, 0), retryInfoMetaDataMap),
                        ManagedIndexMetaData.INFO to fun(info: Any?): Boolean = expectedInfoString == info.toString()
                    )
                ), getExplainMap(indexName))
        }
    }

    @Suppress("UNCHECKED_CAST") // Do assertion of the response map here so we don't have many places to do suppression.
    private fun assertResponseMap(expected: Map<String, Any>, actual: Map<String, Any>) {
        assertEquals("Explain Map does not match", expected.size, actual.size)
        for (metaDataEntry in expected) {
            if (metaDataEntry.key == "total_managed_indices") {
                assertEquals(metaDataEntry.value, actual[metaDataEntry.key])
                continue
            }
            val value = metaDataEntry.value as Map<String, Any?>
            actual as Map<String, Map<String, String?>>
            assertMetaDataEntries(value, actual[metaDataEntry.key]!!)
        }
    }

    private fun assertMetaDataEntries(expected: Map<String, Any?>, actual: Map<String, Any?>) {
        assertEquals("MetaDataSize are not the same", expected.size, actual.size)
        for (entry in expected) {
            assertEquals("Expected and actual values does not match", entry.value, actual[entry.key])
        }
    }
}
