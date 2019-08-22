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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexstatemanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StateMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings
import org.elasticsearch.client.ResponseException
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestStatus
import java.time.Instant
import java.util.Locale

class RestExplainActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test missing indices`() {
        try {
            client().makeRequest(RestRequest.Method.GET.toString(), RestExplainAction.EXPLAIN_BASE_URI)
            fail("Expected a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectedErrorMessage = mapOf(
                "error" to mapOf(
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf("type" to "illegal_argument_exception", "reason" to "Missing indices")
                    ),
                    "type" to "illegal_argument_exception",
                    "reason" to "Missing indices"
                ),
                "status" to 400
            )
            assertEquals(expectedErrorMessage, actualMessage)
        }
    }

    fun `test single index`() {
        val indexName = "${testIndexName}_movies"
        createIndex(indexName, null)
        val response = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/$indexName")
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val expected = mapOf(
            indexName to mapOf<String, String?>(
                ManagedIndexSettings.POLICY_ID.key to null
            )
        )
        val actual = response.asMap()
        assertResponseMap(expected, actual)
    }

    fun `test index pattern`() {
        val indexName1 = "${testIndexName}_video"
        val indexName2 = "${indexName1}_2"
        val indexName3 = "${indexName1}_3"
        createIndex(indexName1, null)
        createIndex(indexName2, null)
        createIndex(indexName3, null)
        val response = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/$indexName1*")
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val expected = mapOf(
            indexName1 to mapOf<String, String?>(
                ManagedIndexSettings.POLICY_ID.key to null
            ),
            indexName2 to mapOf<String, String?>(
                ManagedIndexSettings.POLICY_ID.key to null
            ),
            indexName3 to mapOf<String, String?>(
                ManagedIndexSettings.POLICY_ID.key to null
            )
        )
        val actual = response.asMap()
        assertResponseMap(expected, actual)
    }

    fun `test attached policy`() {
        val indexName = "${testIndexName}_watermelon"
        val policy = createRandomPolicy(refresh = true)
        createIndex(indexName, policy.id)

        Thread.sleep(2000)

        val managedIndexConfig = getManagedIndexConfig(indexName)
        assertNotNull("ManagedIndexConfig is null", managedIndexConfig)
        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig!!, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        val response = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/$indexName")
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())

        val expectedInfoString = mapOf("message" to "Successfully initialized policy: ${policy.id}").toString()
        val actual = response.asMap()
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
                        assertStateEquals(StateMetaData(policy.defaultState, Instant.now().toEpochMilli()), stateMetaDataMap),
                    PolicyRetryInfoMetaData.RETRY_INFO to fun(retryInfoMetaDataMap: Any?): Boolean =
                        assertRetryInfoEquals(PolicyRetryInfoMetaData(false, 0), retryInfoMetaDataMap),
                    ManagedIndexMetaData.INFO to fun(info: Any?): Boolean = expectedInfoString == info.toString()
                )
            ), actual)
    }

    fun `test failed policy`() {
        val indexName = "${testIndexName}_melon"
        val policyID = "does_not_exist"
        createIndex(indexName, policyID)

        Thread.sleep(2000)

        val managedIndexConfig = getManagedIndexConfig(indexName)
        assertNotNull("ManagedIndexConfig is null", managedIndexConfig)
        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig!!, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        val response = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/$indexName")
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())

        val expectedInfoString = mapOf("message" to "Fail to load policy: $policyID").toString()
        val actual = response.asMap()
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
            ), actual)
    }

    @Suppress("UNCHECKED_CAST") // Do assertion of the response map here so we don't have many places to do suppression.
    private fun assertResponseMap(expected: Map<String, Map<String, Any?>>, actual: Map<String, Any>) {
        actual as Map<String, Map<String, String?>>
        assertEquals("Explain Map does not match", expected.size, actual.size)
        for (metaDataEntry in expected) {
            assertMetaDataEntries(metaDataEntry.value, actual[metaDataEntry.key]!!)
        }
    }

    private fun assertMetaDataEntries(expected: Map<String, Any?>, actual: Map<String, String?>) {
        assertEquals("MetaDataSize are not the same", expected.size, actual.size)
        for (entry in expected) {
            assertEquals("Expected and actual values does not match", entry.value, actual[entry.key])
        }
    }
}
