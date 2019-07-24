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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.RetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StateMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings
import org.elasticsearch.client.ResponseException
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestStatus
import org.junit.Before
import java.time.Instant

class RestExplainActionIT : IndexStateManagementRestTestCase() {

    @Before
    fun setup() {
        createIndex("movies", null)
        createIndex("movies_1", null)
        createIndex("movies_2", null)
        createIndex("other_index", null)
    }

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
        val response = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/movies")
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val expected = mapOf(
            "movies" to mapOf<String, String?>(
                ManagedIndexSettings.POLICY_ID.key to null
            )
        )
        val actual = response.asMap()
        assertResponseMap(expected, actual)
    }

    fun `test index pattern`() {
        val response = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/movies*")
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val expected = mapOf(
            "movies" to mapOf<String, String?>(
                ManagedIndexSettings.POLICY_ID.key to null
            ),
            "movies_1" to mapOf<String, String?>(
                ManagedIndexSettings.POLICY_ID.key to null
            ),
            "movies_2" to mapOf<String, String?>(
                ManagedIndexSettings.POLICY_ID.key to null
            )
        )
        val actual = response.asMap()
        assertResponseMap(expected, actual)
    }

    fun `test attached policy`() {
        val policyIndexName = "test_policy_index"
        val policy = createRandomPolicy(refresh = true)
        createIndex(policyIndexName, policy.id)

        Thread.sleep(2000)

        val managedIndexConfig = getManagedIndexConfig(policyIndexName)
        assertNotNull("ManagedIndexConfig is null", managedIndexConfig)
        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig!!, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        val response = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/$policyIndexName")
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())

        val expectedInfoString = mapOf("message" to "Successfully initialized policy: ${policy.id}").toString()
        val actual = response.asMap()
        assertPredicatesOnMetaData(
            listOf(
                policyIndexName to listOf(
                    ManagedIndexSettings.POLICY_ID.key to policy.id::equals,
                    ManagedIndexMetaData.INDEX to managedIndexConfig.index::equals,
                    ManagedIndexMetaData.INDEX_UUID to managedIndexConfig.indexUuid::equals,
                    ManagedIndexMetaData.POLICY_ID to managedIndexConfig.policyID::equals,
                    ManagedIndexMetaData.POLICY_SEQ_NO to policy.seqNo.toInt()::equals,
                    ManagedIndexMetaData.POLICY_PRIMARY_TERM to policy.primaryTerm.toInt()::equals,
                    StateMetaData.STATE to fun(stateMetaDataMap: Any?): Boolean =
                        assertState(StateMetaData(policy.defaultState, Instant.now().toEpochMilli()), stateMetaDataMap),
                    RetryInfoMetaData.RETRY_INFO to fun(retryInfoMetaDataMap: Any?): Boolean =
                        assertRetryInfo(RetryInfoMetaData(false, 0), retryInfoMetaDataMap),
                    ManagedIndexMetaData.INFO to fun(info: Any?): Boolean = expectedInfoString == info.toString()
                )
            ), actual)
    }

    fun `test failed policy`() {
        val policyIndexName = "test_failed_policy_index"
        val policyID = "does_not_exist"
        createIndex(policyIndexName, policyID)

        Thread.sleep(2000)

        val managedIndexConfig = getManagedIndexConfig(policyIndexName)
        assertNotNull("ManagedIndexConfig is null", managedIndexConfig)
        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig!!, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        val response = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/$policyIndexName")
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())

        val expectedInfoString = mapOf("message" to "Fail to load policy: $policyID").toString()
        val actual = response.asMap()
        assertPredicatesOnMetaData(
            listOf(
                policyIndexName to listOf(
                    ManagedIndexSettings.POLICY_ID.key to policyID::equals,
                    ManagedIndexMetaData.INDEX to managedIndexConfig.index::equals,
                    ManagedIndexMetaData.INDEX_UUID to managedIndexConfig.indexUuid::equals,
                    ManagedIndexMetaData.POLICY_ID to managedIndexConfig.policyID::equals,
                    RetryInfoMetaData.RETRY_INFO to fun(retryInfoMetaDataMap: Any?): Boolean =
                        assertRetryInfo(RetryInfoMetaData(true, 0), retryInfoMetaDataMap),
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

    /**
     * indexPredicates is a list of pairs where first is index name and second is a list of pairs
     * where first is key property and second is predicate function to assert on
     */
    @Suppress("UNCHECKED_CAST")
    private fun assertPredicatesOnMetaData(indexPredicates: List<Pair<String, List<Pair<String, (Any?) -> Boolean>>>>, response: Map<String, Any?>) {
        indexPredicates.forEach { (index, predicates) ->
            assertTrue("The index: $index was not found in the response", response.containsKey(index))
            val indexResponse = response[index] as Map<String, String?>
            assertEquals("The fields do not match, response=($indexResponse) predicates=$predicates", predicates.map { it.first }.toSet(), indexResponse.keys.toSet())
            predicates.forEach { (fieldName, predicate) ->
                assertTrue("The key: $fieldName was not found in the response", indexResponse.containsKey(fieldName))
                assertTrue("Failed predicate assertion for $fieldName response=($indexResponse) predicates=$predicates", predicate(indexResponse[fieldName]))
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun assertRetryInfo(expectedRetryInfo: RetryInfoMetaData, actualRetryInfoMetaDataMap: Any?): Boolean {
        actualRetryInfoMetaDataMap as Map<String, Any>
        assertEquals(expectedRetryInfo.failed, actualRetryInfoMetaDataMap[RetryInfoMetaData.FAILED] as Boolean)
        assertEquals(expectedRetryInfo.consumedRetries, actualRetryInfoMetaDataMap[RetryInfoMetaData.CONSUMED_RETRIES] as Int)
        return true
    }

    @Suppress("UNCHECKED_CAST")
    private fun assertState(expectedState: StateMetaData, actualStateMap: Any?): Boolean {
        actualStateMap as Map<String, Any>
        assertEquals(expectedState.name, actualStateMap[ManagedIndexMetaData.NAME] as String)
        assertTrue((actualStateMap[ManagedIndexMetaData.START_TIME] as Long) < expectedState.startTime)
        return true
    }
}
