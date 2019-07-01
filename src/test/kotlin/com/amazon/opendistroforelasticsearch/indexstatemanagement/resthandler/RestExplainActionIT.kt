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
            fail("Excepted a failure.")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus.", RestStatus.BAD_REQUEST, e.response.restStatus())
            logger.info(e.response)
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
        assertEquals("Unexpected RestStatus.", RestStatus.OK, response.restStatus())
        val expected = mapOf(
            "movies" to mapOf<String, String?>(
                "index.opendistro.index_state_management.policy_name" to null
            )
        )
        val actual = response.asMap()
        assertResponseMap(expected, actual)
    }

    fun `test index pattern`() {
        val response = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/movies*")
        assertEquals("Unexpected RestStatus.", RestStatus.OK, response.restStatus())
        val expected = mapOf(
            "movies" to mapOf<String, String?>(
                "index.opendistro.index_state_management.policy_name" to null
            ),
            "movies_1" to mapOf<String, String?>(
                "index.opendistro.index_state_management.policy_name" to null
            ),
            "movies_2" to mapOf<String, String?>(
                "index.opendistro.index_state_management.policy_name" to null
            )
        )
        val actual = response.asMap()
        assertResponseMap(expected, actual)
    }

    fun `test attached policy`() {
        val policyIndexName = "test_policy_index"
        val policy = createRandomPolicy(refresh = true)
        createIndex(policyIndexName, policyName = policy.id)

        Thread.sleep(2000)

        val managedIndexConfig = getManagedIndexConfig(policyIndexName)
        assertNotNull("ManagedIndexConfig is null", managedIndexConfig)
        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig!!, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        val response = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/$policyIndexName")
        assertEquals("Unexpected RestStatus.", RestStatus.OK, response.restStatus())

        val actual = response.asMap()

        assertPredicatesOnMetaData(listOf(
            policyIndexName to listOf(
            "index.opendistro.index_state_management.policy_name" to policy.id::equals,
            ManagedIndexMetaData.INDEX to managedIndexConfig.index::equals,
            ManagedIndexMetaData.INDEX_UUID to managedIndexConfig.indexUuid::equals,
            ManagedIndexMetaData.POLICY_NAME to managedIndexConfig.policyName::equals,
            ManagedIndexMetaData.POLICY_SEQ_NO to policy.seqNo.toInt()::equals,
            ManagedIndexMetaData.POLICY_PRIMARY_TERM to policy.primaryTerm.toInt()::equals,
            ManagedIndexMetaData.STATE to policy.defaultState::equals,
            ManagedIndexMetaData.STATE_START_TIME to fun(startTime: Any?): Boolean = (startTime as Long) < Instant.now().toEpochMilli(),
            ManagedIndexMetaData.FAILED to false::equals
        )), actual)
    }

    fun `test failed policy`() {
        val policyIndexName = "test_policy_index"
        val policyName = "does_not_exist"
        createIndex(policyIndexName, policyName = policyName)

        Thread.sleep(2000)

        val managedIndexConfig = getManagedIndexConfig(policyIndexName)
        assertNotNull("ManagedIndexConfig is null", managedIndexConfig)
        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig!!, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        val response = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/$policyIndexName")
        assertEquals("Unexpected RestStatus.", RestStatus.OK, response.restStatus())

        val expectedInfoString = mapOf("message" to "Could not load policy: $policyName").toString()
        val actual = response.asMap()
        assertPredicatesOnMetaData(listOf(
                policyIndexName to listOf(
                    "index.opendistro.index_state_management.policy_name" to policyName::equals,
                    ManagedIndexMetaData.INDEX to managedIndexConfig.index::equals,
                    ManagedIndexMetaData.INDEX_UUID to managedIndexConfig.indexUuid::equals,
                    ManagedIndexMetaData.POLICY_NAME to managedIndexConfig.policyName::equals,
                    ManagedIndexMetaData.FAILED to true::equals,
                    ManagedIndexMetaData.INFO to fun(info: Any?): Boolean = expectedInfoString == info.toString()
                )), actual)
    }

    @Suppress("UNCHECKED_CAST") // Do assertion of the response map here so we don't have many places to do suppression.
    private fun assertResponseMap(expected: Map<String, Map<String, String?>>, actual: Map<String, Any>) {
        actual as Map<String, Map<String, String?>>
        assertEquals("Explain Map does not match.", expected.size, actual.size)
        for (metaDataEntry in expected) {
            assertMetaDataEntries(metaDataEntry.value, actual[metaDataEntry.key]!!)
        }
    }

    private fun assertMetaDataEntries(expected: Map<String, String?>, actual: Map<String, String?>) {
        assertEquals("MetaDataSize are not the same", expected.size, actual.size)
        for (entry in expected) {
            assertEquals("Expected and actual values does not match.", entry.value, actual[entry.key])
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
                assertTrue("Failed predicate assertion for $fieldName", predicate(indexResponse[fieldName]))
            }
        }
    }
}
