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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ManagedIndexMetaData
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

    fun `test missing Indices`() {
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
        createIndex(policyIndexName, policyName = policy.name)

        Thread.sleep(2000)

        val managedIndexConfig = getManagedIndexConfig(policyIndexName)
        assertNotNull("ManagedIndexConfig is null", managedIndexConfig)
        val update = managedIndexConfig!!.copy(jobEnabledTime = Instant.now().minusSeconds(57))
        updateManagedIndexConfigEnabledTime(update)

        Thread.sleep(3000)

        val response = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/$policyIndexName")
        assertEquals("Unexpected RestStatus.", RestStatus.OK, response.restStatus())

        val expected = mapOf(
            policyIndexName to mapOf<String, String?>(
                "index.opendistro.index_state_management.policy_name" to policy.name,
                ManagedIndexMetaData.INDEX_UUID to managedIndexConfig.indexUuid,
                ManagedIndexMetaData.POLICY_NAME to "${policyIndexName}_POLICY_NAME",
                ManagedIndexMetaData.POLICY_VERSION to "${policyIndexName}_POLICY_VERSION",
                ManagedIndexMetaData.STATE to "${policyIndexName}_STATE",
                ManagedIndexMetaData.STATE_START_TIME to "${policyIndexName}_STATE_START_TIME",
                ManagedIndexMetaData.ACTION_INDEX to "${policyIndexName}_ACTION_INDEX",
                ManagedIndexMetaData.ACTION to "${policyIndexName}_ACTION",
                ManagedIndexMetaData.ACTION_START_TIME to "${policyIndexName}_ACTION_START_TIME",
                ManagedIndexMetaData.STEP to "${policyIndexName}_STEP",
                ManagedIndexMetaData.STEP_START_TIME to "${policyIndexName}_STEP_START_TIME",
                ManagedIndexMetaData.FAILED_STEP to "${policyIndexName}_FAILED_STEP"
            )
        )
        val actual = response.asMap()
        assertResponseMap(expected, actual)
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
}
