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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.FAILED_INDICES
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.FAILURES
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.UPDATED_INDICES
import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.elasticsearch.client.ResponseException
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.RestStatus

class RestAddPolicyActionIT : IndexStateManagementRestTestCase() {

    fun `test missing indices`() {
        try {
            client().makeRequest(POST.toString(), RestAddPolicyAction.ADD_POLICY_BASE_URI)
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

    fun `test closed index`() {
        val index = "movies"
        createIndex(index, null)
        closeIndex(index)

        val response = client().makeRequest(
            POST.toString(),
            "${RestAddPolicyAction.ADD_POLICY_BASE_URI}/$index",
            StringEntity("{ \"policy_id\": \"somePolicy\" }", APPLICATION_JSON)
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedMessage = mapOf(
            FAILURES to true,
            UPDATED_INDICES to 0,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to index,
                    "index_uuid" to getUuid(index),
                    "reason" to "This index is closed"
                )
            )
        )

        assertAffectedIndicesResponseIsEqual(expectedMessage, actualMessage)
    }

    fun `test index with existing policy`() {
        val index = "movies"
        createIndex(index, "somePolicy")

        val response = client().makeRequest(
            POST.toString(),
            "${RestAddPolicyAction.ADD_POLICY_BASE_URI}/$index",
            StringEntity("{ \"policy_id\": \"someOtherPolicy\" }", APPLICATION_JSON)
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedMessage = mapOf(
            FAILURES to true,
            UPDATED_INDICES to 0,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to index,
                    "index_uuid" to getUuid(index),
                    "reason" to "This index already has a policy, use the update policy API to update index policies"
                )
            )
        )

        assertAffectedIndicesResponseIsEqual(expectedMessage, actualMessage)
    }

    fun `test index list`() {
        val indexOne = "movies_1"
        val indexTwo = "movies_2"

        createIndex(indexOne, null)
        createIndex(indexTwo, "somePolicy")

        closeIndex(indexOne)

        val response = client().makeRequest(
            POST.toString(),
            "${RestAddPolicyAction.ADD_POLICY_BASE_URI}/$indexOne,$indexTwo",
            StringEntity("{ \"policy_id\": \"someOtherPolicy\" }", APPLICATION_JSON)
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedMessage = mapOf(
            FAILURES to true,
            UPDATED_INDICES to 0,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to indexOne,
                    "index_uuid" to getUuid(indexOne),
                    "reason" to "This index is closed"
                ),
                mapOf(
                    "index_name" to indexTwo,
                    "index_uuid" to getUuid(indexTwo),
                    "reason" to "This index already has a policy, use the update policy API to update index policies"
                )
            )
        )

        assertAffectedIndicesResponseIsEqual(expectedMessage, actualMessage)
    }

    fun `test index pattern`() {
        val indexPattern = "movies"
        val indexOne = "movies_1"
        val indexTwo = "movies_2"
        val indexThree = "movies_3"

        createIndex(indexOne, null)
        createIndex(indexTwo, "somePolicy")
        createIndex(indexThree, null)

        closeIndex(indexOne)

        val response = client().makeRequest(
            POST.toString(),
            "${RestAddPolicyAction.ADD_POLICY_BASE_URI}/$indexPattern*",
            StringEntity("{ \"policy_id\": \"someOtherPolicy\" }", APPLICATION_JSON)
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedMessage = mapOf(
            UPDATED_INDICES to 1,
            FAILURES to true,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to indexOne,
                    "index_uuid" to getUuid(indexOne),
                    "reason" to "This index is closed"
                ),
                mapOf(
                    "index_name" to indexTwo,
                    "index_uuid" to getUuid(indexTwo),
                    "reason" to "This index already has a policy, use the update policy API to update index policies"
                )
            )
        )

        assertAffectedIndicesResponseIsEqual(expectedMessage, actualMessage)

        // Check if indexThree had policy set
        assertEquals("someOtherPolicy", getPolicyFromIndex(indexThree))
    }
}
