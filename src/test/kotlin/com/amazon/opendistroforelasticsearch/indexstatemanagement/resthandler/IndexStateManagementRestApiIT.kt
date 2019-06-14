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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.INDEX_STATE_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.POLICY_BASE_URI
import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexstatemanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomPolicy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util._ID
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util._PRIMARY_TERM
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util._SEQ_NO
import org.elasticsearch.client.ResponseException
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.ESTestCase
import org.elasticsearch.test.junit.annotations.TestLogging

@TestLogging("level:DEBUG")
@Suppress("UNCHECKED_CAST")
class IndexStateManagementRestApiIT : IndexStateManagementRestTestCase() {

    @Throws(Exception::class)
    fun `test plugins are loaded`() {
        val response = entityAsMap(client().makeRequest("GET", "_nodes/plugins"))
        val nodesInfo = response["nodes"] as Map<String, Map<String, Any>>
        var hasIndexStateMangementPlugin = false
        var hasJobSchedulerPlugin = false
        for (nodeInfo in nodesInfo.values) {
            val plugins = nodeInfo["plugins"] as List<Map<String, Any>>

            for (plugin in plugins) {
                if (plugin["name"] == "opendistro-index-state-management") {
                    hasIndexStateMangementPlugin = true
                }
                if (plugin["name"] == "opendistro-job-scheduler") {
                    hasJobSchedulerPlugin = true
                }
            }

            if (hasIndexStateMangementPlugin && hasJobSchedulerPlugin) {
                return
            }
        }
        fail("Plugins not installed, ISMPlugin loaded: $hasIndexStateMangementPlugin, JobScheduler loaded: $hasJobSchedulerPlugin")
    }

    @Throws(Exception::class)
    fun `test creating a policy`() {
        val policy = randomPolicy()
        val policyId = ESTestCase.randomAlphaOfLength(10)
        val createResponse = client().makeRequest("PUT", "$POLICY_BASE_URI/$policyId", emptyMap(), policy.toHttpEntity())

        assertEquals("Create policy failed", RestStatus.CREATED, createResponse.restStatus())
        val responseBody = createResponse.asMap()
        val createdId = responseBody["_id"] as String
        val createdSeqNo = responseBody[_SEQ_NO] as Int
        val createdPrimaryTerm = responseBody[_PRIMARY_TERM] as Int
        assertNotEquals("response is missing Id", Policy.NO_ID, createdId)
        assertEquals("not same id", policyId, createdId)
        assertEquals("incorrect seqNo", 0, createdSeqNo)
        assertEquals("incorrect primaryTerm", 1, createdPrimaryTerm)

        assertEquals("Incorrect Location header", "$POLICY_BASE_URI/$createdId", createResponse.getHeader("Location"))
    }

    @Throws(Exception::class)
    fun `test creating a policy with no id fails`() {
        try {
            val policy = randomPolicy()
            client().makeRequest("PUT", POLICY_BASE_URI, emptyMap(), policy.toHttpEntity())
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test creating a policy with POST fails`() {
        try {
            val policy = randomPolicy()
            client().makeRequest("POST", "$POLICY_BASE_URI/some_policy", emptyMap(), policy.toHttpEntity())
            fail("Expected 405 Method Not Allowed response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.METHOD_NOT_ALLOWED, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test mappings after policy creation`() {
        createRandomPolicy(refresh = true)

        val response = client().makeRequest("GET", "/$INDEX_STATE_MANAGEMENT_INDEX/_mapping")
        val parserMap = createParser(XContentType.JSON.xContent(), response.entity.content).map() as Map<String, Map<String, Any>>
        val mappingsMap = parserMap[INDEX_STATE_MANAGEMENT_INDEX]!!["mappings"] as Map<String, Any>
        val expected = createParser(
                XContentType.JSON.xContent(),
                javaClass.classLoader.getResource("mappings/opendistro-ism-config.json").readText())
        val expectedMap = expected.map()

        assertEquals("Mappings are different", expectedMap, mappingsMap)
    }

    @Throws(Exception::class)
    fun `test update policy with wrong seq_no and primary_term`() {
        val policy = createRandomPolicy(refresh = true)

        try {
            client().makeRequest("PUT",
                    "$POLICY_BASE_URI/${policy.id}?refresh=true&if_seq_no=10251989&if_primary_term=2342",
                    emptyMap(), policy.toHttpEntity())
            fail("expected 409 ResponseException")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.CONFLICT, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test update policy with correct seq_no and primary_term`() {
        val policy = createRandomPolicy(refresh = true)
        val updateResponse = client().makeRequest("PUT",
                "$POLICY_BASE_URI/${policy.id}?refresh=true&if_seq_no=${policy.seqNo}&if_primary_term=${policy.primaryTerm}",
                emptyMap(), policy.toHttpEntity())

        assertEquals("Update policy failed", RestStatus.OK, updateResponse.restStatus())
        val responseBody = updateResponse.asMap()
        val updatedId = responseBody[_ID] as String
        val updatedSeqNo = (responseBody[_SEQ_NO] as Int).toLong()
        assertNotEquals("response is missing Id", Policy.NO_ID, updatedId)
        assertEquals("not same id", policy.id, updatedId)
        assertEquals("incorrect seqNo", policy.seqNo + 1, updatedSeqNo)
    }

    @Throws(Exception::class)
    fun `test deleting a policy`() {
        val policy = createRandomPolicy()

        val deleteResponse = client().makeRequest("DELETE", "$POLICY_BASE_URI/${policy.id}?refresh=true")
        assertEquals("Delete failed", RestStatus.OK, deleteResponse.restStatus())

        val getResponse = client().makeRequest("HEAD", "$POLICY_BASE_URI/${policy.id}")
        assertEquals("Deleted policy still exists", RestStatus.NOT_FOUND, getResponse.restStatus())
    }

    @Throws(Exception::class)
    fun `test deleting a policy that doesn't exist`() {
        try {
            client().makeRequest("DELETE", "$POLICY_BASE_URI/foobarbaz")
            fail("expected 404 ResponseException")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test getting a policy`() {
        val policy = createRandomPolicy()

        val indexedPolicy = getPolicy(policy.id)

        assertEquals("Indexed and retrieved policy differ", policy, indexedPolicy)
    }

    @Throws(Exception::class)
    fun `test getting a policy that doesn't exist`() {
        try {
            getPolicy(randomAlphaOfLength(20))
            fail("expected response exception")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test checking if a policy exists`() {
        val policy = createRandomPolicy()

        val headResponse = client().makeRequest("HEAD", "$POLICY_BASE_URI/${policy.id}")
        assertEquals("Unable to HEAD policy", RestStatus.OK, headResponse.restStatus())
        assertNull("Response contains unexpected body", headResponse.entity)
    }

    @Throws(Exception::class)
    fun `test checking if a non-existent policy exists`() {
        val headResponse = client().makeRequest("HEAD", "$POLICY_BASE_URI/foobarbaz")
        assertEquals("Unexpected status", RestStatus.NOT_FOUND, headResponse.restStatus())
    }
}
