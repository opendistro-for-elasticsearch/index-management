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

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.POLICY_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomPolicy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomReadOnlyActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomState
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.util._ID
import com.amazon.opendistroforelasticsearch.indexmanagement.util._PRIMARY_TERM
import com.amazon.opendistroforelasticsearch.indexmanagement.util._SEQ_NO
import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.ResponseException
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.common.xcontent.json.JsonXContent.jsonXContent
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.ESTestCase
import org.elasticsearch.test.junit.annotations.TestLogging

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
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
                if (plugin["name"] == "opendistro_index_management") {
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
    fun `test creating a policy with a disallowed actions fails`() {
        try {
            // remove read_only from the allowlist
            val allowedActions = ActionConfig.ActionType.values().toList()
                .filter { actionType -> actionType != ActionConfig.ActionType.READ_ONLY }
                .joinToString(prefix = "[", postfix = "]") { string -> "\"$string\"" }
            updateClusterSetting(ManagedIndexSettings.ALLOW_LIST.key, allowedActions, escapeValue = false)
            val policy = randomPolicy(states = listOf(randomState(actions = listOf(randomReadOnlyActionConfig()))))
            client().makeRequest("PUT", "$POLICY_BASE_URI/some_id", emptyMap(), policy.toHttpEntity())
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test updating a policy with a disallowed actions fails`() {
        try {
            // remove read_only from the allowlist
            val allowedActions = ActionConfig.ActionType.values().toList()
                .filter { actionType -> actionType != ActionConfig.ActionType.READ_ONLY }
                .joinToString(prefix = "[", postfix = "]") { string -> "\"$string\"" }
            updateClusterSetting(ManagedIndexSettings.ALLOW_LIST.key, allowedActions, escapeValue = false)
            // createRandomPolicy currently does not create a random list of actions so it won't accidentally create one with read_only
            val policy = createRandomPolicy()
            // update the policy to have read_only action which is not allowed
            val updatedPolicy = policy.copy(defaultState = "some_state", states = listOf(randomState(name = "some_state", actions = listOf(randomReadOnlyActionConfig()))))
            client().makeRequest("PUT",
                "$POLICY_BASE_URI/${updatedPolicy.id}?refresh=true&if_seq_no=${updatedPolicy.seqNo}&if_primary_term=${updatedPolicy.primaryTerm}",
                emptyMap(), updatedPolicy.toHttpEntity())
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
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
        createRandomPolicy()

        val response = client().makeRequest("GET", "/$INDEX_MANAGEMENT_INDEX/_mapping")
        val parserMap = createParser(XContentType.JSON.xContent(), response.entity.content).map() as Map<String, Map<String, Any>>
        val mappingsMap = parserMap[INDEX_MANAGEMENT_INDEX]!!["mappings"] as Map<String, Any>
        val expected = createParser(
                XContentType.JSON.xContent(),
                javaClass.classLoader.getResource("mappings/opendistro-ism-config.json").readText())
        val expectedMap = expected.map()

        assertEquals("Mappings are different", expectedMap, mappingsMap)
    }

    @Throws(Exception::class)
    fun `test update policy with wrong seq_no and primary_term`() {
        val policy = createRandomPolicy()

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
        val policy = createRandomPolicy()
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

    @Throws(Exception::class)
    fun `test able to fuzzy search policies`() {
        val policy = createRandomPolicy()

        val request = """
            {
                "query": {
                    "query_string": {
                        "default_field": "${Policy.POLICY_TYPE}.${Policy.POLICY_ID_FIELD}",
                        "default_operator": "AND",
                        "query": "*${policy.id.substring(4, 7)}*"
                    }
                }
            }
        """.trimIndent()
        val response = client().makeRequest("POST", "$INDEX_MANAGEMENT_INDEX/_search", emptyMap(),
                StringEntity(request, APPLICATION_JSON))
        assertEquals("Request failed", RestStatus.OK, response.restStatus())
        val searchResponse = SearchResponse.fromXContent(createParser(jsonXContent, response.entity.content))
        assertTrue("Did not find policy using fuzzy search", searchResponse.hits.hits.size == 1)
    }
}
