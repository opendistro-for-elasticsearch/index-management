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

package com.amazon.opendistroforelasticsearch.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.INDEX_STATE_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.POLICY_BASE_URI
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ChangePolicy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy.Companion.POLICY_TYPE
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StateMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.FAILED_INDICES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.FAILURES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.UPDATED_INDICES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util._ID
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util._PRIMARY_TERM
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util._SEQ_NO
import org.apache.http.HttpEntity
import org.apache.http.HttpHeaders
import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHeader
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Request
import org.elasticsearch.client.Response
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.common.xcontent.json.JsonXContent
import org.elasticsearch.common.xcontent.json.JsonXContent.jsonXContent
import org.elasticsearch.index.seqno.SequenceNumbers
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.ESTestCase
import org.elasticsearch.test.rest.ESRestTestCase
import org.junit.rules.DisableOnDebug
import java.util.Locale

abstract class IndexStateManagementRestTestCase : ESRestTestCase() {

    private val isDebuggingTest = DisableOnDebug(null).isDebugging
    private val isDebuggingRemoteCluster = System.getProperty("cluster.debug", "false")!!.toBoolean()

    fun Response.asMap(): Map<String, Any> = entityAsMap(this)

    protected fun createPolicy(
        policy: Policy,
        policyId: String = ESTestCase.randomAlphaOfLength(10),
        refresh: Boolean = true
    ): Policy {
        val response = createPolicyJson(policy.toJsonString(), policyId, refresh)

        val policyJson = JsonXContent.jsonXContent
            .createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                response.entity.content
            ).map()
        val createdId = policyJson["_id"] as String
        assertEquals("policy ids are not the same", policyId, createdId)
        return policy.copy(
            id = createdId,
            seqNo = (policyJson["_seq_no"] as Int).toLong(),
            primaryTerm = (policyJson["_primary_term"] as Int).toLong()
        )
    }

    protected fun createPolicyJson(
        policyString: String,
        policyId: String,
        refresh: Boolean = true
    ): Response {
        val response = client()
            .makeRequest(
                "PUT",
                "$POLICY_BASE_URI/$policyId?refresh=$refresh",
                emptyMap(),
                StringEntity(policyString, APPLICATION_JSON)
            )
        assertEquals("Unable to create a new policy", RestStatus.CREATED, response.restStatus())
        return response
    }

    protected fun createRandomPolicy(refresh: Boolean = false): Policy {
        val policy = randomPolicy()
        val policyId = createPolicy(policy, refresh = refresh).id
        return getPolicy(policyId = policyId)
    }

    protected fun getPolicy(
        policyId: String,
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
    ): Policy {
        val response = client().makeRequest("GET", "$POLICY_BASE_URI/$policyId", null, header)
        assertEquals("Unable to get policy $policyId", RestStatus.OK, response.restStatus())

        val parser = createParser(XContentType.JSON.xContent(), response.entity.content)
        ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation)

        lateinit var id: String
        var primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        var seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO
        lateinit var policy: Policy

        while (parser.nextToken() != Token.END_OBJECT) {
            parser.nextToken()

            when (parser.currentName()) {
                _ID -> id = parser.text()
                _SEQ_NO -> seqNo = parser.longValue()
                _PRIMARY_TERM -> primaryTerm = parser.longValue()
                POLICY_TYPE -> policy = Policy.parse(parser)
            }
        }
        return policy.copy(id = id, seqNo = seqNo, primaryTerm = primaryTerm)
    }

    protected fun createIndex(
        index: String = randomAlphaOfLength(10).toLowerCase(Locale.ROOT),
        policyID: String? = randomAlphaOfLength(10)
    ): Pair<String, String?> {
        val settings = Settings.builder().let {
            if (policyID == null) {
                it.putNull(ManagedIndexSettings.POLICY_ID.key)
            } else {
                it.put(ManagedIndexSettings.POLICY_ID.key, policyID)
            }
        }.build()
        createIndex(index, settings)
        return index to policyID
    }

    protected fun addPolicyToIndex(
        index: String,
        policyID: String
    ) {
        val settings = Settings.builder().put(ManagedIndexSettings.POLICY_ID.key, policyID)
        updateIndexSettings(index, settings)
    }

    @Suppress("UNCHECKED_CAST")
    protected fun getPolicyFromIndex(index: String): String? {
        val indexSettings = getIndexSettings(index) as Map<String, Map<String, Map<String, Any?>>>
        return indexSettings[index]!!["settings"]!![ManagedIndexSettings.POLICY_ID.key] as? String
    }

    protected fun getManagedIndexConfig(index: String): ManagedIndexConfig? {
        val params = mapOf("version" to "true")
        val request = """
            {
                "version": true,
                "query": {
                    "term": {
                        "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.INDEX_FIELD}": "$index"
                    }
                }
            }
        """.trimIndent()
        val response = client().makeRequest("POST", "$INDEX_STATE_MANAGEMENT_INDEX/_search", params,
                StringEntity(request, APPLICATION_JSON))
        assertEquals("Request failed", RestStatus.OK, response.restStatus())
        val searchResponse = SearchResponse.fromXContent(createParser(jsonXContent, response.entity.content))
        assertTrue("Found more than one managed index config", searchResponse.hits.hits.size < 2)
        val hit = searchResponse.hits.hits.firstOrNull()
        return hit?.run {
            val xcp = createParser(jsonXContent, this.sourceRef)
            ManagedIndexConfig.parseWithType(xcp, id, seqNo, primaryTerm)
        }
    }

    protected fun updateManagedIndexConfigStartTime(update: ManagedIndexConfig, desiredStartTimeMillis: Long) {
        val response = client().makeRequest("POST", "$INDEX_STATE_MANAGEMENT_INDEX/_update/${update.id}",
            StringEntity(
                "{\"doc\":{\"managed_index\":{\"schedule\":{\"interval\":{\"start_time\":" +
                    "\"$desiredStartTimeMillis\"}}}}}",
                APPLICATION_JSON
            ))

        assertEquals("Request failed", RestStatus.OK, response.restStatus())
    }

    protected fun Response.restStatus(): RestStatus = RestStatus.fromCode(this.statusLine.statusCode)

    protected fun Policy.toHttpEntity(): HttpEntity = StringEntity(toJsonString(), APPLICATION_JSON)

    protected fun ChangePolicy.toHttpEntity(): HttpEntity = StringEntity(toJsonString(), APPLICATION_JSON)

    // Useful settings when debugging to prevent timeouts
    override fun restClientSettings(): Settings {
        return if (isDebuggingTest || isDebuggingRemoteCluster) {
            Settings.builder()
                    .put(CLIENT_SOCKET_TIMEOUT, TimeValue.timeValueMinutes(10))
                    .build()
        } else {
            super.restClientSettings()
        }
    }

    @Suppress("UNCHECKED_CAST")
    protected fun getIndexState(indexName: String): String {
        val request = Request("GET", "/_cluster/state")
        val response = client().performRequest(request)

        val responseMap = response.asMap()
        val metadata = responseMap["metadata"] as Map<String, Any>
        val indexMetaData = metadata["indices"] as Map<String, Any>
        val myIndex = indexMetaData[indexName] as Map<String, Any>
        return myIndex["state"] as String
    }

    @Suppress("UNCHECKED_CAST")
    protected fun getIndexBlocksWriteSetting(indexName: String): String {
        val indexSettings = getIndexSettings(indexName) as Map<String, Map<String, Map<String, Any?>>>
        return indexSettings[indexName]!!["settings"]!!["index.blocks.write"] as String
    }

    @Suppress("UNCHECKED_CAST")
    protected fun getUuid(indexName: String): String {
        val indexSettings = getIndexSettings(indexName) as Map<String, Map<String, Map<String, Any?>>>
        return indexSettings[indexName]!!["settings"]!!["index.uuid"] as String
    }

    /**
     * Compares responses returned by APIs such as those defined in [RetryFailedManagedIndexAction] and [RestAddPolicyAction]
     *
     * Example response with no failures:
     * {
     *   "failures": false,
     *   "updated_indices": 3
     *   "failed_indices": []
     * }
     *
     * Example response with failures:
     * {
     *   "failures": true,
     *   "failed_indices": [
     *     {
     *       "index_name": "indexName",
     *       "index_uuid": "s1PvTKzaThWoeA43eTPYxQ"
     *       "reason": "Reason for failure"
     *     }
     *   ]
     * }
     */
    @Suppress("UNCHECKED_CAST")
    protected fun assertAffectedIndicesResponseIsEqual(expected: Map<String, Any>, actual: Map<String, Any>) {
        for (entry in actual) {
            val key = entry.key
            val value = entry.value

            when {
                key == FAILURES && value is Boolean -> assertEquals(expected[key] as Boolean, value)
                key == UPDATED_INDICES && value is Int -> assertEquals(expected[key] as Int, value)
                key == FAILED_INDICES && value is List<*> -> {
                    value as List<Map<String, String>>

                    val actualArray = value.toTypedArray()
                    actualArray.sortWith(compareBy { it["index_name"] })
                    val expectedArray = (expected[key] as List<Map<String, String>>).toTypedArray()
                    expectedArray.sortWith(compareBy { it["index_name"] })

                    assertArrayEquals(expectedArray, actualArray)
                }
                else -> fail("Unknown field: [$key] or incorrect type for value: [$value]")
            }
        }
    }

    /**
     * indexPredicates is a list of pairs where first is index name and second is a list of pairs
     * where first is key property and second is predicate function to assert on
     *
     * @param indexPredicates list of index to list of predicates to assert on
     * @param response explain response to use for assertions
     * @param strict if true all fields must be handled in assertions
     */
    @Suppress("UNCHECKED_CAST")
    protected fun assertPredicatesOnMetaData(
        indexPredicates: List<Pair<String, List<Pair<String, (Any?) -> Boolean>>>>,
        response: Map<String, Any?>,
        strict: Boolean = true
    ) {
        indexPredicates.forEach { (index, predicates) ->
            assertTrue("The index: $index was not found in the response", response.containsKey(index))
            val indexResponse = response[index] as Map<String, String?>
            if (strict) {
                assertEquals("The fields do not match, response=($indexResponse) predicates=$predicates", predicates.map { it.first }.toSet(), indexResponse.keys.toSet())
            }
            predicates.forEach { (fieldName, predicate) ->
                assertTrue("The key: $fieldName was not found in the response", indexResponse.containsKey(fieldName))
                assertTrue("Failed predicate assertion for $fieldName response=($indexResponse) predicates=$predicates", predicate(indexResponse[fieldName]))
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    protected fun assertRetryInfo(expectedRetryInfo: PolicyRetryInfoMetaData, actualRetryInfoMetaDataMap: Any?): Boolean {
        actualRetryInfoMetaDataMap as Map<String, Any>
        assertEquals(expectedRetryInfo.failed, actualRetryInfoMetaDataMap[PolicyRetryInfoMetaData.FAILED] as Boolean)
        assertEquals(expectedRetryInfo.consumedRetries, actualRetryInfoMetaDataMap[PolicyRetryInfoMetaData.CONSUMED_RETRIES] as Int)
        return true
    }

    @Suppress("UNCHECKED_CAST")
    protected fun assertState(expectedState: StateMetaData, actualStateMap: Any?): Boolean {
        actualStateMap as Map<String, Any>
        assertEquals(expectedState.name, actualStateMap[ManagedIndexMetaData.NAME] as String)
        assertTrue((actualStateMap[ManagedIndexMetaData.START_TIME] as Long) < expectedState.startTime)
        return true
    }

    @Suppress("UNCHECKED_CAST")
    protected fun assertAction(expectedAction: ActionMetaData, actualActionMap: Any?): Boolean {
        actualActionMap as Map<String, Any>
        assertEquals(expectedAction.name, actualActionMap[ManagedIndexMetaData.NAME] as String)
        assertTrue((actualActionMap[ManagedIndexMetaData.START_TIME] as Long) < expectedAction.startTime)
        return true
    }
}
