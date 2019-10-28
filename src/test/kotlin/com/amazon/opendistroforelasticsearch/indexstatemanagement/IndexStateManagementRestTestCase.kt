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

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.INDEX_STATE_MANAGEMENT_HISTORY_TYPE
import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.INDEX_STATE_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.POLICY_BASE_URI
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ChangePolicy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy.Companion.POLICY_TYPE
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.StateFilter
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StateMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler.RestExplainAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.FAILED_INDICES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.FAILURES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.UPDATED_INDICES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util._ID
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util._PRIMARY_TERM
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util._SEQ_NO
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import org.apache.http.HttpEntity
import org.apache.http.HttpHeaders
import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHeader
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Request
import org.elasticsearch.client.Response
import org.elasticsearch.cluster.metadata.IndexMetaData
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
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.ESTestCase
import org.elasticsearch.test.rest.ESRestTestCase
import org.junit.rules.DisableOnDebug
import java.time.Duration
import java.time.Instant
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

    protected fun createRandomPolicy(refresh: Boolean = true): Policy {
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
        policyID: String? = randomAlphaOfLength(10),
        alias: String? = null
    ): Pair<String, String?> {
        val settings = Settings.builder().let {
            if (policyID == null) {
                it.putNull(ManagedIndexSettings.POLICY_ID.key)
            } else {
                it.put(ManagedIndexSettings.POLICY_ID.key, policyID)
            }
            if (alias == null) {
                it.putNull(ManagedIndexSettings.ROLLOVER_ALIAS.key)
            } else {
                it.put(ManagedIndexSettings.ROLLOVER_ALIAS.key, alias)
            }
        }.build()
        val aliases = if (alias == null) "" else "\"$alias\": { \"is_write_index\": true }"
        createIndex(index, settings, "", aliases)
        return index to policyID
    }

    /** Refresh all indices in the cluster */
    protected fun refresh() {
        val request = Request("POST", "/_refresh")
        client().performRequest(request)
    }

    /**
     * Inserts [docCount] sample documents into [index], optionally waiting [delay] milliseconds
     * in between each insertion
     */
    protected fun insertSampleData(index: String, docCount: Int, delay: Long = 0) {
        for (i in 1..docCount) {
            val request = Request("POST", "/$index/_doc/?refresh=true")
            request.setJsonEntity("{ \"test_field\": \"test_value\" }")
            client().performRequest(request)

            Thread.sleep(delay)
        }
    }

    protected fun addPolicyToIndex(
        index: String,
        policyID: String
    ) {
        val settings = Settings.builder().put(ManagedIndexSettings.POLICY_ID.key, policyID)
        updateIndexSettings(index, settings)
    }

    protected fun removePolicyFromIndex(index: String) {
        val settings = Settings.builder().putNull(ManagedIndexSettings.POLICY_ID.key)
        updateIndexSettings(index, settings)
    }

    @Suppress("UNCHECKED_CAST")
    protected fun getPolicyFromIndex(index: String): String? {
        val indexSettings = getIndexSettings(index) as Map<String, Map<String, Map<String, Any?>>>
        return indexSettings[index]!!["settings"]!![ManagedIndexSettings.POLICY_ID.key] as? String
    }

    protected fun updateClusterSetting(key: String, value: String, escapeValue: Boolean = true) {
        val formattedValue = if (escapeValue) "\"$value\"" else value
        val request = """
            {
                "persistent": {
                    "$key": $formattedValue
                }
            }
        """.trimIndent()
        client().makeRequest("PUT", "_cluster/settings", emptyMap(),
            StringEntity(request, APPLICATION_JSON))
    }

    protected fun getManagedIndexConfig(index: String): ManagedIndexConfig? {
        val request = """
            {
                "seq_no_primary_term": true,
                "query": {
                    "term": {
                        "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.INDEX_FIELD}": "$index"
                    }
                }
            }
        """.trimIndent()
        val response = client().makeRequest("POST", "$INDEX_STATE_MANAGEMENT_INDEX/_search", emptyMap(),
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

    @Suppress("UNCHECKED_CAST")
    protected fun getHistorySearchResponse(index: String): SearchResponse {
        val request = """
            {
                "seq_no_primary_term": true,
                "sort": [
                    {"$INDEX_STATE_MANAGEMENT_HISTORY_TYPE.history_timestamp": {"order": "desc"}}
                ],
                "query": {
                    "term": {
                        "$INDEX_STATE_MANAGEMENT_HISTORY_TYPE.index": "$index"
                    }
                }
            }
        """.trimIndent()
        val response = client().makeRequest("POST", "${IndexStateManagementIndices.HISTORY_ALL}/_search", emptyMap(),
            StringEntity(request, APPLICATION_JSON))
        assertEquals("Request failed", RestStatus.OK, response.restStatus())
        return SearchResponse.fromXContent(createParser(jsonXContent, response.entity.content))
    }

    protected fun getLatestHistory(searchResponse: SearchResponse): ManagedIndexMetaData {
        val hit = searchResponse.hits.hits.first()
        return hit.run {
            val xcp = createParser(jsonXContent, this.sourceRef)
            xcp.nextToken()
            ManagedIndexMetaData.parse(xcp)
        }
    }

    protected fun getExistingManagedIndexConfig(index: String): ManagedIndexConfig {
        return waitFor {
            val config = getManagedIndexConfig(index)
            assertNotNull("ManagedIndexConfig is null", config)
            config!!
        }
    }

    protected fun updateManagedIndexConfigStartTime(update: ManagedIndexConfig, desiredStartTimeMillis: Long? = null) {
        val intervalSchedule = (update.jobSchedule as IntervalSchedule)
        val millis = Duration.of(intervalSchedule.interval.toLong(), intervalSchedule.unit).minusSeconds(2).toMillis()
        val startTimeMillis = desiredStartTimeMillis ?: Instant.now().toEpochMilli() - millis
        val response = client().makeRequest("POST", "$INDEX_STATE_MANAGEMENT_INDEX/_update/${update.id}",
            StringEntity(
                "{\"doc\":{\"managed_index\":{\"schedule\":{\"interval\":{\"start_time\":" +
                    "\"$startTimeMillis\"}}}}}",
                APPLICATION_JSON
            ))

        assertEquals("Request failed", RestStatus.OK, response.restStatus())
    }

    protected fun updateManagedIndexConfigPolicySeqNo(update: ManagedIndexConfig) {
        val response = client().makeRequest("POST", "$INDEX_STATE_MANAGEMENT_INDEX/_update/${update.id}",
            StringEntity(
                "{\"doc\":{\"managed_index\":{\"policy_seq_no\":\"${update.policySeqNo}\"}}}",
                APPLICATION_JSON
            ))
        assertEquals("Request failed", RestStatus.OK, response.restStatus())
    }

    protected fun Response.restStatus(): RestStatus = RestStatus.fromCode(this.statusLine.statusCode)

    protected fun Policy.toHttpEntity(): HttpEntity = StringEntity(toJsonString(), APPLICATION_JSON)

    protected fun ManagedIndexConfig.toHttpEntity(): HttpEntity = StringEntity(toJsonString(), APPLICATION_JSON)

    protected fun ChangePolicy.toHttpEntity(): HttpEntity {
        var string = "{\"${ChangePolicy.POLICY_ID_FIELD}\":\"$policyID\","
        if (state != null) {
            string += "\"${ChangePolicy.STATE_FIELD}\":\"$state\","
        }
        string += "\"${ChangePolicy.INCLUDE_FIELD}\":${include.map { "{\"${StateFilter.STATE_FIELD}\":\"${it.state}\"}" }}}"

        return StringEntity(string, APPLICATION_JSON)
    }

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
    protected fun getSegmentCount(index: String): Int {
        val statsResponse: Map<String, Any> = getStats(index)

        // Assert that shard count of stats response is 1 since the stats request being used is at the index level
        // (meaning the segment count in the response is aggregated) but segment count for force merge
        // (which this method is primarily being used for) is going to be validated per shard
        val shardsInfo = statsResponse["_shards"] as Map<String, Int>
        assertEquals("Shard count higher than expected", 1, shardsInfo["successful"])

        val indicesStats = statsResponse["indices"] as Map<String, Map<String, Map<String, Map<String, Any?>>>>
        return indicesStats[index]!!["primaries"]!!["segments"]!!["count"] as Int
    }

    /** Get stats for [index] */
    private fun getStats(index: String): Map<String, Any> {
        val response = client().makeRequest("GET", "/$index/_stats")

        assertEquals("Stats request failed", RestStatus.OK, response.restStatus())

        return response.asMap()
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
        return indexSettings[indexName]!!["settings"]!![IndexMetaData.SETTING_BLOCKS_WRITE] as String
    }

    @Suppress("UNCHECKED_CAST")
    protected fun getNumberOfReplicasSetting(indexName: String): Int {
        val indexSettings = getIndexSettings(indexName) as Map<String, Map<String, Map<String, Any?>>>
        return (indexSettings[indexName]!!["settings"]!!["index.number_of_replicas"] as String).toInt()
    }

    @Suppress("UNCHECKED_CAST")
    protected fun getUuid(indexName: String): String {
        val indexSettings = getIndexSettings(indexName) as Map<String, Map<String, Map<String, Any?>>>
        return indexSettings[indexName]!!["settings"]!!["index.uuid"] as String
    }

    @Suppress("UNCHECKED_CAST")
    protected fun getFlatSettings(indexName: String) =
            (getIndexSettings(indexName) as Map<String, Map<String, Map<String, Any?>>>)[indexName]!!["settings"] as Map<String, String>

    protected fun getExplainMap(indexName: String): Map<String, Any> {
        val response = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/$indexName")
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        return response.asMap()
    }

    // Calls explain API for a single concrete index and converts the response into a ManagedIndexMetaData
    // This only works for indices with a ManagedIndexMetaData that has been initialized
    protected fun getExplainManagedIndexMetaData(indexName: String): ManagedIndexMetaData {
        if (indexName.contains("*") || indexName.contains(",")) {
            throw IllegalArgumentException("This method is only for a single concrete index")
        }

        val response = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/$indexName")
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())

        lateinit var metadata: ManagedIndexMetaData
        val xcp = createParser(XContentType.JSON.xContent(), response.entity.content)
        ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
        while (xcp.nextToken() != Token.END_OBJECT) {
            xcp.currentName()
            xcp.nextToken()

            metadata = ManagedIndexMetaData.parse(xcp)
        }
        return metadata
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
                    val actualArray = (value as List<Map<String, String>>).toTypedArray()
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
            assertTrue("The index: $index was not found in the response: $response", response.containsKey(index))
            val indexResponse = response[index] as Map<String, String?>
            if (strict) {
                val predicatesSet = predicates.map { it.first }.toSet()
                assertEquals("The fields do not match, response=($indexResponse) predicates=$predicatesSet", predicatesSet, indexResponse.keys.toSet())
            }
            predicates.forEach { (fieldName, predicate) ->
                assertTrue("The key: $fieldName was not found in the response: $indexResponse", indexResponse.containsKey(fieldName))
                assertTrue("Failed predicate assertion for $fieldName response=($indexResponse) predicates=$predicates", predicate(indexResponse[fieldName]))
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    protected fun assertRetryInfoEquals(expectedRetryInfo: PolicyRetryInfoMetaData, actualRetryInfoMetaDataMap: Any?): Boolean {
        actualRetryInfoMetaDataMap as Map<String, Any>
        assertEquals(expectedRetryInfo.failed, actualRetryInfoMetaDataMap[PolicyRetryInfoMetaData.FAILED] as Boolean)
        assertEquals(expectedRetryInfo.consumedRetries, actualRetryInfoMetaDataMap[PolicyRetryInfoMetaData.CONSUMED_RETRIES] as Int)
        return true
    }

    @Suppress("UNCHECKED_CAST")
    protected fun assertStateEquals(expectedState: StateMetaData, actualStateMap: Any?): Boolean {
        actualStateMap as Map<String, Any>
        assertEquals(expectedState.name, actualStateMap[ManagedIndexMetaData.NAME] as String)
        assertTrue((actualStateMap[ManagedIndexMetaData.START_TIME] as Long) < expectedState.startTime)
        return true
    }

    @Suppress("UNCHECKED_CAST")
    protected fun assertActionEquals(expectedAction: ActionMetaData, actualActionMap: Any?): Boolean {
        actualActionMap as Map<String, Any>
        assertEquals(expectedAction.name, actualActionMap[ManagedIndexMetaData.NAME] as String)
        assertEquals(expectedAction.failed, actualActionMap[ActionMetaData.FAILED] as Boolean)
        val expectedStartTime = expectedAction.startTime
        if (expectedStartTime != null) {
            assertTrue((actualActionMap[ManagedIndexMetaData.START_TIME] as Long) < expectedStartTime)
        }
        return true
    }
}
