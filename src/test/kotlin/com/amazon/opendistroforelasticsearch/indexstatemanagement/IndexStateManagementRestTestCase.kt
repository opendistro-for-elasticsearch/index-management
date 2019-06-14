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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.Policy.Companion.POLICY_TYPE
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util._ID
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util._PRIMARY_TERM
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util._SEQ_NO
import org.apache.http.HttpEntity
import org.apache.http.HttpHeaders
import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHeader
import org.elasticsearch.action.search.SearchResponse
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
import java.util.*

abstract class IndexStateManagementRestTestCase : ESRestTestCase() {

    private val isDebuggingTest = DisableOnDebug(null).isDebugging
    private val isDebuggingRemoteCluster = System.getProperty("cluster.debug", "false")!!.toBoolean()

    fun Response.asMap(): Map<String, Any> = entityAsMap(this)

    protected fun createPolicy(policy: Policy, policyId: String = ESTestCase.randomAlphaOfLength(10), refresh: Boolean = true): Policy {
        val response = client().makeRequest("PUT", "$POLICY_BASE_URI/$policyId?refresh=$refresh", emptyMap(),
                policy.toHttpEntity())
        assertEquals("Unable to create a new policy", RestStatus.CREATED, response.restStatus())

        val policyJson = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
                response.entity.content).map()
        val createdId = policyJson["_id"] as String
        assertEquals("policy ids are not the same", policyId, createdId)
        return policy.copy(id = createdId, seqNo = (policyJson["_seq_no"] as Int).toLong(), primaryTerm = (policyJson["_primary_term"] as Int).toLong())
    }

    protected fun createRandomPolicy(refresh: Boolean = false): Policy {
        val policy = randomPolicy()
        val policyId = createPolicy(policy, refresh = refresh).id
        return getPolicy(policyId = policyId)
    }

    protected fun getPolicy(policyId: String, header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")): Policy {
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
        index: String = ESTestCase.randomAlphaOfLength(10).toLowerCase(Locale.ROOT),
        policyName: String? = ESTestCase.randomAlphaOfLength(10)
    ): Pair<String, String?> {
        val settings = Settings.builder().let {
            if (policyName == null) {
                it.putNull(ManagedIndexSettings.POLICY_NAME.key)
            } else {
                it.put(ManagedIndexSettings.POLICY_NAME.key, policyName)
            }
        }.build()
        createIndex(index, settings)
        return index to policyName
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
            ManagedIndexConfig.parseWithType(xcp)
        }
    }

    protected fun Response.restStatus(): RestStatus = RestStatus.fromCode(this.statusLine.statusCode)

    protected fun Policy.toHttpEntity(): HttpEntity = StringEntity(toJsonString(), APPLICATION_JSON)

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
}
