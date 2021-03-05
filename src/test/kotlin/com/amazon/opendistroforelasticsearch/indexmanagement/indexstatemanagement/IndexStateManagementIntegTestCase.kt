/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StateMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler.RestExplainAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.explain.ExplainAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.explain.TransportExplainAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.TransportUpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.elasticsearch.ElasticsearchParseException
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Request
import org.elasticsearch.client.Response
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand
import org.elasticsearch.common.Strings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.DeprecationHandler
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.common.xcontent.json.JsonXContent
import org.elasticsearch.plugins.ActionPlugin
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.ESIntegTestCase
import org.elasticsearch.test.rest.ESRestTestCase.entityAsMap
import java.io.IOException
import java.time.Duration
import java.time.Instant

abstract class IndexStateManagementIntegTestCase : ESIntegTestCase() {

    protected val isMixedNodeRegressionTest = System.getProperty("cluster.mixed", "false")!!.toBoolean()

    var metadataToClusterState = ManagedIndexMetaData(
        index = "",
        indexUuid = "",
        policyID = "",
        policySeqNo = 0,
        policyPrimaryTerm = 1,
        policyCompleted = false,
        rolledOver = false,
        transitionTo = null,
        stateMetaData = StateMetaData("ReplicaCountState", 1234),
        actionMetaData = null,
        stepMetaData = null,
        policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
        info = mapOf("message" to "Happy moving")
    )

    override fun nodePlugins(): Collection<Class<out Plugin>> {
        return listOf(IndexManagementPlugin::class.java)
    }

    class TestPlugin : ActionPlugin, Plugin() {
        override fun getActions(): List<ActionPlugin.ActionHandler<out ActionRequest, out ActionResponse>> {
            return listOf(
                ActionPlugin.ActionHandler(
                    UpdateManagedIndexMetaDataAction.INSTANCE,
                    TransportUpdateManagedIndexMetaDataAction::class.java
                ),
                ActionPlugin.ActionHandler(ExplainAction.INSTANCE, TransportExplainAction::class.java)
            )
        }
    }

    override fun transportClientPlugins(): Collection<Class<out Plugin>> {
        return listOf(TestPlugin::class.java)
    }

    protected fun getIndexMetadata(indexName: String): IndexMetadata {
        return client().admin().cluster().prepareState()
            .setIndices(indexName)
            .setMetadata(true).get()
            .state.metadata.indices[indexName]
    }

    // reuse utility fun from RestTestCase
    fun createPolicy(
        policy: Policy,
        policyId: String = randomAlphaOfLength(10),
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
        val response = getRestClient()
            .makeRequest(
                "PUT",
                "${IndexManagementPlugin.POLICY_BASE_URI}/$policyId?refresh=$refresh",
                emptyMap(),
                StringEntity(policyString, ContentType.APPLICATION_JSON)
            )
        assertEquals("Unable to create a new policy", RestStatus.CREATED, response.restStatus())
        return response
    }

    protected fun Response.restStatus(): RestStatus = RestStatus.fromCode(this.statusLine.statusCode)

    protected fun addPolicyToIndex(
        index: String,
        policyID: String
    ) {
        val body = """
            {
              "policy_id": "$policyID"
            }
        """.trimIndent()
        val response = getRestClient()
            .makeRequest("POST", "/_opendistro/_ism/add/$index", StringEntity(body, ContentType.APPLICATION_JSON))
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
    }

    protected fun getExistingManagedIndexConfig(index: String): ManagedIndexConfig {
        return waitFor {
            val config = getManagedIndexConfig(index)
            assertNotNull("ManagedIndexConfig is null", config)
            config!!
        }
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
        val response = getRestClient().makeRequest(
            "POST", "${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX}/_search", emptyMap(),
            StringEntity(request, ContentType.APPLICATION_JSON)
        )
        assertEquals("Request failed", RestStatus.OK, response.restStatus())
        val searchResponse =
            SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, response.entity.content))
        assertTrue("Found more than one managed index config", searchResponse.hits.hits.size < 2)
        val hit = searchResponse.hits.hits.firstOrNull()
        return hit?.run {
            val xcp = createParser(JsonXContent.jsonXContent, this.sourceRef)
            xcp.parseWithType(id, seqNo, primaryTerm, ManagedIndexConfig.Companion::parse)
        }
    }

    protected fun updateManagedIndexConfigStartTime(update: ManagedIndexConfig, desiredStartTimeMillis: Long? = null) {
        val intervalSchedule = (update.jobSchedule as IntervalSchedule)
        val millis = Duration.of(intervalSchedule.interval.toLong(), intervalSchedule.unit).minusSeconds(2).toMillis()
        val startTimeMillis = desiredStartTimeMillis ?: Instant.now().toEpochMilli() - millis
        val response = getRestClient().makeRequest(
            "POST", "${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX}/_update/${update.id}",
            StringEntity(
                "{\"doc\":{\"managed_index\":{\"schedule\":{\"interval\":{\"start_time\":" +
                    "\"$startTimeMillis\"}}}}}",
                ContentType.APPLICATION_JSON
            )
        )

        assertEquals("Request failed", RestStatus.OK, response.restStatus())
    }

    protected fun updateManagedIndexConfigPolicy(update: ManagedIndexConfig, policy: Policy) {
        val policyJsonString = policy.toJsonString()
        logger.info("policy string: $policyJsonString")
        var response = getRestClient().makeRequest(
            "POST", "${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX}/_update/${update.id}",
            StringEntity(
                "{\"doc\":{\"managed_index\": $policyJsonString }}",
                ContentType.APPLICATION_JSON
            )
        )

        assertEquals("Request failed", RestStatus.OK, response.restStatus())

        response = getRestClient().makeRequest(
            "POST", "${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX}/_update/${update.id}",
            StringEntity(
                "{\"doc\":{\"managed_index\": {\"policy_seq_no\": \"0\", \"policy_primary_term\": \"1\"} }}",
                ContentType.APPLICATION_JSON
            )
        )

        assertEquals("Request failed", RestStatus.OK, response.restStatus())
    }

    @Suppress("UNCHECKED_CAST")
    protected fun getNumberOfReplicasSetting(indexName: String): Int {
        val indexSettings = getIndexSettings(indexName) as Map<String, Map<String, Map<String, Any?>>>
        return (indexSettings[indexName]!!["settings"]!!["index.number_of_replicas"] as String).toInt()
    }

    @Throws(IOException::class)
    protected open fun getIndexSettings(index: String): Map<String?, Any?>? {
        val request = Request("GET", "/$index/_settings")
        request.addParameter("flat_settings", "true")
        val response = getRestClient().performRequest(request)
        response.entity.content.use { `is` ->
            return XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                `is`,
                true
            )
        }
    }

    protected fun getExplainManagedIndexMetaData(indexName: String): ManagedIndexMetaData {
        if (indexName.contains("*") || indexName.contains(",")) {
            throw IllegalArgumentException("This method is only for a single concrete index")
        }

        val response = getRestClient().makeRequest(
            RestRequest.Method.GET.toString(),
            "${RestExplainAction.EXPLAIN_BASE_URI}/$indexName"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())

        lateinit var metadata: ManagedIndexMetaData
        val xcp = createParser(XContentType.JSON.xContent(), response.entity.content)
        XContentParserUtils.ensureExpectedToken(
            XContentParser.Token.START_OBJECT,
            xcp.nextToken(),
            xcp
        )
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            xcp.currentName()
            xcp.nextToken()

            metadata = ManagedIndexMetaData.parse(xcp)
        }
        return metadata
    }

    protected fun assertIndexExists(index: String) {
        val response = getRestClient().makeRequest("HEAD", index)
        assertEquals("Index $index does not exist.", RestStatus.OK, response.restStatus())
    }

    fun getShardSegmentStats(index: String): Map<String, Any> {
        val response = getRestClient().makeRequest("GET", "/$index/_stats/segments?level=shards")

        assertEquals("Stats request failed", RestStatus.OK, response.restStatus())

        return response.asMap()
    }

    fun catIndexShard(index: String): List<Any> {
        val response = getRestClient().makeRequest("GET", "_cat/shards/$index?format=json")

        assertEquals("Stats request failed", RestStatus.OK, response.restStatus())

        try {
            return JsonXContent.jsonXContent
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    response.entity.content
                )
                .use { parser -> parser.list() }
        } catch (e: IOException) {
            throw ElasticsearchParseException("Failed to parse content to list", e)
        }
    }

    fun Response.asMap(): Map<String, Any> = entityAsMap(this)

    fun rerouteShard(configIndexName: String, fromNode: String, toNode: String) {
        logger.info("Reallocating Shard. From Node: $fromNode To Node: $toNode ")
        val moveCommand = MoveAllocationCommand(configIndexName, 0, fromNode, toNode)
        val rerouteResponse = client().admin().cluster()
            .reroute(ClusterRerouteRequest().add(moveCommand)).actionGet()
        logger.info("reroute success? ${rerouteResponse.isAcknowledged}")
    }

    fun updateIndexSettings(index: String, settings: Settings) {
        val request = Request("PUT", "/$index/_settings")
        request.setJsonEntity(Strings.toString(settings))
        getRestClient().performRequest(request)
    }
}