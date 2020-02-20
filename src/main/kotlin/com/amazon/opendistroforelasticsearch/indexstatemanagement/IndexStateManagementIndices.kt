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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.INDEX_NUMBER_OF_REPLICAS
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.INDEX_NUMBER_OF_SHARDS
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.OpenForTesting
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util._DOC
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ResourceAlreadyExistsException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.alias.Alias
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.elasticsearch.client.Client
import org.elasticsearch.client.IndicesAdminClient
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType

@OpenForTesting
class IndexStateManagementIndices(
    private val client: IndicesAdminClient,
    private val clusterService: ClusterService
) {

    private val logger = LogManager.getLogger(javaClass)

    val indexStateManagementMappings = javaClass.classLoader.getResource("mappings/opendistro-ism-config.json").readText()

    val indexStateManagementHistoryMappings = javaClass.classLoader.getResource("mappings/opendistro-ism-history.json").readText()

    fun initIndexStateManagementIndex(actionListener: ActionListener<CreateIndexResponse>) {
        if (!indexStateManagementIndexExists()) {
            val indexRequest = CreateIndexRequest(INDEX_STATE_MANAGEMENT_INDEX)
                .mapping(_DOC, indexStateManagementMappings, XContentType.JSON)
                .settings(Settings.builder().put(INDEX_NUMBER_OF_SHARDS, 1).put(INDEX_NUMBER_OF_REPLICAS, 1).build())
            client.create(indexRequest, actionListener)
        }
    }

    fun indexStateManagementIndexExists(): Boolean = clusterService.state().routingTable.hasIndex(INDEX_STATE_MANAGEMENT_INDEX)

    /**
     * Attempt to create [INDEX_STATE_MANAGEMENT_INDEX] and return whether it exists
     */
    @Suppress("ReturnCount")
    suspend fun attemptInitStateManagementIndex(client: Client): Boolean {
        if (indexStateManagementIndexExists()) return true

        return try {
            val response: CreateIndexResponse = client.suspendUntil { initIndexStateManagementIndex(it) }
            if (response.isAcknowledged) {
                return true
            }
            logger.error("Creating $INDEX_STATE_MANAGEMENT_INDEX with mappings NOT acknowledged")
            return false
        } catch (e: ResourceAlreadyExistsException) {
            true
        } catch (e: Exception) {
            logger.error("Error trying to create $INDEX_STATE_MANAGEMENT_INDEX", e)
            false
        }
    }

    /**
     * ============== History =============
     */
    fun indexStateManagementIndexHistoryExists(): Boolean = clusterService.state().metaData.hasAlias(HISTORY_WRITE_INDEX_ALIAS)

    suspend fun initHistoryIndex() {
        if (!indexStateManagementIndexHistoryExists())
            createHistoryIndex(HISTORY_INDEX_PATTERN, HISTORY_WRITE_INDEX_ALIAS)
    }

    private suspend fun createHistoryIndex(index: String, alias: String? = null): Boolean {
        // This should be a fast check of local cluster state. Should be exceedingly rare that the local cluster
        // state does not contain the index and multiple nodes concurrently try to create the index.
        // If it does happen that error is handled we catch the ResourceAlreadyExistsException
        val existsResponse: IndicesExistsResponse = client.suspendUntil {
            client.exists(IndicesExistsRequest(index).local(true), it)
        }
        if (existsResponse.isExists) return true

        val request = CreateIndexRequest(index)
            .mapping(_DOC, indexStateManagementHistoryMappings, XContentType.JSON)
            .settings(Settings.builder().put(INDEX_NUMBER_OF_SHARDS, 1).put(INDEX_NUMBER_OF_REPLICAS, 1).build())
        if (alias != null) request.alias(Alias(alias))
        return try {
            val createIndexResponse: CreateIndexResponse = client.suspendUntil { client.create(request, it) }
            if (createIndexResponse.isAcknowledged) {
                true
            } else {
                logger.error("Creating $index with mappings NOT acknowledged")
                false
            }
        } catch (e: ResourceAlreadyExistsException) {
            true
        } catch (e: Exception) {
            logger.error("Error trying to create $index", e)
            false
        }
    }

    companion object {
        const val HISTORY_INDEX_BASE = ".opendistro-ism-managed-index-history"
        const val HISTORY_WRITE_INDEX_ALIAS = "$HISTORY_INDEX_BASE-write"
        const val HISTORY_INDEX_PATTERN = "<$HISTORY_INDEX_BASE-{now/d{yyyy.MM.dd}}-1>"
        const val HISTORY_ALL = "$HISTORY_INDEX_BASE*"
    }
}
