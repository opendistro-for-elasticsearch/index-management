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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.explain

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getPolicyID
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.index.query.Operator
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.FetchSourceContext.FETCH_SOURCE
import org.elasticsearch.search.sort.SortBuilders
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

private val log = LogManager.getLogger(TransportExplainAction::class.java)

class TransportExplainAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters
) : HandledTransportAction<ExplainRequest, ExplainResponse>(
        ExplainAction.NAME, transportService, actionFilters, ::ExplainRequest
) {
    override fun doExecute(task: Task, request: ExplainRequest, listener: ActionListener<ExplainResponse>) {
        ExplainHandler(client, listener, request).start()
    }

    inner class ExplainHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<ExplainResponse>,
        private val request: ExplainRequest
    ) {
        private var managedIndexMetaDataMap = mutableMapOf<String, String?>()

        @Suppress("SpreadOperator")
        fun start() {
            if (request.indices.isNotEmpty()) {
                getMetadata(request.indices)
                return
            }

            val params = request.params

            val sortBuilder = SortBuilders
                .fieldSort(params.sortField)
                .order(SortOrder.fromString(params.sortOrder))

            val queryStringQuery = QueryBuilders.queryStringQuery(params.queryString).defaultField("managed_index.name").defaultOperator(Operator.AND)
            val queryBuilder = QueryBuilders.boolQuery()
                .filter(QueryBuilders.existsQuery("managed_index"))
                .must(queryStringQuery)

            val searchSourceBuilder = SearchSourceBuilder()
                .sort(sortBuilder)
                .from(params.from)
                .size(params.size)
                .fetchSource(FETCH_SOURCE)
                .seqNoAndPrimaryTerm(true)
                .version(true)
                .query(queryBuilder)

            val searchRequest = SearchRequest()
                .indices(INDEX_MANAGEMENT_INDEX)
                .source(searchSourceBuilder)

            client.search(searchRequest, object : ActionListener<SearchResponse> {
                override fun onResponse(response: SearchResponse) {
                    val managedIndices = mutableListOf<String>()
                    log.info("check response size: ${response.hits.hits.size}")
                    response.hits.hits.map {
                        val hitMap = it.sourceAsMap["managed_index"] as Map<String, Any>
                        log.info("see hits content: $hitMap")
                        managedIndices.add(hitMap["index"] as String)
                        managedIndexMetaDataMap["index"] = hitMap["index"] as String?
                        managedIndexMetaDataMap["index_uuid"] = hitMap["index_uuid"] as String?
                        managedIndexMetaDataMap["policy_id"] = hitMap["policy_id"] as String?
                        managedIndexMetaDataMap["enabled"] = hitMap["enabled"]?.toString()
                    }
                    log.info("explain indices: $managedIndices")
                    log.info("managed index metadata map: $managedIndexMetaDataMap")
                    getMetadata(managedIndices)
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(t)
                }
            })
        }

        fun getMetadata(indices: List<String>) {
            val clusterStateRequest = ClusterStateRequest()
            val strictExpandIndicesOptions = IndicesOptions.strictExpand()

            clusterStateRequest.clear()
                .indices(*indices.toTypedArray())
                .metadata(true)
                .local(request.local)
                .masterNodeTimeout(request.masterTimeout)
                .indicesOptions(strictExpandIndicesOptions)

            client.admin().cluster().state(clusterStateRequest, object : ActionListener<ClusterStateResponse> {
                override fun onResponse(response: ClusterStateResponse) {
                    processResponse(response)
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(t)
                }
            })
        }

        fun processResponse(clusterStateResponse: ClusterStateResponse) {
            val state = clusterStateResponse.state
            val indexNames = mutableListOf<String>()
            val indexPolicyIDs = mutableListOf<String?>()
            val indexMetadatas = mutableListOf<ManagedIndexMetaData?>()

            for (indexMetadataEntry in state.metadata.indices) {
                indexNames.add(indexMetadataEntry.key)

                val indexMetadata = indexMetadataEntry.value
                indexPolicyIDs.add(indexMetadata.getPolicyID())

                val savedMetadata = indexMetadata.getCustomData(ManagedIndexMetaData.MANAGED_INDEX_METADATA)
                if (savedMetadata != null) {
                    managedIndexMetaDataMap = savedMetadata
                }
                val managedIndexMetadata = ManagedIndexMetaData.fromMap(managedIndexMetaDataMap)
                indexMetadatas.add(managedIndexMetadata)
            }

            actionListener.onResponse(ExplainResponse(indexNames, indexPolicyIDs, indexMetadatas))
        }
    }
}
