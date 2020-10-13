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
import org.elasticsearch.index.IndexNotFoundException
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

    /**
     * do search request first to find out the managed indices
     * then retrieve metadata of these managed indices
     * special case is when user explicitly query for an un-managed index
     * return this index with its policy id shown 'null' meaning it's not managed
     */
    inner class ExplainHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<ExplainResponse>,
        private val request: ExplainRequest
    ) {
        private val indices = request.indices
        private val wildcard = indices.size == 1 && indices[0].contains("*")

        // map of (index : index metadata map)
        private val managedIndicesMetaDataMap = mutableMapOf<String, Map<String, String?>>()
        private val managedIndices = mutableListOf<String>()

        private val indexNames = mutableListOf<String>()
        private var totalManagedIndices = 0

        @Suppress("SpreadOperator")
        fun start() {
            val params = request.params

            val sortBuilder = SortBuilders
                .fieldSort(params.sortField)
                .order(SortOrder.fromString(params.sortOrder))

            val queryStringQuery = QueryBuilders
                .queryStringQuery(params.queryString)
                .defaultField("managed_index.name")
                .defaultOperator(Operator.AND)
            val queryBuilder = QueryBuilders.boolQuery()
                .must(queryStringQuery)

            if (indices.isNotEmpty()) {
                if (wildcard) { // explain/index*
                    queryBuilder.filter(QueryBuilders.wildcardQuery("managed_index.index", indices[0]))
                } else { // explain/{index}
                    queryBuilder.filter(QueryBuilders.termsQuery("managed_index.index", indices))
                }
            } else { // explain all
                queryBuilder.filter(QueryBuilders.existsQuery("managed_index"))
            }

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
                    val totalHits = response.hits.totalHits
                    if (totalHits != null) {
                        totalManagedIndices = totalHits.value.toInt()
                    }

                    response.hits.hits.map {
                        val hitMap = it.sourceAsMap["managed_index"] as Map<String, Any>
                        val managedIndex = hitMap["index"] as String
                        managedIndices.add(managedIndex)
                        managedIndicesMetaDataMap[managedIndex] = mapOf(
                            "index" to hitMap["index"] as String?,
                            "index_uuid" to hitMap["index_uuid"] as String?,
                            "policy_id" to hitMap["policy_id"] as String?,
                            "enabled" to hitMap["enabled"]?.toString()
                        )
                    }

                    if (managedIndices.size > 0) {
                        if (managedIndices.size < indices.size) {
                            // explain/{index} but has not managed index
                            indexNames.addAll(indices)
                            getMetadata(indices)
                            return
                        }
                        indexNames.addAll(managedIndices)
                        getMetadata(managedIndices)
                        return
                    }
                    // getAll (after filtered, no managed indices match happened when using searchBox on frontend)
                    actionListener.onResponse(ExplainResponse(emptyList(), emptyList(), emptyList(), 0))
                }

                override fun onFailure(t: Exception) {
                    if (t is IndexNotFoundException) { // config index hasn't been initialized
                        if (indices.isNotEmpty()) {
                            indexNames.addAll(indices)
                            getMetadata(indices)
                            return
                        }
                        actionListener.onResponse(ExplainResponse(emptyList(), emptyList(), emptyList(), 0))
                        return
                    }
                    actionListener.onFailure(t)
                }
            })
        }

        @Suppress("SpreadOperator")
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
            val indexPolicyIDs = mutableListOf<String?>()
            val indexMetadatas = mutableListOf<ManagedIndexMetaData?>()

            // cluster state response won't resist the sort order
            for (indexName in indexNames) {
                val indexMetadata = state.metadata.indices[indexName]
                indexPolicyIDs.add(indexMetadata.getPolicyID())

                var managedIndexMetadata: ManagedIndexMetaData? = null
                var managedIndexMetadataMap = managedIndicesMetaDataMap[indexName]
                val savedMetadata = indexMetadata.getCustomData(ManagedIndexMetaData.MANAGED_INDEX_METADATA)
                if (managedIndexMetadataMap != null) {
                    if (savedMetadata != null) { // if has metadata saved, use that
                        managedIndexMetadataMap = savedMetadata
                    }
                    if (managedIndexMetadataMap.isNotEmpty()) {
                        // empty because of index not managed
                        managedIndexMetadata = ManagedIndexMetaData.fromMap(managedIndexMetadataMap)
                    }
                }
                indexMetadatas.add(managedIndexMetadata)
            }

            managedIndicesMetaDataMap.clear()

            actionListener.onResponse(ExplainResponse(indexNames, indexPolicyIDs, indexMetadatas, totalManagedIndices))
        }
    }
}
