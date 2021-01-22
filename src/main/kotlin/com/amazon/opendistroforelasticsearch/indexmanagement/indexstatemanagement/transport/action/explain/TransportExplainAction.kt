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
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator.Companion.MAX_HITS
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
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
        client.threadPool().threadContext.stashContext().use {
            ExplainHandler(client, listener, request).start()
        }
    }

    /**
     * first search config index to find out managed indices
     * then retrieve metadata of these managed indices
     * special case: when user explicitly query for an un-managed index
     * return this index with its policy id shown 'null' meaning it's not managed
     */
    inner class ExplainHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<ExplainResponse>,
        private val request: ExplainRequest
    ) {
        private val indices: List<String> = request.indices
        private val explainAll: Boolean = indices.isEmpty()
        private val wildcard: Boolean = indices.any { it.contains("*") }

        // map of index to index metadata got from config index job
        private val managedIndicesMetaDataMap: MutableMap<String, Map<String, String?>> = mutableMapOf()
        private val managedIndices: MutableList<String> = mutableListOf()

        private val indexNames: MutableList<String> = mutableListOf()
        private val enabledState: MutableMap<String, Boolean> = mutableMapOf()
        private var totalManagedIndices = 0

        @Suppress("SpreadOperator")
        fun start() {
            val params = request.searchParams

            val sortBuilder = SortBuilders
                .fieldSort(params.sortField)
                .order(SortOrder.fromString(params.sortOrder))

            val queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders
                    .queryStringQuery(params.queryString)
                    .defaultField("managed_index.name")
                    .defaultOperator(Operator.AND))

            var searchSourceBuilder = SearchSourceBuilder()
                .from(params.from)
                .fetchSource(FETCH_SOURCE)
                .seqNoAndPrimaryTerm(true)
                .version(true)
                .sort(sortBuilder)

            if (!explainAll) {
                searchSourceBuilder = searchSourceBuilder.size(MAX_HITS)
                if (wildcard) { // explain/index*
                    indices.forEach {
                        if (it.contains("*")) {
                            queryBuilder.should(QueryBuilders.wildcardQuery("managed_index.index", it))
                        } else {
                            queryBuilder.should(QueryBuilders.termsQuery("managed_index.index", it))
                        }
                    }
                } else { // explain/{index}
                    queryBuilder.filter(QueryBuilders.termsQuery("managed_index.index", indices))
                }
            } else { // explain all
                searchSourceBuilder = searchSourceBuilder.size(params.size)
                queryBuilder.filter(QueryBuilders.existsQuery("managed_index"))
            }

            searchSourceBuilder = searchSourceBuilder.query(queryBuilder)

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
                        enabledState[managedIndex] = hitMap["enabled"] as Boolean
                        managedIndicesMetaDataMap[managedIndex] = mapOf(
                            "index" to hitMap["index"] as String?,
                            "index_uuid" to hitMap["index_uuid"] as String?,
                            "policy_id" to hitMap["policy_id"] as String?,
                            "enabled" to hitMap["enabled"]?.toString()
                        )
                    }

                    // explain all only return managed indices
                    if (explainAll) {
                        if (managedIndices.size == 0) {
                            // edge case: if specify query param pagination size to be 0
                            // we still show total managed indices
                            emptyResponse(totalManagedIndices)
                            return
                        } else {
                            indexNames.addAll(managedIndices)
                            getMetadata(managedIndices)
                            return
                        }
                    }

                    // explain/{index} return results for all indices
                    indexNames.addAll(indices)
                    getMetadata(indices)
                }

                override fun onFailure(t: Exception) {
                    if (t is IndexNotFoundException) {
                        // config index hasn't been initialized
                        // show all requested indices not managed
                        if (indices.isNotEmpty()) {
                            indexNames.addAll(indices)
                            getMetadata(indices)
                            return
                        }
                        emptyResponse()
                        return
                    }
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
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
                    onClusterStateResponse(response)
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                }
            })
        }

        fun onClusterStateResponse(clusterStateResponse: ClusterStateResponse) {
            val state = clusterStateResponse.state
            val indexPolicyIDs = mutableListOf<String?>()
            val indexMetadatas = mutableListOf<ManagedIndexMetaData?>()

            if (wildcard) {
                indexNames.clear() // clear wildcard (index*) from indexNames
                state.metadata.indices.forEach { indexNames.add(it.key) }
            }

            // cluster state response will not resisting the sort order
            // so use the order from previous search result saved in indexNames
            for (indexName in indexNames) {
                val indexMetadata = state.metadata.indices[indexName]

                var managedIndexMetadataMap = managedIndicesMetaDataMap[indexName]
                indexPolicyIDs.add(managedIndexMetadataMap?.get("policy_id")) // use policyID from metadata

                var managedIndexMetadata: ManagedIndexMetaData? = null
                val clusterStateMetadata = indexMetadata.getCustomData(ManagedIndexMetaData.MANAGED_INDEX_METADATA)
                if (managedIndexMetadataMap != null) {
                    if (clusterStateMetadata != null) { // if has metadata saved, use that
                        managedIndexMetadataMap = clusterStateMetadata
                    }
                    if (managedIndexMetadataMap.isNotEmpty()) {
                        managedIndexMetadata = ManagedIndexMetaData.fromMap(managedIndexMetadataMap)
                    }
                }
                indexMetadatas.add(managedIndexMetadata)
            }

            managedIndicesMetaDataMap.clear()

            if (explainAll) {
                actionListener.onResponse(ExplainAllResponse(indexNames, indexPolicyIDs, indexMetadatas, totalManagedIndices, enabledState))
                return
            }
            actionListener.onResponse(ExplainResponse(indexNames, indexPolicyIDs, indexMetadatas))
        }

        fun emptyResponse(size: Int = 0) {
            if (explainAll) {
                actionListener.onResponse(ExplainAllResponse(emptyList(), emptyList(), emptyList(), size, emptyMap()))
                return
            }
            actionListener.onResponse(ExplainResponse(emptyList(), emptyList(), emptyList()))
        }
    }
}
