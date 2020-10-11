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
import org.elasticsearch.index.Index
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
        private val indices = request.indices // query indices
        private var wildcard = false // true if query like "index*"

        // map of index to indexMetadataMap
        private val managedIndicesMetaDataMap = mutableMapOf<String, Map<String, String?>>()
        private val managedIndices = mutableListOf<String>()
        private var totalManagedIndices = 0

        private val indexNames = mutableListOf<String>()

        @Suppress("SpreadOperator")
        fun start() {
            log.info("indices in the request $indices")

            val params = request.params

            val sortBuilder = SortBuilders
                .fieldSort(params.sortField)
                .order(SortOrder.fromString(params.sortOrder))

            val queryStringQuery = QueryBuilders.queryStringQuery(params.queryString).defaultField("managed_index.name").defaultOperator(Operator.AND)
            val queryBuilder = QueryBuilders.boolQuery()
                .must(queryStringQuery)

            if (indices.size == 1 && indices[0].contains("*")) wildcard = true
            if (indices.isNotEmpty()) {
                if (wildcard) { // explain/index*
                    queryBuilder.filter(QueryBuilders.wildcardQuery("managed_index.name", indices[0]))
                } else { // explain/{index}
                    queryBuilder.filter(QueryBuilders.termsQuery("managed_index.name", indices))
                }
            } else { // explain
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
                    log.info("check response hits number: ${response.hits.hits.size}")
                    val totalHits = response.hits.totalHits
                    if (totalHits != null) {
                        totalManagedIndices = totalHits.value.toInt()
                    }
                    response.hits.hits.map {
                        val hitMap = it.sourceAsMap["managed_index"] as Map<String, Any>
                        val managedIndex = hitMap["index"] as String
                        managedIndices.add(managedIndex)

                        val managedIndexMetaDataMap = mutableMapOf<String, String?>()
                        managedIndexMetaDataMap["index"] = hitMap["index"] as String?
                        managedIndexMetaDataMap["index_uuid"] = hitMap["index_uuid"] as String?
                        managedIndexMetaDataMap["policy_id"] = hitMap["policy_id"] as String?
                        managedIndexMetaDataMap["enabled"] = hitMap["enabled"]?.toString()
                        managedIndicesMetaDataMap[managedIndex] = mapOf(
                            "index" to hitMap["index"] as String?,
                            "index_uuid" to hitMap["index_uuid"] as String?,
                            "policy_id" to hitMap["policy_id"] as String?,
                            "enabled" to hitMap["enabled"]?.toString()
                        )
                    }
                    log.info("managed indices: $managedIndices")
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
                    // getAll, but after filtered, no managed indices match; happened when using searchBox on frontend
                    actionListener.onResponse(ExplainResponse(emptyList(), emptyList(), emptyList(), 0))
                }

                override fun onFailure(t: Exception) {
                    if (t is IndexNotFoundException) { // config index hasn't been initialized
                        actionListener.onResponse(ExplainResponse(emptyList(), emptyList(), emptyList(), 0))
                        return
                    }
                    actionListener.onFailure(t)
                }
            })
        }

        fun getMetadata(indices: List<String>) {
            val clusterStateRequest = ClusterStateRequest()
            val strictExpandIndicesOptions = IndicesOptions.strictExpand()

            log.info("get metadata indices list: $indices")
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

            // cluster state response don't resist sort order
            for (indexName in indexNames) {
                log.info("retrieve metadata for $indexName")
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
