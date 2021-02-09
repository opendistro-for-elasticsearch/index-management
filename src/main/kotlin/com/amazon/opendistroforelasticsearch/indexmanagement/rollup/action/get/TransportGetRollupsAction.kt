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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.util.use
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.Client
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.ExistsQueryBuilder
import org.elasticsearch.index.query.WildcardQueryBuilder
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService
import kotlin.Exception

class TransportGetRollupsAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetRollupsRequest, GetRollupsResponse> (
    GetRollupsAction.NAME, transportService, actionFilters, ::GetRollupsRequest
) {

    override fun doExecute(task: Task, request: GetRollupsRequest, listener: ActionListener<GetRollupsResponse>) {
        val searchString = request.searchString.trim()
        val from = request.from
        val size = request.size
        val sortField = request.sortField
        val sortDirection = request.sortDirection
        // TODO: Allow filtering for [continuous, job state, metadata status, targetindex, sourceindex]
        val boolQueryBuilder = BoolQueryBuilder().filter(ExistsQueryBuilder(Rollup.ROLLUP_TYPE))
        if (searchString.isNotEmpty()) {
            boolQueryBuilder.filter(WildcardQueryBuilder("${Rollup.ROLLUP_TYPE}.${Rollup.ROLLUP_ID_FIELD}.keyword", "*$searchString*"))
        }
        val searchSourceBuilder = SearchSourceBuilder().query(boolQueryBuilder).from(from).size(size).seqNoAndPrimaryTerm(true)
            .sort(sortField, SortOrder.fromString(sortDirection))
        val searchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX).source(searchSourceBuilder)
        client.threadPool().threadContext.stashContext().use {
            client.search(searchRequest, object : ActionListener<SearchResponse> {
                override fun onResponse(response: SearchResponse) {
                    val totalRollups = response.hits.totalHits?.value ?: 0

                    if (response.shardFailures.isNotEmpty()) {
                        val failure =
                            response.shardFailures.reduce { s1, s2 -> if (s1.status().status > s2.status().status) s1 else s2 }
                        listener.onFailure(
                            ElasticsearchStatusException(
                                "Get rollups failed on some shards",
                                failure.status(),
                                failure.cause
                            )
                        )
                    } else {
                        try {
                            val rollups = response.hits.hits.map {
                                contentParser(it.sourceRef).parseWithType(
                                    it.id,
                                    it.seqNo,
                                    it.primaryTerm,
                                    Rollup.Companion::parse
                                )
                            }
                            listener.onResponse(GetRollupsResponse(rollups, totalRollups.toInt(), RestStatus.OK))
                        } catch (e: Exception) {
                            listener.onFailure(
                                ElasticsearchStatusException(
                                    "Failed to parse rollups",
                                    RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.unwrapCause(e)
                                )
                            )
                        }
                    }
                }

                override fun onFailure(e: Exception) = listener.onFailure(e)
            })
        }
    }

    private fun contentParser(bytesReference: BytesReference): XContentParser {
        return XContentHelper.createParser(NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON)
    }
}
