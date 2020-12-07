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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.Client
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.Operator
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortBuilders
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

private val log = LogManager.getLogger(TransportGetPoliciesAction::class.java)

class TransportGetPoliciesAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetPoliciesRequest, GetPoliciesResponse>(
        GetPoliciesAction.NAME, transportService, actionFilters, ::GetPoliciesRequest
) {

    override fun doExecute(
        task: Task,
        getPoliciesRequest: GetPoliciesRequest,
        actionListener: ActionListener<GetPoliciesResponse>
    ) {
        val params = getPoliciesRequest.searchParams

        val sortBuilder = SortBuilders
            .fieldSort(params.sortField)
            .order(SortOrder.fromString(params.sortOrder))

        val queryBuilder = QueryBuilders.boolQuery()
            .must(QueryBuilders.existsQuery("policy"))

        queryBuilder.must(QueryBuilders
            .queryStringQuery(params.queryString)
            .defaultOperator(Operator.AND)
            .field("policy.policy_id"))

        val searchSourceBuilder = SearchSourceBuilder()
            .query(queryBuilder)
            .sort(sortBuilder)
            .from(params.from)
            .size(params.size)
            .seqNoAndPrimaryTerm(true)

        val searchRequest = SearchRequest()
            .source(searchSourceBuilder)
            .indices(getPoliciesRequest.index)

        client.search(searchRequest, object : ActionListener<SearchResponse> {
            override fun onResponse(response: SearchResponse) {
                val totalPolicies = response.hits.totalHits?.value ?: 0
                log.info("search response hits $totalPolicies")
                val policies = response.hits.hits.map {
                    val id = it.id
                    val seqNo = it.seqNo
                    val primaryTerm = it.primaryTerm
                    val xcp = XContentFactory.xContent(XContentType.JSON)
                            .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, it.sourceAsString)
                    xcp.parseWithType(id, seqNo, primaryTerm, Policy.Companion::parse)
                            .copy(id = id, seqNo = seqNo, primaryTerm = primaryTerm)
                }

                actionListener.onResponse(GetPoliciesResponse(policies, totalPolicies.toInt()))
            }

            override fun onFailure(t: Exception) {
                actionListener.onFailure(t)
            }
        })
    }
}