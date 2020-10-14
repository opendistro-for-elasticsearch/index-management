package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicies

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

class TransportGetPoliciesAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetPoliciesRequest, GetPoliciesResponse>(
        GetPoliciesAction.NAME, transportService, actionFilters, ::GetPoliciesRequest
) {

    private val log = LogManager.getLogger(TransportGetPoliciesAction::class.java)

    override fun doExecute(
        task: Task,
        getPoliciesRequest: GetPoliciesRequest,
        actionListener: ActionListener<GetPoliciesResponse>
    ) {
        client.threadPool().threadContext.stashContext().use {
            val tableProp = getPoliciesRequest.table

            val sortBuilder = SortBuilders
                    .fieldSort(tableProp.sortString)
                    .order(SortOrder.fromString(tableProp.sortOrder))

            val queryBuilder = QueryBuilders.boolQuery()
                    .must(QueryBuilders.existsQuery("policy"))

            if (!tableProp.searchString.isNullOrBlank()) {
                queryBuilder
                        .must(QueryBuilders
                                .queryStringQuery(tableProp.searchString)
                                .defaultOperator(Operator.AND)
                                .field("policy.policy_id"))
            }

            val searchSourceBuilder = SearchSourceBuilder()
                    .query(queryBuilder)
                    .sort(sortBuilder)
                    .from(tableProp.startIndex)
                    .size(tableProp.size)
                    .seqNoAndPrimaryTerm(true)

            val searchRequest = SearchRequest()
                    .source(searchSourceBuilder)
                    .indices(getPoliciesRequest.index)

            client.search(searchRequest, object : ActionListener<SearchResponse> {
                override fun onResponse(response: SearchResponse) {
                    val totalPolicies = response.hits.totalHits?.value?.toInt()
                    val policies = response.hits.hits.map {
                        val seqNo = it.seqNo
                        val primaryTerm = it.primaryTerm
                        val id = it.id
                        val xcp = XContentFactory.xContent(XContentType.JSON)
                                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, it.sourceAsString)
                        Policy.parseWithType(xcp, id, seqNo, primaryTerm)
                    }

                    actionListener.onResponse(GetPoliciesResponse(policies, totalPolicies))
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(t)
                }
            })
        }
    }
}
