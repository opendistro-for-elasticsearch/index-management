package com.amazon.opendistroforelasticsearch.indexmanagement.util

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupsResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get.GetTransformsResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Client
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.ExistsQueryBuilder
import org.elasticsearch.index.query.WildcardQueryBuilder
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder

fun executeJobsSearch(
    listener: ActionListener<ActionResponse>,
    contentParser: (b: BytesReference) -> XContentParser,
    client: Client,
    jobType: String,
    jobIDField: String,
    searchString: String,
    from: Int,
    size: Int,
    sortField: String,
    sortDirection: String,
    jobClass: String,
    parse: (xcp: XContentParser, id: String, seqNo: Long, primaryTerm: Long) -> Any
) {
    val boolQueryBuilder = BoolQueryBuilder().filter(ExistsQueryBuilder(jobType))
    if (searchString.isNotEmpty()) {
        boolQueryBuilder.filter(WildcardQueryBuilder(
            "$jobType.$jobIDField.keyword",
            "*$searchString*"))
    }
    val searchSourceBuilder = SearchSourceBuilder().query(boolQueryBuilder)
        .from(from).size(size).seqNoAndPrimaryTerm(true)
        .sort(sortField, SortOrder.fromString(sortDirection))
    val searchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX).source(searchSourceBuilder)
    client.search(searchRequest, object : ActionListener<SearchResponse> {
        override fun onResponse(response: SearchResponse) {
            val totalHits = response.hits.totalHits?.value ?: 0

            if (response.shardFailures.isNotEmpty()) {
                val failure = response.shardFailures.reduce { s1, s2 ->
                    if (s1.status().status > s2.status().status) s1 else s2
                }
                listener.onFailure(ElasticsearchStatusException("Get job failed on some shards",
                    failure.status(), failure.cause))
            } else {
                try {
                    val jobs = response.hits.hits.map {
                        contentParser(it.sourceRef).parseWithType(it.id,
                            it.seqNo, it.primaryTerm, parse)
                    }
                    listener.onResponse(getResponse(jobs, totalHits.toInt(), RestStatus.OK, jobClass))
                } catch (e: Exception) {
                    listener.onFailure(ElasticsearchStatusException("Failed to parse jobs",
                        RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.unwrapCause(e)))
                }
            }
        }

        override fun onFailure(e: Exception) = listener.onFailure(e)
    })
}

private fun getResponse(
    jobs: List<Any>,
    hits: Int,
    status: RestStatus,
    jobClass: String
): ActionResponse {
    return when (jobClass) {
        "rollup" -> {
            GetRollupsResponse(jobs as List<Rollup>, hits, status)
        }
        "transform" -> {
            GetTransformsResponse(jobs as List<Transform>, hits, status)
        }
        else -> {
            throw ElasticsearchStatusException("Failed to parse jobs",
                RestStatus.INTERNAL_SERVER_ERROR)
        }
    }
}
