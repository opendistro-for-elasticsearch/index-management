package com.amazon.opendistroforelasticsearch.indexmanagement.util

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
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
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.builder.SearchSourceBuilder

fun getJobs(
    client: Client,
    searchSourceBuilder: SearchSourceBuilder,
    listener: ActionListener<ActionResponse>,
    scheduledJobType: String,
    contentParser: (b: BytesReference) -> XContentParser = ::contentParser
) {
    val searchRequest = SearchRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX).source(searchSourceBuilder)
    client.search(searchRequest, object : ActionListener<SearchResponse> {
        override fun onResponse(response: SearchResponse) {
            val totalJobs = response.hits.totalHits?.value ?: 0

            if (response.shardFailures.isNotEmpty()) {
                val failure = response.shardFailures.reduce { s1, s2 -> if (s1.status().status > s2.status().status) s1 else s2 }
                listener.onFailure(ElasticsearchStatusException("Get $scheduledJobType failed on some shards", failure.status(), failure.cause))
            } else {
                try {
                    val jobs = response.hits.hits.map {
                        contentParser(it.sourceRef).parseWithType(it.id, it.seqNo, it.primaryTerm, getParser(scheduledJobType))
                    }
                    listener.onResponse(populateResponse(scheduledJobType, jobs, RestStatus.OK, totalJobs.toInt()))
                } catch (e: Exception) {
                    listener.onFailure(
                        ElasticsearchStatusException("Failed to parse $scheduledJobType", RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.unwrapCause(e))
                    )
                }
            }
        }

        override fun onFailure(e: Exception) = listener.onFailure(e)
    })
}

private fun populateResponse(
    jobType: String,
    jobs: List<Any>,
    status: RestStatus,
    totalJobs: Int
): ActionResponse {
    return when (jobType) {
        Rollup.ROLLUP_TYPE -> GetRollupsResponse(jobs as List<Rollup>, totalJobs, status)
        Transform.TRANSFORM_TYPE -> GetTransformsResponse(jobs as List<Transform>, totalJobs, status)
        else -> {
            throw ElasticsearchStatusException("Unknown scheduled job type", RestStatus.INTERNAL_SERVER_ERROR)
        }
    }
}

private fun getParser(jobType: String): (XContentParser, String, Long, Long) -> Any {
    return when (jobType) {
        Transform.TRANSFORM_TYPE -> Transform.Companion::parse
        Rollup.ROLLUP_TYPE -> Rollup.Companion::parse
        else -> {
            throw ElasticsearchStatusException("Unknown scheduled job type", RestStatus.INTERNAL_SERVER_ERROR)
        }
    }
}

private fun contentParser(bytesReference: BytesReference): XContentParser {
    return XContentHelper.createParser(NamedXContentRegistry.EMPTY,
        LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON)
}
