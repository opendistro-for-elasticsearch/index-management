package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
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
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.ExistsQueryBuilder
import org.elasticsearch.index.query.WildcardQueryBuilder
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.SearchModule
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

class TransportGetTransformsAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetTransformsRequest, GetTransformsResponse> (
    GetTransformsAction.NAME, transportService, actionFilters, ::GetTransformsRequest
) {

    override fun doExecute(task: Task, request: GetTransformsRequest, listener: ActionListener<GetTransformsResponse>) {
        val searchString = request.searchString.trim()
        val from = request.from
        val size = request.size
        val sortField = request.sortField
        val sortDirection = request.sortDirection

        val boolQueryBuilder = BoolQueryBuilder().filter(ExistsQueryBuilder(Transform.TRANSFORM_TYPE))
        if (searchString.isNotEmpty()) {
            boolQueryBuilder.filter(WildcardQueryBuilder("${Transform.TRANSFORM_TYPE}.${Transform.TRANSFORM_ID_FIELD}.keyword", "*$searchString*"))
        }
        val searchSourceBuilder = SearchSourceBuilder().query(boolQueryBuilder).from(from).size(size).seqNoAndPrimaryTerm(true)
            .sort(sortField, SortOrder.fromString(sortDirection))
        val searchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX).source(searchSourceBuilder)
        client.search(searchRequest, object : ActionListener<SearchResponse> {
            override fun onResponse(response: SearchResponse) {
                val totalTransforms = response.hits.totalHits?.value ?: 0

                if (response.shardFailures.isNotEmpty()) {
                    val failure = response.shardFailures.reduce { s1, s2 ->
                        if (s1.status().status > s2.status().status) s1 else s2
                    }
                    listener.onFailure(ElasticsearchStatusException("Get transforms failed on some shards", failure.status(), failure.cause))
                } else {
                    try {
                        val transforms = response.hits.hits.map {
                            contentParser(it.sourceRef).parseWithType(it.id, it.seqNo, it.primaryTerm, Transform.Companion::parse)
                        }
                        listener.onResponse(GetTransformsResponse(transforms, totalTransforms.toInt(), RestStatus.OK))
                    } catch (e: Exception) {
                        listener.onFailure(ElasticsearchStatusException("Failed to parse transforms",
                            RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.unwrapCause(e)))
                    }
                }
            }

            override fun onFailure(e: Exception) = listener.onFailure(e)
        })
    }

    private fun contentParser(bytesReference: BytesReference): XContentParser {
        return XContentHelper.createParser(NamedXContentRegistry(SearchModule(Settings.EMPTY, false, emptyList()).namedXContents),
            LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON)
    }
}
