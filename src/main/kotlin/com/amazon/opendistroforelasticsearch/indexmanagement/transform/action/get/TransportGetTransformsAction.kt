package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get

import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import com.amazon.opendistroforelasticsearch.indexmanagement.util.getJobs
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.ActionResponse
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

        getJobs(
            client,
            searchSourceBuilder,
            listener as ActionListener<ActionResponse>,
            Transform.TRANSFORM_TYPE,
            ::contentParser
        )
    }

    private fun contentParser(bytesReference: BytesReference): XContentParser {
        return XContentHelper.createParser(xContentRegistry,
            LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON)
    }
}
