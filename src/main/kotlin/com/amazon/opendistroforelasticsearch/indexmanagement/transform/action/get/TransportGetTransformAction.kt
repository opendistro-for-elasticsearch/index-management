package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.Client
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

class TransportGetTransformAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetTransformRequest, GetTransformResponse> (
    GetTransformAction.NAME, transportService, actionFilters, ::GetTransformRequest
) {

    override fun doExecute(task: Task, request: GetTransformRequest, listener: ActionListener<GetTransformResponse>) {
        val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.id)
            .fetchSourceContext(request.srcContext).preference(request.preference)
        client.get(getRequest, object : ActionListener<GetResponse> {
            override fun onResponse(response: GetResponse) {
                if (!response.isExists) {
                    return listener.onFailure(ElasticsearchStatusException("Transform not found", RestStatus.NOT_FOUND))
                }

                var transform: Transform? = null
                if (!response.isSourceEmpty) {
                    XContentHelper.createParser(xContentRegistry,
                        LoggingDeprecationHandler.INSTANCE,
                        response.sourceAsBytesRef, XContentType.JSON).use { xcp ->
                            transform = xcp.parseWithType(
                                response.id,
                                response.seqNo,
                                response.primaryTerm,
                                Transform.Companion::parse)
                        }
                    }

                listener.onResponse(GetTransformResponse(
                    response.id,
                    response.version,
                    response.seqNo,
                    response.primaryTerm,
                    RestStatus.OK,
                    transform))
            }

            override fun onFailure(e: Exception) {
                    listener.onFailure(e)
            }
        })
    }
}
