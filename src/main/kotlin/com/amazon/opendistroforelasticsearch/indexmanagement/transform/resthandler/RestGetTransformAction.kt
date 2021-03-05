package com.amazon.opendistroforelasticsearch.indexmanagement.transform.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get.GetTransformAction
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get.GetTransformRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get.GetTransformsAction
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get.GetTransformsRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.GET
import org.elasticsearch.rest.RestRequest.Method.HEAD
import org.elasticsearch.rest.action.RestToXContentListener
import org.elasticsearch.search.fetch.subphase.FetchSourceContext

class RestGetTransformAction  : BaseRestHandler() {

    override fun routes(): List<Route> {
        return listOf(
            Route(GET, TRANSFORM_BASE_URI),
            Route(GET, "$TRANSFORM_BASE_URI/{transformID}"),
            Route(HEAD, "$TRANSFORM_BASE_URI/{transformID}")
        )
    }

    override fun getName(): String {
        return "opendistro_get_transform_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val transformID = request.param("transformID")
        val searchString = request.param("search", )
        val from = request.paramAsInt("from", )
        val size = request.paramAsInt("size", )
        val sortField = request.param("sortField", )
        val sortDirection = request.param("sortDirection", )
        return RestChannelConsumer { channel ->
            if (transformID == null || transformID.isEmpty()) {
                val req = GetTransformsRequest(
                    searchString,
                    from,
                    size,
                    sortField,
                    sortDirection
                )
                client.execute(GetTransformsAction.INSTANCE, req, RestToXContentListener(channel))
            } else {
                val req = GetTransformRequest(transformID, if (request.method() == HEAD) FetchSourceContext.DO_NOT_FETCH_SOURCE else null)
                client.execute(GetTransformAction.INSTANCE, req, RestToXContentListener(channel))
            }
        }
    }

}
