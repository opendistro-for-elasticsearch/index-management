package com.amazon.opendistroforelasticsearch.indexmanagement.transform.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.start.StartTransformAction
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.start.StartTransformRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class RestStartTransformAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return listOf(
            Route(POST, "$TRANSFORM_BASE_URI/{transformID}/_start")
        )
    }

    override fun getName(): String {
        return "opendistro_start_transform_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("transformID", Transform.NO_ID)
        val startRequest = StartTransformRequest(id)
        return RestChannelConsumer { channel ->
            client.execute(StartTransformAction.INSTANCE, startRequest, RestToXContentListener(channel))
        }
    }
}
