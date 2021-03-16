package com.amazon.opendistroforelasticsearch.indexmanagement.transform.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.delete.DeleteTransformAction
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.delete.DeleteTransformRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.delete.DeleteTransformsAction
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.delete.DeleteTransformsRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.util.REFRESH
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.DELETE
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class RestDeleteTransformAction : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            Route(DELETE, "$TRANSFORM_BASE_URI/{transformID}")
        )
    }

    override fun getName(): String = "opendistro_delete_transform_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val transformID = request.param("transformID")
        val refreshPolicy = RefreshPolicy.parse(request.param(REFRESH, RefreshPolicy.IMMEDIATE.value))
        if (transformID.contains(",")) {
            // multiple IDs
            return RestChannelConsumer { channel ->
                channel.newBuilder()
                val deleteTransformsRequest = DeleteTransformsRequest(transformID.split(","))
                client.execute(DeleteTransformsAction.INSTANCE, deleteTransformsRequest, RestToXContentListener(channel))
            }
        }

        return RestChannelConsumer { channel ->
            channel.newBuilder()
            val deleteTransformRequest = DeleteTransformRequest(transformID)
                .setRefreshPolicy(refreshPolicy)
            client.execute(DeleteTransformAction.INSTANCE, deleteTransformRequest, RestToXContentListener(channel))
        }
    }
}
