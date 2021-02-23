package com.amazon.opendistroforelasticsearch.indexmanagement.transform.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.index.IndexTransformAction
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.index.IndexTransformRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.index.IndexTransformResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IF_PRIMARY_TERM
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IF_SEQ_NO
import com.amazon.opendistroforelasticsearch.indexmanagement.util.REFRESH
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.index.seqno.SequenceNumbers
import org.elasticsearch.rest.*
import org.elasticsearch.rest.RestRequest.Method.PUT
import org.elasticsearch.rest.action.RestResponseListener
import java.io.IOException
import java.lang.IllegalArgumentException
import java.time.Instant

class RestIndexTransformAction : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(
                RestHandler.Route(PUT, TRANSFORM_BASE_URI),
                RestHandler.Route(PUT, "$TRANSFORM_BASE_URI/{transformID}")
        )
    }

    override fun getName(): String {
        return "opendistro_index_transform_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("transformID", Transform.NO_ID)
        if (Transform.NO_ID == id) {
            throw IllegalArgumentException("Missing transform ID")
        }

        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val xcp = request.contentParser()
        val transform = xcp.parseWithType(id = id, seqNo = seqNo, primaryTerm = primaryTerm, parse = Transform.Companion::parse)
                .copy(updatedAt = Instant.now())
        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }
        val indexTransformRequest = IndexTransformRequest(transform, refreshPolicy)
        return RestChannelConsumer { channel ->
            client.execute(IndexTransformAction.INSTANCE, indexTransformRequest, indexTransformResponse(channel))
        }
    }

    private fun indexTransformResponse(channel: RestChannel):
        RestResponseListener<IndexTransformResponse> {
        return object : RestResponseListener<IndexTransformResponse>(channel) {
            @Throws(Exception::class)
            override fun buildResponse(response: IndexTransformResponse): RestResponse {
                val restResponse =
                        BytesRestResponse(response.status, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                if (response.status == RestStatus.CREATED) {
                    val location = "$TRANSFORM_BASE_URI/${response.id}"
                    restResponse.addHeader("Location", location)
                }
                return restResponse
            }
        }
    }
}
