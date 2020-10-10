package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Table
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicies.GetPoliciesAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicies.GetPoliciesRequest
import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class RestGetPoliciesAction : BaseRestHandler() {

    private val log = LogManager.getLogger(RestGetPoliciesAction::class.java)

    companion object {
        private const val DEFAULT_SORT_STRING = "policy.policy_id.keyword"
        private const val DEFAULT_SORT_ORDER = "desc"
        private const val DEFAULT_SIZE = 20
        private const val DEFAULT_START_INDEX = 0
        private const val DEFAULT_SEARCH_STRING = ""
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(
                RestHandler.Route(RestRequest.Method.GET, IndexManagementPlugin.POLICY_BASE_URI)
        )
    }

    override fun getName(): String = "get_policies_action"

    @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {

        val sortString = request.param("sortString", DEFAULT_SORT_STRING)
        val sortOrder = request.param("sortOrder", DEFAULT_SORT_ORDER)
        val size = request.paramAsInt("size", DEFAULT_SIZE)
        val startIndex = request.paramAsInt("startIndex", DEFAULT_START_INDEX)
        val searchString = request.param("searchString", DEFAULT_SEARCH_STRING)
        val table = Table(
                sortOrder,
                sortString,
                size,
                startIndex,
                searchString
        )

        val getPoliciesRequest = GetPoliciesRequest(table)

        return RestChannelConsumer { channel ->
            client.execute(GetPoliciesAction.INSTANCE, getPoliciesRequest, RestToXContentListener(channel))
        }
    }
}
